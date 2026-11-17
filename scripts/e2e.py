"""End-to-end demo: Temporal activities running on workers inside Archil
sandboxes, with real Claude agents.

    python scripts/e2e.py [URL ...]

1. Start a Temporal server in a sandbox of its own (a stand-in for Temporal
   Cloud), and control worker A.
2. Start an IngestWorkflow. Each agent task gets a sandbox running a Temporal
   worker, and the agent runs inside that worker.
3. While the agents run:
   - replace control worker A with worker B, as a deploy would. The agents
     shouldn't notice.
   - restart the Temporal worker inside the second task's sandbox. Its agent is
     retried in the same sandbox and resumes its Claude session from the disk.
4. Check the result, the merged file, and that no sandbox is left behind.
5. Delete the test's sandboxes and disks, pass or fail (--keep keeps the disks).

Needs ARCHIL_API_KEY, ARCHIL_REGION, ANTHROPIC_API_KEY and ARCHIL_IMAGE_ID.
Writes control worker logs, merged.parquet and summary.md to ./e2e-out/.
"""

from __future__ import annotations

import asyncio
import os
import signal
import subprocess
import sys
import threading
import time
import urllib.request
from pathlib import Path

import pyarrow.parquet as pq
from temporalio.api.enums.v1 import PendingActivityState

from archil_temporal import config
from archil_temporal.cleanup import clean_up_workflow
from archil_temporal.sandboxes import WORKER_SERVICE, client, find_disk, find_sandboxes, spin_down
from archil_temporal.shared import CONTROL_QUEUE, IngestRequest, all_names, plan_tasks, task_name
from archil_temporal.workflows import IngestWorkflow

DEFAULT_URLS = [
    "https://raw.githubusercontent.com/datasets/gdp/main/data/gdp.csv",
    "https://raw.githubusercontent.com/datasets/co2-ppm/main/data/co2-mm-mlo.csv",
]
OUT = Path("e2e-out")
WORK_BEFORE_DISRUPTING_S = 3

INSTALL_TEMPORAL = """python3 -c "
import io, tarfile, urllib.request
url = 'https://temporal.download/cli/archive/latest?platform=linux&arch=amd64'
data = urllib.request.urlopen(urllib.request.Request(url, headers={'User-Agent': 'curl/8'})).read()
tarfile.open(fileobj=io.BytesIO(data)).extract('temporal', '/usr/local/bin')
\""""

# Never let a credential reach a log line or the uploaded artifact.
# (Actions masks its own logs, not artifacts.)
_SECRETS = [v for k in ("ANTHROPIC_API_KEY", "ARCHIL_API_KEY") if len(v := os.environ.get(k, "")) >= 8]


def redact(text: str) -> str:
    for secret in _SECRETS:
        text = text.replace(secret, "***")
    return text


def log(msg: str) -> None:
    print(f"\n=== {time.strftime('%H:%M:%S')} {msg}", flush=True)


class ControlWorker:
    """A control worker in its own process, logging (redacted) to e2e-out/worker-<name>.log."""

    def __init__(self, name: str) -> None:
        self.name = name
        self.log_path = OUT / f"worker-{name}.log"
        self.proc = subprocess.Popen(
            [sys.executable, "-m", "archil_temporal.worker"],
            env={**os.environ, "PYTHONUNBUFFERED": "1"},
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        self._tee = threading.Thread(target=self._copy_output, daemon=True)
        self._tee.start()

    def _copy_output(self) -> None:
        with self.log_path.open("w") as f:
            for line in self.proc.stdout:
                line = redact(line)
                f.write(line)
                f.flush()
                if "HTTP Request" not in line:
                    print(f"[control {self.name}] {line}", end="", flush=True)

    def stop(self) -> float:
        started = time.time()
        if self.proc.poll() is None:
            self.proc.send_signal(signal.SIGTERM)
            self.proc.wait(timeout=120)
        self._tee.join(timeout=10)
        return time.time() - started


async def start_temporal_server():
    """A Temporal dev server in a sandbox, reachable through its exposed port."""
    sandbox = await client().sandboxes.create.aio(
        name=f"e2e-temporal-{int(time.time())}", base_image="python:3.12-slim", vcpu_count=2,
        mem_size_mib=2048, idle_ttl_seconds=0, max_ttl_seconds=3 * 3600,
    )
    try:
        install = await sandbox.run.aio(INSTALL_TEMPORAL)
        if (await install.wait.aio()).exit_code != 0:
            raise RuntimeError("could not install the Temporal CLI")
        await sandbox.exec.aio(
            "archil services create temporal -- temporal server start-dev --ip 0.0.0.0 --headless --log-level warn"
        )
        for _ in range(60):
            health = await sandbox.exec.aio("temporal operator cluster health --address 127.0.0.1:7233 2>&1 || true")
            if "SERVING" in health.stdout:
                break
            await asyncio.sleep(2)
        else:
            raise RuntimeError("Temporal server did not start")
        return sandbox, await sandbox.expose_port.aio(7233)
    except BaseException:
        await spin_down(sandbox)
        raise


async def wait_until_agents_run(handle, n: int) -> None:
    while True:
        description = await handle.describe()
        if description.status.name != "RUNNING":
            raise RuntimeError(f"workflow finished ({description.status.name}) before every agent was seen running")
        running = [
            p for p in description.raw_description.pending_activities
            if p.activity_type.name == "run_agent_task" and p.state == PendingActivityState.PENDING_ACTIVITY_STATE_STARTED
        ]
        if len(running) == n:
            return
        await asyncio.sleep(1)


async def live_sandboxes(names: list[str]) -> int:
    count = 0
    for name in names:
        if (disk := await find_disk(name)) is not None:
            count += len(await find_sandboxes(disk.id, name))
    return count


async def main() -> int:
    missing = [k for k in ("ARCHIL_API_KEY", "ARCHIL_REGION", "ANTHROPIC_API_KEY", "ARCHIL_IMAGE_ID") if not os.environ.get(k)]
    if missing:
        print(f"missing environment: {', '.join(missing)}", file=sys.stderr)
        return 2
    keep = "--keep" in sys.argv
    urls = [a for a in sys.argv[1:] if not a.startswith("--")] or DEFAULT_URLS
    if len(urls) < 2:
        print("needs at least two URLs: one agent is left alone, one has its worker restarted", file=sys.stderr)
        return 2
    OUT.mkdir(exist_ok=True)
    workflow_id = f"e2e-{int(time.time())}"

    log("starting a Temporal server in a sandbox")
    server, host = await start_temporal_server()
    os.environ["TEMPORAL_TUNNEL_HOST"] = host  # for the control workers and the sandboxes' workers
    try:
        return await run(workflow_id, urls)
    finally:
        await clean_up_workflow(workflow_id, urls, delete_disks=not keep)
        await spin_down(server)
        log("deleted the test's sandboxes" + ("" if keep else " and disks"))


async def run(workflow_id: str, urls: list[str]) -> int:
    t0 = time.time()
    tasks = plan_tasks(workflow_id, urls)
    temporal = await config.connect(config.load())
    worker_a = ControlWorker("A")
    worker_b = None
    try:
        handle = await temporal.start_workflow(
            IngestWorkflow.run, IngestRequest(urls=urls), id=workflow_id, task_queue=CONTROL_QUEUE
        )
        log(f"started workflow {workflow_id} with {len(urls)} agent(s)")
        await asyncio.wait_for(wait_until_agents_run(handle, len(urls)), timeout=900)
        sandbox_ids = {}
        for t in tasks:
            disk = await find_disk(task_name(t))
            [sandbox] = await find_sandboxes(disk.id, task_name(t))
            sandbox_ids[t.key] = sandbox.id
        log(f"every agent is running on the worker in its own sandbox: {sandbox_ids}")
        await asyncio.sleep(WORK_BEFORE_DISRUPTING_S)

        log("replacing control worker A with B, as a deploy would")
        log(f"control worker A exited in {worker_a.stop():.1f}s")
        worker_b = ControlWorker("B")

        restarted = tasks[1]
        log(f"restarting the Temporal worker inside {restarted.key}'s sandbox")
        [sandbox] = await find_sandboxes((await find_disk(task_name(restarted))).id, task_name(restarted))
        await sandbox.exec.aio(f"archil services restart {WORKER_SERVICE}")

        result = await asyncio.wait_for(handle.result(), timeout=3600)
    finally:
        for w in (worker_a, worker_b):
            if w is not None:
                w.stop()

    log("checking the result")
    by_key = {r.key: r for r in result.succeeded}
    undisturbed, restarted = by_key.get(tasks[0].key), by_key.get(tasks[1].key)
    checks: list[tuple[str, bool]] = [
        (f"all {len(urls)} tasks succeeded (failed: {result.failed or 'none'})", len(result.succeeded) == len(urls)),
        (
            "each agent ran on the Temporal worker inside its own sandbox",
            all(by_key.get(k) and by_key[k].sandbox_id == sid for k, sid in sandbox_ids.items())
            and len(set(sandbox_ids.values())) == len(urls),
        ),
        (
            "replacing the control worker mid-run didn't interrupt any agent "
            f"({tasks[0].key} finished on attempt {undisturbed.attempt if undisturbed else '?'})",
            bool(undisturbed and undisturbed.attempt == 1 and not undisturbed.resumed),
        ),
        (
            f"after its sandbox's worker restarted, {tasks[1].key} was retried in the same sandbox and "
            f"resumed its Claude session (attempt {restarted.attempt if restarted else '?'})",
            bool(restarted and restarted.attempt >= 2 and restarted.resumed and restarted.sandbox_id == sandbox_ids[tasks[1].key]),
        ),
    ]

    rows = 0
    merged = OUT / "merged.parquet"
    if result.merged:
        urllib.request.urlretrieve(result.merged.download_url, merged)
        table = pq.read_table(merged)
        rows = table.num_rows
        checks.append((f"merged.parquet downloads from its signed URL with {rows} rows", rows == result.merged.rows > 0))
    else:
        checks.append(("merged.parquet was produced", False))

    live = await live_sandboxes(all_names(workflow_id, urls))
    checks.append((f"every sandbox was deleted when its work was done ({live} left)", live == 0))

    lines = [
        f"## Temporal on Archil sandboxes, end to end: `{workflow_id}`",
        "",
        f"{len(urls)} agents, each on a Temporal worker in its own sandbox; {time.time() - t0:.0f}s total.",
        "",
        *[f"- {'✅' if ok else '❌'} {text}" for text, ok in checks],
        "",
        "| task | url | attempts | cost | agent summary |",
        "|---|---|---|---|---|",
        *[
            f"| {r.key} | {next(t.url for t in tasks if t.key == r.key)} | {r.attempt} | "
            f"${r.cost_usd or 0:.2f} | {' '.join(r.summary.split())[:300]} |"
            for r in result.succeeded
        ],
    ]
    if rows:
        sample = table.slice(0, 8).to_pylist()
        lines += ["", "First rows of `merged.parquet`:", "", "| " + " | ".join(table.column_names) + " |",
                  "|" + "---|" * len(table.column_names),
                  *["| " + " | ".join(str(v) for v in row.values()) + " |" for row in sample]]
    summary = redact("\n".join(lines))
    (OUT / "summary.md").write_text(summary)
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as f:
            f.write(summary + "\n")
    print("\n" + summary)
    return 0 if all(ok for _, ok in checks) else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
