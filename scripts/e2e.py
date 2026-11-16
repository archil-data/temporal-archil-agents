"""End-to-end demo: real Claude agents in real Archil sandboxes, with the
Temporal worker replaced while they run.

    python scripts/e2e.py [URL ...]

1. Start a local Temporal dev server and worker A, then start an IngestWorkflow.
2. Once every agent is running in its sandbox, stop worker A (SIGTERM, as a
   deploy or scale-in would) and start worker B.
3. Wait for the result and check: every task succeeded, worker B reattached to
   the agents worker A left running, the merged file downloads and has rows,
   and no sandbox is left running.
4. Delete the test's sandboxes and disks, pass or fail (pass --keep to keep
   the disks for inspection).

Needs ARCHIL_API_KEY, ARCHIL_REGION, ANTHROPIC_API_KEY and ARCHIL_IMAGE_ID.
Writes worker logs, merged.parquet and summary.md to ./e2e-out/.
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
from temporalio.testing import WorkflowEnvironment

from archil_temporal.activities import clean_up_workflow
from archil_temporal.sandboxes import find_disk, find_sandboxes
from archil_temporal.shared import TASK_QUEUE, AgentTask, IngestRequest, merged_disk_name, plan_tasks, task_disk_name
from archil_temporal.workflows import IngestWorkflow

DEFAULT_URLS = [
    "https://raw.githubusercontent.com/datasets/gdp/main/data/gdp.csv",
    "https://raw.githubusercontent.com/datasets/co2-ppm/main/data/co2-mm-mlo.csv",
]
OUT = Path("e2e-out")
# How long to let the agents work before replacing the worker. Short, so the
# swap lands while every agent is still running.
WORK_BEFORE_SWAP_S = 3


# Never let a credential reach a log line or the uploaded artifact, even if an
# agent prints its environment. (Actions masks its own logs, not artifacts.)
_SECRETS = [v for k in ("ANTHROPIC_API_KEY", "ARCHIL_API_KEY") if len(v := os.environ.get(k, "")) >= 8]


def redact(text: str) -> str:
    for secret in _SECRETS:
        text = text.replace(secret, "***")
    return text


def log(msg: str) -> None:
    print(f"\n=== {time.strftime('%H:%M:%S')} {msg}", flush=True)


class WorkerProcess:
    """A worker in its own process, logging (redacted) to e2e-out/worker-<name>.log."""

    def __init__(self, name: str, address: str) -> None:
        self.name = name
        self.log_path = OUT / f"worker-{name}.log"
        self.proc = subprocess.Popen(
            [sys.executable, "-m", "archil_temporal.worker"],
            env={**os.environ, "TEMPORAL_ADDRESS": address, "PYTHONUNBUFFERED": "1"},
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
                    print(f"[worker {self.name}] {line}", end="", flush=True)

    def stop(self) -> float:
        """SIGTERM, as a deploy would; returns how long the worker took to exit."""
        started = time.time()
        if self.proc.poll() is None:
            self.proc.send_signal(signal.SIGTERM)
            self.proc.wait(timeout=120)
        self._tee.join(timeout=10)
        return time.time() - started

    def count(self, text: str) -> int:
        return self.log_path.read_text().count(text)


async def wait_until_all_agents_run(handle, n: int) -> None:
    while True:
        pending = (await handle.describe()).raw_description.pending_activities
        running = [p for p in pending if p.activity_type.name == "run_agent_task" and p.HasField("heartbeat_details")]
        if len(running) == n and all("process_id" in p.heartbeat_details.payloads[0].data.decode() for p in running):
            return
        await asyncio.sleep(2)


async def live_sandboxes(tasks: list[AgentTask], workflow_id: str) -> int:
    count = 0
    for name in [task_disk_name(t) for t in tasks] + [merged_disk_name(workflow_id)]:
        disk = await find_disk(name)
        if disk is not None:
            count += len(await find_sandboxes(disk.id, name))
    return count


async def main() -> int:
    missing = [k for k in ("ARCHIL_API_KEY", "ARCHIL_REGION", "ANTHROPIC_API_KEY", "ARCHIL_IMAGE_ID") if not os.environ.get(k)]
    if missing:
        # Without these every agent fails until it runs out of retries.
        print(f"missing environment: {', '.join(missing)}", file=sys.stderr)
        return 2
    keep = "--keep" in sys.argv
    urls = [a for a in sys.argv[1:] if not a.startswith("--")] or DEFAULT_URLS
    OUT.mkdir(exist_ok=True)
    workflow_id = f"e2e-{int(time.time())}"
    tasks = plan_tasks(workflow_id, urls)
    try:
        return await run(workflow_id, tasks, urls)
    finally:
        # Pass or fail, so a broken run doesn't leave sandboxes or disks behind.
        await clean_up_workflow(workflow_id, [t.key for t in tasks], delete_disks=not keep)
        log("deleted the test's sandboxes" + ("" if keep else " and disks"))


async def run(workflow_id: str, tasks: list[AgentTask], urls: list[str]) -> int:
    t0 = time.time()
    log("starting a local Temporal dev server")
    async with await WorkflowEnvironment.start_local() as env:
        address = env.client.service_client.config.target_host
        worker_a = WorkerProcess("A", address)
        worker_b = None
        try:
            handle = await env.client.start_workflow(
                IngestWorkflow.run, IngestRequest(urls=urls), id=workflow_id, task_queue=TASK_QUEUE
            )
            log(f"started workflow {workflow_id} with {len(urls)} agent(s)")

            await asyncio.wait_for(wait_until_all_agents_run(handle, len(urls)), timeout=600)
            log(f"all {len(urls)} agents are running in sandboxes; letting them work for {WORK_BEFORE_SWAP_S}s")
            await asyncio.sleep(WORK_BEFORE_SWAP_S)

            log("stopping worker A (SIGTERM), as a deploy or scale-in would")
            log(f"worker A exited in {worker_a.stop():.1f}s; starting worker B")
            worker_b = WorkerProcess("B", address)

            result = await asyncio.wait_for(handle.result(), timeout=3600)
        finally:
            for w in (worker_a, worker_b):
                if w is not None:
                    w.stop()

    log("checking the result")
    checks: list[tuple[str, bool]] = []
    detached = worker_a.count("detached from process")
    reattached = worker_b.count("reattached to process")
    checks.append((f"all {len(urls)} tasks succeeded (failed: {result.failed or 'none'})", len(result.succeeded) == len(urls)))
    checks.append((f"worker A detached from {detached} of {len(urls)} running agents on shutdown", detached == len(urls)))
    checks.append((f"worker B reattached to {reattached} of them instead of restarting", reattached == detached))

    rows = 0
    merged = OUT / "merged.parquet"
    if result.merged:
        urllib.request.urlretrieve(result.merged.download_url, merged)
        table = pq.read_table(merged)
        rows = table.num_rows
        checks.append((f"merged.parquet downloads from its signed URL with {rows} rows", rows == result.merged.rows > 0))
    else:
        checks.append(("merged.parquet was produced", False))

    live = await live_sandboxes(tasks, workflow_id)
    checks.append((f"no sandboxes left behind ({live} found)", live == 0))

    lines = [
        f"## Temporal + Archil end-to-end: `{workflow_id}`",
        "",
        f"{len(urls)} agents, worker replaced mid-run, {time.time() - t0:.0f}s total.",
        "",
        *[f"- {'✅' if ok else '❌'} {text}" for text, ok in checks],
        "",
        "| task | url | cost | agent summary |",
        "|---|---|---|---|",
        *[
            f"| {r.key} | {next(t.url for t in tasks if t.key == r.key)} | "
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
