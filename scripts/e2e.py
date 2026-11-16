"""End-to-end demo: real Claude agents in real Archil sandboxes, with the
Temporal worker replaced while they run.

    python scripts/e2e.py [URL ...]

1. Start a local Temporal dev server and worker A, then start an IngestWorkflow.
2. Once every agent is running in its sandbox, stop worker A (SIGTERM, as a
   deploy or scale-in would) and start worker B.
3. Wait for the result and check: every task succeeded, worker B reattached to
   the agents worker A left running, the merged file downloads and has rows,
   and no sandbox is left running.
4. Delete the test's disks (pass --keep to keep them for inspection).

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

from archil_temporal.sandboxes import client as archil
from archil_temporal.shared import TASK_QUEUE, AgentTask, IngestRequest, merged_disk_name, task_disk_name
from archil_temporal.workflows import IngestWorkflow

DEFAULT_URLS = [
    "https://raw.githubusercontent.com/datasets/gdp/main/data/gdp.csv",
    "https://raw.githubusercontent.com/datasets/co2-ppm/main/data/co2-mm-mlo.csv",
]
OUT = Path("e2e-out")
# How long to let the agents work before replacing the worker.
WORK_BEFORE_SWAP_S = 30


# Never let a credential reach a log line or the uploaded artifact, even if an
# agent prints its environment. (Actions masks its own logs, not artifacts.)
_SECRETS = [v for k in ("ANTHROPIC_API_KEY", "ARCHIL_API_KEY") if len(v := os.environ.get(k, "")) >= 8]


def redact(text: str) -> str:
    for secret in _SECRETS:
        text = text.replace(secret, "***")
    return text


def log(msg: str) -> None:
    print(f"\n=== {time.strftime('%H:%M:%S')} {msg}", flush=True)


def start_worker(name: str, address: str) -> tuple[subprocess.Popen, Path]:
    path = OUT / f"worker-{name}.log"
    proc = subprocess.Popen(
        [sys.executable, "-m", "archil_temporal.worker"],
        env={**os.environ, "TEMPORAL_ADDRESS": address, "PYTHONUNBUFFERED": "1"},
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )

    def tee() -> None:
        with path.open("w") as f:
            for line in proc.stdout:
                line = redact(line)
                f.write(line)
                f.flush()
                if "HTTP Request" not in line:
                    print(f"[worker {name}] {line}", end="", flush=True)

    threading.Thread(target=tee, daemon=True).start()
    return proc, path


async def wait_until_all_agents_run(handle, n: int) -> None:
    while True:
        pending = (await handle.describe()).raw_description.pending_activities
        running = [p for p in pending if p.activity_type.name == "run_agent_task" and p.HasField("heartbeat_details")]
        if len(running) == n:
            return
        await asyncio.sleep(2)


async def main() -> int:
    keep = "--keep" in sys.argv
    urls = [a for a in sys.argv[1:] if not a.startswith("--")] or DEFAULT_URLS
    OUT.mkdir(exist_ok=True)
    checks: list[tuple[str, bool]] = []
    t0 = time.time()

    log("starting a local Temporal dev server")
    async with await WorkflowEnvironment.start_local() as env:
        address = env.client.service_client.config.target_host
        worker_a, log_a = start_worker("A", address)

        workflow_id = f"e2e-{int(time.time())}"
        tasks = [AgentTask(workflow_id=workflow_id, key=f"t{i:03d}", url=u) for i, u in enumerate(urls)]
        handle = await env.client.start_workflow(
            IngestWorkflow.run, IngestRequest(urls=urls), id=workflow_id, task_queue=TASK_QUEUE
        )
        log(f"started workflow {workflow_id} with {len(urls)} agent(s)")

        worker_b = None
        try:
            await asyncio.wait_for(wait_until_all_agents_run(handle, len(urls)), timeout=600)
            log(f"all {len(urls)} agents are running in sandboxes; letting them work for {WORK_BEFORE_SWAP_S}s")
            await asyncio.sleep(WORK_BEFORE_SWAP_S)

            log("stopping worker A (SIGTERM), as a deploy or scale-in would")
            swap_started = time.time()
            worker_a.send_signal(signal.SIGTERM)
            worker_a.wait(timeout=120)
            log(f"worker A exited in {time.time() - swap_started:.1f}s; starting worker B")
            worker_b, log_b = start_worker("B", address)

            result = await asyncio.wait_for(handle.result(), timeout=3600)
        finally:
            for w in (worker_a, worker_b):
                if w is not None and w.poll() is None:
                    w.terminate()
                    w.wait(timeout=60)

    log("checking the result")
    detached = log_a.read_text().count("sandbox left running for reattach")
    reattached = log_b.read_text().count("reattached to process")
    checks.append((f"all {len(urls)} tasks succeeded (failed: {result.failed or 'none'})", len(result.succeeded) == len(urls)))
    checks.append((f"worker A detached from {detached} running agent(s) on shutdown", detached > 0))
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

    disks = [d for name in [task_disk_name(t) for t in tasks] + [merged_disk_name(workflow_id)]
             for d in await archil().disks.list.aio(name=name) if d.name == name]
    live = [s for d in disks for s in await archil().sandboxes.list.aio(disk=d.id)
            if s.status in ("pending", "running", "pausing", "paused", "stopping")]
    checks.append((f"no sandboxes left running ({len(live)} found)", not live))

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

    if not keep:
        for d in disks:
            await d.delete.aio()
        log(f"deleted {len(disks)} test disk(s)")
    return 0 if all(ok for _, ok in checks) else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
