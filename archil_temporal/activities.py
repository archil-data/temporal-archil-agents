"""The two activities: one agent per URL, then one step that merges their output."""

from __future__ import annotations

import json

from archil import SandboxMountSpec
from temporalio import activity
from temporalio.exceptions import ApplicationError

from . import config
from .sandboxes import SandboxSpec, spin_down, ensure_disk, run_supervised
from .shared import (
    OUTPUT_COLUMNS,
    AgentTask,
    AssembleInput,
    AssembleResult,
    TaskResult,
    merged_disk_name,
    session_id,
    task_disk_name,
)

# Exit code run_agent.py uses when the agent says the task can't be done.
# Retrying won't help, so the activity fails without retrying.
EXIT_GAVE_UP = 3


@activity.defn
async def run_agent_task(task: AgentTask) -> TaskResult:
    cfg = config.load()
    # One disk per task: the agent's working files *and* its Claude session
    # live here, so any attempt, on any worker, picks up where the last left off.
    disk = await ensure_disk(task_disk_name(task))
    spec = SandboxSpec(
        name=disk.name,
        mounts=[SandboxMountSpec(disk=disk.id, path="/mnt/archil")],
        vcpus=cfg.agent_vcpus,
        mem_mib=cfg.agent_mem_mib,
    )
    env = {
        "ANTHROPIC_API_KEY": cfg.anthropic_api_key,
        "AGENT_MODEL": cfg.agent_model,
        "AGENT_SESSION_ID": session_id(task),
        "AGENT_TASK": json.dumps({"url": task.url, "columns": OUTPUT_COLUMNS}),
    }
    sandbox, result = await run_supervised(
        cfg, spec, "python3 /opt/agent/run_agent.py", env=env, label=task.key
    )
    try:
        if result.status != "completed" or result.exit_code != 0:
            raise ApplicationError(
                f"agent exited: status={result.status} code={result.exit_code} reason={result.exit_reason}",
                type="AgentGaveUp" if result.exit_code == EXIT_GAVE_UP else "AgentFailed",
                non_retryable=result.exit_code == EXIT_GAVE_UP,
            )
        report = json.loads((await sandbox.exec.aio("cat /mnt/archil/result.json")).stdout)
        return TaskResult(key=task.key, disk_id=disk.id, summary=report.get("summary", ""), cost_usd=report.get("cost_usd"))
    finally:
        # Done (or failed) either way: spin the VM down. The disk stays, so a
        # retry resumes the Claude session against the same files.
        await spin_down(sandbox)


@activity.defn
async def assemble(inp: AssembleInput) -> AssembleResult:
    cfg = config.load()
    out = await ensure_disk(merged_disk_name(inp.workflow_id))
    # Every task disk is mounted read-only (no delegation needed, so this works
    # while other clients have them open), plus one writable output disk.
    mounts = [SandboxMountSpec(disk=out.id, path="/mnt/out")] + [
        SandboxMountSpec(disk=t.disk_id, path=f"/mnt/tasks/{t.key}", read_only=True) for t in inp.tasks
    ]
    spec = SandboxSpec(name=out.name, mounts=mounts, vcpus=4, mem_mib=8192)
    sandbox, result = await run_supervised(cfg, spec, "python3 /opt/agent/assemble.py", env={}, label="assemble")
    try:
        if result.status != "completed" or result.exit_code != 0:
            raise ApplicationError(f"assemble exited: status={result.status} code={result.exit_code}")
        manifest = json.loads((await sandbox.exec.aio("cat /mnt/out/manifest.json")).stdout)
    finally:
        await spin_down(sandbox)
    # A signed URL anyone (e.g. ClickHouse's url() table function) can GET,
    # no Archil credentials needed. Keys are paths relative to the disk root.
    share = await out.share.aio("merged.parquet", expires_in=24 * 3600)
    return AssembleResult(disk_id=out.id, rows=manifest["rows"], download_url=share.url)
