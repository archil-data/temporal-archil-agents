"""The activities: one agent per URL, one step that merges their output, and a
cleanup step that makes sure no sandbox outlives the workflow."""

from __future__ import annotations

import json
from typing import Optional
from urllib.parse import urlparse

from archil import SandboxEgressPolicy, SandboxEgressRule, SandboxEgressTransform, SandboxMountSpec, SandboxNetwork
from temporalio import activity
from temporalio.exceptions import ApplicationError

from . import config
from .sandboxes import Heartbeater, SandboxSpec, ensure_disk, find_disk, find_sandboxes, run_supervised, spin_down
from .shared import (
    OUTPUT_COLUMNS,
    AgentTask,
    AssembleInput,
    AssembleResult,
    CleanupInput,
    TaskResult,
    merged_disk_name,
    session_id,
    task_disk_name,
)

# Exit code run_agent.py uses when the agent says the task can't be done.
# Retrying won't help, so the activity fails without retrying.
EXIT_GAVE_UP = 3

# What the agent sees as its API key when egress is restricted. The real key
# is added to requests to the Anthropic API by Archil's egress proxy, outside
# the VM, so nothing the agent runs (or downloads) can read it.
KEY_PLACEHOLDER = "set-by-archil-egress-proxy"


def agent_network(cfg: config.Config, url: str) -> Optional[SandboxNetwork]:
    """Deny all outbound traffic except the Anthropic API and the URL's host."""
    if not cfg.restrict_egress:
        return None
    anthropic = SandboxEgressRule(
        target="api.anthropic.com",
        transform=SandboxEgressTransform(headers={"x-api-key": cfg.anthropic_api_key}),
    )
    hosts = {urlparse(url).hostname or "", *cfg.extra_allowed_hosts} - {""}
    return SandboxNetwork(egress=SandboxEgressPolicy(default="deny", allow=[anthropic, *sorted(hosts)]))


@activity.defn
async def run_agent_task(task: AgentTask) -> TaskResult:
    cfg = config.load()
    async with Heartbeater() as hb:
        # One disk per task: the agent's working files *and* its Claude session
        # live here, so any attempt, on any worker, picks up where the last left off.
        disk = await ensure_disk(task_disk_name(task))
        spec = SandboxSpec(
            name=disk.name,
            mounts=[SandboxMountSpec(disk=disk.id, path="/mnt/archil")],
            vcpus=cfg.agent_vcpus,
            mem_mib=cfg.agent_mem_mib,
            network=agent_network(cfg, task.url),
        )
        env = {
            "ANTHROPIC_API_KEY": KEY_PLACEHOLDER if cfg.restrict_egress else cfg.anthropic_api_key,
            "AGENT_MODEL": cfg.agent_model,
            "AGENT_SESSION_ID": session_id(task),
            "AGENT_TASK": json.dumps({"url": task.url, "columns": OUTPUT_COLUMNS}),
        }
        sandbox, result = await run_supervised(cfg, hb, spec, "python3 /opt/agent/run_agent.py", env=env, label=task.key)
        try:
            if result.status != "completed" or result.exit_code != 0:
                gave_up = result.exit_code == EXIT_GAVE_UP
                # Count real agent failures in the checkpoint, which Temporal
                # hands to the next attempt. Detaches and dropped connections
                # never get here, so they don't use up this budget.
                # The next attempt starts a new process rather than reattaching.
                failures = hb.checkpoint.get("agent_failures", 0) + 1
                hb.beat(agent_failures=failures, process_id=None)
                raise ApplicationError(
                    f"agent exited (failure {failures} of {cfg.max_agent_failures}): "
                    f"status={result.status} code={result.exit_code} reason={result.exit_reason}",
                    type="AgentGaveUp" if gave_up else "AgentFailed",
                    non_retryable=gave_up or failures >= cfg.max_agent_failures,
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
    async with Heartbeater() as hb:
        out = await ensure_disk(merged_disk_name(inp.workflow_id))
        # Every task disk is mounted read-only (no delegation needed, so this works
        # while other clients have them open), plus one writable output disk.
        mounts = [SandboxMountSpec(disk=out.id, path="/mnt/out")] + [
            SandboxMountSpec(disk=t.disk_id, path=f"/mnt/tasks/{t.key}", read_only=True) for t in inp.tasks
        ]
        spec = SandboxSpec(name=out.name, mounts=mounts, vcpus=4, mem_mib=8192)
        sandbox, result = await run_supervised(cfg, hb, spec, "python3 /opt/agent/assemble.py", env={}, label="assemble")
        try:
            if result.status != "completed" or result.exit_code != 0:
                # The inputs are already validated Parquet; a failure here
                # won't fix itself on retry.
                raise ApplicationError(
                    f"assemble exited: status={result.status} code={result.exit_code}",
                    type="AssembleFailed",
                    non_retryable=True,
                )
            manifest = json.loads((await sandbox.exec.aio("cat /mnt/out/manifest.json")).stdout)
        finally:
            await spin_down(sandbox)
    # A signed URL anyone (e.g. ClickHouse's url() table function) can GET,
    # no Archil credentials needed. Keys are paths relative to the disk root.
    share = await out.share.aio("merged.parquet", expires_in=24 * 3600)
    return AssembleResult(disk_id=out.id, rows=manifest["rows"], download_url=share.url)


async def clean_up_workflow(workflow_id: str, keys: list[str], *, delete_disks: bool = False) -> int:
    """Delete every sandbox this workflow's tasks could have left behind (and,
    optionally, their disks). Returns how many sandboxes it deleted."""
    names = [task_disk_name(AgentTask(workflow_id, key, "")) for key in keys] + [merged_disk_name(workflow_id)]
    deleted = 0
    for name in names:
        disk = await find_disk(name)
        if disk is None:
            continue
        for sandbox in await find_sandboxes(disk.id, name):
            await spin_down(sandbox)
            deleted += 1
        if delete_disks:
            await disk.delete.aio()
    return deleted


@activity.defn
async def clean_up(inp: CleanupInput) -> int:
    deleted = await clean_up_workflow(inp.workflow_id, inp.keys)
    if deleted:
        activity.logger.info("deleted %d leftover sandbox(es)", deleted)
    return deleted
