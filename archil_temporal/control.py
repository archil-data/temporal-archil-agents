"""Control activities: start a sandbox whose Temporal worker runs a piece of
work, delete it when the work is done, and share the merged file.

They run on the control worker and finish in seconds, so the control worker can
be deployed or scaled down at any time: no long-running work lives on it.
"""

from __future__ import annotations

import asyncio

from archil import SandboxMountSpec
from temporalio import activity
from temporalio.api.enums.v1 import TaskQueueKind, TaskQueueType
from temporalio.api.taskqueue.v1 import TaskQueue
from temporalio.api.workflowservice.v1 import DescribeTaskQueueRequest
from temporalio.client import Client

from . import config
from .sandboxes import (
    KEY_PLACEHOLDER,
    WORKER_COMMAND,
    WORKER_SERVICE,
    delete_sandboxes,
    ensure_disk,
    ensure_service,
    get_or_create_sandbox,
    network_policy,
)
from .shared import CleanupInput, SandboxRequest, SandboxWorker, ShareRequest, sandbox_queue


class ControlActivities:
    def __init__(self, client: Client, cfg: config.Config) -> None:
        self.client = client
        self.cfg = cfg

    @activity.defn
    async def start_sandbox(self, req: SandboxRequest) -> SandboxWorker:
        """Make sure the sandbox for ``req`` is running a Temporal worker on its
        own task queue, and return that queue. Safe to call again: it reuses
        the sandbox and its worker if they are still there."""
        cfg = self.cfg
        mounts = []
        for m in req.mounts:
            disk = await ensure_disk(m.disk)
            activity.heartbeat()
            mounts.append(SandboxMountSpec(disk=disk.id, path=m.path, read_only=m.read_only))
        sandbox = await get_or_create_sandbox(
            cfg, req.name, mounts, vcpus=req.vcpus, mem_mib=req.mem_mib, network=network_policy(cfg, req.allow_hosts)
        )
        activity.heartbeat()
        queue = sandbox_queue(req.name)
        env = {
            **config.temporal_env(cfg),
            "SANDBOX_ID": sandbox.id,
            "SANDBOX_TASK_QUEUE": queue,
            "AGENT_MODEL": cfg.agent_model,
            "ANTHROPIC_API_KEY": KEY_PLACEHOLDER if network_policy(cfg, req.allow_hosts) else cfg.anthropic_api_key,
        }
        await ensure_service(sandbox, WORKER_SERVICE, WORKER_COMMAND, env)
        await self._wait_for_worker(queue, identity=f"sandbox:{sandbox.id}")
        activity.logger.info("sandbox %s is running a worker on %s", sandbox.id, queue)
        return SandboxWorker(sandbox_id=sandbox.id, task_queue=queue)

    async def _wait_for_worker(self, queue: str, identity: str) -> None:
        # Return once this sandbox's worker is polling its queue, so the
        # activity sent there next starts right away.
        request = DescribeTaskQueueRequest(
            namespace=self.client.namespace,
            task_queue=TaskQueue(name=queue, kind=TaskQueueKind.TASK_QUEUE_KIND_NORMAL),
            task_queue_type=TaskQueueType.TASK_QUEUE_TYPE_ACTIVITY,
        )
        while True:
            response = await self.client.workflow_service.describe_task_queue(request)
            if any(p.identity == identity for p in response.pollers):
                return
            activity.heartbeat()
            await asyncio.sleep(2)

    @activity.defn
    async def stop_sandbox(self, name: str) -> int:
        """Delete the sandbox (its disks are kept). The work is done or given up on."""
        deleted = await delete_sandboxes(name)
        if deleted:
            activity.logger.info("deleted sandbox %s", name)
        return deleted

    @activity.defn
    async def share_file(self, req: ShareRequest) -> str:
        """A signed URL for a file on a disk, which anyone (e.g. ClickHouse's
        url() table function) can GET without Archil credentials."""
        disk = await ensure_disk(req.disk)
        share = await disk.share.aio(req.path, expires_in=24 * 3600)
        return share.url

    @activity.defn
    async def clean_up(self, inp: CleanupInput) -> int:
        """Delete any sandbox the workflow's work left behind."""
        deleted = 0
        for name in inp.names:
            deleted += await delete_sandboxes(name)
        if deleted:
            activity.logger.info("deleted %d leftover sandbox(es)", deleted)
        return deleted
