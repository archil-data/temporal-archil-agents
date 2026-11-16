"""Sandbox supervision against the live Archil API, with a shell loop standing
in for the agent (no Anthropic key or custom image needed).

Skipped unless ARCHIL_API_KEY and ARCHIL_REGION are set.
"""

from __future__ import annotations

import asyncio
import dataclasses
import os
import time

import pytest
from archil import ArchilApiError, SandboxMountSpec
from temporalio.exceptions import ApplicationError
from temporalio.testing import ActivityEnvironment

from archil_temporal import config
from archil_temporal.sandboxes import SandboxSpec, client, ensure_disk, run_supervised, spin_down

pytestmark = pytest.mark.skipif(
    not (os.environ.get("ARCHIL_API_KEY") and os.environ.get("ARCHIL_REGION")),
    reason="needs ARCHIL_API_KEY and ARCHIL_REGION",
)

# Counts to 20, then leaves a file on the disk.
COMMAND = "for i in $(seq 1 20); do echo tick $i; sleep 1; done; echo done > /mnt/archil/marker; sync"


@pytest.fixture
async def spec():
    os.environ.setdefault("AGENT_BASE_IMAGE", "python:3.12-slim")
    name = f"test-lifecycle-{int(time.time() * 1000)}"
    disk = await ensure_disk(name)
    yield SandboxSpec(name=name, mounts=[SandboxMountSpec(disk=disk.id, path="/mnt/archil")], vcpus=1, mem_mib=1024)
    for sandbox in await client().sandboxes.list.aio(disk=disk.id):
        if sandbox.status not in ("deleting", "deleted"):
            await spin_down(sandbox)
    await disk.delete.aio()


async def _status(sandbox_id: str) -> str:
    try:
        return (await client().sandboxes.get.aio(sandbox_id)).status
    except ArchilApiError as e:
        if e.status == 404:
            return "gone"
        raise


async def test_worker_shutdown_detaches_and_the_retry_reattaches(spec):
    cfg = config.load()
    heartbeats: list[dict] = []

    async def attempt():
        return await run_supervised(cfg, spec, COMMAND, env={}, label="lifecycle")

    first = ActivityEnvironment()
    first.on_heartbeat = lambda *details: heartbeats.append(details[0])
    running = asyncio.ensure_future(first.run(attempt))
    await asyncio.sleep(8)
    first.worker_shutdown()
    with pytest.raises(ApplicationError) as detached:
        await running
    assert detached.value.type == "WorkerShutdown"

    checkpoint = heartbeats[-1]
    assert checkpoint["cursor"] > 0
    assert await _status(checkpoint["sandbox_id"]) == "running"  # left running for the retry

    second = ActivityEnvironment()
    second.info = dataclasses.replace(second.info, heartbeat_details=[checkpoint])
    sandbox, result = await second.run(attempt)
    assert sandbox.id == checkpoint["sandbox_id"]  # same sandbox, same process
    assert (result.status, result.exit_code) == ("completed", 0)
    assert (await sandbox.exec.aio("cat /mnt/archil/marker")).stdout.strip() == "done"

    await spin_down(sandbox)
    assert await _status(sandbox.id) in ("gone", "deleting", "deleted")


async def test_workflow_cancel_spins_the_sandbox_down(spec):
    cfg = config.load()
    heartbeats: list[dict] = []
    env = ActivityEnvironment()
    env.on_heartbeat = lambda *details: heartbeats.append(details[0])

    async def attempt():
        return await run_supervised(cfg, spec, COMMAND, env={}, label="lifecycle")

    running = asyncio.ensure_future(env.run(attempt))
    await asyncio.sleep(8)
    env.cancel()  # cancel_requested, as when the workflow is cancelled
    with pytest.raises(asyncio.CancelledError):
        await running
    assert await _status(heartbeats[-1]["sandbox_id"]) in ("gone", "deleting", "deleted")
