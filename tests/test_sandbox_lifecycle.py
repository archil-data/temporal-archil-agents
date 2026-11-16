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
from temporalio import activity
from temporalio.exceptions import ApplicationError
from temporalio.testing import ActivityEnvironment

from archil_temporal import config
from archil_temporal.sandboxes import Heartbeater, SandboxSpec, client, ensure_disk, run_supervised, spin_down

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


async def _attempt(spec):
    async with Heartbeater() as hb:
        return await run_supervised(config.load(), hb, spec, COMMAND, env={}, label="lifecycle")


async def test_worker_shutdown_detaches_and_the_retry_reattaches(spec):
    heartbeats: list[dict] = []
    retry_heartbeats: list[dict] = []

    async def attempt():
        return await _attempt(spec)

    first = ActivityEnvironment()
    first.on_heartbeat = lambda *details: heartbeats.append(details[0])
    running = asyncio.ensure_future(first.run(attempt))
    await asyncio.sleep(8)
    first.worker_shutdown()
    with pytest.raises(ApplicationError) as detached:
        await running
    assert detached.value.type == "WorkerShutdown"
    assert not detached.value.non_retryable

    checkpoint = heartbeats[-1]
    assert checkpoint["cursor"] > 0
    assert await _status(checkpoint["sandbox_id"]) == "running"  # left running for the retry

    second = ActivityEnvironment()
    second.info = dataclasses.replace(second.info, heartbeat_details=[checkpoint])
    second.on_heartbeat = lambda *details: retry_heartbeats.append(details[0])
    sandbox, result = await second.run(attempt)
    # The retry took over the same process in the same sandbox, not a new one.
    assert sandbox.id == checkpoint["sandbox_id"]
    assert {h.get("process_id") for h in retry_heartbeats} == {checkpoint["process_id"]}
    assert (result.status, result.exit_code) == ("completed", 0)
    assert (await sandbox.exec.aio("cat /mnt/archil/marker")).stdout.strip() == "done"

    await spin_down(sandbox)
    assert await _status(sandbox.id) in ("gone", "deleting", "deleted")


async def test_workflow_cancel_spins_the_sandbox_down(spec):
    heartbeats: list[dict] = []
    env = ActivityEnvironment()
    env.on_heartbeat = lambda *details: heartbeats.append(details[0])

    async def attempt():
        return await _attempt(spec)

    running = asyncio.ensure_future(env.run(attempt))
    await asyncio.sleep(8)
    env.cancel()  # cancel_requested, as when the workflow is cancelled
    with pytest.raises(asyncio.CancelledError):
        await running
    assert await _status(heartbeats[-1]["sandbox_id"]) in ("gone", "deleting", "deleted")


async def test_superseded_attempt_leaves_the_sandbox_for_the_new_one(spec):
    # After a heartbeat timeout Temporal starts a new attempt, which reattaches
    # to the same process. If the old attempt is still alive, its heartbeats
    # come back "not found": it must walk away, not delete the sandbox.
    heartbeats: list[dict] = []
    env = ActivityEnvironment()
    env.on_heartbeat = lambda *details: heartbeats.append(details[0])

    async def attempt():
        return await _attempt(spec)

    running = asyncio.ensure_future(env.run(attempt))
    await asyncio.sleep(8)
    env.cancel(activity.ActivityCancellationDetails(not_found=True))
    with pytest.raises(asyncio.CancelledError):
        await running
    assert await _status(heartbeats[-1]["sandbox_id"]) == "running"


async def test_retry_without_a_checkpoint_finds_the_running_process(spec):
    # A worker that crashes right after starting the process may never get a
    # heartbeat to Temporal. The retry must still find that process rather
    # than start a second agent next to it.
    heartbeats: list[dict] = []
    retry_heartbeats: list[dict] = []

    async def attempt():
        return await _attempt(spec)

    first = ActivityEnvironment()
    first.on_heartbeat = lambda *details: heartbeats.append(details[0])
    running = asyncio.ensure_future(first.run(attempt))
    await asyncio.sleep(8)
    first.worker_shutdown()
    with pytest.raises(ApplicationError):
        await running

    second = ActivityEnvironment()  # no heartbeat details at all
    second.on_heartbeat = lambda *details: retry_heartbeats.append(details[0])
    sandbox, result = await second.run(attempt)
    assert sandbox.id == heartbeats[-1]["sandbox_id"]
    assert {h.get("process_id") for h in retry_heartbeats} == {heartbeats[-1]["process_id"]}
    assert (result.status, result.exit_code) == ("completed", 0)
    await spin_down(sandbox)
