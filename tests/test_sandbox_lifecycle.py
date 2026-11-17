"""The sandbox side of the control activities, against the live Archil API,
with a shell loop standing in for the Temporal worker.

Skipped unless ARCHIL_API_KEY and ARCHIL_REGION are set.
"""

from __future__ import annotations

import asyncio
import os
import time

import pytest
from archil import SandboxMountSpec

from archil_temporal import config
from archil_temporal.sandboxes import (
    client,
    delete_sandboxes,
    ensure_disk,
    ensure_service,
    find_sandboxes,
    get_or_create_sandbox,
    service_state,
)

pytestmark = pytest.mark.skipif(
    not (os.environ.get("ARCHIL_API_KEY") and os.environ.get("ARCHIL_REGION")),
    reason="needs ARCHIL_API_KEY and ARCHIL_REGION",
)

# Stands in for the worker: records each start on the disk, then runs.
COMMAND = "sh -c 'echo started >> /mnt/archil/starts; sync; exec sleep 600'"


async def starts(sandbox) -> int:
    return len((await sandbox.exec.aio("cat /mnt/archil/starts 2>/dev/null || true")).stdout.split())


async def wait_for(predicate, timeout=60):
    deadline = time.time() + timeout
    while not await predicate():
        assert time.time() < deadline, "timed out"
        await asyncio.sleep(1)


@pytest.fixture
async def disk():
    name = f"test-lifecycle-{int(time.time() * 1000)}"
    disk = await ensure_disk(name)
    yield disk
    await delete_sandboxes(name)
    await disk.delete.aio()


async def test_sandbox_and_worker_service_are_started_once_restarted_and_deleted(disk):
    cfg = config.load()
    mounts = [SandboxMountSpec(disk=disk.id, path="/mnt/archil")]

    async def start():
        sandbox = await get_or_create_sandbox(cfg, disk.name, mounts, vcpus=1, mem_mib=1024, network=None)
        await ensure_service(sandbox, "worker", COMMAND, {"FOO": "bar"})
        return sandbox

    sandbox = await start()
    await wait_for(lambda: _eq(starts(sandbox), 1))

    # Starting again (as a retried control activity would) reuses both.
    again = await start()
    assert again.id == sandbox.id
    await asyncio.sleep(3)
    assert await starts(sandbox) == 1

    # The service is supervised: if the worker dies, it is started again.
    await sandbox.exec.aio("archil services signal worker kill")
    await wait_for(lambda: _eq(starts(sandbox), 2))
    assert (await service_state(sandbox, "worker"))["status"] == "active"

    # When the work is done, the sandbox is deleted and the disk is kept.
    assert await delete_sandboxes(disk.name) == 1
    assert await find_sandboxes(disk.id, disk.name) == []
    assert (await client().disks.get.aio(disk.id)).status not in ("deleting", "deleted")


async def _eq(value, expected) -> bool:
    return await value == expected
