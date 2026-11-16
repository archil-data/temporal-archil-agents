"""Run one command in an Archil sandbox for the lifetime of a Temporal activity.

The activity is the sandbox's supervisor: it finds or creates the sandbox,
starts (or reattaches to) the process, heartbeats a checkpoint while it runs,
and deletes the sandbox when the work is finished or cancelled. The only time
it walks away from a live sandbox is when its own worker is shutting down; the
next attempt, on any worker, reattaches to the same process from the
heartbeat checkpoint, so a deploy or scale-in never interrupts the agent.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from dataclasses import dataclass
from typing import Optional

import archil
from archil import ArchilApiError, SandboxMountSpec
from temporalio import activity
from temporalio.exceptions import ApplicationError

from .config import Config

log = logging.getLogger(__name__)

HEARTBEAT_EVERY_S = 10

# Statuses from which a sandbox can be brought back to "running".
_REUSABLE = {"pending", "running", "pausing", "paused", "stopping", "stopped"}

_client: Optional[archil.Archil] = None


def client() -> archil.Archil:
    """One Archil client per worker process (reads ARCHIL_API_KEY / ARCHIL_REGION)."""
    global _client
    if _client is None:
        _client = archil.Archil()
    return _client


async def ensure_disk(name: str):
    """Return the disk with this name, creating it on first use."""
    for disk in await client().disks.list.aio(name=name):
        if disk.name == name and disk.status not in ("failed", "deleting", "deleted"):
            return await disk.wait_until_ready.aio()
    created = await client().disks.create.aio(name=name)
    return await created.disk.wait_until_ready.aio()


@dataclass
class SandboxSpec:
    name: str
    mounts: list[SandboxMountSpec]
    vcpus: int
    mem_mib: int


async def _find(spec: SandboxSpec, hint_id: Optional[str]):
    if hint_id:
        try:
            sandbox = await client().sandboxes.get.aio(hint_id)
            if sandbox.status in _REUSABLE:
                return sandbox
        except ArchilApiError as e:
            if e.status != 404:
                raise
    # The checkpoint may be missing if the last attempt died before its first
    # heartbeat; the name is deterministic, so look it up by its first disk.
    for sandbox in await client().sandboxes.list.aio(disk=spec.mounts[0].disk):
        if sandbox.name == spec.name and sandbox.status in _REUSABLE:
            return sandbox
    return None


async def _bring_up(sandbox):
    while sandbox.status in ("pending", "pausing", "stopping"):
        await asyncio.sleep(1)
        sandbox = await sandbox.refresh.aio()
    if sandbox.status == "paused":
        sandbox = await sandbox.resume.aio()
    elif sandbox.status == "stopped":
        sandbox = await sandbox.start.aio()
    return sandbox


async def get_or_create_sandbox(cfg: Config, spec: SandboxSpec, hint_id: Optional[str] = None):
    sandbox = await _find(spec, hint_id)
    if sandbox is not None:
        sandbox = await _bring_up(sandbox)
        if sandbox.status == "running":
            activity.logger.info("reusing sandbox %s", sandbox.id)
            return sandbox
    sandbox = await client().sandboxes.create.aio(
        name=spec.name,
        image_id=cfg.image_id,
        base_image=cfg.base_image,
        vcpu_count=spec.vcpus,
        mem_size_mib=spec.mem_mib,
        mounts=spec.mounts,
        # Backstops for when no activity is left to clean up (e.g. the
        # workflow was terminated). Either TTL pauses the sandbox: it stops
        # using CPU, and a late retry can still resume it and reattach.
        max_ttl_seconds=cfg.sandbox_max_ttl_s,
        idle_ttl_seconds=cfg.sandbox_orphan_ttl_s,
    )
    activity.logger.info("created sandbox %s", sandbox.id)
    return sandbox


async def spin_down(sandbox) -> None:
    """Stop and delete the sandbox (its mounted disks are kept). Best effort:
    the TTLs set at creation catch anything this misses."""
    try:
        sandbox = await sandbox.stop.aio()  # a running sandbox can't be deleted
        await sandbox.delete.aio()
    except Exception as e:
        activity.logger.warning("could not delete sandbox %s: %s", sandbox.id, e)


def _printer(label: str):
    def on_output(output) -> None:
        for line in output.data.decode(errors="replace").splitlines():
            log.info("[%s] %s", label, line)

    return on_output


async def run_supervised(cfg: Config, spec: SandboxSpec, command: str, *, env: dict[str, str], label: str):
    """Run ``command`` to completion in the sandbox described by ``spec``.

    Returns ``(sandbox, result)`` with the sandbox still running, so the caller
    can read its outputs before deleting it. On cancellation the sandbox is
    deleted here; on worker shutdown it is left running for the next attempt.
    """
    details = activity.info().heartbeat_details
    checkpoint: dict = details[0] if details else {}

    sandbox = await get_or_create_sandbox(cfg, spec, checkpoint.get("sandbox_id"))
    on_output = _printer(label)

    process = None
    if checkpoint.get("process_id") and checkpoint.get("sandbox_id") == sandbox.id:
        try:
            process = await sandbox.attach.aio(
                checkpoint["process_id"],
                offset=checkpoint.get("cursor", 0),
                on_output=on_output,
                collect_output=False,
            )
            activity.logger.info("reattached to process %s", process.id)
        except Exception as e:  # the process is gone (sandbox restarted); start over
            activity.logger.info("could not reattach (%s); starting a new process", e)
    if process is None:
        process = await sandbox.run.aio(command, env=env, on_output=on_output, collect_output=False)

    def heartbeat() -> None:
        activity.heartbeat({"sandbox_id": sandbox.id, "process_id": process.id, "cursor": process.cursor})

    heartbeat()
    exited = asyncio.ensure_future(process.wait.aio())
    shutdown = asyncio.ensure_future(activity.wait_for_worker_shutdown())
    try:
        while not exited.done():
            await asyncio.wait({exited, shutdown}, timeout=HEARTBEAT_EVERY_S, return_when=asyncio.FIRST_COMPLETED)
            heartbeat()
            if shutdown.done() and not exited.done():
                # Detach right away instead of waiting out the graceful
                # shutdown timeout: the process keeps running, and the retry
                # reattaches on a worker that is staying up.
                exited.cancel()
                with contextlib.suppress(Exception):
                    await process.disconnect.aio()
                raise ApplicationError("worker shutting down; sandbox left running for reattach", type="WorkerShutdown")
        return sandbox, exited.result()
    except asyncio.CancelledError:
        exited.cancel()
        reason = activity.cancellation_details()
        if reason is not None and (reason.cancel_requested or reason.not_found):
            # The workflow was cancelled or no longer exists: nobody will come
            # back for this work, so spin the sandbox down now.
            with contextlib.suppress(Exception):
                await process.kill.aio()
            await spin_down(sandbox)
        else:
            # Worker shutdown, or a heartbeat timeout after which Temporal may
            # already have handed the task to another worker that reattached.
            # Deleting here would kill that attempt, so only disconnect; the
            # orphan TTL reaps the sandbox if no one ever comes back.
            with contextlib.suppress(Exception):
                await process.disconnect.aio()
        raise
    finally:
        shutdown.cancel()
