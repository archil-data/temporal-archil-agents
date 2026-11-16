"""Run one command in an Archil sandbox for the lifetime of a Temporal activity.

The activity is the sandbox's supervisor: it finds or creates the sandbox,
starts (or reattaches to) the process, heartbeats a checkpoint while it runs,
and deletes the sandbox when the work is finished or cancelled. The only time
it walks away from a live sandbox is when it can't supervise it any more (its
worker is shutting down, or its connection dropped); the next attempt, on any
worker, reattaches to the same process from the heartbeat checkpoint, so a
deploy or scale-in never interrupts the agent.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from dataclasses import dataclass
from datetime import timedelta
from typing import Optional

import archil
from archil import ArchilApiError, SandboxMountSpec, SandboxNetwork
from temporalio import activity
from temporalio.exceptions import ApplicationError, ApplicationErrorCategory

from .config import Config

log = logging.getLogger(__name__)

HEARTBEAT_EVERY_S = 10

# Statuses from which a sandbox can be brought back to "running".
_REUSABLE = {"pending", "running", "pausing", "paused", "stopping", "stopped"}
# Statuses of a sandbox that still exists and may be using resources.
_LIVE = _REUSABLE | {"exited", "failed"}

_client: Optional[archil.Archil] = None


def client() -> archil.Archil:
    """One Archil client per worker process (reads ARCHIL_API_KEY / ARCHIL_REGION)."""
    global _client
    if _client is None:
        _client = archil.Archil()
    return _client


async def find_disk(name: str):
    for disk in await client().disks.list.aio(name=name):
        if disk.status not in ("failed", "deleting", "deleted"):
            return disk
    return None


async def ensure_disk(name: str):
    """Return the disk with this name, creating it on first use."""
    disk = await find_disk(name)
    if disk is not None:
        return await disk.wait_until_ready.aio()
    created = await client().disks.create.aio(name=name)
    return await created.disk.wait_until_ready.aio()


@dataclass
class SandboxSpec:
    name: str
    mounts: list[SandboxMountSpec]
    vcpus: int
    mem_mib: int
    network: Optional[SandboxNetwork] = None


async def find_sandboxes(disk_id: str, name: str) -> list:
    """Sandboxes with this name that mount this disk and still exist."""
    return [s for s in await client().sandboxes.list.aio(disk=disk_id) if s.name == name and s.status in _LIVE]


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
    for sandbox in await find_sandboxes(spec.mounts[0].disk, spec.name):
        if sandbox.status in _REUSABLE:
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
        network=spec.network,
        # Backstops for when no activity is left to supervise the sandbox
        # (e.g. the workflow was terminated). Either one pauses it: a paused
        # sandbox uses no CPU, and a late retry can still resume and reattach.
        max_ttl_seconds=cfg.sandbox_max_ttl_s,
        idle_ttl_seconds=cfg.sandbox_orphan_ttl_s,
    )
    activity.logger.info("created sandbox %s", sandbox.id)
    return sandbox


async def spin_down(sandbox) -> None:
    """Stop and delete the sandbox (its mounted disks are kept). Best effort:
    the workflow's cleanup step retries anything this misses."""
    try:
        if sandbox.status not in ("stopped", "exited", "failed"):
            sandbox = await sandbox.stop.aio()  # delete takes a stopped sandbox
        await sandbox.delete.aio()
    except Exception as e:
        log.warning("could not delete sandbox %s: %s", sandbox.id, e)


class Heartbeater:
    """Heartbeats the activity's checkpoint every few seconds from the moment
    it starts, including while disks and sandboxes are still being set up, so
    a slow start never looks like a dead worker.

    Until this attempt has a process of its own, it re-sends the previous
    attempt's checkpoint, so a retry that dies early doesn't lose it."""

    def __init__(self) -> None:
        details = activity.info().heartbeat_details
        self.checkpoint: dict = dict(details[0]) if details else {}
        self._task: Optional[asyncio.Task] = None

    async def __aenter__(self) -> "Heartbeater":
        self._task = asyncio.create_task(self._run())
        return self

    async def __aexit__(self, *exc) -> None:
        if self._task is not None:
            self._task.cancel()

    def beat(self, **checkpoint) -> None:
        self.checkpoint.update(checkpoint)
        activity.heartbeat(dict(self.checkpoint))

    async def _run(self) -> None:
        while True:
            activity.heartbeat(dict(self.checkpoint))
            await asyncio.sleep(HEARTBEAT_EVERY_S)


def _printer(label: str):
    def on_output(output) -> None:
        for line in output.data.decode(errors="replace").splitlines():
            log.info("[%s] %s", label, line)

    return on_output


def _retry_soon(message: str, type: str) -> ApplicationError:
    # Not a failure of the work: retry right away (instead of after the
    # policy's backoff) and log it quietly (BENIGN) rather than as an error.
    return ApplicationError(message, type=type, next_retry_delay=timedelta(seconds=1), category=ApplicationErrorCategory.BENIGN)


async def run_supervised(
    cfg: Config, hb: Heartbeater, spec: SandboxSpec, command: str, *, env: dict[str, str], label: str
):
    """Run ``command`` to completion in the sandbox described by ``spec``.

    Returns ``(sandbox, result)`` with the sandbox still running, so the caller
    can read its outputs before deleting it. On cancellation the sandbox is
    deleted here; if this attempt can't keep supervising it (worker shutdown,
    lost connection) it is left running for the next attempt to reattach.
    """
    checkpoint = hb.checkpoint
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
        except RuntimeError as e:
            # The runtime answered, and the process isn't there (the sandbox
            # was restarted). Anything else, such as a network error, fails
            # this attempt instead: starting a second agent next to one that
            # may still be running would be worse than retrying.
            if not str(e).startswith("process_not_found"):
                raise
            activity.logger.info("process %s is gone; starting a new one", checkpoint["process_id"])
    if process is None:
        process = await sandbox.run.aio(command, env=env, on_output=on_output, collect_output=False)

    def checkpoint_now() -> None:
        hb.beat(sandbox_id=sandbox.id, process_id=process.id, cursor=process.cursor)

    checkpoint_now()
    exited = asyncio.ensure_future(process.wait.aio())
    shutdown = asyncio.ensure_future(activity.wait_for_worker_shutdown())
    try:
        while not exited.done():
            await asyncio.wait({exited, shutdown}, timeout=HEARTBEAT_EVERY_S, return_when=asyncio.FIRST_COMPLETED)
            checkpoint_now()
            if shutdown.done() and not exited.done():
                # Detach right away instead of waiting out the graceful
                # shutdown timeout: the process keeps running, and the retry
                # reattaches on a worker that is staying up.
                exited.cancel()
                with contextlib.suppress(Exception):
                    await process.disconnect.aio()
                activity.logger.info("detached from process %s; sandbox %s left running", process.id, sandbox.id)
                raise _retry_soon("worker shutting down; sandbox left running for reattach", "WorkerShutdown")
        try:
            return sandbox, exited.result()
        except ConnectionError as e:
            # The WebSocket dropped, not the process: let the retry reattach.
            activity.logger.info("lost connection to process %s (%s); retrying to reattach", process.id, e)
            raise _retry_soon(f"lost connection to the sandbox process: {e}", "ConnectionLost") from e
    except asyncio.CancelledError:
        exited.cancel()
        reason = activity.cancellation_details()
        if reason is not None and reason.cancel_requested:
            # The workflow asked for this work to stop: nobody will come back
            # for it, so spin the sandbox down now.
            with contextlib.suppress(Exception):
                await process.kill.aio()
            await spin_down(sandbox)
        else:
            # Anything else (worker shutdown, heartbeat timeout, this attempt
            # being superseded) can mean another attempt has already
            # reattached to this process. Deleting here would kill that
            # attempt's work, so only disconnect.
            with contextlib.suppress(Exception):
                await process.disconnect.aio()
        raise
    finally:
        shutdown.cancel()
