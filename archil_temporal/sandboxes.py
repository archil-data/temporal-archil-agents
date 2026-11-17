"""Archil sandboxes that each run a Temporal worker for one piece of work.

Used by the control activities: find or create a task's sandbox with its disks
mounted, run a Temporal worker in it as a supervised service, and delete the
sandbox when the work is done.
"""

from __future__ import annotations

import asyncio
import json
import logging
import shlex
from typing import Optional

import archil
from archil import (
    SandboxEgressPolicy,
    SandboxEgressRule,
    SandboxEgressTransform,
    SandboxMountSpec,
    SandboxNetwork,
)

from .config import Config

log = logging.getLogger(__name__)

# The name of the Archil service that runs the Temporal worker in each sandbox.
WORKER_SERVICE = "temporal-worker"
WORKER_COMMAND = "python3 -m archil_temporal.sandbox_worker"

# What agents see as their Anthropic API key when egress is restricted. Archil's
# egress proxy adds the real key to requests to the Anthropic API outside the
# VM, so nothing that runs in the sandbox can read it.
KEY_PLACEHOLDER = "set-by-archil-egress-proxy"

# Statuses from which a sandbox can be brought back to "running".
_REUSABLE = {"pending", "running", "pausing", "paused", "stopping", "stopped"}
# Statuses of a sandbox that still exists.
_LIVE = _REUSABLE | {"exited", "failed"}

_client: Optional[archil.Archil] = None


def client() -> archil.Archil:
    """One Archil client per process (reads ARCHIL_API_KEY / ARCHIL_REGION)."""
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


async def find_sandboxes(disk_id: str, name: str) -> list:
    """Sandboxes with this name that mount this disk and still exist."""
    return [s for s in await client().sandboxes.list.aio(disk=disk_id) if s.name == name and s.status in _LIVE]


async def spin_down(sandbox) -> None:
    """Stop and delete the sandbox. Its disks are kept."""
    try:
        if sandbox.status not in ("stopped", "exited", "failed"):
            sandbox = await sandbox.stop.aio()  # delete takes a stopped sandbox
        await sandbox.delete.aio()
    except Exception as e:
        log.warning("could not delete sandbox %s: %s", sandbox.id, e)


async def delete_sandboxes(name: str) -> int:
    """Delete every sandbox with this name (named after its first disk)."""
    disk = await find_disk(name)
    if disk is None:
        return 0
    sandboxes = await find_sandboxes(disk.id, name)
    for sandbox in sandboxes:
        await spin_down(sandbox)
    return len(sandboxes)


def network_policy(cfg: Config, allow_hosts: Optional[list[str]]) -> Optional[SandboxNetwork]:
    """Deny everything except Temporal and ``allow_hosts``. The Anthropic API
    gets the API key added by Archil's egress proxy."""
    if allow_hosts is None or not cfg.restrict_egress:
        return None
    rules: list = []
    hosts = {cfg.temporal_host, *allow_hosts, *cfg.extra_allowed_hosts}
    if "api.anthropic.com" in hosts:
        hosts.discard("api.anthropic.com")
        rules.append(
            SandboxEgressRule(
                target="api.anthropic.com",
                transform=SandboxEgressTransform(headers={"x-api-key": cfg.anthropic_api_key}),
            )
        )
    rules += sorted(h for h in hosts if h)
    return SandboxNetwork(egress=SandboxEgressPolicy(default="deny", allow=rules))


async def get_or_create_sandbox(cfg: Config, name: str, mounts: list[SandboxMountSpec], *, vcpus: int, mem_mib: int, network):
    """Find the running sandbox with this name, or create it. A sandbox that
    failed, or that can't be brought back, is replaced; its disks are kept."""
    for sandbox in await find_sandboxes(mounts[0].disk, name):
        while sandbox.status in ("pending", "pausing", "stopping"):
            await asyncio.sleep(1)
            sandbox = await sandbox.refresh.aio()
        if sandbox.status == "paused":
            sandbox = await sandbox.resume.aio()
        elif sandbox.status == "stopped":
            sandbox = await sandbox.start.aio()
        if sandbox.status == "running":
            log.info("reusing sandbox %s", sandbox.id)
            return sandbox
        await spin_down(sandbox)
    sandbox = await client().sandboxes.create.aio(
        name=name,
        image_id=cfg.image_id,
        vcpu_count=vcpus,
        mem_size_mib=mem_mib,
        mounts=mounts,
        network=network,
        # The worker runs as a service with no connection attached, so idle
        # expiry is off; the workflow deletes the sandbox when its work is done.
        # The max TTL pauses it if nobody ever does (say, the workflow was
        # terminated): a paused sandbox uses no CPU.
        idle_ttl_seconds=0,
        max_ttl_seconds=cfg.sandbox_max_ttl_s,
    )
    log.info("created sandbox %s", sandbox.id)
    return sandbox


async def ensure_service(sandbox, name: str, command: str, env: dict[str, str]) -> None:
    """Run ``command`` as a supervised Archil service, which restarts it if it
    exits. Does nothing if the service already exists."""
    existing = await sandbox.exec.aio(f"archil services get {shlex.quote(name)} >/dev/null 2>&1 && echo yes || echo no")
    if existing.stdout.strip() == "yes":
        return
    env_args = " ".join(f"--env {shlex.quote(f'{k}={v}')}" for k, v in env.items())
    created = await sandbox.exec.aio(f"archil services create {shlex.quote(name)} {env_args} -- {command}")
    if created.exit_code != 0:
        raise RuntimeError(f"could not create service {name}: {created.stderr.strip()}")


async def service_state(sandbox, name: str) -> dict:
    result = await sandbox.exec.aio(f"archil services get {shlex.quote(name)}")
    return json.loads(result.stdout)["state"] if result.exit_code == 0 else {}
