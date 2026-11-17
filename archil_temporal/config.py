"""Settings read from the environment (see .env.example), and the Temporal connection."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional

from temporalio.client import Client


def _flag(name: str, default: bool) -> bool:
    return os.environ.get(name, str(default)).strip().lower() in ("1", "true", "yes", "on")


@dataclass(frozen=True)
class Config:
    temporal_address: str
    temporal_namespace: str
    # Temporal Cloud API key; implies TLS.
    temporal_api_key: Optional[str]
    temporal_tls: bool
    # For testing against a Temporal server in an Archil sandbox: its exposed
    # port's hostname. Connections go through a local TLS tunnel (see tunnel.py).
    temporal_tunnel_host: Optional[str]
    # The sandbox image: image_id from `python -m archil_temporal.build_image`.
    image_id: Optional[str]
    anthropic_api_key: str
    agent_model: str
    # Deny agents all outbound traffic except Temporal, the Anthropic API (with
    # the API key added outside the VM), and the host of the agent's URL.
    # Needs a paid Archil plan; set RESTRICT_EGRESS=false on a free one.
    restrict_egress: bool
    extra_allowed_hosts: tuple[str, ...]
    # Pause a sandbox that has been running this long. A backstop for
    # sandboxes whose workflow was terminated; set it to at least your
    # longest task.
    sandbox_max_ttl_s: int
    max_concurrent_activities: int

    @property
    def temporal_host(self) -> str:
        """The host a sandbox's worker connects to, for its egress policy."""
        return self.temporal_tunnel_host or self.temporal_address.rsplit(":", 1)[0]


def load() -> Config:
    tls = _flag("TEMPORAL_TLS", False) or bool(os.environ.get("TEMPORAL_API_KEY"))
    return Config(
        temporal_address=os.environ.get("TEMPORAL_ADDRESS", "localhost:7233"),
        temporal_namespace=os.environ.get("TEMPORAL_NAMESPACE", "default"),
        temporal_api_key=os.environ.get("TEMPORAL_API_KEY") or None,
        temporal_tls=tls,
        temporal_tunnel_host=os.environ.get("TEMPORAL_TUNNEL_HOST") or None,
        image_id=os.environ.get("ARCHIL_IMAGE_ID") or None,
        anthropic_api_key=os.environ.get("ANTHROPIC_API_KEY", ""),
        agent_model=os.environ.get("AGENT_MODEL", "claude-opus-5-5"),
        restrict_egress=_flag("RESTRICT_EGRESS", True),
        extra_allowed_hosts=tuple(
            h.strip().lower() for h in os.environ.get("AGENT_ALLOWED_HOSTS", "").split(",") if h.strip()
        ),
        sandbox_max_ttl_s=int(os.environ.get("SANDBOX_MAX_TTL_SECONDS", str(6 * 3600))),
        max_concurrent_activities=int(os.environ.get("MAX_CONCURRENT_ACTIVITIES", "100")),
    )


def temporal_env(cfg: Config) -> dict[str, str]:
    """What a sandbox's worker needs to connect to the same Temporal."""
    env = {"TEMPORAL_ADDRESS": cfg.temporal_address, "TEMPORAL_NAMESPACE": cfg.temporal_namespace}
    if cfg.temporal_tls:
        env["TEMPORAL_TLS"] = "true"
    if cfg.temporal_api_key:
        env["TEMPORAL_API_KEY"] = cfg.temporal_api_key
    if cfg.temporal_tunnel_host:
        env["TEMPORAL_TUNNEL_HOST"] = cfg.temporal_tunnel_host
    return env


async def connect(cfg: Config, identity: Optional[str] = None) -> Client:
    extra = {"identity": identity} if identity else {}
    if cfg.temporal_tunnel_host:
        from .tunnel import open_tunnel

        address = await open_tunnel(cfg.temporal_tunnel_host)
        return await Client.connect(address, namespace=cfg.temporal_namespace, **extra)
    return await Client.connect(
        cfg.temporal_address,
        namespace=cfg.temporal_namespace,
        api_key=cfg.temporal_api_key,
        tls=cfg.temporal_tls,
        **extra,
    )
