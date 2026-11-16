"""Settings read from the environment. See .env.example."""

from __future__ import annotations

import os
from dataclasses import dataclass


def _flag(name: str, default: bool) -> bool:
    return os.environ.get(name, str(default)).strip().lower() in ("1", "true", "yes", "on")


@dataclass(frozen=True)
class Config:
    temporal_address: str
    temporal_namespace: str
    # Exactly one of these chooses the sandbox image. image_id comes from
    # `python -m archil_temporal.build_image`; base_image is any public OCI
    # reference and is only useful for trying the plumbing out.
    image_id: str | None
    base_image: str | None
    anthropic_api_key: str
    agent_model: str
    agent_vcpus: int
    agent_mem_mib: int
    # How many times an agent may fail (crash, invalid output) before its task
    # is given up on. Worker shutdowns and dropped connections don't count.
    max_agent_failures: int
    # Deny the agent's sandbox all outbound traffic except the Anthropic API
    # (with the API key added outside the VM) and the host of its URL. Needs a
    # paid Archil plan; set RESTRICT_EGRESS=false on a free one.
    restrict_egress: bool
    # Extra hosts agents may reach when egress is restricted.
    extra_allowed_hosts: tuple[str, ...]
    # Pause a sandbox that has been running this long without stopping. Set
    # it to at least the activity's schedule_to_close_timeout.
    sandbox_max_ttl_s: int
    # How long a sandbox keeps running with no worker attached before Archil
    # pauses it. Long enough for Temporal to retry onto another worker, short
    # enough that an abandoned sandbox doesn't keep using CPU.
    sandbox_orphan_ttl_s: int
    max_concurrent_activities: int


def load(require_image: bool = True) -> Config:
    image_id = os.environ.get("ARCHIL_IMAGE_ID") or None
    base_image = os.environ.get("AGENT_BASE_IMAGE") or None
    if require_image and not image_id and not base_image:
        raise SystemExit("Set ARCHIL_IMAGE_ID (see README: Build the image) or AGENT_BASE_IMAGE")
    return Config(
        temporal_address=os.environ.get("TEMPORAL_ADDRESS", "localhost:7233"),
        temporal_namespace=os.environ.get("TEMPORAL_NAMESPACE", "default"),
        image_id=image_id,
        base_image=None if image_id else base_image,
        anthropic_api_key=os.environ.get("ANTHROPIC_API_KEY", ""),
        agent_model=os.environ.get("AGENT_MODEL", "claude-opus-5-5"),
        agent_vcpus=int(os.environ.get("AGENT_VCPUS", "2")),
        agent_mem_mib=int(os.environ.get("AGENT_MEM_MIB", "4096")),
        max_agent_failures=int(os.environ.get("MAX_AGENT_FAILURES", "3")),
        restrict_egress=_flag("RESTRICT_EGRESS", True),
        extra_allowed_hosts=tuple(
            h.strip().lower() for h in os.environ.get("AGENT_ALLOWED_HOSTS", "").split(",") if h.strip()
        ),
        sandbox_max_ttl_s=int(os.environ.get("SANDBOX_MAX_TTL_SECONDS", str(6 * 3600))),
        sandbox_orphan_ttl_s=int(os.environ.get("SANDBOX_ORPHAN_TTL_SECONDS", "900")),
        max_concurrent_activities=int(os.environ.get("MAX_CONCURRENT_ACTIVITIES", "500")),
    )
