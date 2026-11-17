"""Types passed between the workflow and its activities, and the names derived from them."""

from __future__ import annotations

import hashlib
import uuid
from dataclasses import dataclass, field
from typing import Optional

# The control worker (the workflow, and the activities that start and delete
# sandboxes) polls this queue. Each sandbox's worker polls a queue of its own.
CONTROL_QUEUE = "archil-control"

# Every task's agent writes rows in this shape, so the final step can union
# them without knowing anything about the sources. Change it to your
# warehouse table's schema.
OUTPUT_COLUMNS = {
    "source_url": "string",
    "entity": "string",
    "period": "string",
    "metric": "string",
    "value": "double",
}


@dataclass
class IngestRequest:
    urls: list[str]
    # How long work sent to a sandbox may wait for its worker before the
    # sandbox is presumed lost and a new one is started.
    sandbox_start_timeout_s: int = 300


@dataclass
class AgentTask:
    workflow_id: str
    key: str  # stable within the workflow, e.g. "t007"
    url: str


@dataclass
class TaskResult:
    key: str
    disk: str
    summary: str = ""
    cost_usd: Optional[float] = None
    # Where and how it ran, for the end-to-end checks.
    sandbox_id: str = ""
    attempt: int = 1
    resumed: bool = False


@dataclass
class Mount:
    disk: str  # disk name; created on first use
    path: str
    read_only: bool = False


@dataclass
class SandboxRequest:
    """A sandbox that runs a Temporal worker for one piece of work."""

    name: str
    mounts: list[Mount]
    vcpus: int = 2
    mem_mib: int = 4096
    # Hosts the sandbox may reach besides Temporal, or None for no egress
    # restriction. "api.anthropic.com" gets the Anthropic key added outside
    # the VM.
    allow_hosts: Optional[list[str]] = None


@dataclass
class SandboxWorker:
    sandbox_id: str
    task_queue: str


@dataclass
class AssembleInput:
    workflow_id: str
    tasks: list[TaskResult]


@dataclass
class AssembleOutput:
    rows: int
    path: str  # relative to the output disk's root


@dataclass
class ShareRequest:
    disk: str
    path: str


@dataclass
class AssembleResult:
    disk: str
    rows: int
    download_url: str


@dataclass
class CleanupInput:
    names: list[str]


@dataclass
class IngestResult:
    merged: Optional[AssembleResult]
    succeeded: list[TaskResult] = field(default_factory=list)
    failed: dict[str, str] = field(default_factory=dict)


def plan_tasks(workflow_id: str, urls: list[str]) -> list[AgentTask]:
    return [AgentTask(workflow_id=workflow_id, key=f"t{i:03d}", url=url) for i, url in enumerate(urls)]


# Names are derived from the workflow ID and task key, so every attempt, on any
# worker, finds the same disk, sandbox, task queue, and Claude session.


def _workflow_hash(workflow_id: str) -> str:
    return hashlib.sha256(workflow_id.encode()).hexdigest()[:12]


def task_name(task: AgentTask) -> str:
    """The task's disk and sandbox name."""
    return f"agent-{_workflow_hash(task.workflow_id)}-{task.key}"


def merged_name(workflow_id: str) -> str:
    """The merge step's disk and sandbox name."""
    return f"agent-{_workflow_hash(workflow_id)}-merged"


def all_names(workflow_id: str, urls: list[str]) -> list[str]:
    return [task_name(t) for t in plan_tasks(workflow_id, urls)] + [merged_name(workflow_id)]


def sandbox_queue(name: str) -> str:
    return f"sandbox-{name}"


def session_id(task: AgentTask) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"archil-temporal/{task.workflow_id}/{task.key}"))
