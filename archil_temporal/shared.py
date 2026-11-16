"""Types passed between the workflow and its activities, and the names derived from them."""

from __future__ import annotations

import hashlib
import uuid
from dataclasses import dataclass, field
from typing import Optional

TASK_QUEUE = "archil-agents"

# Every task's agent writes rows in this shape, so the final step can union
# them without knowing anything about the sources. Change it to your
# ClickHouse table's schema.
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


@dataclass
class AgentTask:
    workflow_id: str
    key: str  # stable within the workflow, e.g. "t007"
    url: str


@dataclass
class TaskResult:
    key: str
    disk_id: str
    summary: str = ""
    cost_usd: Optional[float] = None


@dataclass
class AssembleInput:
    workflow_id: str
    tasks: list[TaskResult]


@dataclass
class AssembleResult:
    disk_id: str
    rows: int
    download_url: str


@dataclass
class IngestResult:
    merged: Optional[AssembleResult]
    succeeded: list[TaskResult] = field(default_factory=list)
    failed: dict[str, str] = field(default_factory=dict)


# Names are derived from the workflow ID and task key, so a retried activity
# on any worker finds the same disk, sandbox, and Claude session.


def _workflow_hash(workflow_id: str) -> str:
    return hashlib.sha256(workflow_id.encode()).hexdigest()[:12]


def task_disk_name(task: AgentTask) -> str:
    return f"agent-{_workflow_hash(task.workflow_id)}-{task.key}"


def merged_disk_name(workflow_id: str) -> str:
    return f"agent-{_workflow_hash(workflow_id)}-merged"


def session_id(task: AgentTask) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"archil-temporal/{task.workflow_id}/{task.key}"))
