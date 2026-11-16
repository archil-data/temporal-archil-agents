"""IngestWorkflow's orchestration, with the two activities replaced by stubs.

Runs against Temporal's time-skipping test server; needs no credentials.
"""

from __future__ import annotations

import uuid

from temporalio import activity
from temporalio.exceptions import ApplicationError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from archil_temporal.shared import AgentTask, AssembleInput, AssembleResult, IngestRequest, TaskResult
from archil_temporal.workflows import IngestWorkflow


async def test_retries_detached_tasks_skips_gave_up_and_merges_the_rest():
    attempts: dict[str, int] = {}
    merged_keys: list[str] = []

    @activity.defn(name="run_agent_task")
    async def run_agent_task(task: AgentTask) -> TaskResult:
        attempts[task.key] = attempts.get(task.key, 0) + 1
        if task.url == "impossible":
            raise ApplicationError("not tabular", type="AgentGaveUp", non_retryable=True)
        if task.url == "interrupted" and attempts[task.key] == 1:
            raise ApplicationError("worker shutting down", type="WorkerShutdown")
        return TaskResult(key=task.key, disk_id=f"dsk-{task.key}")

    @activity.defn(name="assemble")
    async def assemble(inp: AssembleInput) -> AssembleResult:
        merged_keys.extend(t.key for t in inp.tasks)
        return AssembleResult(disk_id="dsk-merged", rows=42, download_url="https://example.invalid/merged.parquet")

    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with Worker(env.client, task_queue="test", workflows=[IngestWorkflow], activities=[run_agent_task, assemble]):
            result = await env.client.execute_workflow(
                IngestWorkflow.run,
                IngestRequest(urls=["ok", "interrupted", "impossible"]),
                id=f"test-{uuid.uuid4()}",
                task_queue="test",
            )

    assert attempts == {"t000": 1, "t001": 2, "t002": 1}
    assert [t.key for t in result.succeeded] == ["t000", "t001"]
    assert list(result.failed) == ["t002"] and "AgentGaveUp" in result.failed["t002"]
    assert merged_keys == ["t000", "t001"]
    assert result.merged is not None and result.merged.rows == 42
