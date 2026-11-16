"""IngestWorkflow's orchestration, with the activities replaced by stubs.

Runs against Temporal's time-skipping test server; needs no credentials.
"""

from __future__ import annotations

import asyncio
import uuid

import pytest
from temporalio import activity
from temporalio.client import WorkflowFailureError
from temporalio.exceptions import ApplicationError, CancelledError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from archil_temporal.shared import AgentTask, AssembleInput, AssembleResult, CleanupInput, IngestRequest, TaskResult
from archil_temporal.workflows import IngestWorkflow


class Stubs:
    def __init__(self) -> None:
        self.attempts: dict[str, int] = {}
        self.merged_keys: list[str] = []
        self.cleaned: list[list[str]] = []
        self.agent_started = asyncio.Event()

        @activity.defn(name="run_agent_task")
        async def run_agent_task(task: AgentTask) -> TaskResult:
            self.attempts[task.key] = self.attempts.get(task.key, 0) + 1
            if task.url == "impossible":
                raise ApplicationError("not tabular", type="AgentGaveUp", non_retryable=True)
            if task.url in ("deploy-storm", "interrupted"):
                # Detaches use up attempts; there must be no limit on them.
                if self.attempts[task.key] <= (25 if task.url == "deploy-storm" else 1):
                    raise ApplicationError("worker shutting down", type="WorkerShutdown")
            if task.url == "slow":
                self.agent_started.set()
                while True:
                    activity.heartbeat()
                    await asyncio.sleep(0.1)
            return TaskResult(key=task.key, disk_id=f"dsk-{task.key}")

        @activity.defn(name="assemble")
        async def assemble(inp: AssembleInput) -> AssembleResult:
            self.merged_keys.extend(t.key for t in inp.tasks)
            return AssembleResult(disk_id="dsk-merged", rows=42, download_url="https://example.invalid/merged.parquet")

        @activity.defn(name="clean_up")
        async def clean_up(inp: CleanupInput) -> int:
            self.cleaned.append(inp.keys)
            return 0

        self.activities = [run_agent_task, assemble, clean_up]


async def test_retries_detached_tasks_skips_gave_up_and_merges_the_rest():
    stubs = Stubs()
    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with Worker(env.client, task_queue="test", workflows=[IngestWorkflow], activities=stubs.activities):
            result = await env.client.execute_workflow(
                IngestWorkflow.run,
                IngestRequest(urls=["ok", "interrupted", "impossible", "deploy-storm"]),
                id=f"test-{uuid.uuid4()}",
                task_queue="test",
            )

    assert stubs.attempts == {"t000": 1, "t001": 2, "t002": 1, "t003": 26}
    assert [t.key for t in result.succeeded] == ["t000", "t001", "t003"]
    assert list(result.failed) == ["t002"] and "AgentGaveUp" in result.failed["t002"]
    assert stubs.merged_keys == ["t000", "t001", "t003"]
    assert result.merged is not None and result.merged.rows == 42
    assert stubs.cleaned == [["t000", "t001", "t002", "t003"]]


async def test_cancelling_the_workflow_still_cleans_up():
    stubs = Stubs()
    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with Worker(env.client, task_queue="test", workflows=[IngestWorkflow], activities=stubs.activities):
            handle = await env.client.start_workflow(
                IngestWorkflow.run, IngestRequest(urls=["slow", "ok"]), id=f"test-{uuid.uuid4()}", task_queue="test"
            )
            await asyncio.wait_for(stubs.agent_started.wait(), timeout=30)
            await handle.cancel()
            with pytest.raises(WorkflowFailureError) as failure:
                await handle.result()

    assert isinstance(failure.value.cause, CancelledError)
    assert stubs.cleaned == [["t000", "t001"]]
