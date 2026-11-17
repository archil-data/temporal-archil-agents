"""IngestWorkflow's orchestration, with the activities replaced by stubs: a
control worker that "starts sandboxes", and a worker standing in for the one
inside each sandbox.

Runs against Temporal's time-skipping test server; needs no credentials.
"""

from __future__ import annotations

import asyncio
import uuid
from collections import Counter

import pytest
from temporalio import activity
from temporalio.client import WorkflowFailureError
from temporalio.exceptions import ApplicationError, CancelledError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from archil_temporal.shared import (
    CONTROL_QUEUE,
    AgentTask,
    AssembleInput,
    AssembleOutput,
    CleanupInput,
    IngestRequest,
    SandboxRequest,
    SandboxWorker,
    ShareRequest,
    TaskResult,
    all_names,
    plan_tasks,
    task_name,
)
from archil_temporal.workflows import IngestWorkflow

SANDBOX_QUEUE = "sandbox-stub"


class Stubs:
    def __init__(self, workflow_id: str, urls: list[str]) -> None:
        self.url_of = {task_name(t): t.url for t in plan_tasks(workflow_id, urls)}
        self.launches: Counter[str] = Counter()
        self.attempts: Counter[str] = Counter()
        self.stopped: list[str] = []
        self.cleaned: list[list[str]] = []
        self.merged_keys: list[str] = []
        self.agent_started = asyncio.Event()

        @activity.defn(name="start_sandbox")
        async def start_sandbox(req: SandboxRequest) -> SandboxWorker:
            self.launches[req.name] += 1
            if self.url_of.get(req.name) == "lost" and self.launches[req.name] == 1:
                # A sandbox whose worker never picks anything up.
                return SandboxWorker(sandbox_id="sbx-lost", task_queue="nobody-polls-this")
            return SandboxWorker(sandbox_id=f"sbx-{req.name}", task_queue=SANDBOX_QUEUE)

        @activity.defn(name="stop_sandbox")
        async def stop_sandbox(name: str) -> int:
            self.stopped.append(name)
            return 1

        @activity.defn(name="share_file")
        async def share_file(req: ShareRequest) -> str:
            return f"https://example.invalid/{req.disk}/{req.path}"

        @activity.defn(name="clean_up")
        async def clean_up(inp: CleanupInput) -> int:
            self.cleaned.append(inp.names)
            return 0

        @activity.defn(name="run_agent_task")
        async def run_agent_task(task: AgentTask) -> TaskResult:
            self.attempts[task.key] += 1
            if task.url == "impossible":
                raise ApplicationError("not tabular", type="AgentGaveUp", non_retryable=True)
            if task.url == "flaky" and self.attempts[task.key] == 1:
                raise ApplicationError("invalid output", type="AgentFailed")
            if task.url == "slow":
                self.agent_started.set()
                while True:
                    activity.heartbeat()
                    await asyncio.sleep(0.1)
            return TaskResult(key=task.key, disk=task_name(task))

        @activity.defn(name="assemble")
        async def assemble(inp: AssembleInput) -> AssembleOutput:
            self.merged_keys.extend(t.key for t in inp.tasks)
            return AssembleOutput(rows=42, path="merged.parquet")

        self.control = [start_sandbox, stop_sandbox, share_file, clean_up]
        self.sandbox = [run_agent_task, assemble]


def workers(env, stubs: Stubs) -> tuple[Worker, Worker]:
    control = Worker(env.client, task_queue=CONTROL_QUEUE, workflows=[IngestWorkflow], activities=stubs.control)
    sandbox = Worker(env.client, task_queue=SANDBOX_QUEUE, activities=stubs.sandbox)
    return control, sandbox


async def test_each_task_runs_in_its_own_sandbox_which_is_deleted_after():
    urls = ["ok", "flaky", "impossible", "lost"]
    workflow_id = f"test-{uuid.uuid4()}"
    stubs = Stubs(workflow_id, urls)
    async with await WorkflowEnvironment.start_time_skipping() as env:
        control, sandbox = workers(env, stubs)
        async with control, sandbox:
            result = await env.client.execute_workflow(
                IngestWorkflow.run,
                IngestRequest(urls=urls, sandbox_start_timeout_s=3),
                id=workflow_id,
                task_queue=CONTROL_QUEUE,
            )

    names = all_names(workflow_id, urls)
    tasks = names[:-1]
    # One sandbox per task (and one for the merge); the lost one was relaunched.
    assert stubs.launches == {**{n: 1 for n in names}, tasks[3]: 2}
    # An agent failure is retried in the same sandbox; giving up is not retried.
    assert stubs.attempts == {"t000": 1, "t001": 2, "t002": 1, "t003": 1}
    assert [t.key for t in result.succeeded] == ["t000", "t001", "t003"]
    assert list(result.failed) == ["t002"] and "AgentGaveUp" in result.failed["t002"]
    assert stubs.merged_keys == ["t000", "t001", "t003"]
    assert result.merged is not None and result.merged.rows == 42
    # Every sandbox was deleted when its work was done, then the cleanup ran.
    assert sorted(stubs.stopped) == sorted(names)
    assert stubs.cleaned == [names]


async def test_cancelling_the_workflow_deletes_the_running_sandbox():
    urls = ["slow", "ok"]
    workflow_id = f"test-{uuid.uuid4()}"
    stubs = Stubs(workflow_id, urls)
    async with await WorkflowEnvironment.start_time_skipping() as env:
        control, sandbox = workers(env, stubs)
        async with control, sandbox:
            handle = await env.client.start_workflow(
                IngestWorkflow.run, IngestRequest(urls=urls), id=workflow_id, task_queue=CONTROL_QUEUE
            )
            await asyncio.wait_for(stubs.agent_started.wait(), timeout=30)
            await handle.cancel()
            with pytest.raises(WorkflowFailureError) as failure:
                await handle.result()

    assert isinstance(failure.value.cause, CancelledError)
    names = all_names(workflow_id, urls)
    assert sorted(stubs.stopped) == sorted(names[:2])
    assert stubs.cleaned == [names]
