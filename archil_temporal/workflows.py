from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Any, Awaitable, Callable

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError, TimeoutError, TimeoutType
from temporalio.workflow import ActivityCancellationType

with workflow.unsafe.imports_passed_through():
    from .control import ControlActivities
    from .shared import (
        CONTROL_QUEUE,
        AgentTask,
        AssembleInput,
        AssembleOutput,
        AssembleResult,
        CleanupInput,
        IngestRequest,
        IngestResult,
        Mount,
        SandboxRequest,
        ShareRequest,
        TaskResult,
        all_names,
        merged_name,
        plan_tasks,
        task_name,
    )

# How many times to start a fresh sandbox for one piece of work if its sandbox
# is lost (no worker picks the work up). A crash of the worker process alone is
# handled by the sandbox, which restarts it.
MAX_SANDBOX_LAUNCHES = 3

_CONTROL = dict(
    task_queue=CONTROL_QUEUE,
    start_to_close_timeout=timedelta(minutes=10),
    heartbeat_timeout=timedelta(minutes=1),
)


def agent_sandbox(task: AgentTask) -> SandboxRequest:
    from urllib.parse import urlparse

    return SandboxRequest(
        name=task_name(task),
        mounts=[Mount(disk=task_name(task), path="/mnt/archil")],
        vcpus=2,
        mem_mib=4096,
        # The agent can reach the Anthropic API and the host of its URL, and
        # nothing else (besides Temporal).
        allow_hosts=["api.anthropic.com", urlparse(task.url).hostname or ""],
    )


def assemble_sandbox(workflow_id: str, tasks: list[TaskResult]) -> SandboxRequest:
    # Every task disk read-only (several sandboxes can mount a disk read-only
    # at once), plus one writable output disk.
    return SandboxRequest(
        name=merged_name(workflow_id),
        mounts=[Mount(disk=merged_name(workflow_id), path="/mnt/out")]
        + [Mount(disk=t.disk, path=f"/mnt/tasks/{t.key}", read_only=True) for t in tasks],
        vcpus=4,
        mem_mib=8192,
        allow_hosts=[],
    )


async def _and_then(work: Awaitable[Any], cleanup: Callable[[], Awaitable[Any]]) -> Any:
    """Await ``work``, then ``cleanup``, also when ``work`` fails or is
    cancelled. Unlike ``finally``, it skips the cleanup when the worker tears
    the workflow down (on shutdown, Python closes its coroutines): the workflow
    isn't over, and another worker carries on with it."""
    try:
        result = await work
    except (Exception, asyncio.CancelledError):
        await cleanup()
        raise
    await cleanup()
    return result


def _sandbox_lost(e: ActivityError) -> bool:
    # Nothing polled the sandbox's queue: the sandbox itself is gone.
    return isinstance(e.cause, TimeoutError) and e.cause.type == TimeoutType.SCHEDULE_TO_START


@workflow.defn
class IngestWorkflow:
    """Run one Claude agent per URL, each in its own Archil sandbox, then merge."""

    @workflow.run
    async def run(self, req: IngestRequest) -> IngestResult:
        wf_id = workflow.info().workflow_id
        self.start_timeout = timedelta(seconds=req.sandbox_start_timeout_s)
        # Whether the workflow succeeds, fails, or is cancelled, finish by
        # deleting any sandbox that is somehow still there.
        return await _and_then(
            self._ingest(wf_id, plan_tasks(wf_id, req.urls)),
            lambda: workflow.execute_activity_method(
                ControlActivities.clean_up, CleanupInput(names=all_names(wf_id, req.urls)), **_CONTROL
            ),
        )

    async def _ingest(self, wf_id: str, tasks: list[AgentTask]) -> IngestResult:
        outcomes = await asyncio.gather(
            *[
                self.in_sandbox(
                    agent_sandbox(task),
                    "run_agent_task",
                    task,
                    TaskResult,
                    timeout=timedelta(hours=6),
                    # An agent that crashes or writes invalid output gets two
                    # more tries, resuming its Claude session each time.
                    retry_policy=RetryPolicy(maximum_attempts=3, non_retryable_error_types=["AgentGaveUp"]),
                )
                for task in tasks
            ],
            return_exceptions=True,
        )

        result = IngestResult(merged=None)
        for task, outcome in zip(tasks, outcomes):
            if isinstance(outcome, ActivityError):
                result.failed[task.key] = str(outcome.cause or outcome)
            elif isinstance(outcome, BaseException):
                raise outcome
            else:
                result.succeeded.append(outcome)

        if result.succeeded:
            merged: AssembleOutput = await self.in_sandbox(
                assemble_sandbox(wf_id, result.succeeded),
                "assemble",
                AssembleInput(workflow_id=wf_id, tasks=result.succeeded),
                AssembleOutput,
                timeout=timedelta(hours=2),
                retry_policy=RetryPolicy(maximum_attempts=3, non_retryable_error_types=["AssembleFailed"]),
            )
            url = await workflow.execute_activity_method(
                ControlActivities.share_file, ShareRequest(disk=merged_name(wf_id), path=merged.path), **_CONTROL
            )
            result.merged = AssembleResult(disk=merged_name(wf_id), rows=merged.rows, download_url=url)
        return result

    async def in_sandbox(
        self, sandbox: SandboxRequest, activity: str, arg: Any, result_type: type, *, timeout: timedelta, retry_policy: RetryPolicy
    ) -> Any:
        """Run ``activity`` on a Temporal worker inside its own sandbox: start
        the sandbox, send the activity to the sandbox's task queue, then delete
        the sandbox once the activity has finished, failed, or been cancelled."""
        return await _and_then(
            self._run_in_sandbox(sandbox, activity, arg, result_type, timeout=timeout, retry_policy=retry_policy),
            lambda: workflow.execute_activity_method(ControlActivities.stop_sandbox, sandbox.name, **_CONTROL),
        )

    async def _run_in_sandbox(
        self, sandbox: SandboxRequest, activity: str, arg: Any, result_type: type, *, timeout: timedelta, retry_policy: RetryPolicy
    ) -> Any:
        for launch in range(1, MAX_SANDBOX_LAUNCHES + 1):
            worker = await workflow.execute_activity_method(ControlActivities.start_sandbox, sandbox, **_CONTROL)
            try:
                return await workflow.execute_activity(
                    activity,
                    arg,
                    result_type=result_type,
                    task_queue=worker.task_queue,
                    schedule_to_close_timeout=timeout,
                    # The worker in the sandbox picks the activity up at
                    # once; if nothing does for this long, the sandbox is gone.
                    schedule_to_start_timeout=self.start_timeout,
                    # Missed heartbeats mean the worker process died; the
                    # sandbox restarts it and the retry runs there.
                    heartbeat_timeout=timedelta(minutes=1),
                    retry_policy=retry_policy,
                    cancellation_type=ActivityCancellationType.WAIT_CANCELLATION_COMPLETED,
                )
            except ActivityError as e:
                if not _sandbox_lost(e) or launch == MAX_SANDBOX_LAUNCHES:
                    raise
                workflow.logger.info("sandbox %s was lost; starting a new one", worker.sandbox_id)
