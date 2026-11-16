from __future__ import annotations

import asyncio
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError
from temporalio.workflow import ActivityCancellationType

with workflow.unsafe.imports_passed_through():
    from .activities import assemble, clean_up, run_agent_task
    from .shared import AgentTask, AssembleInput, CleanupInput, IngestRequest, IngestResult, plan_tasks


@workflow.defn
class IngestWorkflow:
    """Fan out one Claude agent per URL, each in its own Archil sandbox, then merge."""

    @workflow.run
    async def run(self, req: IngestRequest) -> IngestResult:
        wf_id = workflow.info().workflow_id
        tasks = plan_tasks(wf_id, req.urls)
        try:
            return await self._ingest(wf_id, tasks)
        finally:
            # Runs whether the workflow succeeds, fails, or is cancelled, and
            # deletes any sandbox an activity couldn't (for example, one whose
            # retry was still waiting to start when the workflow was cancelled).
            # Terminating a workflow skips this; see the README.
            await workflow.execute_activity(
                clean_up,
                CleanupInput(workflow_id=wf_id, keys=[t.key for t in tasks]),
                start_to_close_timeout=timedelta(minutes=5),
            )

    async def _ingest(self, wf_id: str, tasks: list[AgentTask]) -> IngestResult:
        outcomes = await asyncio.gather(
            *[
                workflow.execute_activity(
                    run_agent_task,
                    task,
                    # The whole budget for the task, across every attempt. There
                    # is no attempt limit: a deploy or a dropped connection is
                    # an attempt too, and must never fail the task. The activity
                    # itself gives up after a few genuine agent failures.
                    schedule_to_close_timeout=timedelta(hours=6),
                    # A worker that dies stops heartbeating; after this long the
                    # task is retried elsewhere and reattaches to the sandbox.
                    heartbeat_timeout=timedelta(minutes=1),
                    retry_policy=RetryPolicy(
                        initial_interval=timedelta(seconds=5),
                        maximum_interval=timedelta(minutes=1),
                        non_retryable_error_types=["AgentGaveUp"],
                    ),
                    # On cancel, wait for the activity to delete its sandbox.
                    cancellation_type=ActivityCancellationType.WAIT_CANCELLATION_COMPLETED,
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
            result.merged = await workflow.execute_activity(
                assemble,
                AssembleInput(workflow_id=wf_id, tasks=result.succeeded),
                schedule_to_close_timeout=timedelta(hours=2),
                heartbeat_timeout=timedelta(minutes=1),
                retry_policy=RetryPolicy(maximum_interval=timedelta(minutes=1)),
                cancellation_type=ActivityCancellationType.WAIT_CANCELLATION_COMPLETED,
            )
        return result
