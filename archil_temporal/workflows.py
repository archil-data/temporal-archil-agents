from __future__ import annotations

import asyncio
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError

with workflow.unsafe.imports_passed_through():
    from .activities import assemble, run_agent_task
    from .shared import AgentTask, AssembleInput, IngestRequest, IngestResult


@workflow.defn
class IngestWorkflow:
    """Fan out one Claude agent per URL, each in its own Archil sandbox, then merge."""

    @workflow.run
    async def run(self, req: IngestRequest) -> IngestResult:
        wf_id = workflow.info().workflow_id
        tasks = [AgentTask(workflow_id=wf_id, key=f"t{i:03d}", url=url) for i, url in enumerate(req.urls)]

        outcomes = await asyncio.gather(
            *[
                workflow.execute_activity(
                    run_agent_task,
                    task,
                    # Total budget across all attempts; the sandbox's own max TTL
                    # (SANDBOX_MAX_TTL_SECONDS) should match it.
                    schedule_to_close_timeout=timedelta(hours=6),
                    # A worker that dies stops heartbeating; after this long the
                    # task is retried elsewhere and reattaches to the sandbox.
                    heartbeat_timeout=timedelta(minutes=1),
                    retry_policy=RetryPolicy(
                        initial_interval=timedelta(seconds=5),
                        maximum_attempts=10,
                        non_retryable_error_types=["AgentGaveUp"],
                    ),
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
            )
        return result
