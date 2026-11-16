"""A thin Temporal worker. It runs no agent code itself: each activity only
supervises a sandbox over a WebSocket, so one small pod can drive hundreds of
concurrent agents and can be replaced at any time without losing their work."""

from __future__ import annotations

import asyncio
import logging
import signal
from datetime import timedelta

from temporalio.client import Client
from temporalio.worker import Worker

from . import config
from .activities import assemble, run_agent_task
from .shared import TASK_QUEUE
from .workflows import IngestWorkflow


async def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    cfg = config.load()
    client = await Client.connect(cfg.temporal_address, namespace=cfg.temporal_namespace)
    worker = Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[IngestWorkflow],
        activities=[run_agent_task, assemble],
        max_concurrent_activities=cfg.max_concurrent_activities,
        # Activities detach from their sandboxes as soon as shutdown starts, so
        # this only needs to cover the detach itself.
        graceful_shutdown_timeout=timedelta(seconds=30),
    )

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    async with worker:
        logging.info("worker running on task queue %r", TASK_QUEUE)
        await stop.wait()
        logging.info("shutting down; in-flight sandboxes keep running for the next worker")


if __name__ == "__main__":
    asyncio.run(main())
