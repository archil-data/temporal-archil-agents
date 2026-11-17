"""The Temporal worker that runs inside each sandbox.

It polls a task queue that belongs to this sandbox alone, so the work the
workflow sends there runs here, next to the sandbox's disks. It runs as an
Archil service, which restarts it if it exits; the workflow deletes the
sandbox when the work is done.
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal

from temporalio.worker import Worker

from . import config
from .agent import assemble, run_agent_task


async def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    cfg = config.load()
    sandbox_id = os.environ["SANDBOX_ID"]
    # The control activity that started this sandbox waits for a poller with
    # this identity before sending work here.
    client = await config.connect(cfg, identity=f"sandbox:{sandbox_id}")
    worker = Worker(
        client,
        task_queue=os.environ["SANDBOX_TASK_QUEUE"],
        activities=[run_agent_task, assemble],
        max_concurrent_activities=1,
    )

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    async with worker:
        logging.info("worker for sandbox %s polling %s", sandbox_id, os.environ["SANDBOX_TASK_QUEUE"])
        await stop.wait()


if __name__ == "__main__":
    asyncio.run(main())
