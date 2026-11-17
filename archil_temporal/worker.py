"""The control worker: runs the workflow and the activities that start and
delete sandboxes. The agents themselves run on workers inside the sandboxes
(sandbox_worker.py), so this worker holds no long-running work and can be
deployed, scaled, or restarted at any time."""

from __future__ import annotations

import asyncio
import logging
import signal

from temporalio.worker import Worker

from . import config
from .control import ControlActivities
from .shared import CONTROL_QUEUE
from .workflows import IngestWorkflow


async def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    cfg = config.load()
    if not cfg.image_id:
        raise SystemExit("Set ARCHIL_IMAGE_ID (see README: Build the image)")
    client = await config.connect(cfg)
    control = ControlActivities(client, cfg)
    worker = Worker(
        client,
        task_queue=CONTROL_QUEUE,
        workflows=[IngestWorkflow],
        activities=[control.start_sandbox, control.stop_sandbox, control.share_file, control.clean_up],
        max_concurrent_activities=cfg.max_concurrent_activities,
    )

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    async with worker:
        logging.info("control worker running on task queue %r", CONTROL_QUEUE)
        await stop.wait()
        logging.info("shutting down; agents keep running in their sandboxes")


if __name__ == "__main__":
    asyncio.run(main())
