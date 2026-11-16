"""Delete what a workflow left behind:

    python -m archil_temporal.cleanup <workflow-id> [--disks]

Deletes any sandboxes the workflow's tasks still have (the workflow does this
itself unless it was terminated), and with --disks, the task and merged disks
too. Reads the workflow's URLs from its Temporal history.
"""

from __future__ import annotations

import asyncio
import sys

from temporalio.client import Client

from . import config
from .activities import clean_up_workflow
from .shared import IngestRequest, plan_tasks


async def main() -> None:
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    if len(args) != 1:
        raise SystemExit(__doc__)
    workflow_id = args[0]
    cfg = config.load(require_image=False)
    client = await Client.connect(cfg.temporal_address, namespace=cfg.temporal_namespace)
    history = await client.get_workflow_handle(workflow_id).fetch_history()
    started = history.events[0].workflow_execution_started_event_attributes
    [req] = await client.data_converter.decode(started.input.payloads, [IngestRequest])
    keys = [t.key for t in plan_tasks(workflow_id, req.urls)]
    deleted = await clean_up_workflow(workflow_id, keys, delete_disks="--disks" in sys.argv)
    print(f"deleted {deleted} sandbox(es)" + (" and the workflow's disks" if "--disks" in sys.argv else ""))


if __name__ == "__main__":
    asyncio.run(main())
