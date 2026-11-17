"""Delete what a workflow left behind:

    python -m archil_temporal.cleanup <workflow-id> [--disks]

Deletes any sandboxes the workflow still has (the workflow does this itself
unless it was terminated), and with --disks, its task and merged disks too.
Reads the workflow's URLs from its Temporal history.
"""

from __future__ import annotations

import asyncio
import sys

from . import config
from .sandboxes import delete_sandboxes, find_disk
from .shared import IngestRequest, all_names


async def clean_up_workflow(workflow_id: str, urls: list[str], *, delete_disks: bool = False) -> int:
    deleted = 0
    for name in all_names(workflow_id, urls):
        deleted += await delete_sandboxes(name)
        if delete_disks and (disk := await find_disk(name)) is not None:
            await disk.delete.aio()
    return deleted


async def main() -> None:
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    if len(args) != 1:
        raise SystemExit(__doc__)
    workflow_id = args[0]
    client = await config.connect(config.load())
    history = await client.get_workflow_handle(workflow_id).fetch_history()
    started = history.events[0].workflow_execution_started_event_attributes
    [req] = await client.data_converter.decode(started.input.payloads, [IngestRequest])
    disks = "--disks" in sys.argv
    deleted = await clean_up_workflow(workflow_id, req.urls, delete_disks=disks)
    print(f"deleted {deleted} sandbox(es)" + (" and the workflow's disks" if disks else ""))


if __name__ == "__main__":
    asyncio.run(main())
