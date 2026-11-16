"""Start an ingest workflow: python -m archil_temporal.starter [URL ...]"""

from __future__ import annotations

import asyncio
import dataclasses
import json
import sys
import uuid

from temporalio.client import Client

from . import config
from .shared import TASK_QUEUE, IngestRequest, IngestResult
from .workflows import IngestWorkflow

DEMO_URLS = [
    "https://raw.githubusercontent.com/datasets/gdp/main/data/gdp.csv",
    "https://raw.githubusercontent.com/datasets/population/main/data/population.csv",
    "https://raw.githubusercontent.com/datasets/co2-ppm/main/data/co2-mm-mlo.csv",
]


async def main() -> None:
    cfg = config.load()
    urls = sys.argv[1:] or DEMO_URLS
    client = await Client.connect(cfg.temporal_address, namespace=cfg.temporal_namespace)
    handle = await client.start_workflow(
        IngestWorkflow.run,
        IngestRequest(urls=urls),
        id=f"ingest-{uuid.uuid4().hex[:12]}",
        task_queue=TASK_QUEUE,
    )
    print(f"started {handle.id} with {len(urls)} task(s); waiting for the result...")
    result: IngestResult = await handle.result()
    print(json.dumps(dataclasses.asdict(result), indent=2))


if __name__ == "__main__":
    asyncio.run(main())
