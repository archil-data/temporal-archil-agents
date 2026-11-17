"""The activities that run inside the sandboxes: one Claude agent that turns one
URL into Parquet, and the merge of every agent's output.

They're ordinary Temporal activities, run by the worker in each sandbox (see
sandbox_worker.py), with the task's Archil disk mounted as a local filesystem.
Everything an agent needs to resume lives on that disk, including Claude Code's
session transcript, so a retry picks up the same conversation and files.
"""

from __future__ import annotations

import asyncio
import contextlib
import glob
import json
import os
from pathlib import Path

from temporalio import activity
from temporalio.exceptions import ApplicationError

from .shared import AgentTask, AssembleInput, AssembleOutput, OUTPUT_COLUMNS, TaskResult, session_id, task_name

DISK = Path("/mnt/archil")
WORK = DISK / "work"
OUT = DISK / "out"
CLAUDE_CONFIG_DIR = DISK / ".claude"
MAX_FIX_ROUNDS = 2

TASK_PROMPT = """\
Download {url} into {work}/raw/ and convert it into one or more Parquet files in {out}/
with exactly these columns, in this order: {columns}.

Use long format: one row per observation (entity, period, metric, value). source_url is always {url}.
Python with pandas, pyarrow and duckdb is installed. If a file is too large for memory, stream it with duckdb.

You may be interrupted and resumed at any point. Keep short progress notes in {work}/NOTES.md
so you can tell what is already done.

If the task cannot be done (the URL is gone, or it is not tabular data), write the reason to
{work}/GAVE_UP and stop."""

RESUME_PROMPT = f"""\
You were interrupted. Read {WORK}/NOTES.md and look at what is on disk, then finish the original task."""

FIX_PROMPT = "The output in {out}/ failed validation: {error}. Fix it."

_TYPES = {"string": {"string", "large_string"}, "double": {"double"}}


@contextlib.asynccontextmanager
async def heartbeating(every_s: float = 10):
    """Heartbeat while the body runs: it tells Temporal this worker is alive,
    and delivers cancellation if the workflow is cancelled."""

    async def beat():
        while True:
            activity.heartbeat()
            await asyncio.sleep(every_s)

    task = asyncio.create_task(beat())
    try:
        yield
    finally:
        task.cancel()


def _session_exists(sid: str) -> bool:
    # Claude Code stores each session as <config dir>/projects/<project>/<id>.jsonl.
    return any(CLAUDE_CONFIG_DIR.glob(f"projects/*/{sid}.jsonl"))


def _validate(columns: dict[str, str]) -> str | None:
    import pyarrow.parquet as pq

    files = sorted(OUT.glob("*.parquet"))
    if not files:
        return "there are no .parquet files"
    for f in files:
        schema = pq.read_schema(f)
        if schema.names != list(columns):
            return f"{f.name} has columns {schema.names}, expected {list(columns)}"
        for name, kind in columns.items():
            if str(schema.field(name).type) not in _TYPES[kind]:
                return f"{f.name}: column {name} is {schema.field(name).type}, expected {kind}"
    return None


async def _turn(prompt: str, options):
    from claude_agent_sdk import AssistantMessage, ResultMessage, TextBlock, query

    result = None
    async for message in query(prompt=prompt, options=options):
        if isinstance(message, AssistantMessage):
            for block in message.content:
                if isinstance(block, TextBlock):
                    activity.logger.info("%s", block.text)
        elif isinstance(message, ResultMessage):
            result = message
    if result is None:
        raise ApplicationError("agent produced no result", type="AgentFailed")
    if result.is_error:
        raise ApplicationError(f"agent error: {result.errors or result.subtype}", type="AgentFailed")
    return result


@activity.defn
async def run_agent_task(task: AgentTask) -> TaskResult:
    from claude_agent_sdk import ClaudeAgentOptions

    sid = session_id(task)
    for d in (WORK, OUT, CLAUDE_CONFIG_DIR):
        d.mkdir(parents=True, exist_ok=True)

    def options(**session) -> ClaudeAgentOptions:
        return ClaudeAgentOptions(
            cwd=str(WORK),
            model=os.environ.get("AGENT_MODEL"),
            permission_mode="bypassPermissions",
            max_turns=200,
            # IS_SANDBOX lets Claude Code skip permission prompts as root, which
            # is fine here: the microVM is the security boundary. With egress
            # restricted, only the Anthropic API is reachable, so turn off
            # Claude Code's other traffic (telemetry, update checks).
            env={
                "CLAUDE_CONFIG_DIR": str(CLAUDE_CONFIG_DIR),
                "IS_SANDBOX": "1",
                "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC": "1",
            },
            **session,
        )

    async with heartbeating():
        # A fixed session ID per task means any attempt can find, and resume,
        # the conversation an earlier attempt started.
        resumed = _session_exists(sid)
        if resumed:
            activity.logger.info("resuming Claude session %s", sid)
            results = [await _turn(RESUME_PROMPT, options(resume=sid))]
        else:
            prompt = TASK_PROMPT.format(url=task.url, work=WORK, out=OUT, columns=", ".join(OUTPUT_COLUMNS))
            results = [await _turn(prompt, options(session_id=sid))]

        for fix_round in range(MAX_FIX_ROUNDS + 1):
            if (WORK / "GAVE_UP").exists():
                # Retrying won't help: fail the task without retrying.
                raise ApplicationError(
                    f"agent gave up: {(WORK / 'GAVE_UP').read_text().strip()}", type="AgentGaveUp", non_retryable=True
                )
            error = _validate(OUTPUT_COLUMNS)
            if error is None:
                break
            if fix_round == MAX_FIX_ROUNDS:
                raise ApplicationError(f"output failed validation: {error}", type="AgentFailed")
            activity.logger.info("validation failed: %s", error)
            results.append(await _turn(FIX_PROMPT.format(out=OUT, error=error), options(resume=sid)))

    os.sync()  # make everything durable on the Archil disk before reporting success
    return TaskResult(
        key=task.key,
        disk=task_name(task),
        summary=(results[-1].result or "")[:2000],
        cost_usd=sum(r.total_cost_usd or 0 for r in results),
        sandbox_id=os.environ.get("SANDBOX_ID", ""),
        attempt=activity.info().attempt,
        resumed=resumed,
    )


@activity.defn
async def assemble(inp: AssembleInput) -> AssembleOutput:
    """Union every task's Parquet output into one file.

    Each task disk is mounted read-only at /mnt/tasks/<key>, and the output
    disk at /mnt/out. DuckDB streams the inputs and spills to the output disk,
    so the merged file can be far larger than the sandbox's memory.
    """
    import duckdb

    files = sorted(glob.glob("/mnt/tasks/*/out/*.parquet"))
    if not files:
        raise ApplicationError("no task outputs found under /mnt/tasks", type="AssembleFailed", non_retryable=True)

    def merge() -> int:
        con = duckdb.connect()
        con.execute("SET temp_directory = '/mnt/out/.duckdb-spill'")
        con.execute("SET preserve_insertion_order = false")
        inputs = "[" + ", ".join("'" + f.replace("'", "''") + "'" for f in files) + "]"
        tmp = "/mnt/out/merged.parquet.tmp"
        con.execute(f"COPY (SELECT * FROM read_parquet({inputs}, union_by_name = true)) TO '{tmp}' (FORMAT parquet)")
        os.replace(tmp, "/mnt/out/merged.parquet")  # a retried run never exposes a partial file
        rows = con.execute("SELECT count(*) FROM read_parquet('/mnt/out/merged.parquet')").fetchone()[0]
        with open("/mnt/out/manifest.json", "w") as f:
            json.dump({"rows": rows, "inputs": files}, f)
        os.sync()
        return rows

    async with heartbeating():
        rows = await asyncio.to_thread(merge)
    activity.logger.info("merged %d file(s), %d rows", len(files), rows)
    return AssembleOutput(rows=rows, path="merged.parquet")
