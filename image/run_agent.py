"""Runs inside the sandbox: one Claude agent that turns one URL into Parquet.

Everything the agent needs to resume lives on the Archil disk mounted at
/mnt/archil, including Claude Code's session transcript (CLAUDE_CONFIG_DIR).
If this process, its sandbox, or the Temporal worker supervising it goes away,
the next run resumes the same session against the same files.

Exit codes: 0 done, 3 the agent decided the task is impossible, else retry.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path

import pyarrow.parquet as pq
from claude_agent_sdk import AssistantMessage, ClaudeAgentOptions, ResultMessage, TextBlock, query

DISK = Path("/mnt/archil")
WORK = DISK / "work"
OUT = DISK / "out"
CLAUDE_CONFIG_DIR = DISK / ".claude"
EXIT_GAVE_UP = 3
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


def session_exists(session_id: str) -> bool:
    # Claude Code stores each session as <config dir>/projects/<project>/<id>.jsonl.
    return any(CLAUDE_CONFIG_DIR.glob(f"projects/*/{session_id}.jsonl"))


def validate(columns: dict[str, str]) -> str | None:
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


async def turn(prompt: str, options: ClaudeAgentOptions) -> ResultMessage:
    result = None
    async for message in query(prompt=prompt, options=options):
        if isinstance(message, AssistantMessage):
            for block in message.content:
                if isinstance(block, TextBlock):
                    print(block.text, flush=True)
        elif isinstance(message, ResultMessage):
            result = message
    if result is None:
        raise RuntimeError("agent produced no result")
    return result


async def main() -> int:
    task = json.loads(os.environ["AGENT_TASK"])
    session_id = os.environ["AGENT_SESSION_ID"]
    columns: dict[str, str] = task["columns"]
    for d in (WORK, OUT, CLAUDE_CONFIG_DIR):
        d.mkdir(parents=True, exist_ok=True)

    def options(**session) -> ClaudeAgentOptions:
        return ClaudeAgentOptions(
            cwd=str(WORK),
            model=os.environ.get("AGENT_MODEL"),
            permission_mode="bypassPermissions",
            max_turns=200,
            # IS_SANDBOX lets Claude Code skip permission prompts as root,
            # which is fine here: the microVM is the security boundary.
            env={"CLAUDE_CONFIG_DIR": str(CLAUDE_CONFIG_DIR), "IS_SANDBOX": "1"},
            **session,
        )

    # A fixed session ID per task means a fresh process can always find, and
    # resume, the conversation an earlier attempt started.
    if session_exists(session_id):
        print(f"resuming session {session_id}", flush=True)
        results = [await turn(RESUME_PROMPT, options(resume=session_id))]
    else:
        prompt = TASK_PROMPT.format(url=task["url"], work=WORK, out=OUT, columns=", ".join(columns))
        results = [await turn(prompt, options(session_id=session_id))]

    error = None
    for _ in range(MAX_FIX_ROUNDS + 1):
        if results[-1].is_error:
            print(f"agent error: {results[-1].errors or results[-1].subtype}", file=sys.stderr)
            return 1
        if (WORK / "GAVE_UP").exists():
            print(f"agent gave up: {(WORK / 'GAVE_UP').read_text()}", file=sys.stderr)
            return EXIT_GAVE_UP
        error = validate(columns)
        if error is None:
            break
        print(f"validation failed: {error}", flush=True)
        results.append(await turn(FIX_PROMPT.format(out=OUT, error=error), options(resume=session_id)))
    if error is not None:
        return 1

    report = {
        "session_id": session_id,
        "summary": (results[-1].result or "")[:2000],
        "cost_usd": sum(r.total_cost_usd or 0 for r in results),
        "files": sorted(p.name for p in OUT.glob("*.parquet")),
    }
    (DISK / "result.json").write_text(json.dumps(report))
    return 0


if __name__ == "__main__":
    try:
        code = asyncio.run(main())
    finally:
        os.sync()  # make everything durable on the Archil disk before exiting
    sys.exit(code)
