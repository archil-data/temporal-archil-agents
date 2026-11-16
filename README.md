# Temporal + Archil sandboxes: one agent per activity

A minimal example of running each [Temporal](https://temporal.io) activity as a
[Claude Agent SDK](https://code.claude.com/docs/en/agent-sdk) agent inside its
own [Archil](https://archil.com) sandbox, a microVM with a persistent disk
mounted. The sandbox lives exactly as long as the work does.

The example workflow takes a list of URLs, gives each one to an agent that
downloads it and normalizes it into Parquet with a fixed schema, then merges
every agent's output into one file and returns a signed download URL you can
load straight into ClickHouse (or anything else that reads Parquet over HTTP).

```
                 Temporal                                    Archil
┌──────────────────────────────────────┐   ┌────────────────────────────────────────────┐
│ IngestWorkflow                       │   │                                            │
│  ├─ run_agent_task(url 0) ─ ─ ─ ─ ─ ─│─ ─│─▶ sandbox ── /mnt/archil ◀── disk t000      │
│  ├─ run_agent_task(url 1) ─ ─ ─ ─ ─ ─│─ ─│─▶ sandbox ── /mnt/archil ◀── disk t001      │
│  ├─ ...                              │   │      (Claude agent + its session log)      │
│  └─ assemble ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ │─ ─│─▶ sandbox ── /mnt/tasks/* (read-only)      │
│                                      │   │              /mnt/out ◀── disk merged      │
└──────────────────────────────────────┘   │                  └─▶ merged.parquet + URL  │
   thin workers: each activity only        └────────────────────────────────────────────┘
   supervises its sandbox over a WebSocket
```

## How it works

**Workers are thin.** The agent never runs on the Temporal worker. Each activity
creates (or finds) a sandbox, starts `run_agent.py` in it, and holds a WebSocket
to that process while heartbeating. One small worker pod can supervise hundreds
of agents, so you don't need a custom autoscaler for long-running activities: a
plain Deployment with a few replicas is enough.

**Everything an agent needs to resume lives on its Archil disk.** Each task gets
its own disk, mounted at `/mnt/archil`. The agent's working files go there, and
so does Claude Code's session transcript (`CLAUDE_CONFIG_DIR=/mnt/archil/.claude`).
The session ID is derived from the workflow ID and task key, so any later
attempt finds the conversation and continues it with `resume=`.

**The activity decides when the sandbox goes away.** The autoscaler never does:

| What happens | What the activity does | Agent's work |
|---|---|---|
| Agent finishes | Reads `result.json`, then stops and deletes the sandbox | Done. The disk is kept for the merge step |
| Agent fails (crash, bad output) | Deletes the sandbox; Temporal retries | Next attempt starts a fresh sandbox and resumes the same Claude session on the same disk |
| Worker shuts down (deploy, scale-in, node drain) | Detaches at once, raises a retryable `WorkerShutdown` | **Keeps running.** The retry reattaches to the same process from the heartbeat checkpoint and picks up its output where it left off |
| Worker crashes | Nothing (it's gone) | Keeps running. After `heartbeat_timeout`, Temporal retries on another worker, which reattaches |
| Workflow cancelled | Kills the process, stops and deletes the sandbox | Stopped |
| Workflow terminated, or nobody ever retries | Nothing | The sandbox's idle TTL pauses it once no worker has been attached for `SANDBOX_ORPHAN_TTL_SECONDS`; a paused sandbox uses no CPU. Its max TTL is the hard cap |

That middle row is the one that matters for deploys: rolling out a new worker
version never cancels in-flight agent work, and old workers drain in seconds
rather than waiting for their longest activity to finish.

**Merging happens next to the data.** `assemble` mounts every task disk
read-only plus one output disk, and uses DuckDB to stream all the Parquet files
into one, spilling to the Archil disk, so the result can be larger than the
sandbox's memory. Then it creates a signed URL for `merged.parquet`.

## Run it

You need Python 3.10+, an [Archil API key](https://console.archil.com), an
Anthropic API key, Docker, and a container registry Archil can pull from.

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e .
cp .env.example .env   # fill in the keys
set -a && source .env && set +a
```

**1. Build the sandbox image.** `image/Dockerfile` installs the Claude Agent SDK,
pandas, pyarrow, and DuckDB, and copies the two in-sandbox scripts. Push it,
then register it with Archil once:

```bash
docker buildx build --platform linux/amd64,linux/arm64 -t ghcr.io/you/archil-agent:v1 --push image/
python -m archil_temporal.build_image ghcr.io/you/archil-agent:v1
# prints ARCHIL_IMAGE_ID=...; add it to .env
```

For a private image, set `REGISTRY_USERNAME` and `REGISTRY_PASSWORD` before
running `build_image`. Archil builds the image once, and sandboxes created from
that `image_id` reuse the build. Add your heavier dependencies (LibreOffice,
Playwright and Chromium, ...) to the Dockerfile. Pulling them is part of the
build, not of each sandbox start.

**2. Start Temporal and a worker.**

```bash
temporal server start-dev            # or point TEMPORAL_ADDRESS at your cluster
python -m archil_temporal.worker
```

**3. Start a workflow.** With no arguments, it uses three public CSV datasets:

```bash
python -m archil_temporal.starter
python -m archil_temporal.starter https://example.com/a.csv https://example.com/b.xlsx
```

The result lists which tasks succeeded, which failed, and a `download_url` for the
merged file. To load the file into ClickHouse:

```sql
INSERT INTO observations
SELECT * FROM url('<download_url>', 'Parquet');
```

To watch the worker-shutdown behavior, press Ctrl-C on the worker while agents
are running and start it again. The agents keep going, and the new worker
reattaches to them.

## Tests

```bash
pip install -e ".[test]"
pytest -v
```

- `tests/test_workflow.py` checks the workflow's orchestration with stub activities on
  Temporal's test server. It needs no credentials.
- `tests/test_sandbox_lifecycle.py` runs against the live Archil API with a shell
  loop standing in for the agent. It checks that a worker shutdown leaves the
  sandbox running, that the retry reattaches to the same process, and that
  cancelling the workflow deletes the sandbox. It needs `ARCHIL_API_KEY` and
  `ARCHIL_REGION`, and skips itself without them.
- `scripts/e2e.py` is the full demo: real agents in sandboxes from your image,
  on a local Temporal dev server. It stops the worker mid-run, starts a new one,
  and checks that every agent was reattached rather than restarted, that the
  merged file downloads with rows in it, and that no sandbox is left running.
  The `e2e` GitHub Action runs it on demand. It builds and registers the image,
  then posts the checks, each agent's summary, and sample rows to the run page.
  It needs the `ARCHIL_API_KEY` and `ANTHROPIC_API_KEY` repository secrets.

## Make it yours

| To change | Edit |
|---|---|
| The output schema | `OUTPUT_COLUMNS` in `archil_temporal/shared.py` (the agent is told it, and `run_agent.py` validates against it) |
| What the agent is asked to do | `TASK_PROMPT` in `image/run_agent.py` |
| Tools available to the agent | `image/Dockerfile` |
| Sandbox size, TTLs, concurrency | environment variables in `.env.example` |
| Retries and timeouts | `workflows.py` |

Each task's disk is kept after the workflow finishes, so you can inspect an
agent's files and transcript afterwards. Delete the disks when you're done with
them.

## Layout

```
archil_temporal/
  workflows.py    IngestWorkflow: fan out run_agent_task, then assemble
  activities.py   run_agent_task and assemble
  sandboxes.py    find-or-create, attach-or-start, heartbeat, detach, spin down
  shared.py       dataclasses and the deterministic disk/sandbox/session names
  worker.py       the Temporal worker
  starter.py      starts a workflow and prints the result
  build_image.py  registers an OCI image with Archil
scripts/e2e.py    end-to-end demo (see Tests)
tests/            workflow and sandbox lifecycle tests
image/
  Dockerfile      the sandbox image
  run_agent.py    the agent (runs inside the sandbox)
  assemble.py     the DuckDB merge (runs inside the sandbox)
```

## License

MIT
