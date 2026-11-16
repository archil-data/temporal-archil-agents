# Temporal + Archil sandboxes: one agent per activity

A minimal example of running each [Temporal](https://temporal.io) activity as a
[Claude Agent SDK](https://code.claude.com/docs/en/agent-sdk) agent inside its
own [Archil](https://archil.com) sandbox, a microVM with a persistent disk
mounted. The sandbox lives exactly as long as the work does.

The example workflow takes a list of URLs, gives each one to an agent that
downloads it and normalizes it into Parquet with a fixed schema, then merges
every agent's output into one file and returns a signed download URL you can
load straight into a warehouse such as ClickHouse, DuckDB, or anything else that
reads Parquet over HTTP.

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
to that process while heartbeating. One small worker process can supervise
hundreds of agents, and since the agents don't run on it, it can be replaced at
any time.

**Everything an agent needs to resume lives on its Archil disk.** Each task gets
its own disk, mounted at `/mnt/archil`. The agent's working files go there, and
so does Claude Code's session transcript (`CLAUDE_CONFIG_DIR=/mnt/archil/.claude`).
The session ID is derived from the workflow ID and task key, so any later
attempt finds the conversation and continues it with `resume=`.

**Agents never see your Anthropic API key.** Each agent's sandbox denies all
outbound traffic except to the Anthropic API and to the host of the URL it was
given. Archil's egress proxy adds the API key to the agent's Anthropic requests
outside the VM, so the agent only has a placeholder, and nothing it runs or
downloads can read or send the real key. (Egress policies need a paid Archil
plan. On a free plan, set `RESTRICT_EGRESS=false` and the key is passed into
the sandbox instead.)

**The activity decides when the sandbox goes away**, not the worker's lifetime:

| What happens | What the activity does | Agent's work |
|---|---|---|
| Agent finishes | Reads `result.json`, then stops and deletes the sandbox | Done. The disk is kept for the merge step |
| Agent fails (crash, bad output) | Deletes the sandbox; Temporal retries, up to `MAX_AGENT_FAILURES` failures | Next attempt starts a fresh sandbox and resumes the same Claude session on the same disk |
| Worker shuts down (deploy, scale-in, node drain) | Detaches at once; Temporal retries a second later | **Keeps running.** The retry reattaches to the same process from the heartbeat checkpoint and picks up its output where it left off |
| Connection to the sandbox drops | Same as a worker shutdown | Keeps running, and is reattached |
| Worker crashes | Nothing (it's gone) | Keeps running. After `heartbeat_timeout`, Temporal retries on another worker, which reattaches. If the old attempt turns out to be alive, it sees it was superseded and only disconnects |
| Workflow cancelled | Kills the process, stops and deletes the sandbox. The workflow then runs a cleanup step that deletes any sandbox left over (say, from a task that was waiting to retry) | Stopped |

Detaching never counts as a failure: a task survives any number of deploys
during its run, and old workers drain in seconds rather than waiting for their
longest activity to finish.

Terminating a workflow (rather than cancelling it) runs none of its code. Its
running sandboxes are paused by Archil once no worker has been attached for
`SANDBOX_ORPHAN_TTL_SECONDS`, and a paused sandbox uses no CPU. To delete them,
run `python -m archil_temporal.cleanup <workflow-id>`.

**Merging happens next to the data.** `assemble` mounts every task disk
read-only plus one output disk, and uses DuckDB to stream all the Parquet files
into one, spilling to the Archil disk, so the result can be larger than the
sandbox's memory. Then it creates a signed URL for `merged.parquet`.

## Run it

You need Python 3.10+, the [Temporal CLI](https://docs.temporal.io/cli) (or a
Temporal cluster), an [Archil API key](https://console.archil.com), an Anthropic
API key, Docker, and a container registry Archil can pull from.

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
docker buildx build --platform linux/amd64 -t ghcr.io/you/archil-agent:v1 --push image/
python -m archil_temporal.build_image ghcr.io/you/archil-agent:v1
# prints ARCHIL_IMAGE_ID=...; add it to .env
```

For a private image, set `REGISTRY_USERNAME` and `REGISTRY_PASSWORD` before
running `build_image`. Archil builds the image once, and sandboxes created from
that `image_id` reuse the build. Add your heavier dependencies (system
packages, headless browsers, ...) to the Dockerfile. Pulling them is part of the
build, not of each sandbox start.

**2. Start Temporal and a worker.**

```bash
temporal server start-dev            # or point TEMPORAL_ADDRESS at your cluster
python -m archil_temporal.worker
```

**3. Start a workflow.** With no arguments, it uses three public CSV datasets:

```bash
python -m archil_temporal.starter
python -m archil_temporal.starter https://example.com/a.csv https://example.com/b.json
```

The result lists which tasks succeeded, which failed, and a `download_url` for the
merged file. For example, to load it into ClickHouse:

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

- `tests/test_workflow.py` checks the workflow's orchestration with stub
  activities on Temporal's test server: retries, tasks that give up, a task
  that survives 25 detaches in a row, and cleanup after a cancel. It needs no
  credentials.
- `tests/test_sandbox_lifecycle.py` runs against the live Archil API with a shell
  loop standing in for the agent. It checks that a worker shutdown leaves the
  sandbox running and the retry takes over the same process, that cancelling
  deletes the sandbox, and that a superseded attempt leaves the sandbox to the
  attempt that replaced it. It needs `ARCHIL_API_KEY` and `ARCHIL_REGION`, and
  skips itself without them.
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
| Hosts the agent can reach | `AGENT_ALLOWED_HOSTS`, or `agent_network` in `activities.py` |
| Sandbox size, TTLs, concurrency | environment variables in `.env.example` |
| Retries and timeouts | `workflows.py` |

Each task's disk is kept after the workflow finishes, so you can inspect an
agent's files and transcript afterwards. Delete them when you're done with
`python -m archil_temporal.cleanup <workflow-id> --disks`.

## Layout

```
archil_temporal/
  workflows.py    IngestWorkflow: fan out run_agent_task, assemble, clean up
  activities.py   run_agent_task, assemble, clean_up, and the agents' egress policy
  sandboxes.py    find-or-create, attach-or-start, heartbeat, detach, spin down
  shared.py       dataclasses and the deterministic disk/sandbox/session names
  worker.py       the Temporal worker
  starter.py      starts a workflow and prints the result
  build_image.py  registers an OCI image with Archil
  cleanup.py      deletes a workflow's leftover sandboxes (and, optionally, disks)
scripts/e2e.py    end-to-end demo (see Tests)
tests/            workflow and sandbox lifecycle tests
image/
  Dockerfile      the sandbox image
  run_agent.py    the agent (runs inside the sandbox)
  assemble.py     the DuckDB merge (runs inside the sandbox)
```

## License

MIT
