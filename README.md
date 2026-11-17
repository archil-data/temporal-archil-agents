# Temporal on Archil sandboxes: one sandbox per activity

A minimal example of running [Temporal](https://temporal.io) activities on
workers inside [Archil](https://archil.com) sandboxes. Each piece of work gets
its own sandbox, a microVM with a persistent Archil disk mounted, which runs a
Temporal worker for just that work. The workflow decides when each sandbox is
started and when it is deleted.

The example workflow takes a list of URLs and gives each one to a
[Claude Agent SDK](https://code.claude.com/docs/en/agent-sdk) agent, which
downloads it and normalizes it into Parquet with a fixed schema. A final step
merges every agent's output into one file and returns a signed download URL you
can load straight into a warehouse such as ClickHouse, DuckDB, or anything else
that reads Parquet over HTTP.

```
 Temporal                       control worker                Archil
┌──────────────────────┐       ┌──────────────────┐   ┌─────────────────────────────────────────────┐
│ IngestWorkflow       │       │ start_sandbox ───┼─▶ │ sandbox t000 ── /mnt/archil ◀── disk t000    │
│  ├─ task t000 ───────┼─ ─ ─ ─│─ ─ ─ ─ ─ ─ ─ ─ ─ │─ ─│─▶ Temporal worker ─ run_agent_task (Claude)  │
│  ├─ task t001 ───────┼─ ─ ─ ─│─ ─ ─ ─ ─ ─ ─ ─ ─ │─ ─│─▶ sandbox t001: worker ─ run_agent_task      │
│  ├─ ...              │       │ stop_sandbox     │   │                                              │
│  └─ merge ───────────┼─ ─ ─ ─│─ ─ ─ ─ ─ ─ ─ ─ ─ │─ ─│─▶ sandbox merged: worker ─ assemble          │
│                      │       │ share_file       │   │     /mnt/tasks/* (read-only), /mnt/out       │
└──────────────────────┘       └──────────────────┘   └─────────────────────────────────────────────┘
                                 short activities         each sandbox polls a task queue of its own
```

## How it works

**Every piece of work runs on a worker in its own sandbox.** For each task, the
workflow calls `start_sandbox`, a short activity on the control worker. It
creates a sandbox from your image with the task's disk mounted at
`/mnt/archil`, and starts a Temporal worker in it as an
[Archil service](https://docs.archil.com/compute/sandboxes/introduction#hosting-network-services).
That worker polls a task queue that belongs to the sandbox alone. The workflow
then sends the task's activity to that queue, so it runs inside the sandbox,
with the disk as a local filesystem and your image's tools installed. When the
activity is done, the workflow calls `stop_sandbox`, which deletes the sandbox.
The disk is kept.

**Nothing long-running lives on the control worker.** It runs the workflow and
activities that take seconds: start a sandbox, delete one, share a file. You can
deploy it, scale it, or restart it at any time, without draining it and without
a custom autoscaler, because the agents aren't running on it. And each sandbox
keeps the image it was started with until its work is done, while new work gets
the new image, so rolling out a new image never interrupts work in flight.

**Everything an agent needs to resume lives on its disk.** The agent's working
files are on the disk, and so is Claude Code's session transcript
(`CLAUDE_CONFIG_DIR=/mnt/archil/.claude`). The session ID is derived from the
workflow ID and the task key, so any later attempt finds the conversation and
continues it with `resume=`.

| What happens | What happens next | The agent |
|---|---|---|
| Agent finishes | The workflow deletes the sandbox | Done. The disk is kept for the merge step |
| Agent fails (crash, invalid output) | Temporal retries the activity on the same sandbox's worker, up to 3 attempts | Resumes the same Claude session against the same files |
| The worker process in the sandbox dies | The Archil service restarts it; after the heartbeat timeout, Temporal retries the activity there | Resumes its Claude session |
| The sandbox itself is lost | Nothing picks the activity up; after `sandbox_start_timeout_s` the workflow starts a new sandbox on the same disk | Resumes its Claude session |
| Control worker deployed, scaled in, or restarted | Nothing. Another control worker continues the workflow | Keeps running, undisturbed |
| Workflow cancelled | The agent is cancelled through its heartbeat, then the workflow deletes the sandbox | Stopped |

The workflow ends with a cleanup step that runs whether it succeeds, fails, or
is cancelled, and deletes any sandbox still there. Terminating a workflow
(rather than cancelling it) runs none of its code: its sandboxes are paused by
their max TTL (`SANDBOX_MAX_TTL_SECONDS`), and a paused sandbox uses no CPU. To
delete them, run `python -m archil_temporal.cleanup <workflow-id>`.

**Agents never see your Anthropic API key.** Each agent's sandbox denies all
outbound traffic except to Temporal, the Anthropic API, and the host of the URL
it was given. Archil's egress proxy adds the API key to the agent's Anthropic
requests outside the VM, so the agent only has a placeholder, and nothing it runs
or downloads can read or send the real key. (Egress policies need a paid Archil
plan. On a free plan, set `RESTRICT_EGRESS=false` and the key is passed into
the sandbox instead.)

**Merging happens next to the data.** The merge step's sandbox mounts every
task disk read-only, plus one output disk, and DuckDB streams all the Parquet
files into one, spilling to the Archil disk, so the result can be larger than
the sandbox's memory. Then the control worker creates a signed URL for
`merged.parquet`.

## Run it

You need Python 3.10+, a Temporal server the sandboxes can reach over the
internet (such as [Temporal Cloud](https://temporal.io/cloud)), an
[Archil API key](https://console.archil.com), an Anthropic API key, Docker, and
a container registry Archil can pull from.

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e .
cp .env.example .env   # fill in the keys and your Temporal endpoint
set -a && source .env && set +a
```

**1. Build the sandbox image.** `image/Dockerfile` installs this package, with
the Claude Agent SDK, pandas, pyarrow, and DuckDB. Build it from the repository
root, push it, then register it with Archil once:

```bash
docker buildx build --platform linux/amd64 -f image/Dockerfile -t ghcr.io/you/archil-agent:v1 --push .
python -m archil_temporal.build_image ghcr.io/you/archil-agent:v1
# prints ARCHIL_IMAGE_ID=...; add it to .env
```

For a private image, set `REGISTRY_USERNAME` and `REGISTRY_PASSWORD` before
running `build_image`. Archil builds the image once, and sandboxes created from
that `image_id` reuse the build. Add your heavier dependencies (system
packages, headless browsers, ...) to the Dockerfile. Pulling them is part of the
build, not of each sandbox start.

**2. Start a control worker.** It can run anywhere that reaches Temporal and
the Archil API, including a plain Kubernetes Deployment:

```bash
python -m archil_temporal.worker
```

**3. Start a workflow.** With no arguments, it uses three public CSV datasets:

```bash
python -m archil_temporal.starter
python -m archil_temporal.starter https://example.com/a.csv https://example.com/b.json
```

The result lists which tasks succeeded, which failed, and a `download_url` for
the merged file. For example, to load it into ClickHouse:

```sql
INSERT INTO observations
SELECT * FROM url('<download_url>', 'Parquet');
```

## Tests

```bash
pip install -e ".[test]"
pytest -v
```

- `tests/test_workflow.py` checks the workflow with stub activities on
  Temporal's test server: one sandbox per task, retries in the same sandbox, a
  new sandbox when one is lost, and every sandbox deleted afterwards, including
  after a cancel. It needs no credentials.
- `tests/test_sandbox_lifecycle.py` runs against the live Archil API with a
  shell loop standing in for the worker. It checks that starting a task's
  sandbox twice reuses it, that the worker service is restarted if it dies,
  and that deleting the sandbox keeps the disk. It needs `ARCHIL_API_KEY` and
  `ARCHIL_REGION`, and skips itself without them.
- `scripts/e2e.py` is the full demo with real agents. It runs a Temporal server
  in a sandbox of its own, replaces the control worker while the agents run, and
  restarts one sandbox's worker mid-run. Then it checks that each agent ran in
  its own sandbox, that replacing the control worker interrupted nothing, that
  the restarted agent resumed its Claude session in the same sandbox, that the
  merged file downloads, and that every sandbox was deleted. The `e2e` GitHub
  Action runs it on demand. It builds and registers the image, then posts the
  checks, each agent's summary, and sample rows to the run page. It needs the
  `ARCHIL_API_KEY` and `ANTHROPIC_API_KEY` repository secrets.

The e2e's Temporal server runs in a sandbox and is reached through its exposed
port. That port's TLS handshake doesn't negotiate HTTP/2, which gRPC clients
need, so the test connects through a small local tunnel
(`archil_temporal/tunnel.py`, enabled by `TEMPORAL_TUNNEL_HOST`). With Temporal
Cloud or your own cluster, leave it unset.

## Make it yours

| To change | Edit |
|---|---|
| The work done in the sandbox | `archil_temporal/agent.py` (ordinary Temporal activities) |
| The output schema | `OUTPUT_COLUMNS` in `archil_temporal/shared.py` |
| What the agent is asked to do | `TASK_PROMPT` in `archil_temporal/agent.py` |
| Tools available in the sandbox | `image/Dockerfile` |
| Sandbox size, disks, and allowed hosts | `agent_sandbox` in `archil_temporal/workflows.py` |
| Retries and timeouts | `archil_temporal/workflows.py` |

Each task's disk is kept after the workflow finishes, so you can inspect an
agent's files and transcript afterwards. Delete them when you're done with
`python -m archil_temporal.cleanup <workflow-id> --disks`.

## Layout

```
archil_temporal/
  workflows.py       IngestWorkflow: a sandbox per task, then the merge, then cleanup
  control.py         control activities: start_sandbox, stop_sandbox, share_file, clean_up
  sandboxes.py       find or create a sandbox, run the worker as a service, delete it
  agent.py           the activities that run in the sandboxes: the agent and the merge
  sandbox_worker.py  the Temporal worker inside each sandbox
  worker.py          the control worker
  shared.py          dataclasses and the deterministic disk/sandbox/queue/session names
  config.py          settings and the Temporal connection
  starter.py         starts a workflow and prints the result
  build_image.py     registers an OCI image with Archil
  cleanup.py         deletes a workflow's leftover sandboxes (and, optionally, disks)
  tunnel.py          test-only: reach a Temporal server in a sandbox
scripts/e2e.py       end-to-end demo (see Tests)
tests/               workflow and sandbox lifecycle tests
image/Dockerfile     the sandbox image
```

## License

MIT
