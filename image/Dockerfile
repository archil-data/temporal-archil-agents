# The sandbox image: the Temporal worker that runs in each sandbox, the agent's
# tools, and this package. Build it from the repository root:
#
#   docker buildx build --platform linux/amd64 -f image/Dockerfile -t <image> --push .
FROM python:3.12-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl git \
    && rm -rf /var/lib/apt/lists/*

# Put the heavier tools your agents reach for here (system packages, headless
# browsers, ...). They're paid for once, when Archil builds the image, not on
# every sandbox start.

WORKDIR /opt/app
COPY pyproject.toml README.md ./
COPY archil_temporal ./archil_temporal
# The "sandbox" extra adds the Claude Agent SDK (which bundles the Claude Code
# CLI; no Node.js needed), DuckDB, pandas, and pyarrow.
RUN pip install --no-cache-dir ".[sandbox]"
