FROM python:3.12-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl git \
    && rm -rf /var/lib/apt/lists/*

# Put the heavy tools your agents reach for here (LibreOffice, Playwright and
# Chromium, ...). They're paid for once, when Archil builds the image, not on
# every sandbox start.
#
# RUN apt-get update && apt-get install -y --no-install-recommends libreoffice-calc \
#     && rm -rf /var/lib/apt/lists/*
# RUN pip install --no-cache-dir playwright && playwright install --with-deps chromium

# claude-agent-sdk bundles the Claude Code CLI; no Node.js needed.
RUN pip install --no-cache-dir \
    "claude-agent-sdk==0.2.182" \
    "duckdb>=1.1" \
    "openpyxl>=3.1" \
    "pandas>=2.2" \
    "pyarrow>=17"

COPY run_agent.py assemble.py /opt/agent/
