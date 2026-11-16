"""Runs inside the sandbox: union every task's Parquet output into one file.

Each task disk is mounted read-only at /mnt/tasks/<key>; the output disk is at
/mnt/out. DuckDB streams the inputs and spills to the output disk, so the
merged dataset can be far larger than the sandbox's memory.
"""

from __future__ import annotations

import glob
import json
import os

import duckdb

files = sorted(glob.glob("/mnt/tasks/*/out/*.parquet"))
if not files:
    raise SystemExit("no task outputs found under /mnt/tasks")

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
print(f"merged {len(files)} file(s), {rows} rows")
