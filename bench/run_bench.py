#!/usr/bin/env python3
"""parquet-gsi benchmark harness.

Runs every query in the suite under both arms (GSI on / GSI off), cold and warm,
N repetitions each, and writes one row per (query, arm, cache, repetition) to
results/results.csv.

Every number in the README must come from this CSV. Nothing here estimates,
extrapolates, or rounds a measurement into a headline.

What "cold" and "warm" mean here
--------------------------------
cold  a brand new backend, executing this query for the first time. Nothing in
      that backend's parquet reader state, plan cache, or catalogue cache is
      populated. Shared buffers and the OS page cache are NOT empty.
warm  the same backend, having already executed the identical query once. The
      warm-up execution is discarded and not written to the CSV.

This container cannot drop the host page cache (no CAP_SYS_ADMIN) and
PostgreSQL offers no supported way to evict shared_buffers from SQL: a large
sequential scan deliberately uses a small ring buffer precisely so it will not
evict anything. So "cold" here means cold *backend*, not cold storage. Parquet
bytes read by an earlier repetition may still be in the VM's page cache when a
later "cold" repetition runs. This is a real limitation and the README says so
in those words. Do not describe these as cold-storage numbers.

The CSV records `gsi_active` and `row_groups_read` straight out of the plan, so
the A/B is checkable from the committed data alone without re-reading the JSON.
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import platform
import subprocess
import sys
import time
from pathlib import Path

import psycopg2

sys.path.insert(0, str(Path(__file__).resolve().parent))
from queries import QUERIES, pick_literals  # noqa: E402

GSI_KEY = "Global Secondary Index"


def connect(a):
    return psycopg2.connect(dbname=a.dbname, user=a.user, password=a.password,
                            host=a.host, port=a.port)


def apply_arm(a, arm: str) -> None:
    script = Path(__file__).resolve().parent / (
        "gsi_on.sql" if arm == "gsi_on" else "gsi_off.sql")
    conn = connect(a)
    try:
        with conn.cursor() as cur:
            cur.execute(script.read_text())
        conn.commit()
    finally:
        conn.close()


def count_rowgroups(rg: str) -> int:
    """Count row group ids in the FDW's 'Row groups' text blob.

    One line per file, "<basename>: 1, 2, 3", with the prefix omitted when only
    one file is scanned. Digits inside chunk_0007.parquet must not be counted.
    """
    total = 0
    for line in (rg or "").splitlines():
        ids = line.rsplit(":", 1)[-1] if ":" in line else line
        total += sum(1 for tok in ids.split(",") if tok.strip().isdigit())
    return total


def measure(cur, sql_text: str, value) -> dict:
    """Run one EXPLAIN (ANALYZE ...) on an already-open cursor and parse it."""
    explain = "EXPLAIN (ANALYZE, BUFFERS, TIMING, FORMAT JSON) " + sql_text
    t0 = time.perf_counter()
    cur.execute(explain, {"v": value})
    wall_ms = (time.perf_counter() - t0) * 1000.0
    plan = cur.fetchone()[0][0]
    node = plan["Plan"]
    return {
        "execution_ms": plan["Execution Time"],
        "planning_ms": plan["Planning Time"],
        "wall_ms": round(wall_ms, 3),
        "actual_rows": node.get("Actual Rows"),
        "plan_rows": node.get("Plan Rows"),
        "rows_removed_by_filter": node.get("Rows Removed by Filter"),
        "node_type": node.get("Node Type"),
        # The three fields that make the A/B auditable from the CSV alone.
        "gsi_active": node.get(GSI_KEY) is not None,
        "reader": node.get("Reader"),
        "row_groups_read": count_rowgroups(node.get("Row groups", "")),
        "shared_hit": node.get("Shared Hit Blocks"),
        "shared_read": node.get("Shared Read Blocks"),
        "plan_json": json.dumps(plan, separators=(",", ":")),
    }


def run_cell(a, sql_text, value, cache: str, reps: int) -> list[dict]:
    """Measure one (query, arm, cache) cell.

    cold: one fresh backend per repetition, measuring its first execution.
    warm: one backend, warmed with a discarded execution, then measured `reps`
          times without reconnecting.
    """
    rows = []
    if cache == "cold":
        for _ in range(reps):
            # No DISCARD ALL: the backend is new, so there is nothing to
            # discard, and psycopg2 has already opened a transaction by the
            # time the first statement lands, which DISCARD ALL forbids.
            conn = connect(a)
            try:
                with conn.cursor() as cur:
                    cur.execute("SET max_parallel_workers_per_gather = 0")
                    cur.execute("SET jit = off")
                    rows.append(measure(cur, sql_text, value))
            finally:
                conn.close()
    else:
        conn = connect(a)
        try:
            with conn.cursor() as cur:
                cur.execute("SET max_parallel_workers_per_gather = 0")
                cur.execute("SET jit = off")
                measure(cur, sql_text, value)  # warm-up, discarded
                for _ in range(reps):
                    rows.append(measure(cur, sql_text, value))
        finally:
            conn.close()
    return rows


def hardware() -> dict:
    def sh(cmd):
        try:
            return subprocess.check_output(cmd, shell=True, text=True).strip()
        except Exception:
            return "unknown"

    def pg(q):
        return sh(f"psql -U postgres -d gsi -At -c {q!r} 2>/dev/null")

    return {
        "hostname": platform.node(),
        "platform": platform.platform(),
        "python": platform.python_version(),
        "postgres": pg("select version()"),
        "arrow_cpp": sh("dpkg-query -W -f='${Version}' libarrow-dev 2>/dev/null"),
        "pyarrow": sh("python3 -c 'import pyarrow; print(pyarrow.__version__)'"),
        "cpu_model": sh("grep -m1 'model name' /proc/cpuinfo | cut -d: -f2- | xargs"),
        "cpu_count": os.cpu_count(),
        "mem_total_kb": sh("grep MemTotal /proc/meminfo | awk '{print $2}'"),
        "disk": sh("df -h /tmp/data_lake 2>/dev/null | tail -1 | awk '{print $1, $2, $4}'"),
        "shared_buffers": pg("show shared_buffers"),
        "work_mem": pg("show work_mem"),
        "max_parallel_workers_per_gather": pg("show max_parallel_workers_per_gather"),
        "jit": pg("show jit"),
        "lake_bytes": sh("du -sb /tmp/data_lake 2>/dev/null | awk '{print $1}'"),
        "note": ("Timings are PostgreSQL 'Execution Time' from EXPLAIN ANALYZE inside "
                 "this container. 'cold' means a fresh backend, not cold storage; the "
                 "host page cache is not dropped."),
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default=os.environ.get("PGHOST", "localhost"))
    ap.add_argument("--port", default=os.environ.get("PGPORT", "5432"))
    ap.add_argument("--dbname", default=os.environ.get("PGDATABASE", "gsi"))
    ap.add_argument("--user", default=os.environ.get("PGUSER", "postgres"))
    ap.add_argument("--password", default=os.environ.get("PGPASSWORD", "gsi"))
    ap.add_argument("--reps", type=int, default=5, help="repetitions per cell")
    ap.add_argument("--out", default="results/results.csv")
    ap.add_argument("--meta-out", default="results/hardware.json")
    args = ap.parse_args()

    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    Path(args.meta_out).write_text(json.dumps(hardware(), indent=2))

    apply_arm(args, "gsi_on")
    literals = pick_literals(connect, args)
    print("[bench] literals:", {k: str(v)[:16] for k, v in literals.items()}, flush=True)

    fields = ["query_id", "table", "column", "cardinality", "arm", "cache", "rep",
              "execution_ms", "planning_ms", "wall_ms", "actual_rows", "plan_rows",
              "rows_removed_by_filter", "node_type", "gsi_active", "reader",
              "row_groups_read", "shared_hit", "shared_read", "value", "plan_json"]

    with open(args.out, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=fields)
        w.writeheader()
        for arm in ("gsi_off", "gsi_on"):
            apply_arm(args, arm)
            for qid, table, column, card, sql_text in QUERIES:
                value = literals[qid]
                for cache in ("cold", "warm"):
                    for rep, r in enumerate(
                            run_cell(args, sql_text, value, cache, args.reps), start=1):
                        w.writerow({"query_id": qid, "table": table, "column": column,
                                    "cardinality": card, "arm": arm, "cache": cache,
                                    "rep": rep, "value": str(value), **r})
                        fh.flush()
                        print(f"[bench] {arm:8s} {cache:4s} {qid:16s} rep{rep} "
                              f"{r['execution_ms']:9.2f} ms  rows={r['actual_rows']} "
                              f"rg={r['row_groups_read']} gsi={r['gsi_active']}",
                              flush=True)

    apply_arm(args, "gsi_on")
    print(f"\n[bench] wrote {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
