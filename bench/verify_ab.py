#!/usr/bin/env python3
"""Prove the A/B toggle actually toggles, and fail loudly if it does not.

There is no `enable_gsi` GUC. The FDW decides at plan time by looking up
public.gsi_registry by (foreigntable_oid, column); bench/gsi_off.sql therefore
moves every registry row into a holding table, which makes the planner behave
as if no index existed while leaving every posting row on disk.

That is an indirect mechanism, so it has to be verified rather than assumed. If
the two arms produced the same plan, the benchmark would be measuring nothing
at all and every number downstream would be meaningless. This script is a gate,
not a report: it exits non-zero when the arms do not differ, and run_bench.sh
runs it before run_bench.py.

Writes results/explain_ab.txt so the evidence is committed alongside the CSV.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
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


def explain(a, sql_text: str, value) -> tuple[dict, str]:
    """Return (plan node, plain-text plan) for one query in the current arm."""
    conn = connect(a)
    try:
        with conn.cursor() as cur:
            cur.execute("SET max_parallel_workers_per_gather = 0")
            cur.execute("SET jit = off")
            cur.execute("EXPLAIN (ANALYZE, FORMAT JSON) " + sql_text, {"v": value})
            node = cur.fetchone()[0][0]["Plan"]
        with conn.cursor() as cur:
            cur.execute("SET max_parallel_workers_per_gather = 0")
            cur.execute("SET jit = off")
            cur.execute("EXPLAIN (ANALYZE) " + sql_text, {"v": value})
            text = "\n".join(r[0] for r in cur.fetchall())
        return node, text
    finally:
        conn.close()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default=os.environ.get("PGHOST", "localhost"))
    ap.add_argument("--port", default=os.environ.get("PGPORT", "5432"))
    ap.add_argument("--dbname", default=os.environ.get("PGDATABASE", "gsi"))
    ap.add_argument("--user", default=os.environ.get("PGUSER", "postgres"))
    ap.add_argument("--password", default=os.environ.get("PGPASSWORD", "gsi"))
    ap.add_argument("--out", default="results/explain_ab.txt")
    args = ap.parse_args()

    apply_arm(args, "gsi_on")
    literals = pick_literals(connect, args)

    report: list[str] = []
    failures: list[str] = []

    for qid, _table, _column, card, sql_text in QUERIES:
        value = literals[qid]
        arms = {}
        for arm in ("gsi_off", "gsi_on"):
            apply_arm(args, arm)
            node, text = explain(args, sql_text, value)
            arms[arm] = node
            report.append(f"{'=' * 78}\n{qid}  ({card} cardinality)  arm={arm}\n"
                          f"value = {value!r}\n{'=' * 78}\n{text}\n")

        off, on = arms["gsi_off"], arms["gsi_on"]
        off_gsi = off.get(GSI_KEY)
        on_gsi = on.get(GSI_KEY)
        off_reader, on_reader = off.get("Reader"), on.get("Reader")
        off_rg, on_rg = off.get("Row groups", ""), on.get("Row groups", "")

        verdict = []
        if off_gsi is not None:
            failures.append(f"{qid}: gsi_off arm still reports {GSI_KEY}={off_gsi!r}; "
                            "the registry was not emptied")
        if on_gsi is None:
            failures.append(f"{qid}: gsi_on arm does not report {GSI_KEY}; the planner "
                            "did not pick the index path")
        if off_reader == on_reader and off_rg == on_rg and off_gsi == on_gsi:
            failures.append(f"{qid}: the two arms produced an identical plan "
                            f"(Reader={off_reader!r}); the A/B is measuring nothing")

        verdict.append(f"  {GSI_KEY:24s} off={off_gsi!r}  on={on_gsi!r}")
        verdict.append(f"  {'Reader':24s} off={off_reader!r}  on={on_reader!r}")
        verdict.append(f"  {'Rows removed by filter':24s} "
                       f"off={off.get('Rows Removed by Filter')}  "
                       f"on={on.get('Rows Removed by Filter')}")
        verdict.append(f"  {'Row groups read':24s} "
                       f"off={_count_rowgroups(off_rg)}  on={_count_rowgroups(on_rg)}")
        print(f"[verify] {qid} ({card} cardinality)")
        print("\n".join(verdict), flush=True)
        report.append("PLAN DIFFERENCE\n" + "\n".join(verdict) + "\n")

    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    Path(args.out).write_text("\n".join(report))
    apply_arm(args, "gsi_on")

    if failures:
        print("\n".join("[verify] FAIL " + f for f in failures), file=sys.stderr)
        print("\nThe A/B toggle does not toggle. Every number the benchmark would "
              "produce from here is meaningless, so it stops here.", file=sys.stderr)
        return 1

    print(f"\n[verify] OK - both arms differ on every query. wrote {args.out}")
    return 0


def _count_rowgroups(rg: str) -> int:
    """'Row groups' is a formatted text blob, not a number. Count the ids in it.

    The FDW emits one line per file as "<basename>: 1, 2, 3", but omits the
    "<basename>: " prefix entirely when only one file is scanned. Both shapes
    have to parse, and the digits inside a filename like chunk_0007.parquet
    must not be counted as row group ids.
    """
    total = 0
    for line in rg.splitlines():
        ids = line.rsplit(":", 1)[-1] if ":" in line else line
        total += sum(1 for tok in ids.split(",") if tok.strip().isdigit())
    return total


if __name__ == "__main__":
    raise SystemExit(main())
