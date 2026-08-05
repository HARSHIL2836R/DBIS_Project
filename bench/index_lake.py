#!/usr/bin/env python3
"""One-shot, deterministic GSI builder.

The production path is watcher/watcher_deamon.py, a long-running filesystem
watcher. A benchmark cannot depend on a daemon reaching a steady state at an
unknown time, so this script performs the same work in a single pass and exits:

  1. catalogue every parquet file under each registered index's data_lake_path
  2. extract (value -> row_group_ids) postings per indexed column
  3. write postings into the per-index storage table
  4. mark registry status 'ready'

It reuses watcher/extractor.py so the postings produced here are byte-identical
to the ones the daemon would produce. Index build time is reported and recorded
so the README can state the maintenance cost alongside the query speedup.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
import psycopg2.extras
from psycopg2 import sql

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "watcher"))
from extractor import extract_index_coordinates  # noqa: E402


def connect(args):
    return psycopg2.connect(
        dbname=args.dbname, user=args.user, password=args.password,
        host=args.host, port=args.port,
    )


def list_parquet(root: str) -> list[str]:
    return sorted(str(p) for p in Path(root).rglob("*.parquet"))


def load_registry(conn) -> list[dict]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT index_name, foreigntable_oid, table_name, column_name,
                   storage_table, data_lake_path
              FROM public.gsi_registry ORDER BY index_name
        """)
        return [dict(r) for r in cur.fetchall()]


def catalogue_files(conn, entries: list[dict]) -> dict[str, int]:
    """Insert every file once, return {file_path: file_id}."""
    seen: dict[str, int] = {}
    roots = {e["data_lake_path"]: (e["foreigntable_oid"], e["table_name"]) for e in entries}
    with conn.cursor() as cur:
        for root, (oid, table) in roots.items():
            for path in list_parquet(root):
                st = os.stat(path)
                cur.execute("""
                    INSERT INTO public.gsi_file_catalog
                        (foreigntable_oid, table_name, data_lake_path, file_path,
                         file_size, file_mtime, is_active, last_seen_at)
                    VALUES (%s,%s,%s,%s,%s,%s,true,now())
                    ON CONFLICT (file_path) DO UPDATE
                        SET file_size = EXCLUDED.file_size,
                            file_mtime = EXCLUDED.file_mtime,
                            is_active = true,
                            last_seen_at = now()
                    RETURNING file_id
                """, (oid, table, root, path, st.st_size,
                      datetime.fromtimestamp(st.st_mtime, tz=timezone.utc)))
                seen[path] = cur.fetchone()[0]
    conn.commit()
    return seen


def _extract_one(job):
    path, columns = job
    return path, extract_index_coordinates(path, columns)


def build(conn, entries: list[dict], file_ids: dict[str, int], workers: int) -> None:
    # Group indexes by the lake root they cover so each file is read once for
    # all columns indexed on it.
    by_root: dict[str, list[dict]] = {}
    for e in entries:
        by_root.setdefault(e["data_lake_path"], []).append(e)

    for root, group in by_root.items():
        columns = [e["column_name"] for e in group]
        col_to_index = {e["column_name"]: e for e in group}
        files = list_parquet(root)
        print(f"[index] {root}: {len(files)} files, columns={columns}", flush=True)

        jobs = [(f, columns) for f in files]
        with ProcessPoolExecutor(max_workers=workers) as pool:
            for path, extracted in pool.map(_extract_one, jobs, chunksize=1):
                fid = file_ids[path]
                with conn.cursor() as cur:
                    for column, postings in extracted.items():
                        entry = col_to_index[column]
                        storage = sql.Identifier(entry["storage_table"])
                        cur.execute(
                            sql.SQL("DELETE FROM {} WHERE file_id = %s").format(storage),
                            (fid,))
                        if postings:
                            psycopg2.extras.execute_values(
                                cur,
                                sql.SQL("INSERT INTO {} (indexed_val, file_id, rowgroup_ids) "
                                        "VALUES %s ON CONFLICT (indexed_val, file_id) "
                                        "DO UPDATE SET rowgroup_ids = EXCLUDED.rowgroup_ids"
                                        ).format(storage).as_string(cur),
                                [(p["value"], fid, p["rowgroup_ids"]) for p in postings],
                                page_size=1000)
                        cur.execute("""
                            INSERT INTO public.gsi_index_file_state
                                (index_name, file_id, status, last_indexed_at)
                            VALUES (%s,%s,'indexed',now())
                            ON CONFLICT (index_name, file_id) DO UPDATE
                                SET status='indexed', last_indexed_at=now(), last_error=NULL
                        """, (entry["index_name"], fid))
                conn.commit()

    with conn.cursor() as cur:
        cur.execute("UPDATE public.gsi_registry SET status='ready', last_synced_at=now()")
    conn.commit()


def analyze(conn) -> None:
    """The GSI cost estimator reads pg_statistic. Without ANALYZE the planner
    has no ndistinct and falls back to sqrt(selectivity), which changes which
    path it picks. Running it is part of the measured setup, not a thumb on
    the scale."""
    with conn.cursor() as cur:
        for t in ("public.customers", "public.products", "public.transactions"):
            print(f"[analyze] {t}", flush=True)
            cur.execute(f"ANALYZE {t}")
        cur.execute("ANALYZE public.gsi_file_catalog")
    conn.commit()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default=os.environ.get("PGHOST", "localhost"))
    ap.add_argument("--port", default=os.environ.get("PGPORT", "5432"))
    ap.add_argument("--dbname", default=os.environ.get("PGDATABASE", "gsi"))
    ap.add_argument("--user", default=os.environ.get("PGUSER", "postgres"))
    ap.add_argument("--password", default=os.environ.get("PGPASSWORD", "gsi"))
    ap.add_argument("--workers", type=int, default=max(1, (os.cpu_count() or 2) // 2))
    ap.add_argument("--stats-out", default="results/index_build.json")
    args = ap.parse_args()

    conn = connect(args)
    entries = load_registry(conn)
    if not entries:
        print("no rows in public.gsi_registry; run bench/00_schema.sql first", file=sys.stderr)
        return 1

    t0 = time.perf_counter()
    file_ids = catalogue_files(conn, entries)
    build(conn, entries, file_ids, args.workers)
    build_seconds = time.perf_counter() - t0

    analyze(conn)

    sizes = {}
    with conn.cursor() as cur:
        for e in entries:
            cur.execute(
                sql.SQL("SELECT count(*), pg_total_relation_size(%s) FROM {}").format(
                    sql.Identifier(e["storage_table"])), (e["storage_table"],))
            rows, bytes_ = cur.fetchone()
            sizes[e["index_name"]] = {"postings_rows": rows, "bytes": bytes_}

    stats = {
        "build_seconds": round(build_seconds, 3),
        "files_indexed": len(file_ids),
        "workers": args.workers,
        "indexes": sizes,
    }
    Path(args.stats_out).parent.mkdir(parents=True, exist_ok=True)
    Path(args.stats_out).write_text(json.dumps(stats, indent=2))
    print(json.dumps(stats, indent=2))
    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
