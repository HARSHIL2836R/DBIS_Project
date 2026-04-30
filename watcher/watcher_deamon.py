from __future__ import annotations

from concurrent.futures import Future, ThreadPoolExecutor
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from queue import Empty, Queue
from threading import RLock
from typing import Any

import psycopg2
from psycopg2 import sql
from psycopg2.extras import DictCursor, execute_values
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

from extractor import extract_index_coordinates


def load_env_file() -> None:
    env_path = Path(__file__).with_name(".env")

    if not env_path.exists():
        return

    for raw_line in env_path.read_text().splitlines():
        line = raw_line.strip()

        if not line or line.startswith("#"):
            continue

        if line.startswith("export "):
            line = line[len("export ") :]

        if "=" not in line:
            continue

        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


load_env_file()


def normalize_path(path: str) -> str:
    return os.path.realpath(os.path.abspath(path))


def utc_datetime_from_timestamp(timestamp: float) -> datetime:
    return datetime.fromtimestamp(timestamp, tz=timezone.utc)


WATCHER_LOOP_INTERVAL = float(os.environ.get("WATCHER_LOOP_INTERVAL", "1.0"))
REGISTRY_REFRESH_SECONDS = int(os.environ.get("REGISTRY_REFRESH_SECONDS", "60"))
RECONCILE_SECONDS = int(os.environ.get("RECONCILE_SECONDS", "300"))
FILE_STABLE_SECONDS = float(os.environ.get("FILE_STABLE_SECONDS", "2.0"))
INDEX_WORKERS = max(1, int(os.environ.get("INDEX_WORKERS", "4")))

DB_PARAMS = {
    "dbname": os.environ.get("DB_NAME", "postgres"),
    "user": os.environ.get("DB_USER", "postgres"),
    "password": os.environ.get("DB_PASSWORD", "password"),
    "host": os.environ.get("DB_HOST", "localhost"),
    "port": os.environ.get("DB_PORT", "5432"),
}


@dataclass(frozen=True)
class IndexConfig:
    index_name: str
    foreigntable_oid: int
    table_name: str
    column_name: str
    column_type: str
    storage_table: str
    data_lake_path: str
    status: str


@dataclass(frozen=True)
class FileEvent:
    kind: str
    path: str


@dataclass
class PendingFile:
    reason: str
    last_signature: tuple[int, int] | None = None
    stable_since: float | None = None


@dataclass
class WatcherState:
    observer: Observer
    handler: FileSystemEventHandler
    event_queue: Queue[FileEvent] = field(default_factory=Queue)
    pending_files: dict[str, PendingFile] = field(default_factory=dict)
    active_configs: dict[str, IndexConfig] = field(default_factory=dict)
    configs_by_root: dict[str, list[IndexConfig]] = field(default_factory=dict)
    watch_handles: dict[str, Any] = field(default_factory=dict)
    in_progress_files: set[str] = field(default_factory=set)
    running_tasks: dict[Future[Any], str] = field(default_factory=dict)
    queue_complete_logged: bool = False
    lock: RLock = field(default_factory=RLock)


class DataLakeHandler(FileSystemEventHandler):
    def __init__(self, event_queue: Queue[FileEvent]) -> None:
        super().__init__()
        self.event_queue = event_queue

    def _queue_event(self, kind: str, path: str) -> None:
        if path.endswith(".parquet"):
            self.event_queue.put(FileEvent(kind=kind, path=normalize_path(path)))

    def on_created(self, event) -> None:
        if not event.is_directory:
            self._queue_event("upsert", event.src_path)

    def on_modified(self, event) -> None:
        if not event.is_directory:
            self._queue_event("upsert", event.src_path)

    def on_moved(self, event) -> None:
        if event.is_directory:
            return

        self._queue_event("delete", event.src_path)
        self._queue_event("upsert", event.dest_path)

    def on_deleted(self, event) -> None:
        if not event.is_directory:
            self._queue_event("delete", event.src_path)


def get_connection():
    return psycopg2.connect(**DB_PARAMS)


def table_identifier(table_name: str) -> sql.Identifier:
    if "." in table_name:
        schema_name, relation_name = table_name.split(".", 1)
        return sql.Identifier(schema_name, relation_name)

    return sql.Identifier(table_name)


def fetch_registry_rows(conn, statuses: tuple[str, ...]) -> list[dict]:
    with conn.cursor(cursor_factory=DictCursor) as cur:
        cur.execute(
            """
            SELECT index_name,
                   foreigntable_oid,
                   table_name,
                   column_name,
                   column_type::text AS column_type,
                   storage_table,
                   data_lake_path,
                   status
              FROM public.gsi_registry
             WHERE status = ANY(%s)
             ORDER BY index_name
            """,
            (list(statuses),),
        )
        return [dict(row) for row in cur.fetchall()]


def index_config_from_row(row: dict) -> IndexConfig:
    return IndexConfig(
        index_name=row["index_name"],
        foreigntable_oid=row["foreigntable_oid"],
        table_name=row["table_name"],
        column_name=row["column_name"],
        column_type=row["column_type"],
        storage_table=row["storage_table"],
        data_lake_path=normalize_path(row["data_lake_path"]),
        status=row["status"],
    )


def resolve_configs_for_path(state: WatcherState, file_path: str) -> list[IndexConfig]:
    file_path = normalize_path(file_path)
    matched_root = None

    for root in state.configs_by_root:
        try:
            if os.path.commonpath([file_path, root]) == root:
                if matched_root is None or len(root) > len(matched_root):
                    matched_root = root
        except ValueError:
            continue

    if matched_root is None:
        return []

    return state.configs_by_root[matched_root]


def schedule_file(state: WatcherState, file_path: str, reason: str) -> None:
    with state.lock:
        if not resolve_configs_for_path(state, file_path):
            return

        pending = state.pending_files.get(file_path)

        if pending is None:
            state.pending_files[file_path] = PendingFile(reason=reason)
            return

        pending.reason = reason
        pending.last_signature = None
        pending.stable_since = None


def queue_paths_from_db(conn, state: WatcherState) -> None:
    active_index_names = sorted(state.active_configs)

    if not active_index_names:
        return

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT fc.file_path
              FROM public.gsi_file_catalog fc
              JOIN public.gsi_index_file_state s
                ON s.file_id = fc.file_id
             WHERE fc.is_active
               AND s.index_name = ANY(%s)
               AND s.status IN ('pending', 'indexing', 'failed')
            """,
            (active_index_names,),
        )

        for (file_path,) in cur.fetchall():
            schedule_file(state, normalize_path(file_path), "db-pending")


def sync_observer_paths(state: WatcherState) -> None:
    with state.lock:
        active_roots = set(state.configs_by_root)
        watched_roots = set(state.watch_handles)

    for root in sorted(active_roots - watched_roots):
        os.makedirs(root, exist_ok=True)
        with state.lock:
            state.watch_handles[root] = state.observer.schedule(state.handler, root, recursive=True)
        print(f"Watching parquet root: {root}")

    for root in sorted(watched_roots - active_roots):
        with state.lock:
            handle = state.watch_handles.pop(root, None)
        if handle is not None:
            state.observer.unschedule(handle)
        print(f"Stopped watching parquet root: {root}")


def ensure_runtime_metadata(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT to_regclass('public.gsi_registry'),
                   to_regclass('public.gsi_file_catalog'),
                   to_regclass('public.gsi_index_file_state')
            """
        )
        registry_table, file_catalog_table, index_state_table = cur.fetchone()

    if not all([registry_table, file_catalog_table, index_state_table]):
        raise RuntimeError(
            "Required GSI tables are missing. Run test/starter.sql before starting the watcher."
        )


def requeue_inflight_states(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE public.gsi_index_file_state
               SET status = 'pending',
                   last_error = 'Watcher restarted before indexing completed'
             WHERE status = 'indexing'
            """
        )

    conn.commit()


def handle_dropping_indexes(conn) -> None:
    dropping_rows = fetch_registry_rows(conn, ("dropping",))

    if not dropping_rows:
        return

    with conn.cursor() as cur:
        for row in dropping_rows:
            config = index_config_from_row(row)
            print(f"Dropping index payload for {config.index_name}")

            cur.execute(
                sql.SQL("DELETE FROM {}").format(table_identifier(config.storage_table))
            )
            cur.execute(
                "DELETE FROM public.gsi_index_file_state WHERE index_name = %s",
                (config.index_name,),
            )
            cur.execute(
                """
                UPDATE public.gsi_registry
                   SET status = 'dropped',
                       last_synced_at = now()
                 WHERE index_name = %s
                """,
                (config.index_name,),
            )

    conn.commit()


def load_active_registry(conn, state: WatcherState) -> None:
    registry_rows = fetch_registry_rows(conn, ("building", "ready"))
    with state.lock:
        state.active_configs = {
            row["index_name"]: index_config_from_row(row) for row in registry_rows
        }

        configs_by_root: dict[str, list[IndexConfig]] = {}

        for config in state.active_configs.values():
            configs_by_root.setdefault(config.data_lake_path, []).append(config)

        state.configs_by_root = {
            root: sorted(configs, key=lambda item: item.index_name)
            for root, configs in configs_by_root.items()
        }

        for file_path in list(state.pending_files):
            if not resolve_configs_for_path(state, file_path):
                state.pending_files.pop(file_path, None)


def list_parquet_files(root: str) -> list[str]:
    parquet_files = []

    for current_root, _, names in os.walk(root):
        for name in names:
            if name.endswith(".parquet"):
                parquet_files.append(normalize_path(os.path.join(current_root, name)))

    parquet_files.sort()
    return parquet_files


def ensure_file_catalog_entry(conn, config: IndexConfig, file_path: str, stat_result) -> tuple[int, bool]:
    file_mtime = utc_datetime_from_timestamp(stat_result.st_mtime)

    with conn.cursor(cursor_factory=DictCursor) as cur:
        cur.execute(
            """
            SELECT file_id,
                   file_size,
                   extract(epoch from file_mtime) AS file_mtime_epoch,
                   is_active
              FROM public.gsi_file_catalog
             WHERE file_path = %s
            """,
            (file_path,),
        )
        row = cur.fetchone()

        if row is None:
            cur.execute(
                """
                INSERT INTO public.gsi_file_catalog (
                    foreigntable_oid,
                    table_name,
                    data_lake_path,
                    file_path,
                    file_size,
                    file_mtime,
                    is_active,
                    last_seen_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, true, now())
                RETURNING file_id
                """,
                (
                    config.foreigntable_oid,
                    config.table_name,
                    config.data_lake_path,
                    file_path,
                    stat_result.st_size,
                    file_mtime,
                ),
            )
            return cur.fetchone()[0], True

        previous_mtime = float(row["file_mtime_epoch"])
        changed = (
            row["file_size"] != stat_result.st_size
            or abs(previous_mtime - stat_result.st_mtime) > 0.000001
            or not row["is_active"]
        )

        cur.execute(
            """
            UPDATE public.gsi_file_catalog
               SET foreigntable_oid = %s,
                   table_name = %s,
                   data_lake_path = %s,
                   file_size = %s,
                   file_mtime = %s,
                   is_active = true,
                   last_seen_at = now()
             WHERE file_id = %s
            """,
            (
                config.foreigntable_oid,
                config.table_name,
                config.data_lake_path,
                stat_result.st_size,
                file_mtime,
                row["file_id"],
            ),
        )
        return row["file_id"], changed


def ensure_index_states_for_file(
    conn,
    file_id: int,
    configs: list[IndexConfig],
    force_pending: bool,
) -> list[IndexConfig]:
    if not configs:
        return []

    index_names = [config.index_name for config in configs]

    with conn.cursor(cursor_factory=DictCursor) as cur:
        cur.execute(
            """
            SELECT index_name, status
              FROM public.gsi_index_file_state
             WHERE file_id = %s
               AND index_name = ANY(%s)
            """,
            (file_id, index_names),
        )
        existing_states = {row["index_name"]: row["status"] for row in cur.fetchall()}

        pending_configs = []

        for config in configs:
            current_status = existing_states.get(config.index_name)

            if current_status is None:
                cur.execute(
                    """
                    INSERT INTO public.gsi_index_file_state (
                        index_name,
                        file_id,
                        status,
                        last_error
                    )
                    VALUES (%s, %s, 'pending', NULL)
                    """,
                    (config.index_name, file_id),
                )
                pending_configs.append(config)
                continue

            if force_pending or current_status in {"failed", "deleted"}:
                cur.execute(
                    """
                    UPDATE public.gsi_index_file_state
                       SET status = 'pending',
                           last_error = NULL
                     WHERE index_name = %s
                       AND file_id = %s
                    """,
                    (config.index_name, file_id),
                )
                pending_configs.append(config)
                continue

            if current_status == "pending":
                pending_configs.append(config)

        if pending_configs:
            cur.execute(
                """
                UPDATE public.gsi_registry
                   SET status = 'building'
                 WHERE index_name = ANY(%s)
                   AND status NOT IN ('dropping', 'dropped')
                """,
                ([config.index_name for config in pending_configs],),
            )

    return pending_configs


def mark_index_states(conn, configs: list[IndexConfig], file_id: int, status: str, error: str | None = None) -> None:
    if not configs:
        return

    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE public.gsi_index_file_state
               SET status = %s,
                   last_error = %s,
                   last_indexed_at = CASE WHEN %s = 'indexed' THEN now() ELSE last_indexed_at END
             WHERE file_id = %s
               AND index_name = ANY(%s)
            """,
            (status, error, status, file_id, [config.index_name for config in configs]),
        )


def rewrite_index_entries(cur, config: IndexConfig, file_id: int, postings: list[dict]) -> None:
    stage_table_name = f"tmp_{config.index_name}_stage"
    stage_identifier = sql.Identifier(stage_table_name)
    storage_identifier = table_identifier(config.storage_table)

    cur.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(stage_identifier))
    cur.execute(
        sql.SQL(
            "CREATE TEMP TABLE {} (LIKE {} INCLUDING DEFAULTS) ON COMMIT DROP"
        ).format(stage_identifier, storage_identifier)
    )

    if postings:
        query = sql.SQL(
            "INSERT INTO {} (indexed_val, file_id, rowgroup_ids) VALUES %s"
        ).format(stage_identifier)
        values = [
            (posting["value"], file_id, list(posting["rowgroup_ids"]))
            for posting in postings
        ]
        execute_values(cur, query.as_string(cur.connection), values)

    cur.execute(
        sql.SQL("DELETE FROM {} WHERE file_id = %s").format(storage_identifier),
        (file_id,),
    )
    cur.execute(
        sql.SQL(
            """
            INSERT INTO {} (indexed_val, file_id, rowgroup_ids)
            SELECT indexed_val, file_id, rowgroup_ids
              FROM {}
            """
        ).format(storage_identifier, stage_identifier)
    )


def advance_registry_statuses(conn, configs: list[IndexConfig]) -> None:
    unique_configs = {config.index_name: config for config in configs}

    with conn.cursor() as cur:
        for config in unique_configs.values():
            cur.execute(
                """
                SELECT EXISTS (
                    SELECT 1
                      FROM public.gsi_file_catalog fc
                      LEFT JOIN public.gsi_index_file_state s
                        ON s.file_id = fc.file_id
                       AND s.index_name = %s
                     WHERE fc.foreigntable_oid = %s
                       AND fc.is_active
                       AND COALESCE(s.status, 'pending') <> 'indexed'
                )
                """,
                (config.index_name, config.foreigntable_oid),
            )
            has_outstanding_work = cur.fetchone()[0]

            cur.execute(
                """
                UPDATE public.gsi_registry
                   SET status = CASE
                       WHEN %s THEN 'building'
                       ELSE 'ready'
                   END,
                       last_synced_at = CASE
                           WHEN %s THEN last_synced_at
                           ELSE now()
                       END
                 WHERE index_name = %s
                   AND status NOT IN ('dropping', 'dropped')
                """,
                (has_outstanding_work, has_outstanding_work, config.index_name),
            )


def cleanup_deleted_file(conn, file_path: str) -> None:
    with conn.cursor(cursor_factory=DictCursor) as cur:
        cur.execute(
            """
            SELECT fc.file_id,
                   gr.index_name,
                   gr.storage_table,
                   gr.foreigntable_oid
              FROM public.gsi_file_catalog fc
              LEFT JOIN public.gsi_index_file_state s
                ON s.file_id = fc.file_id
              LEFT JOIN public.gsi_registry gr
                ON gr.index_name = s.index_name
             WHERE fc.file_path = %s
            """,
            (file_path,),
        )
        rows = cur.fetchall()

        if not rows:
            return

        file_id = rows[0]["file_id"]
        configs = []

        cur.execute(
            """
            UPDATE public.gsi_file_catalog
               SET is_active = false,
                   last_seen_at = now()
             WHERE file_id = %s
            """,
            (file_id,),
        )

        for row in rows:
            if row["index_name"] is None or row["storage_table"] is None:
                continue

            configs.append(
                IndexConfig(
                    index_name=row["index_name"],
                    foreigntable_oid=row["foreigntable_oid"],
                    table_name="",
                    column_name="",
                    column_type="",
                    storage_table=row["storage_table"],
                    data_lake_path="",
                    status="",
                )
            )
            cur.execute(
                sql.SQL("DELETE FROM {} WHERE file_id = %s").format(
                    table_identifier(row["storage_table"])
                ),
                (file_id,),
            )
            cur.execute(
                """
                UPDATE public.gsi_index_file_state
                   SET status = 'deleted',
                       last_error = NULL
                 WHERE index_name = %s
                   AND file_id = %s
                """,
                (row["index_name"], file_id),
            )

    if configs:
        advance_registry_statuses(conn, configs)


def reconcile_root(conn, state: WatcherState, root: str, configs: list[IndexConfig]) -> None:
    primary_config = configs[0]
    actual_files = {}

    for path in list_parquet_files(root):
        try:
            actual_files[path] = os.stat(path)
        except FileNotFoundError:
            continue

    with conn.cursor(cursor_factory=DictCursor) as cur:
        cur.execute(
            """
            SELECT file_id,
                   file_path,
                   file_size,
                   extract(epoch from file_mtime) AS file_mtime_epoch,
                   is_active
              FROM public.gsi_file_catalog
             WHERE foreigntable_oid = %s
               AND data_lake_path = %s
            """,
            (primary_config.foreigntable_oid, root),
        )
        existing_rows = {row["file_path"]: row for row in cur.fetchall()}

    touched_configs: list[IndexConfig] = []

    for file_path, stat_result in actual_files.items():
        file_id, file_changed = ensure_file_catalog_entry(conn, primary_config, file_path, stat_result)
        pending_configs = ensure_index_states_for_file(conn, file_id, configs, file_changed)

        if pending_configs:
            touched_configs.extend(pending_configs)
            schedule_file(state, file_path, "bootstrap")

        existing_rows.pop(file_path, None)

    for file_path, row in existing_rows.items():
        if row["is_active"]:
            cleanup_deleted_file(conn, file_path)

    if touched_configs:
        advance_registry_statuses(conn, touched_configs)

    conn.commit()


def refresh_registry(state: WatcherState) -> None:
    with get_connection() as conn:
        ensure_runtime_metadata(conn)
        handle_dropping_indexes(conn)
        load_active_registry(conn, state)
        sync_observer_paths(state)

        for configs in state.configs_by_root.values():
            for config in configs:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO public.gsi_index_file_state (index_name, file_id, status)
                        SELECT %s, fc.file_id, CASE
                            WHEN fc.is_active THEN 'pending'
                            ELSE 'deleted'
                        END
                          FROM public.gsi_file_catalog fc
                         WHERE fc.foreigntable_oid = %s
                        ON CONFLICT (index_name, file_id) DO NOTHING
                        """,
                        (config.index_name, config.foreigntable_oid),
                    )
            conn.commit()

        if state.active_configs:
            advance_registry_statuses(conn, list(state.active_configs.values()))
            conn.commit()

        queue_paths_from_db(conn, state)


def reconcile_all_paths(state: WatcherState) -> None:
    with state.lock:
        roots_snapshot = {
            root: list(configs) for root, configs in state.configs_by_root.items()
        }

    if not roots_snapshot:
        return

    with get_connection() as conn:
        for root, configs in roots_snapshot.items():
            print(f"Reconciling parquet root: {root}")
            reconcile_root(conn, state, root, configs)


def handle_delete_event(state: WatcherState, file_path: str) -> None:
    with state.lock:
        state.pending_files.pop(file_path, None)
        state.in_progress_files.discard(file_path)

    with get_connection() as conn:
        cleanup_deleted_file(conn, file_path)
        conn.commit()

    print(f"Removed stale GSI rows for deleted parquet file: {file_path}")


def drain_event_queue(state: WatcherState) -> None:
    while True:
        try:
            event = state.event_queue.get_nowait()
        except Empty:
            break

        if event.kind == "delete":
            handle_delete_event(state, event.path)
            continue

        schedule_file(state, event.path, event.kind)


def process_file(state: WatcherState, file_path: str) -> None:
    with state.lock:
        configs = list(resolve_configs_for_path(state, file_path))

    if not configs:
        with state.lock:
            state.pending_files.pop(file_path, None)
            state.in_progress_files.discard(file_path)
        return

    try:
        stat_result = os.stat(file_path)
    except FileNotFoundError:
        with state.lock:
            state.pending_files.pop(file_path, None)
            state.in_progress_files.discard(file_path)
        handle_delete_event(state, file_path)
        return

    primary_config = configs[0]

    with get_connection() as conn:
        file_id, file_changed = ensure_file_catalog_entry(conn, primary_config, file_path, stat_result)
        pending_configs = ensure_index_states_for_file(conn, file_id, configs, file_changed)

        if not pending_configs:
            conn.commit()
            with state.lock:
                state.pending_files.pop(file_path, None)
                state.in_progress_files.discard(file_path)
            return

        mark_index_states(conn, pending_configs, file_id, "indexing")
        conn.commit()

    target_columns = sorted({config.column_name for config in pending_configs})

    try:
        extracted_by_column = extract_index_coordinates(file_path, target_columns)
    except Exception as exc:
        error_message = str(exc)

        with get_connection() as conn:
            mark_index_states(conn, pending_configs, file_id, "failed", error_message)
            advance_registry_statuses(conn, pending_configs)
            conn.commit()

        print(f"Index extraction failed for {file_path}: {error_message}")
        with state.lock:
            state.pending_files.pop(file_path, None)
            state.in_progress_files.discard(file_path)
        return

    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                for config in pending_configs:
                    postings = extracted_by_column.get(config.column_name, [])
                    rewrite_index_entries(cur, config, file_id, postings)

                mark_index_states(conn, pending_configs, file_id, "indexed")
                cur.execute(
                    """
                    UPDATE public.gsi_file_catalog
                       SET file_size = %s,
                           file_mtime = %s,
                           is_active = true,
                           last_seen_at = now()
                     WHERE file_id = %s
                    """,
                    (
                        stat_result.st_size,
                        utc_datetime_from_timestamp(stat_result.st_mtime),
                        file_id,
                    ),
                )
                advance_registry_statuses(conn, pending_configs)
            conn.commit()

        print(
            f"Indexed {file_path} into {', '.join(config.index_name for config in pending_configs)}"
        )
    except Exception as exc:
        error_message = str(exc)

        with get_connection() as conn:
            mark_index_states(conn, pending_configs, file_id, "failed", error_message)
            advance_registry_statuses(conn, pending_configs)
            conn.commit()

        print(f"PostgreSQL GSI write failed for {file_path}: {error_message}")

    with state.lock:
        state.pending_files.pop(file_path, None)
        state.in_progress_files.discard(file_path)


def collect_completed_tasks(state: WatcherState) -> None:
    # Snapshot done futures and remove them from running_tasks in one lock
    # acquisition instead of one acquisition per completed future.
    with state.lock:
        completed = {
            future: fp
            for future, fp in state.running_tasks.items()
            if future.done()
        }
        for future in completed:
            state.running_tasks.pop(future, None)

    for future, file_path in completed.items():
        try:
            future.result()
        except Exception as exc:
            print(f"Worker crashed for {file_path}: {exc}")
            with state.lock:
                state.pending_files.pop(file_path, None)
                state.in_progress_files.discard(file_path)


def process_stable_files(state: WatcherState, executor: ThreadPoolExecutor) -> None:
    now = time.time()
    ready_paths = []

    # Single snapshot: also exclude already-in-progress files here to avoid a
    # second per-file lock acquisition in the loop below.
    with state.lock:
        pending_snapshot = [
            (fp, pf)
            for fp, pf in state.pending_files.items()
            if fp not in state.in_progress_files
        ]

    for file_path, pending in pending_snapshot:
        try:
            stat_result = os.stat(file_path)
        except FileNotFoundError:
            with state.lock:
                state.pending_files.pop(file_path, None)
                state.in_progress_files.discard(file_path)
            handle_delete_event(state, file_path)
            continue

        signature = (stat_result.st_size, stat_result.st_mtime_ns)

        # Mutate PendingFile fields inside the lock to avoid a data race with
        # workers that call pending_files.pop() / in_progress_files.discard().
        with state.lock:
            pending = state.pending_files.get(file_path)
            if pending is None:
                continue
            if pending.last_signature != signature:
                pending.last_signature = signature
                pending.stable_since = now
                continue
            if pending.stable_since is None:
                pending.stable_since = now
                continue
            if now - pending.stable_since >= FILE_STABLE_SECONDS:
                ready_paths.append(file_path)

    # Dispatch: check + add + submit in one atomic block to prevent the same
    # file being submitted twice across loop iterations (TOCTOU).
    with state.lock:
        for file_path in sorted(ready_paths):
            if file_path in state.in_progress_files:
                continue
            if len(state.running_tasks) >= INDEX_WORKERS:
                break
            state.in_progress_files.add(file_path)
            future = executor.submit(process_file, state, file_path)
            state.running_tasks[future] = file_path


def log_queue_completion_once(state: WatcherState) -> None:
    # Queue may receive filesystem events asynchronously, so check it before
    # acquiring the lock to avoid a needless acquisition when there is work.
    if not state.event_queue.empty():
        with state.lock:
            state.queue_complete_logged = False
        return

    # Single lock acquisition: has_work check and queue_complete_logged write
    # are now atomic, preventing a spurious "queue complete" log if a worker
    # finishes between two separate acquisitions.
    with state.lock:
        has_work = bool(state.pending_files or state.in_progress_files or state.running_tasks)
        if has_work:
            state.queue_complete_logged = False
            return
        if state.queue_complete_logged:
            return
        state.queue_complete_logged = True

    print("Queue complete. Waiting for new files...")


def start_watcher() -> None:
    observer = Observer()
    state = WatcherState(
        observer=observer,
        handler=DataLakeHandler(event_queue=Queue()),
    )
    state.event_queue = state.handler.event_queue

    with get_connection() as conn:
        ensure_runtime_metadata(conn)
        requeue_inflight_states(conn)

    refresh_registry(state)
    reconcile_all_paths(state)
    observer.start()

    print(f"Watcher daemon started with {INDEX_WORKERS} worker(s).")

    next_registry_refresh = time.time() + REGISTRY_REFRESH_SECONDS
    next_reconcile = time.time() + RECONCILE_SECONDS
    executor = ThreadPoolExecutor(max_workers=INDEX_WORKERS, thread_name_prefix="gsi-worker")

    try:
        while True:
            drain_event_queue(state)
            collect_completed_tasks(state)
            process_stable_files(state, executor)

            now = time.time()

            if now >= next_registry_refresh:
                refresh_registry(state)
                next_registry_refresh = now + REGISTRY_REFRESH_SECONDS

            if now >= next_reconcile:
                reconcile_all_paths(state)
                next_reconcile = now + RECONCILE_SECONDS

            log_queue_completion_once(state)

            time.sleep(WATCHER_LOOP_INTERVAL)
    except KeyboardInterrupt:
        print("Watcher daemon stopped gracefully.")
    finally:
        observer.stop()
        observer.join()
        executor.shutdown(wait=True)


if __name__ == "__main__":
    start_watcher()