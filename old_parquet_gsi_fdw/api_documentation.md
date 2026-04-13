# `parquet_gsi_fdw` — API Reference and Documentation Mapping

> **Project:** Global Secondary Indices for Parquet Files in Data Lakes
> **File under review:** `parquet_gsi_fdw.c`
> **PostgreSQL version target:** 15 / 16
> **Source tree reference:** `contrib/file_fdw/` and `contrib/postgres_fdw/`

---

## Table of Contents

1. [System Macros](#1-system-macros)
2. [FDW Handler and Validator](#2-fdw-handler-and-validator)
3. [Planner Callbacks](#3-planner-callbacks)
4. [Executor Callbacks](#4-executor-callbacks)
5. [Custom Options — `data_lake_path`](#5-custom-options--data_lake_path)
6. [Helper Utilities](#6-helper-utilities)
7. [Quick Reference Table](#7-quick-reference-table)

---

## 1. System Macros

### `PG_MODULE_MAGIC`

```c
PG_MODULE_MAGIC;
```

**Purpose:** Embeds a "magic block" into the compiled shared object (`.so` / `.dll`). When PostgreSQL dynamically loads the extension, it reads this block to verify that the binary was compiled against a compatible major version. If the block is absent or mismatched, the server refuses to load the file, preventing silent data corruption or crashes from ABI mismatches.

**Where it must appear:** Once, at file scope, after all `#include` directives and before any function definitions. It must never appear inside a function.

**PostgreSQL documentation:**
- [C-Language Functions — The `PG_MODULE_MAGIC` macro](https://www.postgresql.org/docs/current/xfunc-c.html#XFUNC-C-PGMODULEMAGIC)

**Source tree analog:**
- [`contrib/file_fdw/file_fdw.c`](https://github.com/postgres/postgres/blob/master/contrib/file_fdw/file_fdw.c) — line immediately after includes
- [`contrib/postgres_fdw/postgres_fdw.c`](https://github.com/postgres/postgres/blob/master/contrib/postgres_fdw/postgres_fdw.c)

---

### `PG_FUNCTION_INFO_V1`

```c
PG_FUNCTION_INFO_V1(parquet_gsi_fdw_handler);
PG_FUNCTION_INFO_V1(parquet_gsi_fdw_validator);
```

**Purpose:** Declares that the named function uses the **Version-1 calling convention** — the only convention supported for dynamically-loaded C functions in modern PostgreSQL. The macro expands to a static descriptor struct (`PGFunction`) that the fmgr (function manager) reads at call time to determine argument/return handling. Without this declaration, PostgreSQL cannot safely call the function from SQL.

**Not required for:** Internal (built-in) functions, which are assumed to use version-1 by convention.

**PostgreSQL documentation:**
- [Version 1 Calling Conventions](https://www.postgresql.org/docs/current/xfunc-c.html#XFUNC-C-V1-CALL-CONV)
- [Function Manager (`fmgr`) internals](https://www.postgresql.org/docs/current/xfunc-c.html#XFUNC-C-FUNCTION-ARGS)

**Source tree analog:**
- `PG_FUNCTION_INFO_V1(file_fdw_handler)` in [`contrib/file_fdw/file_fdw.c`](https://github.com/postgres/postgres/blob/master/contrib/file_fdw/file_fdw.c)

---

## 2. FDW Handler and Validator

### `parquet_gsi_fdw_handler`

```c
Datum parquet_gsi_fdw_handler(PG_FUNCTION_ARGS)
{
    FdwRoutine *fdw_routine = makeNode(FdwRoutine);
    fdw_routine->GetForeignRelSize  = parquet_gsi_get_foreign_rel_size;
    fdw_routine->GetForeignPaths    = parquet_gsi_get_foreign_paths;
    fdw_routine->GetForeignPlan     = parquet_gsi_get_foreign_plan;
    fdw_routine->BeginForeignScan   = parquet_gsi_begin_foreign_scan;
    fdw_routine->IterateForeignScan = parquet_gsi_iterate_foreign_scan;
    fdw_routine->ReScanForeignScan  = parquet_gsi_rescan_foreign_scan;
    fdw_routine->EndForeignScan     = parquet_gsi_end_foreign_scan;
    PG_RETURN_POINTER(fdw_routine);
}
```

**Purpose:** The primary entry point for the FDW. PostgreSQL calls this function — referenced in `pg_foreign_data_wrapper.fdwhandler` — to obtain an `FdwRoutine` node. This node is a struct of function pointers covering every phase of query planning and execution. The planner and executor then invoke these callbacks at the appropriate stage.

**Registration in SQL:**
```sql
CREATE FUNCTION parquet_gsi_fdw_handler()
RETURNS fdw_handler AS 'parquet_gsi_fdw' LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER parquet_gsi_fdw HANDLER parquet_gsi_fdw_handler;
```

**PostgreSQL documentation:**
- [Writing a Foreign Data Wrapper — FDW Handler](https://www.postgresql.org/docs/current/fdw-functions.html)
- [`FdwRoutine` struct reference](https://www.postgresql.org/docs/current/fdw-callbacks.html)

**Source tree analog:**
- `file_fdw_handler()` in [`contrib/file_fdw/file_fdw.c`](https://github.com/postgres/postgres/blob/master/contrib/file_fdw/file_fdw.c)
- `postgres_fdw_handler()` in [`contrib/postgres_fdw/postgres_fdw.c`](https://github.com/postgres/postgres/blob/master/contrib/postgres_fdw/postgres_fdw.c)

---

### `parquet_gsi_fdw_validator`

```c
Datum parquet_gsi_fdw_validator(PG_FUNCTION_ARGS)
{
    List     *options_list = untransformRelOptions(PG_GETARG_DATUM(0));
    ListCell *cell = NULL;
    foreach(cell, options_list)
    {
        DefElem *option = (DefElem *) lfirst(cell);
        if (strcmp(option->defname, PARQUET_GSI_OPTION_DATA_LAKE_PATH) != 0)
            ereport(ERROR, (errcode(ERRCODE_FDW_INVALID_OPTION_NAME), ...));
    }
    PG_RETURN_VOID();
}
```

**Purpose:** Called by PostgreSQL whenever a user runs `CREATE SERVER`, `CREATE FOREIGN TABLE`, or `ALTER` equivalents that include `OPTIONS (...)`. The validator receives the raw options list as a `Datum` (encoded with `transformRelOptions`), decodes it via `untransformRelOptions`, and iterates through each `DefElem` to verify that only the key `data_lake_path` is present. Unknown keys raise `ERRCODE_FDW_INVALID_OPTION_NAME`.

**Key API calls used:**

| Call | Header | Purpose |
|---|---|---|
| `untransformRelOptions(datum)` | `access/reloptions.h` | Decodes the binary options `Datum` back into a `List *` of `DefElem` nodes |
| `lfirst(cell)` | `nodes/pg_list.h` | Dereferences the current `ListCell` pointer |
| `ereport(ERROR, ...)` | `utils/elog.h` | Raises a PostgreSQL error with SQLSTATE and human-readable hint |

**PostgreSQL documentation:**
- [FDW Validator Function](https://www.postgresql.org/docs/current/fdw-functions.html#FDW-FUNCTIONS-VALIDATOR)
- [`untransformRelOptions`](https://www.postgresql.org/docs/current/functions-info.html)
- [Error Reporting — `ereport`](https://www.postgresql.org/docs/current/error-message-reporting.html)

**Source tree analog:**
- `file_fdw_validator()` in [`contrib/file_fdw/file_fdw.c`](https://github.com/postgres/postgres/blob/master/contrib/file_fdw/file_fdw.c)

---

## 3. Planner Callbacks

These three callbacks are invoked sequentially by the query planner. Together they transform the abstract foreign table reference in the parse tree into a concrete, costed `ForeignScan` plan node.

---

### `GetForeignRelSize` → `parquet_gsi_get_foreign_rel_size`

```c
static void parquet_gsi_get_foreign_rel_size(
    PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid)
{
    baserel->rows = 1000;
}
```

**Purpose:** Provides the planner with a row-count estimate for the foreign table. The planner uses `baserel->rows` when computing path costs and choosing join strategies. The current implementation uses a hardcoded stub of 1 000 rows; a production implementation would inspect Parquet file metadata (row-group statistics) to derive a real estimate.

**Key fields written:**

| Field | Type | Meaning |
|---|---|---|
| `baserel->rows` | `double` | Estimated number of output rows after applying base restriction clauses |

**PostgreSQL documentation:**
- [`GetForeignRelSize`](https://www.postgresql.org/docs/current/fdw-callbacks.html#FDW-CALLBACKS-SCAN-GETFOREIGNRELSIZE)
- [`RelOptInfo` struct](https://www.postgresql.org/docs/current/planner-optimizer.html)

**Source tree analog:**
- `fileGetForeignRelSize()` in [`contrib/file_fdw/file_fdw.c`](https://github.com/postgres/postgres/blob/master/contrib/file_fdw/file_fdw.c)

---

### `GetForeignPaths` → `parquet_gsi_get_foreign_paths`

```c
static void parquet_gsi_get_foreign_paths(
    PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid)
{
    Cost startup_cost = 0;
    Cost total_cost   = baserel->rows;

    add_path(baserel,
        (Path *) create_foreignscan_path(root, baserel, NULL,
            baserel->rows, 0, startup_cost, total_cost,
            NIL, NULL, NULL, baserel->baserestrictinfo, NIL));
}
```

**Purpose:** Generates one or more candidate access paths for the planner's `add_path` mechanism. This implementation registers a single sequential-scan path. The startup cost is 0 (no index warm-up), and the total cost is naively set to `baserel->rows`. A production version might add predicate push-down paths with lower estimated costs to encourage filter selectivity at the storage layer.

**Key API calls used:**

| Call | Purpose |
|---|---|
| `create_foreignscan_path(...)` | Allocates and initialises a `ForeignPath` node with cost and output estimates |
| `add_path(baserel, path)` | Registers the path with the planner; cheaper paths may displace existing ones |

**PostgreSQL documentation:**
- [`GetForeignPaths`](https://www.postgresql.org/docs/current/fdw-callbacks.html#FDW-CALLBACKS-SCAN-GETFOREIGNPATHS)
- [`create_foreignscan_path`](https://www.postgresql.org/docs/current/fdw-helper.html)

**Source tree analog:**
- `fileGetForeignPaths()` in [`contrib/file_fdw/file_fdw.c`](https://github.com/postgres/postgres/blob/master/contrib/file_fdw/file_fdw.c)
- `postgresGetForeignPaths()` in [`contrib/postgres_fdw/postgres_fdw.c`](https://github.com/postgres/postgres/blob/master/contrib/postgres_fdw/postgres_fdw.c)

---

### `GetForeignPlan` → `parquet_gsi_get_foreign_plan`

```c
static ForeignScan *parquet_gsi_get_foreign_plan(
    PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid,
    ForeignPath *best_path, List *tlist, List *scan_clauses, Plan *outer_plan)
{
    scan_clauses = extract_actual_clauses(scan_clauses, false);
    return make_foreignscan(tlist, scan_clauses,
                            baserel->relid, NIL, NIL, NIL, NIL, outer_plan);
}
```

**Purpose:** Converts the winning `ForeignPath` into a `ForeignScan` plan node that the executor will run. `extract_actual_clauses` strips the `RestrictInfo` wrapper layer from the clause list, returning the bare boolean expressions the executor evaluates per row. `make_foreignscan` assembles the final plan node, embedding the target list and qual clauses.

**Key API calls used:**

| Call | Purpose |
|---|---|
| `extract_actual_clauses(list, false)` | Unwraps `RestrictInfo` envelopes; `false` = exclude pseudoconstants |
| `make_foreignscan(...)` | Allocates and populates a `ForeignScan` plan node |

**PostgreSQL documentation:**
- [`GetForeignPlan`](https://www.postgresql.org/docs/current/fdw-callbacks.html#FDW-CALLBACKS-SCAN-GETFOREIGNPLAN)
- [`make_foreignscan`](https://www.postgresql.org/docs/current/fdw-helper.html)
- [`extract_actual_clauses`](https://www.postgresql.org/docs/current/fdw-helper.html)

**Source tree analog:**
- `fileGetForeignPlan()` in [`contrib/file_fdw/file_fdw.c`](https://github.com/postgres/postgres/blob/master/contrib/file_fdw/file_fdw.c)

---

## 4. Executor Callbacks

These callbacks implement the execution lifecycle. PostgreSQL calls them in a defined order: `Begin` → (`Iterate` × N) → `End`, with `ReScan` optionally resetting the scan mid-stream.

---

### `BeginForeignScan` → `parquet_gsi_begin_foreign_scan`

```c
static void parquet_gsi_begin_foreign_scan(ForeignScanState *node, int eflags)
{
    if (eflags & EXEC_FLAG_EXPLAIN_ONLY) return;

    state->scan_cxt = AllocSetContextCreate(CurrentMemoryContext,
                          "parquet_gsi_fdw scan context", ALLOCSET_DEFAULT_SIZES);
    old_cxt = MemoryContextSwitchTo(state->scan_cxt);
    parquet_gsi_collect_parquet_files(data_lake_path, &state->files);
    state->next_file = list_head(state->files);
    MemoryContextSwitchTo(old_cxt);
    node->fdw_state = (void *) state;
}
```

**Purpose:** Performs one-time initialization before the first row is fetched. Critically, it:

1. Short-circuits immediately when `EXEC_FLAG_EXPLAIN_ONLY` is set — no I/O is performed during `EXPLAIN` without `ANALYZE`.
2. Creates a dedicated child `MemoryContext` (`scan_cxt`) so all per-scan allocations (the file path list) can be freed atomically in `EndForeignScan`.
3. Calls the recursive directory walker to populate `state->files`.
4. Stores the execution state in `node->fdw_state` for retrieval by subsequent callbacks.

**Key API calls used:**

| Call | Header | Purpose |
|---|---|---|
| `AllocSetContextCreate(...)` | `utils/memutils.h` | Creates a new child memory context |
| `MemoryContextSwitchTo(cxt)` | `utils/memutils.h` | Redirects `palloc` to the named context |
| `palloc0(size)` | `utils/palloc.h` | Zero-initialises an allocation in the current context |
| `RelationGetRelid(rel)` | `utils/rel.h` | Retrieves the `Oid` of the foreign table relation |

**PostgreSQL documentation:**
- [`BeginForeignScan`](https://www.postgresql.org/docs/current/fdw-callbacks.html#FDW-CALLBACKS-SCAN-BEGINFOREIGNSCAN)
- [Memory Context Management](https://www.postgresql.org/docs/current/spi-memory.html)
- [`EXEC_FLAG_EXPLAIN_ONLY`](https://www.postgresql.org/docs/current/executor.html)

**Source tree analog:**
- `fileBeginForeignScan()` in [`contrib/file_fdw/file_fdw.c`](https://github.com/postgres/postgres/blob/master/contrib/file_fdw/file_fdw.c)

---

### `IterateForeignScan` → `parquet_gsi_iterate_foreign_scan`

```c
static TupleTableSlot *parquet_gsi_iterate_foreign_scan(ForeignScanState *node)
{
    ExecClearTuple(slot);
    if (state == NULL || state->next_file == NULL)
        return slot;                          // empty slot = EOF signal

    slot->tts_values[0] = CStringGetTextDatum(file_path);
    slot->tts_isnull[0] = false;
    for (int i = 1; i < natts; i++) {
        slot->tts_values[i] = (Datum) 0;
        slot->tts_isnull[i] = true;
    }
    state->next_file = lnext(state->files, state->next_file);
    ExecStoreVirtualTuple(slot);
    return slot;
}
```

**Purpose:** Called repeatedly by the executor; each invocation must return exactly one row as a `TupleTableSlot`, or signal end-of-scan by returning an empty (cleared) slot. The current implementation:

- Places the file path string into attribute 0 (`file_path` column).
- Marks all remaining attributes as `NULL` (placeholder for future column projection from Parquet data).
- Advances `state->next_file` along the list after each row.

**Key API calls used:**

| Call | Purpose |
|---|---|
| `ExecClearTuple(slot)` | Marks the slot as empty; the EOF signal when returned as-is |
| `CStringGetTextDatum(str)` | Converts a C string to a PostgreSQL `text` `Datum` |
| `ExecStoreVirtualTuple(slot)` | Marks the slot as containing a valid virtual tuple |
| `lnext(list, cell)` | Advances to the next `ListCell` in a `List` |

**PostgreSQL documentation:**
- [`IterateForeignScan`](https://www.postgresql.org/docs/current/fdw-callbacks.html#FDW-CALLBACKS-SCAN-ITERATEFOREIGNSCAN)
- [Tuple Table Slots](https://www.postgresql.org/docs/current/executor.html)

**Source tree analog:**
- `fileIterateForeignScan()` in [`contrib/file_fdw/file_fdw.c`](https://github.com/postgres/postgres/blob/master/contrib/file_fdw/file_fdw.c)

---

### `ReScanForeignScan` → `parquet_gsi_rescan_foreign_scan`

```c
static void parquet_gsi_rescan_foreign_scan(ForeignScanState *node)
{
    if (state != NULL)
        state->next_file = list_head(state->files);
}
```

**Purpose:** Resets the scan cursor to the beginning of the file list without re-executing the directory walk. Called by the executor when a rescan is required — most commonly during a **nested-loop join**, where the inner side of the join must be rescanned once per outer row. Because the file list lives in `scan_cxt` (allocated in `BeginForeignScan`), it remains valid and only the `next_file` pointer needs resetting.

**Key API calls used:**

| Call | Purpose |
|---|---|
| `list_head(list)` | Returns the first `ListCell *` of a PostgreSQL `List` |

**PostgreSQL documentation:**
- [`ReScanForeignScan`](https://www.postgresql.org/docs/current/fdw-callbacks.html#FDW-CALLBACKS-SCAN-RESCANFOREIGNSCAN)

**Source tree analog:**
- `fileReScanForeignScan()` in [`contrib/file_fdw/file_fdw.c`](https://github.com/postgres/postgres/blob/master/contrib/file_fdw/file_fdw.c)

---

### `EndForeignScan` → `parquet_gsi_end_foreign_scan`

```c
static void parquet_gsi_end_foreign_scan(ForeignScanState *node)
{
    if (state == NULL) return;
    if (state->scan_cxt != NULL)
        MemoryContextDelete(state->scan_cxt);
    pfree(state);
    node->fdw_state = NULL;
}
```

**Purpose:** Performs all cleanup when the scan is fully complete (including after an error path or early executor termination). `MemoryContextDelete` recursively frees every allocation made inside `scan_cxt` — including the entire `state->files` list — in a single call. The state struct itself is freed with `pfree`. Setting `node->fdw_state = NULL` prevents double-free if the callback is somehow invoked again.

**Key API calls used:**

| Call | Purpose |
|---|---|
| `MemoryContextDelete(cxt)` | Frees the context and all its children/allocations recursively |
| `pfree(ptr)` | Frees a single palloc'd allocation in the current context |

**PostgreSQL documentation:**
- [`EndForeignScan`](https://www.postgresql.org/docs/current/fdw-callbacks.html#FDW-CALLBACKS-SCAN-ENDFOREIGNSCAN)
- [Memory Context Management](https://www.postgresql.org/docs/current/spi-memory.html)

**Source tree analog:**
- `fileEndForeignScan()` in [`contrib/file_fdw/file_fdw.c`](https://github.com/postgres/postgres/blob/master/contrib/file_fdw/file_fdw.c)

---

## 5. Custom Options — `data_lake_path`

The FDW exposes a single user-configurable option key: `data_lake_path`. It may be set at either the `SERVER` level or the `FOREIGN TABLE` level; table-level settings override server-level ones because `list_concat_copy` places table options after server options and the first match wins.

---

### Validation — `parquet_gsi_fdw_validator`

```c
#define PARQUET_GSI_OPTION_DATA_LAKE_PATH "data_lake_path"

// Inside the validator loop:
if (strcmp(option->defname, PARQUET_GSI_OPTION_DATA_LAKE_PATH) != 0)
    ereport(ERROR,
        (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
         errmsg("invalid option \"%s\"", option->defname),
         errhint("Valid option: %s", PARQUET_GSI_OPTION_DATA_LAKE_PATH)));
```

**Behaviour:** Any option key other than `data_lake_path` causes an immediate `ERROR`-level report with SQLSTATE `HV00B` (`FDW_INVALID_OPTION_NAME`) and a hint string listing the single valid key.

**PostgreSQL documentation:**
- [FDW Validator Function](https://www.postgresql.org/docs/current/fdw-functions.html#FDW-FUNCTIONS-VALIDATOR)
- [SQLSTATE error codes — Class HV (FDW)](https://www.postgresql.org/docs/current/errcodes-appendix.html)

---

### Extraction — `parquet_gsi_get_data_lake_path`

```c
#define PARQUET_GSI_DEFAULT_DATA_LAKE_PATH "/tmp/data_lake"

static char *parquet_gsi_get_data_lake_path(Oid foreigntableid)
{
    ForeignTable  *foreign_table  = GetForeignTable(foreigntableid);
    ForeignServer *foreign_server = GetForeignServer(foreign_table->serverid);
    List          *options = list_concat_copy(foreign_server->options,
                                              foreign_table->options);
    foreach(cell, options) {
        DefElem *option = (DefElem *) lfirst(cell);
        if (strcmp(option->defname, PARQUET_GSI_OPTION_DATA_LAKE_PATH) == 0)
            return defGetString(option);
    }
    return pstrdup(PARQUET_GSI_DEFAULT_DATA_LAKE_PATH);
}
```

**Behaviour:**

1. Resolves the `ForeignTable` and `ForeignServer` catalog objects from the supplied `Oid`.
2. Merges server and table options into a single list (table options are appended last, so they take precedence in a first-match search).
3. Returns the string value of the first `data_lake_path` key found, or falls back to `/tmp/data_lake`.

**Key API calls used:**

| Call | Header | Purpose |
|---|---|---|
| `GetForeignTable(oid)` | `foreign/foreign.h` | Looks up the `pg_foreign_table` catalog row |
| `GetForeignServer(oid)` | `foreign/foreign.h` | Looks up the `pg_foreign_server` catalog row |
| `list_concat_copy(l1, l2)` | `nodes/pg_list.h` | Returns a new list that is the concatenation of `l1` and `l2` |
| `defGetString(option)` | `commands/defrem.h` | Extracts the string value from a `DefElem` option node |
| `pstrdup(str)` | `utils/palloc.h` | Copies a C string into the current memory context |

**PostgreSQL documentation:**
- [`GetForeignTable` / `GetForeignServer`](https://www.postgresql.org/docs/current/fdw-helper.html)
- [`defGetString`](https://www.postgresql.org/docs/current/fdw-helper.html)
- [Foreign Data Wrapper Helper Functions](https://www.postgresql.org/docs/current/fdw-helper.html)

**Source tree analog:**
- `fileGetOptions()` in [`contrib/file_fdw/file_fdw.c`](https://github.com/postgres/postgres/blob/master/contrib/file_fdw/file_fdw.c)

---

## 6. Helper Utilities

### `parquet_gsi_collect_parquet_files`

```c
static void parquet_gsi_collect_parquet_files(const char *dir_path, List **files_out)
```

**Purpose:** Recursively traverses `dir_path` using PostgreSQL's `AllocateDir` / `ReadDir` / `FreeDir` wrappers (which integrate with the backend's resource cleanup mechanism). For each entry it calls `stat()` to distinguish directories from regular files. Directories trigger a recursive call; files ending in `.parquet` (case-insensitive via `pg_strcasecmp`) are appended to `*files_out` via `lappend`.

**Key API calls used:**

| Call | Header | Purpose |
|---|---|---|
| `AllocateDir(path)` | `storage/fd.h` | Opens a directory; registers it with the backend's resource tracker |
| `ReadDir(dir, path)` | `storage/fd.h` | Returns the next `dirent`; handles `EACCES` / `ENOENT` internally |
| `FreeDir(dir)` | `storage/fd.h` | Closes the directory and deregisters it |
| `psprintf(fmt, ...)` | `utils/builtins.h` | `palloc`-backed `sprintf`; returns a newly allocated string |
| `lappend(list, ptr)` | `nodes/pg_list.h` | Appends a pointer to a PostgreSQL `List` |
| `pg_strcasecmp(a, b)` | `utils/builtins.h` | Locale-independent, case-insensitive string comparison |

**PostgreSQL documentation:**
- [Virtual File Descriptors — `AllocateDir` / `ReadDir`](https://www.postgresql.org/docs/current/storage-file-layout.html)
- [List manipulation functions](https://www.postgresql.org/docs/current/xfunc-c.html)

---

### `parquet_gsi_has_parquet_suffix`

```c
static bool parquet_gsi_has_parquet_suffix(const char *name)
{
    const char *suffix   = ".parquet";
    size_t      name_len = strlen(name);
    size_t      sfx_len  = strlen(suffix);
    if (name_len < sfx_len) return false;
    return pg_strcasecmp(name + name_len - sfx_len, suffix) == 0;
}
```

**Purpose:** A pure predicate function that checks whether a filename ends with `.parquet`, using `pg_strcasecmp` so that `.PARQUET`, `.Parquet`, etc. are also accepted. No PostgreSQL catalog access is performed; this is a stateless string utility.

**PostgreSQL documentation:**
- [`pg_strcasecmp`](https://www.postgresql.org/docs/current/functions-string.html)

---

## 7. Quick Reference Table

| Component | Docs Section | Canonical URL | Source Analog |
|---|---|---|---|
| `PG_MODULE_MAGIC` | C-Language Functions — Magic Block | https://www.postgresql.org/docs/current/xfunc-c.html#XFUNC-C-PGMODULEMAGIC | `contrib/file_fdw/file_fdw.c` |
| `PG_FUNCTION_INFO_V1` | Version-1 Calling Conventions | https://www.postgresql.org/docs/current/xfunc-c.html#XFUNC-C-V1-CALL-CONV | `contrib/file_fdw/file_fdw.c` |
| `parquet_gsi_fdw_handler` | FDW Functions — Handler | https://www.postgresql.org/docs/current/fdw-functions.html | `file_fdw_handler()` |
| `parquet_gsi_fdw_validator` | FDW Functions — Validator | https://www.postgresql.org/docs/current/fdw-functions.html#FDW-FUNCTIONS-VALIDATOR | `file_fdw_validator()` |
| `GetForeignRelSize` | FDW Callbacks — Scan | https://www.postgresql.org/docs/current/fdw-callbacks.html#FDW-CALLBACKS-SCAN-GETFOREIGNRELSIZE | `fileGetForeignRelSize()` |
| `GetForeignPaths` | FDW Callbacks — Scan | https://www.postgresql.org/docs/current/fdw-callbacks.html#FDW-CALLBACKS-SCAN-GETFOREIGNPATHS | `fileGetForeignPaths()` |
| `GetForeignPlan` | FDW Callbacks — Scan | https://www.postgresql.org/docs/current/fdw-callbacks.html#FDW-CALLBACKS-SCAN-GETFOREIGNPLAN | `fileGetForeignPlan()` |
| `BeginForeignScan` | FDW Callbacks — Scan | https://www.postgresql.org/docs/current/fdw-callbacks.html#FDW-CALLBACKS-SCAN-BEGINFOREIGNSCAN | `fileBeginForeignScan()` |
| `IterateForeignScan` | FDW Callbacks — Scan | https://www.postgresql.org/docs/current/fdw-callbacks.html#FDW-CALLBACKS-SCAN-ITERATEFOREIGNSCAN | `fileIterateForeignScan()` |
| `ReScanForeignScan` | FDW Callbacks — Scan | https://www.postgresql.org/docs/current/fdw-callbacks.html#FDW-CALLBACKS-SCAN-RESCANFOREIGNSCAN | `fileReScanForeignScan()` |
| `EndForeignScan` | FDW Callbacks — Scan | https://www.postgresql.org/docs/current/fdw-callbacks.html#FDW-CALLBACKS-SCAN-ENDFOREIGNSCAN | `fileEndForeignScan()` |
| `data_lake_path` validation | FDW Validator + Error Codes | https://www.postgresql.org/docs/current/errcodes-appendix.html | `file_fdw_validator()` |
| `data_lake_path` extraction | FDW Helper Functions | https://www.postgresql.org/docs/current/fdw-helper.html | `fileGetOptions()` |
| `AllocateDir` / `ReadDir` | Storage — Virtual FDs | https://www.postgresql.org/docs/current/storage-file-layout.html | `file_fdw.c` file scan loop |
| Memory contexts | SPI Memory Management | https://www.postgresql.org/docs/current/spi-memory.html | `fileBeginForeignScan()` |

---

*Generated from `parquet_gsi_fdw.c` — all URLs target `postgresql.org/docs/current` (PostgreSQL 17 at time of writing).*