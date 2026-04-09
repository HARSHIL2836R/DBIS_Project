#include "postgres.h"

#include <string.h>
#include <sys/stat.h>

#include "access/htup_details.h"
#include "access/reloptions.h"
#include "catalog/pg_foreign_table.h"
#include "commands/defrem.h"
#include "executor/executor.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "storage/fd.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/rel.h"

/*
To ensure that a dynamically loaded object file is not loaded into an incompatible server, PostgreSQL checks that the file contains a “magic block” with the appropriate contents. This allows the server to detect obvious incompatibilities, such as code compiled for a different major version of PostgreSQL
*/
PG_MODULE_MAGIC;

#define PARQUET_GSI_OPTION_DATA_LAKE_PATH "data_lake_path"
#define PARQUET_GSI_DEFAULT_DATA_LAKE_PATH "/tmp/data_lake"

typedef struct ParquetGsiExecutionState
{
    MemoryContext scan_cxt; // memory management abstraction
    List *files; // list of file paths to scan
    ListCell *next_file;
} ParquetGsiExecutionState;

PG_FUNCTION_INFO_V1(parquet_gsi_fdw_handler);
PG_FUNCTION_INFO_V1(parquet_gsi_fdw_validator);
/*
* This macro call is not needed for internal-language functions, since PostgreSQL assumes that all internal functions use the version-1 convention. It is, however, required for dynamically-loaded functions.
*/

static void parquet_gsi_get_foreign_rel_size(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid);
static void parquet_gsi_get_foreign_paths(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid);
static ForeignScan *parquet_gsi_get_foreign_plan(PlannerInfo *root,
                                                RelOptInfo *baserel,
                                                Oid foreigntableid,
                                                ForeignPath *best_path,
                                                List *tlist,
                                                List *scan_clauses,
                                                Plan *outer_plan);
static void parquet_gsi_begin_foreign_scan(ForeignScanState *node, int eflags);
static TupleTableSlot *parquet_gsi_iterate_foreign_scan(ForeignScanState *node);
static void parquet_gsi_rescan_foreign_scan(ForeignScanState *node);
static void parquet_gsi_end_foreign_scan(ForeignScanState *node);

static char *parquet_gsi_get_data_lake_path(Oid foreigntableid);
static void parquet_gsi_collect_parquet_files(const char *dir_path, List **files_out);
static bool parquet_gsi_has_parquet_suffix(const char *name);

/*
* Entry point for the FDW. PostgreSQL calls this to get a struct of function 
* pointers (callback hooks) that define how to plan and execute queries on this FDW.
* headers expect the function's arguments to be supplied as Datums, and likewise they return Datum.
*/
Datum parquet_gsi_fdw_handler(PG_FUNCTION_ARGS)
{
    FdwRoutine *fdw_routine = makeNode(FdwRoutine);

    fdw_routine->GetForeignRelSize = parquet_gsi_get_foreign_rel_size;
    fdw_routine->GetForeignPaths = parquet_gsi_get_foreign_paths;
    fdw_routine->GetForeignPlan = parquet_gsi_get_foreign_plan;
    fdw_routine->BeginForeignScan = parquet_gsi_begin_foreign_scan;
    fdw_routine->IterateForeignScan = parquet_gsi_iterate_foreign_scan;
    fdw_routine->ReScanForeignScan = parquet_gsi_rescan_foreign_scan;
    fdw_routine->EndForeignScan = parquet_gsi_end_foreign_scan;

    PG_RETURN_POINTER(fdw_routine);
}

/*
* Validates the OPTIONS provided when a user runs CREATE SERVER or CREATE FOREIGN TABLE.
* It ensures the user only passes valid keys (like "data_lake_path").
*/
Datum parquet_gsi_fdw_validator(PG_FUNCTION_ARGS)
{
    List *options_list = untransformRelOptions(PG_GETARG_DATUM(0));
    ListCell *cell = NULL;

    foreach(cell, options_list)
    {
        DefElem *option = (DefElem *) lfirst(cell);

        if (strcmp(option->defname, PARQUET_GSI_OPTION_DATA_LAKE_PATH) != 0)
        {
            ereport(ERROR,
                    (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
                    errmsg("invalid option \"%s\"", option->defname),
                    errhint("Valid option: %s", PARQUET_GSI_OPTION_DATA_LAKE_PATH)));
        }
    }

    PG_RETURN_VOID();
}

/*
* Estimates the number of rows and total size of the foreign table.
* Currently hardcoded to 1000 rows as a baseline estimate.
*/
static void parquet_gsi_get_foreign_rel_size(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid)
{
    baserel->rows = 1000;
}

/*
* Generates the possible execution paths (e.g., sequential scan) 
* and calculates their expected startup and total execution costs.
*/
static void parquet_gsi_get_foreign_paths(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid)
{
    Cost startup_cost = 0;
    Cost total_cost = baserel->rows;

    add_path(baserel,
             (Path *) create_foreignscan_path(root,
                                            baserel,
                                            NULL,
                                            baserel->rows,
                                            0,
                                            startup_cost,
                                            total_cost,
                                            NIL,
                                            NULL,
                                            NULL,
                                            baserel->baserestrictinfo,
                                            NIL));
}

/*
* Takes the best path chosen by the optimizer and builds 
* the actual execution plan node (ForeignScan) containing the target list and query clauses.
*/
static ForeignScan * parquet_gsi_get_foreign_plan(PlannerInfo *root,
                            RelOptInfo *baserel,
                            Oid foreigntableid,
                            ForeignPath *best_path,
                            List *tlist,
                            List *scan_clauses,
                            Plan *outer_plan)
{
    scan_clauses = extract_actual_clauses(scan_clauses, false); // scan_clauses is the list of restrictions attached to the foreign table scan, extract_actual_clauses strips away that wrapper layer and returns the actual boolean expressions the executor should use

    return make_foreignscan(tlist,
                            scan_clauses,
                            baserel->relid,
                            NIL,
                            NIL,
                            NIL,
                            NIL,
                            outer_plan);
}

/*
* Execution Phase 1: Runs once before the scan starts. Used to initialize state, 
* allocate memory contexts, and in this case, find all Parquet files in the target directory.
*/
static void parquet_gsi_begin_foreign_scan(ForeignScanState *node, int eflags)
{
    Oid foreigntableid = RelationGetRelid(node->ss.ss_currentRelation);
    char *data_lake_path = parquet_gsi_get_data_lake_path(foreigntableid);
    ParquetGsiExecutionState *state = NULL;
    MemoryContext old_cxt = NULL;

    if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
        return;

    state = (ParquetGsiExecutionState *) palloc0(sizeof(ParquetGsiExecutionState));
    state->scan_cxt = AllocSetContextCreate(CurrentMemoryContext,
                                            "parquet_gsi_fdw scan context",
                                            ALLOCSET_DEFAULT_SIZES);

    old_cxt = MemoryContextSwitchTo(state->scan_cxt);
    parquet_gsi_collect_parquet_files(data_lake_path, &state->files);
    state->next_file = list_head(state->files);
    MemoryContextSwitchTo(old_cxt);

    node->fdw_state = (void *) state;
}

/*
* Execution Phase 2: Called repeatedly by the executor to fetch the next row.
* Currently, it iterates through the collected files and returns the file path 
* as the first column, filling the rest with NULLs.
*/
static TupleTableSlot * parquet_gsi_iterate_foreign_scan(ForeignScanState *node)
{
    ParquetGsiExecutionState *state = (ParquetGsiExecutionState *) node->fdw_state;
    TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
    TupleDesc tupdesc = slot->tts_tupleDescriptor;
    int natts = tupdesc->natts;

    ExecClearTuple(slot);

    if (state == NULL || state->next_file == NULL)
        return slot; // A NULL return signals that all rows have been scanned

    if (natts > 0)
    {
        const char *file_path = (const char *) lfirst(state->next_file);

        slot->tts_values[0] = CStringGetTextDatum(file_path);
        slot->tts_isnull[0] = false;

        for (int i = 1; i < natts; i++)
        {
            slot->tts_values[i] = (Datum) 0;
            slot->tts_isnull[i] = true;
        }
    }

    state->next_file = lnext(state->files, state->next_file);

    ExecStoreVirtualTuple(slot);
    return slot;
}

/*
* Execution Utility: Called if PostgreSQL needs to restart the scan from the beginning 
* (e.g., during nested loop joins). Resets the file pointer to the head of the list.
*/
static void parquet_gsi_rescan_foreign_scan(ForeignScanState *node)
{
    ParquetGsiExecutionState *state = (ParquetGsiExecutionState *) node->fdw_state;

    if (state != NULL)
        state->next_file = list_head(state->files);
}

/*
* Execution Phase 3: Cleans up when the scan is entirely finished.
* Frees the memory context and the execution state struct to prevent memory leaks.
*/
static void parquet_gsi_end_foreign_scan(ForeignScanState *node)
{
    ParquetGsiExecutionState *state = (ParquetGsiExecutionState *) node->fdw_state;

    if (state == NULL)
        return;

    if (state->scan_cxt != NULL)
        MemoryContextDelete(state->scan_cxt);

    pfree(state);
    node->fdw_state = NULL;
}

/*
* Helper Function: Extracts the "data_lake_path" string from the OPTIONS
* defined on the foreign table or foreign server. Falls back to a default if missing.
*/
static char * parquet_gsi_get_data_lake_path(Oid foreigntableid)
{
    ForeignTable *foreign_table = GetForeignTable(foreigntableid);
    ForeignServer *foreign_server = GetForeignServer(foreign_table->serverid);
    List *options = list_concat_copy(foreign_server->options, foreign_table->options);
    ListCell *cell = NULL;

    foreach(cell, options)
    {
        DefElem *option = (DefElem *) lfirst(cell);

        if (strcmp(option->defname, PARQUET_GSI_OPTION_DATA_LAKE_PATH) == 0)
            return defGetString(option);
    }

    return pstrdup(PARQUET_GSI_DEFAULT_DATA_LAKE_PATH);
}

/*
* Helper Function: Recursively traverses a given directory path, looks for files, 
* and appends the absolute paths of any .parquet files to the files_out list.
*/
static void parquet_gsi_collect_parquet_files(const char *dir_path, List **files_out)
{
    DIR *dir = AllocateDir(dir_path);
    struct dirent *entry = NULL;

    if (dir == NULL)
    {
        ereport(ERROR,
                (errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
                errmsg("could not open data lake directory \"%s\"", dir_path)));
    }

    while ((entry = ReadDir(dir, dir_path)) != NULL)
    {
        struct stat stat_buf;
        char *full_path = NULL;

        if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
            continue;

        full_path = psprintf("%s/%s", dir_path, entry->d_name);

        if (stat(full_path, &stat_buf) != 0)
            continue;

        if (S_ISDIR(stat_buf.st_mode))
        {
            parquet_gsi_collect_parquet_files(full_path, files_out);
            continue;
        }

        if (parquet_gsi_has_parquet_suffix(entry->d_name))
            *files_out = lappend(*files_out, full_path);
    }

    FreeDir(dir);
}

/*
* Helper Function: Simply checks if a given file name ends with the ".parquet" string,
* ignoring case differences.
*/
static bool parquet_gsi_has_parquet_suffix(const char *name)
{
    const char *suffix = ".parquet";
    size_t name_len = strlen(name);
    size_t suffix_len = strlen(suffix);

    if (name_len < suffix_len)
        return false;

    return pg_strcasecmp(name + name_len - suffix_len, suffix) == 0;
}