# parquet_gsi_fdw (Checkpoint 1 Baseline)

This directory contains the initial implementation for Task 4 (FDW baseline):

- PGXS extension scaffold
- FDW hook registration
- Recursive directory traversal for parquet discovery
- Baseline full scan state machine that emits one row per discovered parquet file

## Current Behavior

The FDW currently walks the configured data lake path and returns one output row per discovered `.parquet` file.

- First foreign-table column: file path (text)
- Remaining columns: NULL

This is an intentional start point for incremental implementation. The next step is replacing file-path row emission with Arrow-backed tuple decoding.

## Build

From this directory:

make
make install

## SQL Setup Example

CREATE EXTENSION parquet_gsi_fdw;

CREATE SERVER parquet_gsi_srv
FOREIGN DATA WRAPPER parquet_gsi_fdw
OPTIONS (data_lake_path '/absolute/path/to/data_lake');

CREATE FOREIGN TABLE datalake_table (
    file_path text
)
SERVER parquet_gsi_srv;

SELECT * FROM datalake_table;

## Next Implementation Step

Replace `IterateForeignScan` file-path emission with an internal parquet reader pipeline that maps parquet rows to PostgreSQL tuple slots.
