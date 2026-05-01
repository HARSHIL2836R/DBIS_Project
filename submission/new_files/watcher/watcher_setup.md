# Watcher Daemon Setup

This setup is for running `watcher_deamon.py` independently of the parquet extraction engine setup.

## Overview

`watcher_deamon.py` continuously watches the configured data lake directory for `.parquet` file creations, moves, and deletions. When it detects a new or moved parquet file, it calls the external extractor binary, reads the generated coordinate output, and stores the result in PostgreSQL. It also removes stale index entries when a parquet file is deleted. The script initializes the `global_index` table and its B-tree index automatically when it starts. fileciteturn1file0

## Prerequisites

Before running the watcher, make sure you have:

- PostgreSQL running locally or remotely
- The extractor binary already built and available at the path you configure
- Permission to read and write inside the data lake directory

## Python Dependencies

Install the required Python packages:

```bash
pip3 install psycopg2-binary watchdog
```

If you are using a virtual environment, activate it before installing the packages.

## Important Path Notes

Modify the `.env` file in the project root with appropriate contents:

- `DATA_LAKE_DIR` must point to the directory that the watcher should monitor.
- `EXTRACTOR_CMD` must point to the compiled extractor executable.
- `TARGET_COLUMN` must match the column index you want the extractor to use.
- The script writes and deletes `extracted_coordinates.txt` in the current working directory, so run it from the expected project location.

## Start PostgreSQL

Make sure PostgreSQL is running and that the database and user in your `.env` file are valid.

Example check:

```bash
psql -h localhost -U postgres -d postgres
```

If the database does not exist yet, create it before starting the watcher.

## Run the Watcher

From the project directory containing `watcher_deamon.py` and the `.env` file:

```bash
source .env
python3 watcher_deamon.py
```

## What Happens on Startup

When the script starts, it will:

1. Connect to PostgreSQL
2. Create the `global_index` table if it does not already exist
3. Create the `idx_global_value` B-tree index if it does not already exist
4. Start watching `DATA_LAKE_DIR` recursively

## Expected Behavior

- When a `.parquet` file is created, the watcher runs the extractor on it.
- When a `.parquet` file is moved into the watched directory, the watcher processes it.
- When a `.parquet` file is deleted, the watcher removes matching records from `global_index`.
- Duplicate index entries are reduced by deleting old rows for the same `file_path` before inserting fresh results. fileciteturn1file0

## Troubleshooting

### `watchdog` or `psycopg2` import errors
Install the missing package inside the active Python environment:

```bash
python3 -m pip install watchdog psycopg2-binary
```

### Extractor not found
Check that `EXTRACTOR_CMD` points to the correct executable and that it has execute permission:

```bash
chmod +x ../parquet_extraction_engine/build/extractor
```

### No rows inserted into PostgreSQL
Check the following:

- The extractor ran successfully
- `extracted_coordinates.txt` was created
- The output format matches the parser used in the watcher
- The PostgreSQL connection settings in `.env` are correct

### Watcher does not detect files
Confirm that:

- `DATA_LAKE_DIR` is correct
- You are dropping `.parquet` files into that directory
- The process has permission to read the directory

## Stop the Watcher

Press:

```bash
Ctrl+C
```

The daemon will stop gracefully after the interrupt is received.
