#!/usr/bin/env bash
# parquet-gsi: build, load, index, benchmark, plot. One command.
#
#   ./run_bench.sh              # ~200 MB lake, the 15-minute path
#   ./run_bench.sh medium       # ~2 GB lake
#   ./run_bench.sh small 10     # 10 repetitions per warm cell instead of 5
set -euo pipefail

# Git Bash / MSYS2 on Windows rewrites any argument that looks like a POSIX
# absolute path into a Windows path before exec. That turns the container path
# /work/data/generate.py into C:/Program Files/Git/work/data/generate.py and
# every step below fails with ENOENT. Disabling the conversion is a no-op on
# Linux and macOS. Container paths are additionally kept relative to -w /work
# below, which is belt and braces.
export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL='*'

TARGET="${1:-small}"
REPS="${2:-5}"
DC="docker compose"
EXEC="$DC exec -T -w /work db"

step() { printf '\n\033[1m==> %s\033[0m\n' "$*"; }

START=$SECONDS

step "Building image (first run only; pulls Arrow + compiles parquet_fdw)"
$DC build

step "Starting PostgreSQL 16"
$DC up -d
until $DC exec -T db pg_isready -U postgres -d gsi >/dev/null 2>&1; do sleep 2; done

step "Generating the '$TARGET' parquet lake (seed 42, deterministic)"
$EXEC python3 data/generate.py \
    --target "$TARGET" --out /tmp/data_lake --seed 42 --tx-partition month

step "Creating foreign tables and registering indexes"
$EXEC psql -U postgres -d gsi -v ON_ERROR_STOP=1 -f bench/00_schema.sql

step "Building the global secondary indexes"
$EXEC python3 bench/index_lake.py

step "Proving the A/B toggle changes the plan"
$EXEC python3 bench/verify_ab.py

step "Running the benchmark suite ($REPS warm reps per cell)"
$EXEC python3 bench/run_bench.py --reps "$REPS"

step "Plotting"
$EXEC python3 bench/plot.py

step "Done in $(( (SECONDS - START) / 60 ))m $(( (SECONDS - START) % 60 ))s"
echo "results/results.csv       raw measurements, one row per run"
echo "results/results_table.md  the table to paste into README"
echo "results/latency.png       README figure"
echo "results/hardware.json     the machine these numbers came from"
echo "results/index_build.json  index build time and size"
echo "results/explain_ab.txt    the two plans, side by side, proving the A/B"
echo
echo "Commit all six. README numbers come from the CSV and nowhere else."
