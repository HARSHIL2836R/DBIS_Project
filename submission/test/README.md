# Global Secondary Index (GSI) Test Suite

## Overview

This directory contains integration test scripts for the **Global Secondary Index (GSI)** optimization in the Parquet Foreign Data Wrapper (FDW). The tests validate the end-to-end flow: index registration → file catalog management → query planner detection → executor optimization.

---

## Test Files

### Core Scripts

| File | Purpose |
|------|---------|
| **starter.sql** | Initializes the GSI infrastructure, creates 3 test indices, registers them, and executes sample EXPLAIN ANALYZE queries |
| **drop.sql** | Teardown script to clean up all GSI tables and indices created during testing |

---

## Test Scenario: 3-Index Multi-Column Lookup

The test suite now exercises **3 distinct indices** across 2 tables to validate that the planner correctly routes queries when multiple indices are available.

### Indices Registered

#### 1. **gsi_customers_age** (Low Cardinality)
- **Table**: `public.customers`
- **Column**: `age` (integer, ~62 distinct values)
- **Query Pattern**: Equality filter on customer demographics
- **Expected Behavior**: Index may prune some row groups but demonstrates multi-table GSI capability

#### 2. **gsi_transactions_customer_id** (High Cardinality)
- **Table**: `public.transactions`
- **Column**: `customer_id` (varchar, ~1M distinct values)
- **Query Pattern**: "Find all transactions for a specific customer"
- **Expected Behavior**: **Strong selective pruning** — index should dramatically reduce file/rowgroup scans

#### 3. **gsi_transactions_order_id** (Unique, Highest Cardinality)
- **Table**: `public.transactions`
- **Column**: `order_id` (varchar, unique identifier)
- **Query Pattern**: Direct order lookup by ID
- **Expected Behavior**: **Best-case optimization** — should locate exactly 1 rowgroup in 1 file

---

## Running the Tests

### Prerequisites

1. **PostgreSQL server** running locally on `localhost:5432`
2. **parquet_fdw extension** installed and loaded
3. **Data lake** populated at `/tmp/data_lake/` with:
   - `customers/` parquet files
   - `products/` parquet files
   - `transactions/` parquet files (partitioned by month)

### Setup & Execution

```bash
# Option 1: Direct psql execution
psql -U postgres -d postgres -f test/starter.sql

# Option 2: Via the test runner (if available)
cd /home/kunj/Desktop/DBIS-lab/DBIS_Project
make installcheck  # parquet_fdw extension tests
```

### Expected Output

When `starter.sql` completes successfully, you should see:

1. **Registry status** — all 3 indices in `'building'` state:
   ```
    index_name                        | table_name    | column_name    | status
   -----------------------------------+---------------+----------------+---------
    gsi_customers_age                 | customers     | age            | building
    gsi_transactions_customer_id      | transactions  | customer_id    | building
    gsi_transactions_order_id         | transactions  | order_id       | building
   ```

2. **EXPLAIN ANALYZE outputs** — showing plan metrics for each query

3. **Query metrics** — execution time, rows filtered, memory usage

---

## Interpreting EXPLAIN ANALYZE Output

### Key Indicators of GSI Activation

Look for these lines in the EXPLAIN output:

```
Global Secondary Index: Active (B+ Tree Filter)
Reader: Single File                    ← Indicates GSI pruned to 1 file
Reader: Multifile                      ← Multiple files matched; GSI still helped
```

### Performance Metrics to Track

| Metric | What It Shows | Target for GSI Benefit |
|--------|---------------|------------------------|
| **Planning Time** | Time to decide the execution plan | Should be <5ms |
| **Execution Time** | Wall-clock time to run query | Should be **5-100x faster** with GSI |
| **Rows Removed by Filter** | Tuples discarded at scan time | Should be large for selective indices |
| **Row groups** | Metadata showing which parquet rowgroups were scanned | Should be minimal with GSI |

### Example Query: Order ID Lookup (Best Case)

**Query:**
```sql
EXPLAIN ANALYZE
SELECT * FROM public.transactions
WHERE order_id = '1f6f92afdb4baa8a09a733ebd0616fea';
```

**Expected Output Pattern:**
```
Foreign Scan on transactions  (cost=... rows=1)
  Global Secondary Index: Active (B+ Tree Filter)
  Reader: Single File
  Row groups: 1
  Planning Time: 1.xxx ms
  Execution Time: 2.xxx ms
```

**What This Means:**
- ✅ GSI found exactly 1 file to scan
- ✅ Only 1 rowgroup touched (out of potentially 48+)
- ✅ Fast execution (milliseconds, not seconds)

---

## Reporting Results

### Automated Report Generation

To create a summary report, run:

```bash
# Extract EXPLAIN output and timings
psql -U postgres -d postgres \
  -c "SET client_min_messages = WARNING;" \
  -f test/starter.sql \
  | tee test_results.log

# Parse and summarize
grep -E "Global Secondary Index|Execution Time|Planning Time" test_results.log > report.txt
```

### Metrics for Report

| Query | Index | Selectivity | Est. Cost | Actual Time | Speedup |
|-------|-------|-------------|-----------|-------------|---------|
| `age = 30` | gsi_customers_age | ~2% | 8.00 | XXms | — |
| `customer_id = '21a660...'` | gsi_transactions_customer_id | ~0.01% | LOW | XXms | **5-50x** |
| `order_id = '1f6f92...'` | gsi_transactions_order_id | ~0.0001% | LOW | XXms | **100x+** |

### Sample Report Template

```markdown
## GSI Performance Report
**Test Date**: 2026-04-30
**Test Environment**: PostgreSQL + parquet_fdw (Custom)
**Data Lake Size**: ~2GB transactions + 1M customers + 200K products

### Results Summary

| Index Name | Query Type | Planner State | Execution | Verdict |
|---|---|---|---|---|
| gsi_customers_age | Equality on low-cardinality | ✅ Detected | Optimized | PASS |
| gsi_transactions_customer_id | Equality on high-cardinality | ✅ Detected | Optimized | PASS |
| gsi_transactions_order_id | Equality on unique key | ✅ Detected | Optimal | PASS |

### Key Findings

1. **Index Detection**: All 3 indices correctly registered in gsi_registry
2. **Query Planning**: Planner successfully identifies indexable predicates
3. **Row Group Pruning**: Demonstrated selective I/O reduction
```

---

## Cleanup

After testing, remove all GSI artifacts:

```bash
psql -U postgres -d postgres -f test/drop.sql
```

This will drop:
- `gsi_registry` (index metadata table)
- `gsi_file_catalog` (file manifest)
- `gsi_index_file_state` (sync status)
- `gsi_customers_age` (index storage)
- `gsi_transactions_customer_id` (index storage)
- `gsi_transactions_order_id` (index storage)

---

## Troubleshooting

### Issue: "relation gsi_registry does not exist"
**Solution**: Ensure `starter.sql` completed without errors. Check PostgreSQL logs for errors during table creation.

### Issue: "no parquet files found"
**Solution**: Verify data lake is populated:
```bash
ls -la /tmp/data_lake/customers/
ls -la /tmp/data_lake/transactions/
```

### Issue: EXPLAIN shows "Reader: Multifile" instead of optimization
**Solution**: The query may not be selective enough (high cardinality column with common value), or the index is still in `'building'` state. Wait for watcher daemon to finish indexing.

---

## References

- [Parquet FDW Documentation](../parquet_fdw/README.md)
- [GSI Architecture](../COST_ESTIMATION_IMPROVEMENTS.md)
- [Data Generator](../data/README.md)
- [Watcher Daemon](../watcher/watcher_setup.md)

---

## Contributing Test Cases

To add new multi-index scenarios:

1. Register additional indices in `starter.sql` using `register_gsi()`
2. Add corresponding EXPLAIN ANALYZE queries
3. Update `drop.sql` with cleanup for new index storage tables
4. Document expected cardinality and selectivity in this README
5. Run full suite and validate planner output

**Example for new index:**
```sql
SELECT public.register_gsi(
    'public.transactions'::regclass,
    'gsi_transactions_status',
    'status',
    '/tmp/data_lake/transactions'
);

EXPLAIN ANALYZE
SELECT * FROM public.transactions WHERE status = 'completed';
```

