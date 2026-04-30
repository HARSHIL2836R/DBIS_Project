# GSI Cost Estimation Improvements - Implementation Guide

## Overview

This document describes the improvements made to the Global Secondary Index (GSI) cost estimation model in parquet_fdw. The changes replace the heuristic-based `sqrt(selectivity)` formula with a data-driven approach using PostgreSQL ANALYZE statistics.

---

## Problem Statement

**Previous Model (Pure Heuristic):**
- Row-group selectivity = `sqrt(predicate_selectivity)` (unexplained, one-size-fits-all)
- IO factor = `0.1 pages/row` (hardcoded guess)
- No use of actual column statistics from ANALYZE
- Could over/under-estimate costs leading to wrong query plan choices

**Result:** FDW planner might choose expensive GSI plans over parallel scans, or vice versa.

---

## Solution: Data-Driven Cost Model

### Key Principle

**Row-group selectivity depends on column cardinality and data distribution:**
$$\text{Row-Group Selectivity} = \min\left(1.0, \frac{\text{Predicate Selectivity}}{\text{Distinct Values}}\right)$$

**Intuition:**
- Column with 3 distinct values → Can skip many row-groups if one value is queried
- Column with 1M distinct values → Must read almost all row-groups (uniform distribution)
- With NDV stats, we can compute this accurately instead of guessing

---

## Implementation Details

### 1. Helper Function: `get_column_ndistinct_from_stats()`

**File:** [src/parquet_impl.cpp](src/parquet_impl.cpp#L139-L184)

```cpp
static double get_column_ndistinct_from_stats(PlannerInfo *root, Oid relid, AttrNumber attnum)
```

**What it does:**
- Queries PostgreSQL's `pg_statistic` system catalog for column statistics
- Retrieves `n_distinct` computed by ANALYZE
- Handles both absolute counts and fractional representations
- Returns -1.0 sentinel if no stats available (triggers fallback to heuristic)

**PostgreSQL Version Support:** PG 10+ (via conditional compilation `#if PG_VERSION_NUM >= 100000`)

**Key API:**
- `SearchSysCache(STATRELATTINH, ...)` — PostgreSQL's built-in stats cache
- `Form_pg_statistic` — System catalog row type
- `stadistinct` — The n_distinct field in pg_statistic

---

### 2. Updated GSI Cost Calculation

**File:** [src/parquet_impl.cpp](src/parquet_impl.cpp#L1520-L1560)

**Changes:**

Before:
```cpp
double gsi_rg_selectivity = sqrt(gsi_selectivity);  // Pure heuristic
```

After:
```cpp
// Attempt to get statistics for the indexed column
double ndistinct = get_column_ndistinct_from_stats(root, gsi_relid, index_attnum);

if (ndistinct > 0) {
  // Use data-driven selectivity: how many rowgroups will we skip?
  gsi_rg_selectivity = fmin(1.0, gsi_selectivity / ndistinct);
  elog(DEBUG1, "GSI: Using NDV-based rg_selectivity = %f", gsi_rg_selectivity);
} else {
  // No stats available; fall back to conservative sqrt heuristic
  gsi_rg_selectivity = sqrt(gsi_selectivity);
  elog(DEBUG1, "GSI: No ANALYZE stats; using sqrt heuristic");
}
```

**Logic Flow:**
1. Extract indexed column's attribute number from GSI metadata
2. Try to fetch n_distinct from PostgreSQL statistics
3. If available: Use NDV-based formula (data-driven)
4. If not: Fall back to sqrt (conservative, maintains backward compatibility)
5. Always clamp result to valid range [gsi_selectivity, 1.0]

---

## Usage Guide

### Step 1: Collect Statistics

Run ANALYZE on the tables with indexed columns:

```sql
-- For customers table with GSI on 'age'
ANALYZE customers(age);

-- For transactions table with GSI on 'customer_id'
ANALYZE transactions(customer_id);

-- Or ANALYZE the entire table (expensive but thorough)
ANALYZE customers;
ANALYZE transactions;
```

### Step 2: Verify Statistics

Check that statistics were collected:

```sql
SELECT schemaname, tablename, attname, n_distinct
FROM pg_stats
WHERE tablename IN ('customers', 'transactions')
  AND attname IN ('age', 'customer_id');
```

Expected output:
```
 schemaname | tablename | attname     | n_distinct
 public     | customers | age         | 78          
 public     | transactions | customer_id | 1250        
```

### Step 3: Enable Debug Logging (Optional)

To see which cost model is being used:

```sql
-- In PostgreSQL configuration file (postgresql.conf)
log_min_messages = DEBUG1

-- Or set per-session:
SET log_min_messages = DEBUG1;
```

Then run a query and check logs:
```
GSI: Using NDV-based rg_selectivity = 0.00125 (ndistinct=800.0, selectivity=0.001)
```

### Step 4: Validate Improvements

Compare costs with EXPLAIN ANALYZE:

```sql
-- Query 1: Before ANALYZE
EXPLAIN ANALYZE SELECT * FROM customers WHERE age = 25;

-- Run ANALYZE
ANALYZE customers(age);

-- Query 2: After ANALYZE (should show more accurate cost)
EXPLAIN ANALYZE SELECT * FROM customers WHERE age = 25;
```

**What to look for:**
- **Planned Rows** should match **Actual Rows** more closely
- **Cost** should reflect actual execution time
- Look for DEBUG messages in server logs showing which selectivity method was used

---

## Example Scenarios

### Scenario 1: Low-Cardinality Column (Good for GSI)

**Data:**
- Table: 1M rows
- Column `status`: 3 distinct values (A, B, C)
- Predicate: `WHERE status = 'A'` (selectivity = 1/3)
- Number of row-groups: 1000

**Old Model (sqrt heuristic):**
```
gsi_rg_selectivity = sqrt(1/3) = 0.577 → Must read 577 rowgroups
Cost = high (57.7% of table)
Decision: Might avoid GSI, use parallel scan
```

**New Model (NDV-based):**
```
ndistinct = 3
gsi_rg_selectivity = (1/3) / 3 = 0.111 → Must read 111 rowgroups
Cost = lower (11.1% of table)  
Decision: Chooses GSI! ✓ Correct choice
```

### Scenario 2: High-Cardinality Column (Bad for GSI)

**Data:**
- Table: 1M rows
- Column `user_id`: 1M distinct values (nearly unique)
- Predicate: `WHERE user_id = 12345` (selectivity = 1/1M)
- Number of row-groups: 1000

**Old Model (sqrt heuristic):**
```
gsi_rg_selectivity = sqrt(1/1M) = 0.001 → Must read 1 rowgroup
Cost = very low
Decision: Chooses GSI, plan is fast ✓
```

**New Model (NDV-based):**
```
ndistinct = 1000000
gsi_rg_selectivity = (1/1M) / 1M = 1e-12 (clamped to 1e-12) → Must read ~1 rowgroup
Cost = very low
Decision: Chooses GSI, plan is fast ✓
```
Both models agree here (good!).

---

## Building and Installing

### Prerequisites

```bash
# In WSL or Linux environment
sudo apt-get install -y \
  postgresql-server-dev-15 \
  libarrow-dev \
  libparquet-dev \
  build-essential \
  make
```

### Compile

```bash
cd parquet_fdw
make clean
make
```

### Install

```bash
# As root or with sudo
sudo make install

# Then restart PostgreSQL
sudo systemctl restart postgresql
```

### Verify Installation

```sql
-- In PostgreSQL
CREATE EXTENSION IF NOT EXISTS parquet_fdw;
\dx parquet_fdw
```

---

## Troubleshooting

### Issue: "No ANALYZE stats available; using sqrt heuristic"

**Cause:** ANALYZE hasn't been run on the indexed column

**Solution:**
```sql
ANALYZE table_name(column_name);
```

### Issue: Cost estimates still inaccurate after ANALYZE

**Cause 1:** Statistics are outdated (table was modified)
- **Solution:** Re-run `ANALYZE`

**Cause 2:** Data distribution changed significantly
- **Solution:** Consider collecting custom row-group stats (Option B in design doc)

**Cause 3:** The 0.1 pages/row IO factor is not calibrated for your hardware
- **Solution:** Run regression on EXPLAIN ANALYZE vs actual time (Option C in design doc)

### Issue: Compilation errors mentioning pg_statistic

**Cause:** PostgreSQL version < 10
- **Solution:** Upgrade PostgreSQL or use older parquet_fdw (will use sqrt fallback)

---

## Performance Impact

### Query Planner Cost

- **No impact** if ANALYZE stats are up-to-date (cached in memory)
- **Minimal impact** during plan generation (one pg_statistic cache lookup per GSI path)

### Query Execution

**Expected improvements:**
- More accurate cost predictions → Better plan choices
- Better plan choices → Faster query execution (usually)
- In pathological cases, same as before (fallback to sqrt)

**Worst case:** Query takes longer if planner chooses wrong plan based on improved cost estimate (rare; indicates a different problem)

---

## Design Notes

### Why Not Always Use sqrt?

The sqrt heuristic is a "one-size-fits-all" estimate:
- For high-cardinality columns: Over-estimates selectivity → Misses fast GSI opportunities
- For low-cardinality columns: Under-estimates selectivity → Uses slow GSI when shouldn't

### Why Clamp to [gsi_selectivity, 1.0]?

- **Lower bound (gsi_selectivity):** GSI cannot do worse than predicate selectivity
- **Upper bound (1.0):** GSI must read at least 1 rowgroup (can't skip all)

### Why Fallback to sqrt?

- **Safety:** If stats are missing or outdated, sqrt is conservative (errs on side of caution)
- **Compatibility:** Maintains behavior for PG < 10 (no pg_statistic API)
- **Debuggability:** Debug log shows which method was used

---

## Future Enhancements

### Option B: Collect Per-Rowgroup Stats

Store actual rowgroup statistics in gsi_registry:
- Min/max values per rowgroup
- Count of distinct values per rowgroup
- Enables more precise selectivity estimates

### Option C: Empirical Calibration

Collect EXPLAIN ANALYZE metrics and fit regression to tune cost constants:
- Replace 0.1 pages/row with measured value
- Calibrate parallel_setup_cost for GSI extraction overhead

### Option D: Adaptive Statistics

Re-collect stats after significant workload changes:
- Detect when stats are stale
- Auto-ANALYZE indexed columns
- Dynamic cost model adjustment

---

## References

- PostgreSQL Cost Estimation: https://www.postgresql.org/docs/current/cost-evaluation.html
- pg_statistic Catalog: https://www.postgresql.org/docs/current/catalog-pg-statistic.html
- ANALYZE Command: https://www.postgresql.org/docs/current/sql-analyze.html

---

## Questions?

See the implementation in [src/parquet_impl.cpp](src/parquet_impl.cpp):
- Helper function: Lines 139-184
- Updated cost calculation: Lines 1520-1560
