# generate.py Function Explanation

This file explains the helper and generation functions used in `generate.py`.

The generator has three main jobs:

- create full Parquet datasets in `generate` mode
- append new Parquet files in `append` mode
- refresh the PostgreSQL foreign table file list after append mode

## Probability And Count Helpers

### `skewed_probs(size, skew)`

Creates a probability array of length `size`.

Use case:

- Used to make non-uniform data distributions.
- If `skew = 0`, every value gets equal probability.
- If `skew > 0`, some values get higher probability and appear more often.

Used for:

- `MONTH_PROBS`
- `REGION_PROBS`
- `STATUS_PROBS`
- `PAYMENT_PROBS`
- `LOYALTY_PROBS`
- `CAT_PROBS`

Example:

```python
MONTH_PROBS = skewed_probs(len(TX_MONTHS), args.month_skew)
```

This decides whether transaction rows are spread uniformly across months or concentrated in fewer months.

### `exact_counts(total, probs)`

Converts probabilities into integer counts that add up exactly to `total`.

Use case:

- Probabilities produce decimal counts like `3.4` or `10.7`.
- Actual row counts must be integers.
- This function floors the decimal values, then gives remaining rows to the largest fractional parts.

Used in:

```python
month_counts = exact_counts(NUM_TX, MONTH_PROBS)
```

This decides exactly how many transaction rows each month gets.

Also used inside `split_count()` to divide remaining rows across files.

### `split_count(total, pieces, skew)`

Splits a total row count across a fixed number of chunks/files.

Use case:

- Ensures each file gets at least one row.
- Ensures all returned counts sum exactly to `total`.
- Uses `skew` to make file sizes uniform or uneven.

Used in:

- `generate_transactions()` to split each month into Parquet files.
- `append_customers()` to split appended customer rows across new files.
- `append_products()` to split appended product rows across new files.
- `append_transactions()` to split appended transaction rows across new files.

Example:

```python
row_counts = split_count(month_rows, file_count, args.file_skew)
```

### `choose(values, n, probs)`

Chooses `n` random values from a list using the given probabilities.

Use case:

- Makes categorical columns follow the skew settings.

Used for:

- customer regions
- customer loyalty tiers
- product categories
- transaction regions
- transaction statuses
- transaction payment methods

Example:

```python
"region": choose(REGIONS, n, REGION_PROBS)
```

## Date And Time Helpers

### `rand_date(n)`

Generates `n` random dates between `2023-01-01` and roughly two years after that.

Use case:

- Used for `customers.signup_date`.

### `rand_ts_between(n, start, end)`

Generates `n` random timestamps between `start` and `end`.

Use case:

- Used for transaction timestamps.
- In transaction generation, each file is restricted to a selected month.

Example:

```python
"timestamp": rand_ts_between(n, ts_start, ts_end)
```

### `month_bounds(month)`

Returns the start timestamp and exclusive end timestamp for a month.

Use case:

- Converts a month like `2024-05` into:

```text
2024-05-01 00:00:00
2024-06-01 00:00:00
```

Used before generating transaction timestamps for a month.

## File And Directory Helpers

### `write_parquet(df, path)`

Writes a Pandas DataFrame to a Parquet file.

Use case:

- Creates parent directories if needed.
- Uses the configured row group size.
- Uses Snappy compression.

Used by all generate and append functions.

### `readable_size(path)`

Returns a readable file size string like `12.3 MB` or `1.25 GB`.

Use case:

- Used in progress logs after writing Parquet files.

### `id_pool(n)`

Generates `n` random 32-character hex ids.

Use case:

- Creates ids for customers, products, and orders.

Generated ids are used for:

- `customer_id`
- `product_id`
- `order_id`

### `parquet_files(root)`

Returns all `.parquet` files under a directory recursively.

Use case:

- Used for summary output.
- Used to find existing files in append mode.
- Used to load existing ids when appending transactions.

### `reset_table_dirs(tables)`

Prepares table directories.

Use case:

- In `generate` mode, it deletes old table folders and recreates them.
- In `append` mode, it only ensures folders exist.

Current behavior:

- `generate` mode passes all tables, so old `customers/`, `products/`, and `transactions/` folders are removed.
- `append` mode passes an empty set, so no existing files are deleted.

### `next_file_index(root, prefix)`

Finds the next file index to use in append mode.

Use case:

- Prevents overwriting existing Parquet files.
- Scans existing filenames like `transactions_chunk_0004.parquet`.
- Returns the next number, like `5`.

Used by:

- `generate_transactions()`
- `append_customers()`
- `append_products()`
- `append_transactions()`

## PostgreSQL Refresh Helper

### `refresh_foreign_table_files(table)`

Refreshes the PostgreSQL foreign table file list after append mode.

Use case:

- The FDW stores a static list of Parquet filenames.
- When new files are appended, PostgreSQL needs to be told about them.
- This function calls `public.refresh_parquet_foreign_table_files(...)` using `psycopg2`.

It refreshes a table like:

```sql
SELECT public.refresh_parquet_foreign_table_files(
  'public.transactions'::regclass,
  '{"dir": "/absolute/path/to/data_lake/transactions"}'::jsonb
);
```

The database connection comes from `DB_CONFIG` in `generate.py`.

## Existing Id Helpers

### `load_existing_ids(table, column, max_ids)`

Loads existing id values from Parquet files.

Use case:

- In append mode, new transaction rows should refer to existing customers and products when possible.
- This function reads existing `customer_id` or `product_id` values from the data lake.

Example:

```python
load_existing_ids("customers", "customer_id", REFERENCE_SAMPLE_SIZE)
```

### `load_or_make_reference_ids(table, column, fallback_rows, already_loaded)`

Returns ids to use when generating transactions.

Use case:

- If ids were already generated in memory, reuse them.
- If appending to an existing lake, load ids from existing Parquet files.
- If no existing ids are found, create temporary ids.

Used by:

- `append_transactions()`

In normal full generation, customers and products are generated first, so transactions can use those in-memory ids directly.

### `choose_pool_ids(pool, n, skew)`

Chooses `n` ids from an existing id pool.

Use case:

- Used in transaction generation for `customer_id` and `product_id`.
- If `--id-skew = 0`, ids are sampled uniformly.
- If `--id-skew > 0`, a smaller "hot" subset of ids appears more frequently.

This is useful for generating realistic skew, where some customers or products appear in many more transactions.

## Transaction Partition Helpers

### `transaction_dir(month)`

Returns the directory where a transaction file should be written.

Use case:

- Applies the `--tx-partition` layout.

Possible outputs:

- `transactions/month=2024-05/`
- `transactions/year=2024/`
- `transactions/`

### `choose_transaction_month()`

Chooses one transaction month using `MONTH_PROBS`.

Use case:

- Used in append mode when no `--add-partition-value` is given.
- Makes appended transaction files land in a randomly chosen month.
- Honors `--month-skew`.

### `parse_partition_month(value)`

Parses the append partition value into a month.

Use case:

- If value is `None`, choose a random month.
- If value is `YYYY`, choose a random month inside that year.
- If value is `YYYY-MM`, use that exact month.

Examples:

```bash
--add-partition-value 2024
--add-partition-value 2024-05
```

## Row Creation Functions

### `make_customers(customer_ids, start_index, email_prefix="user", ci=0)`

Builds a customer DataFrame.

Columns created:

- `customer_id`
- `email`
- `region`
- `age`
- `loyalty_tier`
- `signup_date`
- `total_orders`
- `lifetime_value`

Use case:

- Used in full customer generation.
- Used when appending customer files.

### `make_products(product_ids, start_index)`

Builds a product DataFrame.

Columns created:

- `product_id`
- `name`
- `category`
- `price`
- `stock`
- `brand`
- `weight_kg`
- `rating`
- `reviews`

Use case:

- Used in full product generation.
- Used when appending product files.

### `skewed_amounts(n)`

Generates transaction amounts.

Use case:

- If `--value-skew = 0`, amounts are uniformly distributed.
- If `--value-skew > 0`, amounts follow a clipped log-normal distribution.

This creates a more realistic distribution where many transactions are moderate and some are much larger.

### `make_transactions(n, customer_ids, product_ids, ts_start, ts_end)`

Builds a transaction DataFrame.

Columns created:

- `order_id`
- `customer_id`
- `product_id`
- `amount`
- `quantity`
- `status`
- `region`
- `timestamp`
- `payment_method`
- `is_returned`
- `warehouse_id`
- `shipping_days`

Use case:

- Used by full transaction generation.
- Used by append transaction generation.

It uses:

- `id_pool()` for `order_id`
- `choose_pool_ids()` for `customer_id` and `product_id`
- `skewed_amounts()` for `amount`
- `rand_ts_between()` for `timestamp`
- `choose()` for categorical columns

## Full Generation Functions

### `generate_customers()`

Generates the full customer table.

Use case:

- Creates all customer ids in memory.
- Writes customer rows in chunks.
- Returns the generated customer ids so transactions can reference them.

Output:

- Parquet files under `data_lake/customers/`

### `generate_products()`

Generates the full product table.

Use case:

- Creates all product ids in memory.
- Writes product rows in chunks.
- Returns the generated product ids so transactions can reference them.

Output:

- Parquet files under `data_lake/products/`

### `generate_transactions(customer_ids, product_ids)`

Generates the full transaction table.

Use case:

- Decides how many rows each month gets using `exact_counts()`.
- Decides how many files each month needs.
- Splits month rows across files using `split_count()`.
- Generates transaction rows month by month.

Output:

- Parquet files under `data_lake/transactions/`
- The exact directory depends on `--tx-partition`.

## Append Functions

### `append_customers(file_count, rows_per_file)`

Appends new customer Parquet files.

Use case:

- Adds `file_count` new customer files.
- Uses `rows_per_file` as the target file size.
- Continues file numbering after existing files.

### `append_products(file_count, rows_per_file)`

Appends new product Parquet files.

Use case:

- Adds `file_count` new product files.
- Uses `rows_per_file` as the target file size.
- Continues file numbering after existing files.

### `append_transactions(file_count, rows_per_file, customer_ids, product_ids)`

Appends new transaction Parquet files.

Use case:

- Loads existing customer/product ids if needed.
- Chooses the month for each appended file.
- Writes new transaction files without deleting old data.
- Continues file numbering after existing transaction files.

After append mode completes, `refresh_foreign_table_files()` is called so PostgreSQL can see the new files.

### `append_files(table, file_count, loaded_customers, loaded_products)`

Dispatches append mode to the correct append function.

Use case:

- If `--add-table transactions`, calls `append_transactions()`.
- If `--add-table customers`, calls `append_customers()`.
- If `--add-table products`, calls `append_products()`.

This keeps the main script flow small and makes table-specific append behavior easier to manage.

## Summary Function

### `summarize()`

Prints the final data lake summary.

Use case:

- Counts Parquet files under each table directory.
- Calculates total size per table.
- Prints total data lake size.
- Reads one transaction file to show schema, row groups, and rows per file.

This runs at the end of both generate mode and append mode.
