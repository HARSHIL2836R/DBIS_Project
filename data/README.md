# Data Lake Generator

`generate.py` creates a Parquet data lake with three datasets:

- `customers`
- `products`
- `transactions`

By default it writes into `./data_lake`, with one subdirectory per table.

The commands in this README assume you are running from the `data/` directory:

```bash
cd data
```

## Requirements

- Python 3
- `numpy`
- `pandas`
- `pyarrow`
- `psycopg2`

Example install:

```bash
pip install numpy pandas pyarrow psycopg2-binary
```

## What the generator writes

The script produces Parquet files with these schemas:

- `customers`: `customer_id`, `email`, `region`, `age`, `loyalty_tier`, `signup_date`, `total_orders`, `lifetime_value`
- `products`: `product_id`, `name`, `category`, `price`, `stock`, `brand`, `weight_kg`, `rating`, `reviews`
- `transactions`: `order_id`, `customer_id`, `product_id`, `amount`, `quantity`, `status`, `region`, `timestamp`, `payment_method`, `is_returned`, `warehouse_id`, `shipping_days`

Transactions can be physically laid out by:

- month: `transactions/month=2023-01/...`
- year: `transactions/year=2023/...`
- none: `transactions/...`

The FDW setup in this repo recursively reads Parquet files from the data lake directory, so the partition folder names are for file organization and data generation strategy rather than SQL partition metadata.

## Quick Start

Generate the full medium-sized dataset with the default layout:

```bash
python3 ./generate.py
```

Important: `generate` mode deletes the selected old table folders before writing new data.

Generate a smaller dataset in a custom directory:

```bash
python3 ./generate.py --target small --out ./data_lake
```

Generate only transactions:

```bash
python3 ./generate.py --table transactions
```

Generate with skewed data and fewer, larger month-heavy files:

```bash
python3 ./generate.py \
  --month-skew 1.2 \
  --file-skew 0.8 \
  --value-skew 1.0 \
  --id-skew 1.5
```

Append new files to an existing lake:

```bash
python3 ./generate.py \
  --mode append \
  --add-table transactions \
  --add-files 10 \
  --add-rows-per-file 50000
```

Append mode does not delete older files. After appending, the script refreshes the PostgreSQL foreign table file list by calling `public.refresh_parquet_foreign_table_files(...)` through `psycopg2`.

## CLI Reference

### `--target`

Controls the base dataset size.

- `small`
- `medium` default
- `large`

The selected target sets the base row counts and default transaction file size.

### `--out`

Output directory for the data lake.

Example:

```bash
python3 ./generate.py --out /tmp/data_lake
```

### `--mode`

Controls whether the script creates a fresh dataset or appends extra files.

- `generate` default: delete and recreate the selected base table folder(s)
- `append`: write new files into an existing lake without deleting old files

### `--table`

Used with `--mode generate`.

- `all` default: generate customers, products, and transactions
- `customers`
- `products`
- `transactions`

In `generate` mode, the selected table folder(s) are deleted first. For example, `--table transactions` removes only `transactions/`, while `--table all` removes `customers/`, `products/`, and `transactions/`.

### `--add-table`

Used with `--mode append` or when `--add-files` is set.

- `customers`
- `products`
- `transactions` default

This selects which table receives the new files.

### `--add-files`

Number of new Parquet files to add in append mode.

This means "how many new Parquet chunks to create", not "how many rows". Use `--add-rows-per-file` to control rows per new file.

Example:

```bash
python3 ./generate.py --mode append --add-table products --add-files 5
```

### `--add-rows-per-file`

Target rows for each appended file.

- If omitted, the script uses the default chunk size for that table.
- For transactions, it falls back to the transaction rows-per-file setting.

### `--skip-db-refresh`

Disables the automatic PostgreSQL refresh after append mode.

Use this only when PostgreSQL is not running or when you plan to refresh the foreign table manually.

Manual refresh example:

```sql
SELECT public.refresh_parquet_foreign_table_files(
  'public.transactions'::regclass,
  '{"dir": "/tmp/data_lake/transactions"}'::jsonb
);
```

### `--add-partition-value`

For transaction appends, this restricts new transaction timestamps to a specific time bucket.

Accepted forms:

- `YYYY-MM` such as `2024-05`
- `YYYY` such as `2024`

Examples:

```bash
python3 ./generate.py --mode append --add-table transactions --add-files 4 --add-partition-value 2024-05
python3 ./generate.py --mode append --add-table transactions --add-files 4 --add-partition-value 2024
```

### `--tx-partition`

Controls the directory layout used for transaction files.

- `month` default: `transactions/month=YYYY-MM/`
- `year`: `transactions/year=YYYY/`
- `none`: all transaction files go directly under `transactions/`

### `--tx-rows-per-file`

Target number of rows per transaction Parquet file.

- If omitted, the target size from `--target` is used.
- Smaller values create more files.
- Larger values create fewer, bigger files.

### `--month-skew`

Controls how uneven the transaction rows are distributed across months.

- `0` means uniform distribution across months.
- Higher values concentrate more rows into fewer months.

### `--file-skew`

Controls how uneven file sizes are within a month.

- `0` keeps file sizes close together.
- Higher values make some files larger and others smaller.

### `--value-skew`

Adds skew to categorical and value fields.

Affected fields:

- `customers.region`
- `customers.loyalty_tier`
- `products.category`
- `transactions.region`
- `transactions.status`
- `transactions.payment_method`
- `transactions.amount`

Higher values make some values appear much more often than others.

### `--id-skew`

Creates hot customers and hot products in transactions.

- `0` samples customer and product ids uniformly.
- Higher values make a smaller set of ids appear more often.

### `--reference-sample-size`

When appending transactions, the script tries to reuse existing customer and product ids from the lake.

- This limits how many ids are loaded from existing files.
- If no existing ids are found, the script falls back to temporary ids.

### `--seed`

Random seed for reproducible generation.

## Common Commands

Generate the default medium lake:

```bash
python3 ./generate.py
```

Generate a small lake in `/tmp/data_lake`:

```bash
python3 ./generate.py --target small --out /tmp/data_lake
```

Generate only the transaction table with monthly partitions:

```bash
python3 ./generate.py --table transactions --tx-partition month
```

Generate transactions with year-based folders:

```bash
python3 ./generate.py --table transactions --tx-partition year
```

Generate a non-uniform lake:

```bash
python3 ./generate.py \
  --month-skew 1.5 \
  --file-skew 1.0 \
  --value-skew 1.2 \
  --id-skew 1.4
```

Append 12 new transaction files:

```bash
python3 ./generate.py \
  --mode append \
  --add-table transactions \
  --add-files 12
```

Append 3 customer files with custom file size:

```bash
python3 ./generate.py --mode append --add-table customers --add-files 3 --add-rows-per-file 50000
```

Append files without touching PostgreSQL:

```bash
python3 ./generate.py \
  --mode append \
  --add-table transactions \
  --add-files 5 \
  --skip-db-refresh
```

## Output Layout

With the default `--tx-partition month`, the output looks like this:

```text
data_lake/
  customers/
  products/
  transactions/
    month=2023-01/
    month=2023-02/
    ...
```

With `--tx-partition year`, transaction files are grouped by year:

```text
data_lake/
  transactions/
    year=2023/
    year=2024/
```

## Notes

- The script prints a summary at the end with file counts and total size.
- In generate mode, selected old table folders are removed before new files are written.
- In append mode, file numbering continues from the highest existing file index it finds.
- In append mode, the script refreshes the PostgreSQL foreign table file list unless `--skip-db-refresh` is used.
- If you want the new data lake to be visible to the repo's FDW setup, point the FDW import path at the generated output directory.

## PostgreSQL Refresh After Append

The FDW setup stores a static list of Parquet filenames in the foreign table options. When append mode creates new files, PostgreSQL will not automatically know about them unless the file list is refreshed.

By default, append mode calls:

```sql
SELECT public.refresh_parquet_foreign_table_files(
  'public.<table>'::regclass,
  '{"dir": "<absolute-table-directory>"}'::jsonb
);
```

For example, after appending transactions under `/tmp/data_lake`, it refreshes:

```sql
SELECT public.refresh_parquet_foreign_table_files(
  'public.transactions'::regclass,
  '{"dir": "/tmp/data_lake/transactions"}'::jsonb
);
```

The append refresh uses the `DB_CONFIG` dictionary in `generate.py`:

```python
DB_CONFIG = {
    "dbname": "postgres",
    "user": "postgres",
    "password": "hello",
    "host": "localhost",
    "port": "5432",
}
```

Edit this config if your local PostgreSQL credentials are different.

## GSI Demo Columns

For visible GSI speedups, prefer high-cardinality equality lookups. These indexes usually narrow the result to very few files or row groups.

Recommended columns:

- `transactions.order_id`: best demo column because it is unique per transaction.
- `customers.customer_id`: good for direct customer lookup.
- `products.product_id`: good for direct product lookup.
- `transactions.customer_id`: good for "all transactions for one customer" queries.
- `transactions.product_id`: good for "all transactions for one product" queries.

Weaker demo columns:

- `customers.age`
- `customers.region`
- `transactions.status`
- `products.category`

These are low-cardinality columns, so many rows share the same value. A GSI on them can still work, but the query may touch many row groups and show little improvement.

Example GSI registrations:

```sql
SELECT public.register_gsi(
  'public.transactions'::regclass,
  'gsi_transactions_order_id',
  'order_id',
  '/tmp/data_lake/transactions'
);

SELECT public.register_gsi(
  'public.transactions'::regclass,
  'gsi_transactions_customer_id',
  'customer_id',
  '/tmp/data_lake/transactions'
);
```

Example query that should benefit from `transactions.order_id`:

```sql
SELECT *
FROM public.transactions
WHERE order_id = '<known-order-id>';
```
