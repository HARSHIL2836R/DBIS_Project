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

Important: `generate` mode deletes the old `customers/`, `products/`, and `transactions/` folders before writing new data.

Generate a smaller dataset in a custom directory:

```bash
python3 ./generate.py --target small --out ./data_lake
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

- `generate` default: delete and recreate all base table folders
- `append`: write new files into an existing lake without deleting old files

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

## Probability Arrays

The generator converts skew CLI values into probability arrays before creating rows:

```python
MONTH_PROBS = skewed_probs(len(TX_MONTHS), args.month_skew)
REGION_PROBS = skewed_probs(len(REGIONS), args.value_skew)
STATUS_PROBS = skewed_probs(len(STATUSES), args.value_skew)
PAYMENT_PROBS = skewed_probs(len(PAYMENTS), args.value_skew)
LOYALTY_PROBS = skewed_probs(len(LOYALTY), args.value_skew)
CAT_PROBS = skewed_probs(len(CATS), args.value_skew)
```

Each array has one probability per possible value. For example, `REGION_PROBS` has one probability for each region in `REGIONS`, and `CAT_PROBS` has one probability for each product category in `CATS`.

When the skew value is `0`, every value gets the same probability. If there are 6 regions, each region gets about `1/6` of the rows.

When the skew value is greater than `0`, some values receive much larger probabilities and others receive smaller probabilities. That creates non-uniform data, like one region or category appearing much more often.

Where these arrays are used:

- `MONTH_PROBS`: decides how many transaction rows go into each month.
- `REGION_PROBS`: chooses customer and transaction regions.
- `STATUS_PROBS`: chooses transaction status values.
- `PAYMENT_PROBS`: chooses transaction payment methods.
- `LOYALTY_PROBS`: chooses customer loyalty tiers.
- `CAT_PROBS`: chooses product categories.

Example:

```bash
python3 ./generate.py --month-skew 1.5 --value-skew 1.2
```

This creates a lake where some months have many more transactions, and some regions, statuses, payment methods, loyalty tiers, and categories are much more common than others.

## Count Helpers

The generator uses two helper functions to convert probabilities and file targets into exact integer row counts:

```python
def exact_counts(total: int, probs: np.ndarray) -> np.ndarray:
    ...

def split_count(total: int, pieces: int, skew: float) -> np.ndarray:
    ...
```

### `exact_counts(total, probs)`

`exact_counts()` converts probability weights into integer counts that add up exactly to `total`.

Example use case:

```python
month_counts = exact_counts(NUM_TX, MONTH_PROBS)
```

If `NUM_TX` is `2,000,000`, this decides exactly how many transaction rows each month gets. The important part is that the final month counts always sum back to exactly `2,000,000`.

Why this is needed:

- `probs * total` produces decimal values.
- Parquet rows must be whole numbers.
- Simple rounding can lose or add rows.
- `exact_counts()` floors the decimal counts, then gives leftover rows to the largest fractional remainders.

Small example:

```text
total = 10
probs = [0.33, 0.33, 0.34]
raw = [3.3, 3.3, 3.4]
floor = [3, 3, 3]
remainder = 1
final = [3, 3, 4]
```

### `split_count(total, pieces, skew)`

`split_count()` splits a total number of rows across a fixed number of files or chunks.

Example use case:

```python
row_counts = split_count(month_rows, file_count, args.file_skew)
```

If one month has `450,000` rows and needs `3` Parquet files, this returns something like:

```text
[150000, 150000, 150000]
```

With `--file-skew` greater than `0`, it can return uneven file sizes, such as:

```text
[240000, 130000, 80000]
```

Why this is needed:

- Every output file should get at least one row.
- The row counts must add up exactly to the month or append total.
- `--file-skew` should make file sizes non-uniform without changing the total row count.

Where it is used:

- In full transaction generation, it splits each month into Parquet files.
- In append mode, it splits appended rows across the requested number of new files.

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

Generate all tables with monthly transaction partitions:

```bash
python3 ./generate.py --tx-partition month
```

Generate all tables with year-based transaction folders:

```bash
python3 ./generate.py --tx-partition year
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
- In generate mode, old `customers/`, `products/`, and `transactions/` folders are removed before new files are written.
- In append mode, file numbering continues from the highest existing file index it finds.
- In append mode, the script always refreshes the PostgreSQL foreign table file list.
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
