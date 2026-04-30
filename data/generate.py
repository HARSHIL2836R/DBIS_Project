import argparse
import math
import os
import random
import re
import shutil
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import psycopg2
import psycopg2.extras



parser = argparse.ArgumentParser()
parser.add_argument(
    "--target",
    choices=["small", "medium", "large"],
    default="medium",
    help="small=~200MB  medium=~2GB  large=~10GB",
)
parser.add_argument("--out", default="./data_lake", help="Output directory")
parser.add_argument(
    "--mode",
    choices=["generate", "append"],
    default="generate",
    help="generate creates all base tables; append only adds new files",
)
parser.add_argument(
    "--add-table",
    choices=["customers", "products", "transactions"],
    default="transactions",
    help="Table to append files to when --mode append or --add-files is used",
)
parser.add_argument(
    "--add-files",
    type=int,
    default=0,
    help="Number of new Parquet files to append",
)
parser.add_argument(
    "--add-rows-per-file",
    type=int,
    default=None,
    help="Target rows per appended file; defaults to the table chunk size",
)
parser.add_argument(
    "--add-partition-value",
    default=None,
    help=(
        "For transaction appends, restrict new timestamps to a month like 2024-05 "
        "or a year like 2024"
    ),
)
parser.add_argument(
    "--tx-partition",
    choices=["month", "year", "none"],
    default="month",
    help="Directory layout for transactions",
)
parser.add_argument(
    "--tx-rows-per-file",
    type=int,
    default=None,
    help="Target rows per transaction Parquet file",
)
parser.add_argument(
    "--month-skew",
    type=float,
    default=0.0,
    help="0 is uniform; larger values concentrate transaction rows in fewer months",
)
parser.add_argument(
    "--file-skew",
    type=float,
    default=0.0,
    help="0 gives similar file sizes; larger values make file row counts uneven",
)
parser.add_argument(
    "--value-skew",
    type=float,
    default=0.0,
    help="0 is uniform; larger values skew regions, categories, statuses, and amounts",
)
parser.add_argument(
    "--id-skew",
    type=float,
    default=0.0,
    help="0 samples ids uniformly; larger values create hot customers/products",
)
parser.add_argument("--seed", type=int, default=42, help="Numpy/random seed")
args = parser.parse_args()

if args.mode == "append" and args.add_files <= 0:
    parser.error("--mode append requires --add-files > 0")
if args.add_files < 0:
    parser.error("--add-files cannot be negative")
if args.add_rows_per_file is not None and args.add_rows_per_file <= 0:
    parser.error("--add-rows-per-file must be positive")
if args.tx_rows_per_file is not None and args.tx_rows_per_file <= 0:
    parser.error("--tx-rows-per-file must be positive")
if min(args.month_skew, args.file_skew, args.value_skew, args.id_skew) < 0:
    parser.error("skew values must be >= 0")

CONFIGS = {
    "small": dict(
        num_customers=200_000,
        num_products=50_000,
        num_tx=2_000_000,
        chunk_tx=200_000,
        chunk_dim=100_000,
        row_group=50_000,
    ),
    "medium": dict(
        num_customers=1_000_000,
        num_products=200_000,
        num_tx=10_000_000,
        chunk_tx=400_000,
        chunk_dim=250_000,
        row_group=100_000,
    ),
    "large": dict(
        num_customers=10_000_000,
        num_products=2_000_000,
        num_tx=70_000_000,
        chunk_tx=800_000,
        chunk_dim=500_000,
        row_group=100_000,
    ),
}

DB_CONFIG = {
    "dbname": "postgres",
    "user": "postgres",
    "password": "hello",
    "host": "localhost",
    "port": "5432",
}
REFERENCE_SAMPLE_SIZE = 1_000_000

cfg = CONFIGS[args.target]

NUM_CUSTOMERS = cfg["num_customers"]
NUM_PRODUCTS = cfg["num_products"]
NUM_TX = cfg["num_tx"]
CHUNK_DIM = cfg["chunk_dim"]
ROW_GROUP = cfg["row_group"]
TX_ROWS_PER_FILE = args.tx_rows_per_file or cfg["chunk_tx"]
DATA_LAKE_DIR = Path(args.out)
TX_MONTHS = list(pd.period_range("2023-01", "2024-12", freq="M"))

REGIONS = ["Gujarat", "Maharashtra", "Rajasthan", "Punjab", "Tamil Nadu", "West Bengal"]
STATUSES = ["completed", "pending", "refunded", "failed", "processing"]
PAYMENTS = ["credit_card", "debit_card", "paypal", "crypto", "upi"]
LOYALTY = ["bronze", "silver", "gold", "platinum"]
CATS = [
    "Electronics",
    "Clothing",
    "Books",
    "Home & Garden",
    "Sports",
    "Toys",
    "Beauty",
    "Automotive",
    "Food",
    "Health",
]
TABLES = ["customers", "products", "transactions"]

np.random.seed(args.seed)
random.seed(args.seed)


# -- Helper functions --------------------------------------------------------------
def skewed_probs(size: int, skew: float) -> np.ndarray:
    if size <= 0:
        return np.array([], dtype=float)
    if skew <= 0:
        return np.full(size, 1 / size, dtype=float)

    ranks = np.arange(1, size + 1, dtype=float)
    weights = 1.0 / np.power(ranks, skew) # formula : weight = 1 / (rank^skew)
    weights = np.random.permutation(weights) # shuffle randomly
    return weights / weights.sum()


MONTH_PROBS = skewed_probs(len(TX_MONTHS), args.month_skew)
REGION_PROBS = skewed_probs(len(REGIONS), args.value_skew)
STATUS_PROBS = skewed_probs(len(STATUSES), args.value_skew)
PAYMENT_PROBS = skewed_probs(len(PAYMENTS), args.value_skew)
LOYALTY_PROBS = skewed_probs(len(LOYALTY), args.value_skew)
CAT_PROBS = skewed_probs(len(CATS), args.value_skew)


def exact_counts(total: int, probs: np.ndarray) -> np.ndarray:
    if total <= 0:
        return np.zeros(len(probs), dtype=np.int64)

    raw = probs * total
    counts = np.floor(raw).astype(np.int64)
    remainder = int(total - counts.sum())
    if remainder > 0:
        order = np.argsort(raw - counts)[::-1]
        counts[order[:remainder]] += 1
    return counts


def split_count(total: int, pieces: int, skew: float) -> np.ndarray:
    if total <= 0 or pieces <= 0:
        return np.array([], dtype=np.int64)

    pieces = min(total, pieces)
    counts = np.ones(pieces, dtype=np.int64)
    remaining = total - pieces
    if remaining > 0:
        counts += exact_counts(remaining, skewed_probs(pieces, skew))
    return counts


def choose(values: list[str], n: int, probs: np.ndarray) -> np.ndarray:
    return np.random.choice(np.asarray(values, dtype=object), n, p=probs)


def rand_date(n: int) -> np.ndarray:
    base = np.datetime64("2023-01-01", "D")
    #generate offsets up to 2 years (730 days) and add to base date
    off = np.random.randint(0, 730, n).astype("timedelta64[D]")
    return pd.to_datetime(base + off).date


def rand_ts_between(n: int, start, end) -> pd.DatetimeIndex:
    start = np.datetime64(start, "s")
    end = np.datetime64(end, "s")
    span_seconds = int((end - start) / np.timedelta64(1, "s"))
    #generates transaction timestamps uniformly between start and end by creating random second offsets and adding to start
    off = np.random.randint(0, span_seconds, n).astype("timedelta64[s]")
    return pd.to_datetime(start + off)


def month_bounds(month: pd.Period) -> tuple[pd.Timestamp, pd.Timestamp]:
    return month.to_timestamp(), (month + 1).to_timestamp()


def write_parquet(df: pd.DataFrame, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.Table.from_pandas(df, preserve_index=False),
        path,
        row_group_size=ROW_GROUP,
        compression="snappy",
    )


def readable_size(path: Path) -> str:
    b = path.stat().st_size
    return f"{b / 1024**2:.1f} MB" if b < 1024**3 else f"{b / 1024**3:.2f} GB"


def id_pool(n: int) -> np.ndarray:
    if n <= 0:
        return np.array([], dtype=object)
    raw = np.frombuffer(os.urandom(16 * n), dtype=np.uint8).reshape(n, 16)
    return np.array([r.tobytes().hex() for r in raw], dtype=object)


def parquet_files(root: Path) -> list[Path]:
    if not root.exists():
        return []
    return sorted(path for path in root.rglob("*.parquet") if path.is_file())


def reset_table_dirs(tables: set[str]) -> None:
    for table in TABLES:
        root = DATA_LAKE_DIR / table
        if table in tables and root.exists():
            print(f"Removing old {table} files from {root}")
            shutil.rmtree(root)
        root.mkdir(parents=True, exist_ok=True)

# After appending files, this function calls the PostgreSQL stored procedure to refresh the foreign table's file list. This is necessary for PostgreSQL to recognize the new files when using parquet_fdw.
def refresh_foreign_table_files(table: str) -> None:
    data_dir = str((DATA_LAKE_DIR / table).resolve())

    print(f"\nRefreshing public.{table} foreign table file list ...")
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT public.refresh_parquet_foreign_table_files(
                    %s::regclass,
                    %s::jsonb
                );
                """,
                (
                    f"public.{table}",
                    psycopg2.extras.Json({"dir": data_dir}),
                ),
            )
        conn.commit()
    except Exception as exc:
        if conn is not None:
            conn.rollback()
        safe_db_config = dict(DB_CONFIG)
        if safe_db_config.get("password"):
            safe_db_config["password"] = "***"
        raise SystemExit(
            "PostgreSQL refresh failed after appending files.\n"
            f"DB config: {safe_db_config}\n"
            f"Error: {exc}\n"
            "Refresh is required after append mode so PostgreSQL sees new files."
        ) from exc
    finally:
        if conn is not None:
            conn.close()

    print(f"Refreshed public.{table} with files from {data_dir}")


def next_file_index(root: Path, prefix: str) -> int:
    pattern = re.compile(rf"^{re.escape(prefix)}_(\d+)\.parquet$")
    highest = -1
    for path in parquet_files(root):
        match = pattern.match(path.name)
        if match:
            highest = max(highest, int(match.group(1)))
    return highest + 1


def load_existing_ids(table: str, column: str, max_ids: int) -> np.ndarray | None:
    ids = []
    remaining = max_ids
    for path in parquet_files(DATA_LAKE_DIR / table):
        if remaining <= 0:
            break
        arr = (
            pq.read_table(path, columns=[column])
            .column(column)
            .to_numpy(zero_copy_only=False)
        )
        arr = np.asarray(arr, dtype=object)
        ids.append(arr[:remaining])
        remaining -= len(ids[-1])

    if not ids:
        return None
    return np.concatenate(ids)


def choose_pool_ids(pool: np.ndarray, n: int, skew: float) -> np.ndarray:
    pool_size = len(pool)
    if pool_size == 0:
        raise ValueError("Cannot sample ids from an empty pool")
    if skew <= 0:
        return pool[np.random.randint(0, pool_size, n)]

    hot_fraction = max(0.001, min(0.25, 0.20 / (1.0 + skew)))
    hot_size = max(1, int(pool_size * hot_fraction))
    hot_prob = min(0.95, skew / (skew + 1.0))
    use_hot = np.random.random(n) < hot_prob

    idx = np.empty(n, dtype=np.int64)
    idx[use_hot] = np.random.randint(0, hot_size, int(use_hot.sum()))
    idx[~use_hot] = np.random.randint(0, pool_size, int((~use_hot).sum()))
    return pool[idx]


def transaction_dir(month: pd.Period) -> Path:
    # parquet_fdw receives recursive file paths from starter.sql. Directory names
    # are only physical layout here unless SQL partitions are created separately.
    if args.tx_partition == "month":
        return DATA_LAKE_DIR / "transactions" / f"month={month}"
    if args.tx_partition == "year":
        return DATA_LAKE_DIR / "transactions" / f"year={month.year}"
    return DATA_LAKE_DIR / "transactions"


def choose_transaction_month() -> pd.Period:
    return TX_MONTHS[int(np.random.choice(len(TX_MONTHS), p=MONTH_PROBS))]


def parse_partition_month(value: str | None) -> pd.Period:
    if not value:
        return choose_transaction_month()

    if re.fullmatch(r"\d{4}", value):
        year = int(value)
        valid_months = [m for m in TX_MONTHS if m.year == year]
        if not valid_months:
            raise ValueError(f"Partition year {value} is outside 2023-2024")
        return valid_months[int(np.random.randint(0, len(valid_months)))]

    month = pd.Period(value, freq="M")
    if month not in TX_MONTHS:
        raise ValueError(f"Partition month {value} is outside 2023-01..2024-12")
    return month


def make_customers(
    customer_ids: np.ndarray,
    start_index: int,
    email_prefix: str = "user",
    ci: int = 0,
) -> pd.DataFrame:
    n = len(customer_ids)
    return pd.DataFrame(
        {
            "customer_id": customer_ids,
            "email": [
                f"{email_prefix}_{start_index + i}@example.com" for i in range(n)
            ],
            "region": choose(REGIONS, n, REGION_PROBS),
            "age": np.random.randint(18+ci, 80, n).astype(np.int32),
            "loyalty_tier": choose(LOYALTY, n, LOYALTY_PROBS),
            "signup_date": rand_date(n),
            "total_orders": np.random.randint(0, 500, n).astype(np.int32),
            "lifetime_value": np.round(np.random.uniform(0, 50_000, n), 2),
        }
    )


def make_products(product_ids: np.ndarray, start_index: int) -> pd.DataFrame:
    n = len(product_ids)
    return pd.DataFrame(
        {
            "product_id": product_ids,
            "name": [f"Product_{start_index + i}" for i in range(n)],
            "category": choose(CATS, n, CAT_PROBS),
            "price": np.round(np.random.uniform(5.0, 999.99, n), 2),
            "stock": np.random.randint(0, 10_000, n).astype(np.int32),
            "brand": [f"Brand_{np.random.randint(1, 500)}" for _ in range(n)],
            "weight_kg": np.round(np.random.uniform(0.1, 50.0, n), 3),
            "rating": np.round(np.random.uniform(1.0, 5.0, n), 1),
            "reviews": np.random.randint(0, 100_000, n).astype(np.int32),
        }
    )


def skewed_amounts(n: int) -> np.ndarray:
    if args.value_skew <= 0:
        return np.round(np.random.uniform(5.0, 1999.99, n), 2)

    sigma = min(2.0, 0.6 + args.value_skew * 0.3)
    values = np.random.lognormal(mean=4.8, sigma=sigma, size=n)
    values = np.clip(values, 5.0, 1999.99)
    return np.round(values, 2)


def make_transactions(
    n: int,
    customer_ids: np.ndarray,
    product_ids: np.ndarray,
    ts_start,
    ts_end,
) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "order_id": id_pool(n),
            "customer_id": choose_pool_ids(customer_ids, n, args.id_skew),
            "product_id": choose_pool_ids(product_ids, n, args.id_skew),
            "amount": skewed_amounts(n),
            "quantity": np.random.randint(1, 20, n).astype(np.int32),
            "status": choose(STATUSES, n, STATUS_PROBS),
            "region": choose(REGIONS, n, REGION_PROBS),
            "timestamp": rand_ts_between(n, ts_start, ts_end),
            "payment_method": choose(PAYMENTS, n, PAYMENT_PROBS),
            "is_returned": np.random.choice([True, False], n, p=[0.05, 0.95]),
            "warehouse_id": np.random.randint(1, 50, n).astype(np.int32),
            "shipping_days": np.random.randint(1, 30, n).astype(np.int32),
        }
    )


def generate_customers() -> np.ndarray:
    print(f"\n[1/3] Generating {NUM_CUSTOMERS:,} customers ...")
    all_customer_ids = id_pool(NUM_CUSTOMERS)
    total_chunks = (NUM_CUSTOMERS + CHUNK_DIM - 1) // CHUNK_DIM

    for ci, start in enumerate(range(0, NUM_CUSTOMERS, CHUNK_DIM)):
        end = min(start + CHUNK_DIM, NUM_CUSTOMERS)
        chunk = make_customers(all_customer_ids[start:end], start,ci)
        path = DATA_LAKE_DIR / "customers" / f"customers_chunk_{ci:04d}.parquet"
        write_parquet(chunk, path)
        if ci % 10 == 0 or end == NUM_CUSTOMERS:
            print(f"  chunk {ci:04d}/{total_chunks - 1:04d}  {readable_size(path)}")

    print("Done")
    return all_customer_ids


def generate_products() -> np.ndarray:
    print(f"\n[2/3] Generating {NUM_PRODUCTS:,} products ...")
    all_product_ids = id_pool(NUM_PRODUCTS)
    total_chunks = (NUM_PRODUCTS + CHUNK_DIM - 1) // CHUNK_DIM

    for ci, start in enumerate(range(0, NUM_PRODUCTS, CHUNK_DIM)):
        end = min(start + CHUNK_DIM, NUM_PRODUCTS)
        chunk = make_products(all_product_ids[start:end], start)
        path = DATA_LAKE_DIR / "products" / f"products_chunk_{ci:04d}.parquet"
        write_parquet(chunk, path)
        if ci % 5 == 0 or end == NUM_PRODUCTS:
            print(f"  chunk {ci:04d}/{total_chunks - 1:04d}  {readable_size(path)}")

    print("Done")
    return all_product_ids


def load_or_make_reference_ids(
    table: str,
    column: str,
    fallback_rows: int,
    already_loaded: np.ndarray | None,
) -> np.ndarray:
    if already_loaded is not None:
        return already_loaded

    loaded = load_existing_ids(table, column, REFERENCE_SAMPLE_SIZE)
    if loaded is not None:
        print(f"  loaded {len(loaded):,} existing {table}.{column} values")
        return loaded

    temporary_rows = min(fallback_rows, REFERENCE_SAMPLE_SIZE)
    print(f"  no existing {table} ids found; using {temporary_rows:,} temporary ids")
    return id_pool(temporary_rows)


def generate_transactions(customer_ids: np.ndarray, product_ids: np.ndarray) -> None:
    print(f"\n[3/3] Generating {NUM_TX:,} transactions ...")
    print(
        f"  partition={args.tx_partition}  rows/file~={TX_ROWS_PER_FILE:,}  "
        f"month_skew={args.month_skew:g}  file_skew={args.file_skew:g}"
    )

    month_counts = exact_counts(NUM_TX, MONTH_PROBS)
    file_idx = next_file_index(DATA_LAKE_DIR / "transactions", "transactions_chunk")
    total_files = sum(
        math.ceil(count / TX_ROWS_PER_FILE) for count in month_counts if count > 0
    )
    files_written = 0

    for month, month_rows in zip(TX_MONTHS, month_counts):
        month_rows = int(month_rows)
        if month_rows <= 0:
            continue

        file_count = math.ceil(month_rows / TX_ROWS_PER_FILE)
        row_counts = split_count(month_rows, file_count, args.file_skew)
        ts_start, ts_end = month_bounds(month)

        for rows in row_counts:
            rows = int(rows)
            path = transaction_dir(month) / f"transactions_chunk_{file_idx:04d}.parquet"
            file_idx += 1
            chunk = make_transactions(rows, customer_ids, product_ids, ts_start, ts_end)
            write_parquet(chunk, path)

            files_written += 1
            if files_written % 10 == 0 or files_written == total_files:
                done_pct = files_written / total_files * 100
                print(
                    f"  file {files_written:04d}/{total_files:04d}  "
                    f"{done_pct:5.1f}%  {month}  rows={rows:,}"
                )

    print("Done")


def append_customers(file_count: int, rows_per_file: int) -> None:
    root = DATA_LAKE_DIR / "customers"
    file_idx = next_file_index(root, "customers_chunk")
    total_rows = file_count * rows_per_file
    row_counts = split_count(total_rows, file_count, args.file_skew)

    print(f"\nAppending {file_count:,} customer files ...")
    for i, rows in enumerate(row_counts):
        rows = int(rows)
        current_idx = file_idx + i
        chunk = make_customers(id_pool(rows), current_idx * rows_per_file, "user_append")
        path = root / f"customers_chunk_{current_idx:04d}.parquet"
        write_parquet(chunk, path)
        print(f"  {path}  rows={rows:,}  {readable_size(path)}")


def append_products(file_count: int, rows_per_file: int) -> None:
    root = DATA_LAKE_DIR / "products"
    file_idx = next_file_index(root, "products_chunk")
    total_rows = file_count * rows_per_file
    row_counts = split_count(total_rows, file_count, args.file_skew)

    print(f"\nAppending {file_count:,} product files ...")
    for i, rows in enumerate(row_counts):
        rows = int(rows)
        current_idx = file_idx + i
        chunk = make_products(id_pool(rows), current_idx * rows_per_file)
        path = root / f"products_chunk_{current_idx:04d}.parquet"
        write_parquet(chunk, path)
        print(f"  {path}  rows={rows:,}  {readable_size(path)}")


def append_transactions(
    file_count: int,
    rows_per_file: int,
    customer_ids: np.ndarray | None,
    product_ids: np.ndarray | None,
) -> None:
    customer_ids = load_or_make_reference_ids(
        "customers", "customer_id", NUM_CUSTOMERS, customer_ids
    )
    product_ids = load_or_make_reference_ids(
        "products", "product_id", NUM_PRODUCTS, product_ids
    )

    root = DATA_LAKE_DIR / "transactions"
    file_idx = next_file_index(root, "transactions_chunk")
    total_rows = file_count * rows_per_file
    row_counts = split_count(total_rows, file_count, args.file_skew)

    print(f"\nAppending {file_count:,} transaction files ...")
    for i, rows in enumerate(row_counts):
        rows = int(rows)
        month = parse_partition_month(args.add_partition_value)
        ts_start, ts_end = month_bounds(month)
        current_idx = file_idx + i
        chunk = make_transactions(rows, customer_ids, product_ids, ts_start, ts_end)
        path = transaction_dir(month) / f"transactions_chunk_{current_idx:04d}.parquet"
        write_parquet(chunk, path)
        print(f"  {path}  rows={rows:,}  {readable_size(path)}")


def append_files(
    table: str,
    file_count: int,
    loaded_customers: np.ndarray | None,
    loaded_products: np.ndarray | None,
) -> None:
    if file_count <= 0:
        return

    if table == "transactions":
        rows_per_file = args.add_rows_per_file or TX_ROWS_PER_FILE
        append_transactions(file_count, rows_per_file, loaded_customers, loaded_products)
    elif table == "customers":
        rows_per_file = args.add_rows_per_file or CHUNK_DIM
        append_customers(file_count, rows_per_file)
    elif table == "products":
        rows_per_file = args.add_rows_per_file or CHUNK_DIM
        append_products(file_count, rows_per_file)
    else:
        raise ValueError(f"Unsupported append table: {table}")


def summarize() -> None:
    print("\n" + "=" * 65)
    print("DATA LAKE SUMMARY")
    print("=" * 65)
    grand = 0

    for folder in ["customers", "products", "transactions"]:
        files = parquet_files(DATA_LAKE_DIR / folder)
        b = sum(path.stat().st_size for path in files)
        grand += b
        print(f"  {folder:15s}: {len(files):4d} files  {b / 1024**3:.3f} GB")

    print(f"  {'-' * 45}")
    print(f"  {'TOTAL':15s}:        {grand / 1024**3:.3f} GB")

    sample_tx = next(iter(parquet_files(DATA_LAKE_DIR / "transactions")), None)
    if sample_tx is not None:
        meta = pq.read_metadata(sample_tx)
        schema = pq.read_schema(sample_tx)
        print(f"\nTransaction file schema  : {schema.names}")
        print(f"Row groups per file      : {meta.num_row_groups}")
        print(f"Rows per file            : {meta.num_rows:,}")


## delete all files in data lake

if __name__ == "__main__":
    print(f"Target: {args.target}  |  Output: {DATA_LAKE_DIR}")
    print(
        f"Mode  : {args.mode}  |  Base tables: all  |  Append table: {args.add_table}"
    )
    print(f"  Customers    : {NUM_CUSTOMERS:>12,} rows")
    print(f"  Products     : {NUM_PRODUCTS:>12,} rows")
    print(f"  Transactions : {NUM_TX:>12,} rows")

    all_customer_ids = None
    all_product_ids = None

    if args.mode == "generate":
        reset_table_dirs(set(TABLES))
        all_customer_ids = generate_customers()
        all_product_ids = generate_products()
        generate_transactions(all_customer_ids, all_product_ids)
    else:
        reset_table_dirs(set())

    append_files(args.add_table, args.add_files, all_customer_ids, all_product_ids)

    if args.add_files > 0:
        refresh_foreign_table_files(args.add_table)

    summarize()
