"""
This script Creates a data lake with three main tables:
    1. Customers: (customer_id, email, region, age, loyalty_tier, 
                    signup_date, total_orders, lifetime_value)
    2. Products: (product_id, name, category, price, stock, brand, weight_kg, rating, reviews)
    3. Transactions: (order_id, customer_id, product_id, amount, quantity, status, region, 
                        timestamp, payment_method, is_returned, warehouse_id, shipping_days)
"""

import os
import random
import uuid
import time
import argparse

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


parser = argparse.ArgumentParser()
parser.add_argument("--target", choices=["small","medium","large"], default="medium",   
                    help="small=~200MB  medium=~2GB  large=~10GB")
parser.add_argument("--out", default="./data_lake", help="Output directory")
args = parser.parse_args()

CONFIGS = {
    "small":  dict(num_customers=200000,  num_products=50000,   num_tx=2000000,  chunk_tx=200000, chunk_dim=100000, row_group=50000),
    "medium": dict(num_customers=1000000, num_products=200000,  num_tx=10000000, chunk_tx=400000, chunk_dim=250000, row_group=100000),
    "large":  dict(num_customers=10000000, num_products=2000000, num_tx=70000000, chunk_tx=800000, chunk_dim=500000, row_group=100000),
}
cfg = CONFIGS[args.target]

NUM_CUSTOMERS = cfg["num_customers"]
NUM_PRODUCTS  = cfg["num_products"]
NUM_TX        = cfg["num_tx"]
CHUNK_TX      = cfg["chunk_tx"]
CHUNK_DIM     = cfg["chunk_dim"]
ROW_GROUP     = cfg["row_group"]
DATA_LAKE_DIR = args.out

REGIONS   = ["Gujarat", "Maharashtra", "Rajasthan", "Punjab", "Tamil Nadu", "West Bengal"]
STATUSES  = ["completed", "pending", "refunded", "failed", "processing"]
PAYMENTS  = ["credit_card", "debit_card", "paypal", "crypto", "upi"]
LOYALTY   = ["bronze", "silver", "gold", "platinum"]
CATS      = ["Electronics","Clothing","Books","Home & Garden","Sports",
             "Toys","Beauty","Automotive","Food","Health"]

np.random.seed(42)
random.seed(42)

print(f"Target: {args.target}  |  Output: {DATA_LAKE_DIR}")
print(f"  Customers    : {NUM_CUSTOMERS:>12,} rows")
print(f"  Products     : {NUM_PRODUCTS:>12,} rows")
print(f"  Transactions : {NUM_TX:>12,} rows")


for sub in ["customers", "products", "transactions"]:
    os.makedirs(f"{DATA_LAKE_DIR}/{sub}", exist_ok=True)


# -- Helper functions --------------------------------------------------------------
## random date generator within a 2 year range starting from 2023-01-01
def rand_date(n: int) -> np.ndarray:
    base = np.datetime64("2023-01-01", "D")
    off  = np.random.randint(0, 730, n).astype("timedelta64[D]")
    return pd.to_datetime(base + off).date

## random timestamp generator across the full 2-year transaction window
def rand_ts(n: int) -> np.ndarray:
    base = np.datetime64("2023-01-01", "s")
    off = np.random.randint(0, 730 * 86400, n).astype("timedelta64[s]")
    return pd.to_datetime(base + off)

def write_parquet(df: pd.DataFrame, path: str):
    pq.write_table(
        pa.Table.from_pandas(df, preserve_index=False),
        path,
        row_group_size=ROW_GROUP,
        compression="snappy",
    )

# print human-readable file size
def readable_size(path: str) -> str:
    b = os.path.getsize(path)
    return f"{b/1024**2:.1f} MB" if b < 1024**3 else f"{b/1024**3:.2f} GB"


# UUID hex generation.
def id_pool(n: int) -> np.ndarray:
    raw = np.frombuffer(os.urandom(16 * n), dtype=np.uint8).reshape(n, 16)
    return np.array([r.tobytes().hex() for r in raw])


# -- Customers ----------------------------------------------------------------------
print(f"\n[1/3] Generating {NUM_CUSTOMERS:,} customers ...")

all_cids = id_pool(NUM_CUSTOMERS)

for ci, start in enumerate(range(0, NUM_CUSTOMERS, CHUNK_DIM)):
    end   = min(start + CHUNK_DIM, NUM_CUSTOMERS)
    n     = end - start
    chunk = pd.DataFrame({
        "customer_id":  all_cids[start:end],
        "email":        [f"user_{start+i}@example.com" for i in range(n)],
        "region":       np.random.choice(REGIONS, n),
        "age":          np.random.randint(18, 80, n).astype(np.int32),
        "loyalty_tier": np.random.choice(LOYALTY, n),
        "signup_date":  rand_date(n),
        "total_orders": np.random.randint(0, 500, n).astype(np.int32),
        "lifetime_value": np.round(np.random.uniform(0, 50000, n), 2),
    })
    path = f"{DATA_LAKE_DIR}/customers/customers_chunk_{ci:04d}.parquet"
    write_parquet(chunk, path)
    if ci % 10 == 0 or end == NUM_CUSTOMERS:
        print(f"  chunk {ci:04d}/{(NUM_CUSTOMERS-1)//CHUNK_DIM:04d}  {readable_size(path)}")

print(f"Done ")


# -- Products -----------------------------------------------------------------------
print(f"\n[2/3] Generating {NUM_PRODUCTS:,} products ...")

all_pids = id_pool(NUM_PRODUCTS)

for ci, start in enumerate(range(0, NUM_PRODUCTS, CHUNK_DIM)):
    end   = min(start + CHUNK_DIM, NUM_PRODUCTS)
    n     = end - start
    chunk = pd.DataFrame({
        "product_id": all_pids[start:end],
        "name":       [f"Product_{start+i}" for i in range(n)],
        "category":   np.random.choice(CATS, n),
        "price":      np.round(np.random.uniform(5.0, 999.99, n), 2),
        "stock":      np.random.randint(0, 10_000, n).astype(np.int32),
        "brand":      [f"Brand_{np.random.randint(1,500)}" for _ in range(n)],
        "weight_kg":  np.round(np.random.uniform(0.1, 50.0, n), 3),
        "rating":     np.round(np.random.uniform(1.0, 5.0, n), 1),
        "reviews":    np.random.randint(0, 100_000, n).astype(np.int32),
    })
    path = f"{DATA_LAKE_DIR}/products/products_chunk_{ci:04d}.parquet"
    write_parquet(chunk, path)
    if ci % 5 == 0 or end == NUM_PRODUCTS:
        print(f"  chunk {ci:04d}/{(NUM_PRODUCTS-1)//CHUNK_DIM:04d}  {readable_size(path)}")

print(f"Done")


# -- Transactions ----------------------------------------------------------------------
print(f"\n[3/3] Generating {NUM_TX:,} transactions ...")
total_chunks = (NUM_TX + CHUNK_TX - 1) // CHUNK_TX
TX_FILE_TARGET = CHUNK_TX
month_buffers = {}
global_tx_file_idx = 0

for ci, start in enumerate(range(0, NUM_TX, CHUNK_TX)):
    end = min(start + CHUNK_TX, NUM_TX)
    n   = end - start

    chunk = pd.DataFrame({
        "order_id":       id_pool(n),
        "customer_id":    all_cids[np.random.randint(0, NUM_CUSTOMERS, n)],
        "product_id":     all_pids[np.random.randint(0, NUM_PRODUCTS,  n)],
        "amount":         np.round(np.random.uniform(5.0, 1999.99, n), 2),
        "quantity":       np.random.randint(1, 20, n).astype(np.int32),
        "status":         np.random.choice(STATUSES, n),
        "region":         np.random.choice(REGIONS, n),
        "timestamp":      rand_ts(n),
        "payment_method": np.random.choice(PAYMENTS, n),
        "is_returned":    np.random.choice([True, False], n, p=[0.05, 0.95]),
        "warehouse_id":   np.random.randint(1, 50, n).astype(np.int32),
        "shipping_days":  np.random.randint(1, 30, n).astype(np.int32),
    })
    chunk["txn_month"] = chunk["timestamp"].dt.to_period("M").astype(str)
    month_count = chunk["txn_month"].nunique()

    for txn_month_str, month_chunk in chunk.groupby("txn_month", sort=False):
        if txn_month_str in month_buffers:
            month_buffers[txn_month_str] = pd.concat(
                [month_buffers[txn_month_str], month_chunk],
                ignore_index=True,
            )
        else:
            month_buffers[txn_month_str] = month_chunk.reset_index(drop=True)

        while len(month_buffers[txn_month_str]) >= TX_FILE_TARGET:
            tx_dir = f"{DATA_LAKE_DIR}/transactions/month={txn_month_str}"
            os.makedirs(tx_dir, exist_ok=True)

            out_chunk = month_buffers[txn_month_str].iloc[:TX_FILE_TARGET].copy()
            month_buffers[txn_month_str] = month_buffers[txn_month_str].iloc[TX_FILE_TARGET:].reset_index(drop=True)

            path = f"{tx_dir}/transactions_chunk_{global_tx_file_idx:04d}.parquet"
            global_tx_file_idx += 1
            write_parquet(out_chunk.drop(columns=["txn_month"]), path)

    done_pct  = end / NUM_TX * 100
    print(f"  chunk {ci:04d}/{total_chunks-1:04d}  {done_pct:5.1f}%  "
          f"{month_count} month partitions")

for txn_month_str, month_buffer in month_buffers.items():
    if len(month_buffer) == 0:
        continue

    tx_dir = f"{DATA_LAKE_DIR}/transactions/month={txn_month_str}"
    os.makedirs(tx_dir, exist_ok=True)

    path = f"{tx_dir}/transactions_chunk_{global_tx_file_idx:04d}.parquet"
    global_tx_file_idx += 1
    write_parquet(month_buffer.drop(columns=["txn_month"]), path)

print(f"Done")


# -- Summary ---------------------------------------------------------------------------
print("\n" + "="*65)
print("DATA LAKE SUMMARY")
print("="*65)
grand = 0
for folder in ["customers", "products", "transactions"]:
    files = []
    for root, _, names in os.walk(f"{DATA_LAKE_DIR}/{folder}"):
        for name in names:
            if name.endswith(".parquet"):
                files.append(os.path.join(root, name))
    b = sum(os.path.getsize(f) for f in files)
    grand += b
    print(f"  {folder:15s}: {len(files):4d} files  {b/1024**3:.3f} GB")
print(f"  {'─'*45}")
print(f"  {'TOTAL':15s}:        {grand/1024**3:.3f} GB")

sample_tx = None
for root, _, names in os.walk(f"{DATA_LAKE_DIR}/transactions"):
    for name in names:
        if name.endswith(".parquet"):
            sample_tx = os.path.join(root, name)
            break
    if sample_tx is not None:
        break

if sample_tx is not None:
    meta   = pq.read_metadata(sample_tx)
    schema = pq.read_schema(sample_tx)
    print(f"\nTransaction file schema  : {schema.names}")
    print(f"Row groups per file      : {meta.num_row_groups}")
    print(f"Rows per file            : {meta.num_rows:,}")
