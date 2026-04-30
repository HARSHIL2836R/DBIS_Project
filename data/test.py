import os
import random
import time
import psycopg2
import psycopg2.extras
import pyarrow.parquet as pq
import pandas as pd

# ─── Configuration ─────────────────────────────────────────────────────────
DB_CONFIG = {
    "dbname": "postgres",
    "user": "postgres",
    "password": "hello",
    "host": "localhost",
    "port": "5432"
}

# Explicitly lock the path to the transactions directory to prevent PyArrow 
# from wandering into the products or customers folders.
TX_DIR = "./data_lake/transactions"

# The PostgreSQL FDW table name
PG_TABLE_NAME = "transactions" 

# Number of random point-lookups to test
NUM_TESTS = 10


# ─── 1. Source of Truth: Raw Parquet Extractor ─────────────────────────────
def fetch_from_parquet(order_id: str) -> list:
    """Scans the raw Parquet dataset using PyArrow to find the exact record."""
    table = pq.read_table(
        TX_DIR, 
        filters=[('order_id', '=', order_id)]
    )
    
    df = table.to_pandas()
    
    # Drop PyArrow's virtual partition column if it exists to match SQL schema
    if 'month' in df.columns:
        df = df.drop(columns=['month'])
    
    # Convert timestamps/decimals to strings to match psycopg2 default output behavior
    df['timestamp'] = df['timestamp'].astype(str)
    df['amount'] = df['amount'].astype(float).round(2)
    
    return df.to_dict(orient='records')


# ─── 2. FDW Extractor: PostgreSQL psycopg2 ─────────────────────────────────
def fetch_from_postgres(order_id: str) -> tuple:
    """Queries the PostgreSQL database to trigger the FDW Index Scan."""
    conn = None
    results = []
    pg_time = 0.0  
    
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        query = f"SELECT * FROM {PG_TABLE_NAME} WHERE order_id = %s;"
        
        start_time = time.time()
        cursor.execute(query, (order_id,))
        rows = cursor.fetchall()
        pg_time = time.time() - start_time
        
        # Clean up data types to match pandas output for strict equality
        for row in rows:
            row['timestamp'] = str(row['timestamp'])
            row['amount'] = round(float(row['amount']), 2)
            results.append(dict(row))
            
    except Exception as e:
        print(f"❌ PostgreSQL Error: {e}")
    finally:
        if conn:
            conn.close()
            
    return results, pg_time


def fetch_scalar_from_postgres(query: str) -> tuple:
    """Executes an aggregate query in PostgreSQL and returns the single result."""
    conn = None
    result = None
    pg_time = 0.0
    
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        start_time = time.time()
        cursor.execute(query)
        res = cursor.fetchone()
        if res:
            result = res[0] 
        pg_time = time.time() - start_time
            
    except Exception as e:
        print(f"❌ PostgreSQL Error: {e}")
    finally:
        if conn:
            conn.close()
            
    return result, pg_time


# ─── 3. Test Setup & Execution ─────────────────────────────────────────────
def get_random_test_keys(num_keys: int) -> list:
    """Extracts random order_ids by recursively finding a Parquet file."""
    first_file = None
    
    # Walk the directory tree to find the first .parquet file
    for root, dirs, files in os.walk(TX_DIR):
        for file in files:
            if file.endswith('.parquet'):
                first_file = os.path.join(root, file)
                break
        if first_file:
            break
            
    if not first_file:
        raise FileNotFoundError(f"No parquet files found anywhere in {TX_DIR}")
    
    table = pq.read_table(first_file, columns=['order_id'])
    
    # Grab random IDs from the chunk
    all_ids = table['order_id'].to_pylist()
    return random.sample(all_ids, min(num_keys, len(all_ids)))


def run_validation():
    print(f"🚀 Starting Point-Lookup Validation Engine...")
    print(f"Targeting {NUM_TESTS} random point-lookups.\n")
    
    try:
        test_keys = get_random_test_keys(NUM_TESTS)
    except Exception as e:
        print(f"Failed to fetch test keys: {e}")
        return

    passed = 0
    failed = 0

    for idx, order_id in enumerate(test_keys, 1):
        print(f"Test [{idx}/{NUM_TESTS}] | order_id: {order_id[:8]}...")
        
        # Fetch from both sources
        pq_truth = fetch_from_parquet(order_id)
        pg_result, pg_latency = fetch_from_postgres(order_id)
        
        # Validation Logic
        if len(pq_truth) == 0:
            print(f"  ⚠️ Warning: Key not found in Parquet (Unexpected). Skipping.")
            continue
            
        if len(pg_result) != len(pq_truth):
            print(f"  ❌ FAILED: Row count mismatch. PG returned {len(pg_result)}, Parquet has {len(pq_truth)}.")
            failed += 1
            continue
            
        # Deep dictionary comparison
        if pg_result[0] == pq_truth[0]:
            print(f"  ✅ PASSED | PG Latency: {pg_latency:.4f}s")
            passed += 1
        else:
            print(f"  ❌ FAILED: Data mismatch.")
            print(f"     PG Data: {pg_result[0]}")
            print(f"     PQ Data: {pq_truth[0]}")
            failed += 1

    print("\n" + "="*40)
    print("POINT-LOOKUP SUMMARY")
    print("="*40)
    print(f"Total Tests : {NUM_TESTS}")
    print(f"Passed      : {passed}")
    print(f"Failed      : {failed}")


def run_aggregate_tests():
    print(f"\n🚀 Starting Aggregate Validation Engine...")
    print(f"Calculating baseline math by streaming Parquet files (memory-efficient)...")
    
    total_rows = 0
    total_amount = 0.0
    completed_count = 0
    max_shipping_days = -1
    
    # Stream through the transactions directory chunk by chunk
    for root, dirs, files in os.walk(TX_DIR):
        for file in files:
            if file.endswith('.parquet'):
                file_path = os.path.join(root, file)
                
                # Column pruning: Only load what we need for the math
                df = pd.read_parquet(
                    file_path, 
                    columns=['amount', 'status', 'shipping_days']
                )
                
                total_rows += len(df)
                total_amount += df['amount'].sum()
                completed_count += len(df[df['status'] == 'completed'])
                
                file_max = df['shipping_days'].max()
                if pd.notna(file_max):
                    max_shipping_days = max(max_shipping_days, int(file_max))

    tests = [
        {
            "name": "Total Row Count",
            "sql": f"SELECT COUNT(*) FROM {PG_TABLE_NAME};",
            "pq_truth": total_rows
        },
        {
            "name": "Sum of Revenue (amount)",
            "sql": f"SELECT SUM(amount) FROM {PG_TABLE_NAME};",
            "pq_truth": round(total_amount, 2) 
        },
        {
            "name": "Filtered Count (Completed Orders)",
            "sql": f"SELECT COUNT(*) FROM {PG_TABLE_NAME} WHERE status = 'completed';",
            "pq_truth": completed_count
        },
        {
            "name": "Max Shipping Days",
            "sql": f"SELECT MAX(shipping_days) FROM {PG_TABLE_NAME};",
            "pq_truth": max_shipping_days
        }
    ]

    passed = 0
    failed = 0

    for idx, test in enumerate(tests, 1):
        print(f"\nTest [{idx}/{len(tests)}] | {test['name']}")
        print(f"  Query: {test['sql']}")
        
        pg_result, pg_latency = fetch_scalar_from_postgres(test['sql'])
        
        if isinstance(pg_result, float) or (pg_result and 'SUM' in test['sql']):
            pg_result = round(float(pg_result), 2)
            
        if pg_result == test['pq_truth']:
            print(f"  ✅ PASSED | Result: {pg_result} | Latency: {pg_latency:.4f}s")
            passed += 1
        else:
            print(f"  ❌ FAILED: Data mismatch.")
            print(f"     PG Returned: {pg_result}")
            print(f"     PQ Truth   : {test['pq_truth']}")
            failed += 1

    print("\n" + "="*40)
    print("AGGREGATE SUMMARY")
    print("="*40)
    print(f"Total Tests : {len(tests)}")
    print(f"Passed      : {passed}")
    print(f"Failed      : {failed}")


if __name__ == "__main__":
    # Run the point-lookup tests first
    run_validation()
    
    # Run the heavy aggregate tests second
    run_aggregate_tests()