# 📄 Project Title

**Global Secondary Index for Parquet-based Data Lakes with PostgreSQL FDW Integration**

---

# 1. Problem Statement

Modern data lakes (using **Parquet + Iceberg/Delta/Hudi**) are optimized for **OLAP workloads** (large scans), but perform poorly for:

* **Point lookups**
* **Highly selective queries**
* **High-cardinality filtering (e.g., user_id = X)**

### Why?

Because queries must:

```text
scan manifests
→ open many Parquet files
→ read metadata footers
→ filter rows
```

Even skipping via min/max stats requires **reading file footers**, causing:

* high I/O cost
* high latency (especially on object storage like S3)

---

# 🎯 Goal of the Project

Design a system that enables:

```text
O(log n) lookup → direct access to Parquet row
```

by building a **Global Secondary Index (GSI)** and integrating it with:

* PostgreSQL query planner
* Foreign Data Wrapper (FDW)
* Parquet storage

---

# 2. Core Idea

Instead of scanning data, maintain an index:

```text
indexed_key → (file_uri, row_group_id, row_offset)
```

Example:

```text
user_id = 'u9'
→ part-004.parquet, row_group=1, offset=24
```

So queries become:

```text
index lookup → direct file access → fetch row
```

---

# 3. Key Concepts Used

---

## 3.1 Parquet Internal Structure

Parquet hierarchy:

```text
File
 └ Row Group
     └ Column Chunk
         └ Data Page
```

Physical storage:

```text
[ Row Groups ][ Metadata Footer ]
```

Footer contains:

* offsets
* schema
* statistics

👉 Problem: reading footer requires **I/O per file**

---

## 3.2 Data Lake Table Formats

### Apache Iceberg (preferred)

* snapshot → manifest → file list
* easy detection of new files

### Delta Lake

* transaction log (`_delta_log`)
* sequential processing

### Apache Hudi

* complex (merge-on-read)
* not ideal for indexing

👉 Use **Iceberg for simplicity**

---

## 3.3 Global Secondary Index (GSI)

Stored in PostgreSQL:

```text
(indexed_key, file_uri, row_group_id, row_offset)
```

Implemented using:

👉 **Covering B-Tree Index**

```sql
CREATE INDEX idx_gsi
ON parquet_index_table (indexed_key)
INCLUDE (file_uri, row_group_id, row_offset);
```

---

## 3.4 Why Covering Index?

Because:

```text
index lookup → result directly from index
(no heap access)
```

Avoids:

```text
index → heap fetch → extra I/O
```

---

## 3.5 FDW (Foreign Data Wrapper)

FDW allows PostgreSQL to treat Parquet files as tables.

Key idea:

👉 **Index Pushdown**

```text
WHERE user_id = X
→ use GSI
→ skip scan
```

---

## 3.6 Asynchronous Indexing (Daemon)

Index is maintained by a **background worker (daemon)**:

```text
detect new files
→ parse Parquet
→ update index
```

Why async?

* avoids blocking ingestion (Spark/Flink)
* avoids locking

---

# 4. System Architecture

```text
           ┌──────────────────────┐
           │   User Query         │
           └─────────┬────────────┘
                     ↓
           ┌──────────────────────┐
           │ PostgreSQL Planner   │
           └─────────┬────────────┘
                     ↓
           ┌──────────────────────┐
           │ FDW (Index Pushdown) │
           └─────────┬────────────┘
                     ↓
           ┌──────────────────────┐
           │ GSI (PostgreSQL)     │
           └─────────┬────────────┘
                     ↓
           ┌──────────────────────┐
           │ Parquet File Access  │
           │ (Apache Arrow)       │
           └──────────────────────┘
```

---

# 5. Query Execution Flow

### Query:

```sql
SELECT * FROM transactions WHERE user_id = 'u9';
```

---

### Without GSI

```text
scan all files
→ read footers
→ filter rows
```

---

### With GSI

```text
FDW detects predicate
→ lookup GSI
→ get file + row group
→ read only that chunk
```

---

# 6. Implementation Plan (Step-by-Step)

---

## 🔹 Phase 1 — Basic Setup

### Step 1: Setup Environment

* Install PostgreSQL
* Install C++ compiler
* Install Apache Arrow C++
* Get Parquet sample files

---

## 🔹 Phase 2 — Parquet Reader

### Step 2: Read Parquet Files

Use Arrow:

* open file
* read schema
* iterate row groups

Extract:

```text
user_id, row_group_id, row_offset
```

---

## 🔹 Phase 3 — Build Index Table

### Step 3: Create PostgreSQL Table

```sql
CREATE TABLE parquet_index_table (
    indexed_key TEXT,
    file_uri TEXT,
    row_group_id INT,
    row_offset INT
);
```

---

### Step 4: Create Covering Index

```sql
CREATE INDEX idx_gsi
ON parquet_index_table (indexed_key)
INCLUDE (file_uri, row_group_id, row_offset);
```

---

## 🔹 Phase 4 — Index Builder (Daemon)

### Step 5: Build Background Worker

Pseudo-code:

```text
while(true):
    detect new files
    parse parquet
    insert into index table
    sleep()
```

Detection methods:

* scan directory OR
* read Iceberg snapshots

---

## 🔹 Phase 5 — FDW Development

### Step 6: Implement FDW Callbacks

Key functions:

1. `GetForeignRelSize`

   * detect WHERE clause

2. `GetForeignPaths`

   * create index path

3. `GetForeignPlan`

   * remove filter

4. `BeginForeignScan`

   * query GSI
   * open file

5. `IterateForeignScan`

   * fetch rows

---

## 🔹 Phase 6 — Query Execution

### Step 7: Integrate Everything

Workflow:

```text
query
→ FDW
→ GSI lookup
→ Arrow reads row group
→ return tuple
```

---

## 🔹 Phase 7 — Optimization

* cache file handles
* batch row reads
* parallel scans
* support range queries

---

# 7. Challenges

---

### 1. Parquet Navigation

Need to map:

```text
row_group + offset → actual row
```

---

### 2. FDW Complexity

* requires C programming
* tight integration with PostgreSQL planner

---

### 3. Consistency

Index must match data lake:

* handle new files
* handle deletions

---

### 4. Object Storage Latency

Minimize:

```text
# of file reads
```

---

# 8. Expected Outcome

Your system will:

* reduce query time from **seconds → milliseconds**
* enable **indexed queries on data lakes**
* simulate **database-like performance on Parquet**

---

# 9. Future Extensions

* multi-column indexes
* range queries
* bloom filters
* integration with Iceberg metadata
* distributed index

---

# ⭐ Final Summary

You are building a system that:

```text
turns a data lake into an indexed database
```

by combining:

* Parquet internals
* PostgreSQL indexing
* FDW pushdown
* async background indexing

---
