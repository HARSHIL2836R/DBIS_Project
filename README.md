# 📊 Global Secondary Indices for Parquet-Based Data Lakes

> **Team:** Kunj Bhesaniya (23B0995), Dhruvraj Merchant (23B1041), Dipen Sojitra (23B0913), Harshil Solanki (23B1016)  
> **Course:** CS349 - Database and Information Systems Lab

---

## 🎯 Project Overview

This project implements **Global Secondary Indices (GSI)** for Parquet-based data lakes in PostgreSQL. Traditional selective queries on data lakes suffer from significant I/O overhead as databases must scan metadata footers of every Parquet file. Our solution introduces indexed column lookups to dramatically reduce query execution time.

### 🚀 Key Achievement
**~7.7x speedup** on selective queries using GSI optimization (2461ms → 318ms)

---

## 🏗️ Architecture

### 1. **Data Generation Pipeline** (`/data/generate.py`)
- Generates test datasets in Parquet format (Small: ~500MB, Medium: ~2GB, Large: ~10GB)
- Creates three main tables: Customers, Products, Transactions
- Supports both **Generate** and **Append** modes for incremental data
- CLI-driven with customizable rows per file and partitioning

### 2. **Parquet File Extraction** (`/watcher/extractor.py`)
- Extracts indexed columns from Parquet files without full file loading
- Processes data in row groups for memory efficiency
- Outputs value → row group ID mappings for GSI posting generation

### 3. **Metadata Watcher Process** (`/watcher/watcher_daemon.py`)
Maintains GSI indices through four key operations:
- **Initialization:** Scans existing files and builds in-memory mappings
- **New Files:** Monitors for stable files, extracts indices in parallel
- **Deleted Files:** Removes stale postings from GSI tables
- **Periodic Refresh:** Detects new indices and syncs metadata

**Metadata Tables:**
- `gsi_registry` - Index definitions and status
- `gsi_file_catalog` - File metadata and activation states  
- `gsi_index_file_state` - Per-file index state tracking

### 4. **Foreign Data Wrapper Extension** (`/parquet_fdw/`)
Extended PostgreSQL's `parquet_fdw` with GSI integration:
- **Plan State:** Carries GSI metadata through query planning
- **Cost Estimation:** Uses `pg_statistic` for accurate row-group selectivity
- **Query Optimization:** Registers GSI access paths alongside full scans
- **Execution:** Resolves index at runtime, reads only targeted files/row groups

---

## 📊 Performance Results

### Query: Point lookup on customer_id
```sql
SELECT * FROM transactions WHERE customer_id = 'ae99a5bf52d91233eaf603b605111ee0'
```

| Metric | Without GSI | With GSI | Improvement |
|--------|------------|----------|------------|
| Execution Time | 2,461 ms | 318 ms | **7.7x faster** |
| Rows Filtered | 3,333,329 | 366,662 | **89% reduction** |
| Planning Time | 3.5 ms | 3.8 ms | Negligible |

---

## 🛠️ Tech Stack

- **Database:** PostgreSQL with extended `parquet_fdw`
- **Data Format:** Apache Parquet (via Arrow)
- **Languages:** C++ (FDW), Python (watcher, data generation)
- **Monitoring:** File system watchers with multi-threaded processing

---

## 📁 Project Structure

```
├── data/                    # Data generation scripts
├── parquet_extraction_engine/  # C++ Parquet reader
├── parquet_fdw/            # Extended FDW with GSI support
├── watcher/                # Metadata watcher daemon
├── test/                   # SQL tests and test data
└── report.tex              # Detailed technical report
```

---

## 🤖 GenAI Tool Usage

- **VS Code Copilot:** Enhanced data generation CLI with file append operations
- **ChatGPT Codex:** Generated complete watcher pipeline with case handling
- **Prompt-based FDW Integration:** Refined PostgreSQL cost estimation and selectivity calculations

---

## 📖 Documentation

See `report.tex` for comprehensive technical details including:
- Detailed motivation and design rationale
- Full pipeline architecture and state transitions
- FDW callback modifications and implementation details
- Complete performance analysis and benchmarks

---

**Repository:** [GitHub](https://github.com/HARSHIL2836R/DBIS_Project.git)

