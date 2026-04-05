## **1\. Document Overview**

This document outlines the formal division of labor for the Checkpoint 1 phase of the Global Secondary Indices (GSI) project. The primary objective of this phase is to establish the core data flow: proving that mock data can be generated, programmatically read by an external C++ process, indexed into a PostgreSQL B-Tree, and successfully queried via a baseline Foreign Data Wrapper (FDW) full scan.

## **2\. Team Roster & Role Assignments**

| Name | Primary Role | Core Domain |
| :---- | :---- | :---- |
| **Kunj** | Data Architect & QA | Mock Data Generation, Edge-Case Engineering, Automated Testing |
| **Dhruvraj** | Extraction Specialist | C++ Apache Arrow Integration, Parquet Metadata Parsing |
| **Dipen** | Watcher & DB Integrator | File System Polling, PostgreSQL Index Ingestion |
| **Harshil** | FDW Engineer | PostgreSQL Extension Environment (PGXS), Baseline C Hooks |

---

## **3\. Detailed Task & Subtask Breakdown**

### **Task 1: Data Architecture & Quality Assurance**

**Owner:** Kunj

**Tech Stack:** Python (pandas, pyarrow, pytest), SQL

**Description:** Develop the foundational data lake environment and ensure the integrity of the database's output through automated testing.

**Subtasks:**

* **1.1 Foundation Data Script:** Write a Python script to generate a baseline set of localized Parquet files mimicking real-world schemas (e.g., E-commerce transactions with transaction\_id, customer\_id, timestamp).  
* **1.2 "Torture Test" Engineering:** Expand the data generator to create edge cases that stress-test the C++ and PostgreSQL components:  
  * Generate highly variable file sizes (mixing 1MB and 1GB row groups).  
  * Introduce severe data skew (e.g., 99% identical values in an indexed column) to test B-Tree bloat.  
  * Apply dictionary encoding to string columns via pyarrow.  
* **1.3 Automated Validation Suite:** Build a Python testing script using pytest and psycopg2.  
  * Connect to the local PostgreSQL instance and execute SELECT queries.  
  * Independently open the raw Parquet files via Python.  
  * Assert that the database output perfectly matches the raw file data to ensure the FDW is not dropping or corrupting rows.  
* **1.4 Integration Support:** Pair-program with the Extraction Specialist to assist in navigating the C++ Apache Arrow documentation and debugging memory issues.

**Deliverables:** ./data\_lake populated with edge-case files, generate\_mock\_data.py, and test\_fdw\_output.py.

### **Task 2: Parquet Extraction Engine**

**Owner:** Dhruvraj

**Tech Stack:** C++, Apache Arrow API, Parquet API, CMake

**Description:** Build the programmatic logic required to read Parquet metadata outside of the database environment and isolate indexable coordinates.

**Subtasks:**

* **2.1 Build Environment Setup:** Configure CMakeLists.txt to correctly link the Apache Arrow and Parquet C++ libraries to the project workspace.  
* **2.2 File I/O & Parsing:** Write C++ logic to open a target .parquet file from the local directory without loading the entire payload into memory.  
* **2.3 Metadata Extraction:** Interface with the Parquet file footer to locate specific columns targeted for indexing (e.g., transaction\_id).  
* **2.4 Coordinate Isolation:** Extract the target values and map them strictly to their exact physical locations, specifically isolating the File\_Path and Row\_Group\_ID.

**Deliverables:** A compiled C++ executable/module capable of printing or returning {indexed\_value, file\_path, row\_group\_id} tuples from a raw file.

### **Task 3: Watcher Daemon & Database Ingestion**

**Owner:** Dipen

**Tech Stack:** C++ or Python, OS File APIs (e.g., inotify or watchdog), SQL

**Description:** Create the asynchronous bridge that detects new data lake entries and updates the Global Secondary Index store.

**Subtasks:**

* **3.1 Directory Polling:** Implement a lightweight background daemon that continuously monitors the ./data\_lake directory for IN\_CREATE or IN\_MOVED\_TO filesystem events indicating a new Parquet file has arrived.  
* **3.2 Pipeline Hand-off:** Upon detecting a file, pass the file path to the Extraction Engine (Task 2\) to retrieve the coordinate mappings.  
* **3.3 GSI Schema Design:** Define and initialize the formal PostgreSQL B-Tree schema (CREATE TABLE global\_index (indexed\_value VARCHAR, file\_path TEXT, row\_group\_id INT);).  
* **3.4 Index Upsertion:** Write the database connection logic (e.g., using libpq for C++ or psycopg2 for Python) to continuously insert the extracted mappings into the PostgreSQL index table.

**Deliverables:** A running daemon process that automatically populates the PostgreSQL global\_index table whenever a file is dropped into the target folder.

### **Task 4: Foreign Data Wrapper (FDW) Baseline**

**Owner:** Harshil

**Tech Stack:** C, PostgreSQL Internals, PGXS

**Description:** Establish the PostgreSQL extension infrastructure and implement a baseline, unoptimized full-scan connection to the local data lake.

**Subtasks:**

* **4.1 PGXS Infrastructure:** Create the Makefile, parquet\_gsi\_fdw.control, and SQL mapping files required to compile a custom PostgreSQL extension using the system's PostgreSQL development headers.  
* **4.2 Hook Registration:** Write the C skeleton that registers standard FDW handler functions (GetForeignRelSize, GetForeignPaths, GetForeignPlan, BeginForeignScan, IterateForeignScan).  
* **4.3 Directory Traversal:** Implement logic within the FDW to map the PostgreSQL foreign table to the ./data\_lake directory.  
* **4.4 Baseline Execution (Full Scan):** Ensure that when a user runs SELECT \* FROM datalake\_table;, the FDW successfully passes all files to the internal reader and returns the raw rows to the Postgres executor. *(Note: Predicate pushdown and index routing are deferred to Phase 2).*

**Deliverables:** A compiled parquet\_gsi\_fdw.so extension loaded into PostgreSQL that successfully executes a full table scan on the mock data.

---

## **4\. Required References & Documentation**

To successfully complete the tasks above, team members should refer to the following specific documentation sources:

**For Kunj (Data & Testing):**

* [Apache Arrow Python (PyArrow) Documentation](https://arrow.apache.org/docs/python/) \- Specifically the chapters on Parquet reading/writing and Dictionary-encoded arrays.  
* [pytest Documentation](https://docs.pytest.org/en/latest/) \- For structuring the automated database verification tests.

**For Dhruvraj (Extraction):**

* [Apache Arrow C++ API Reference](https://arrow.apache.org/docs/cpp/) \- Focus on the parquet::arrow::FileReader and parquet::RowGroupMetaData classes.  
* [Parquet Format Specification (GitHub)](https://github.com/apache/parquet-format) \- To understand the internal hierarchy of Row Groups and Column Chunks.

**For Dipen (Watcher & Ingestion):**

* inotify Man Page (Linux) or watchdog Python library docs \- For efficient filesystem event monitoring without CPU-heavy infinite loops.  
* [PostgreSQL libpq C Library](https://www.google.com/search?q=%5Bhttps://www.postgresql.org/docs/current/libpq.html%5D\(https://www.postgresql.org/docs/current/libpq.html\)) \- If writing the insertion logic in C/C++.

**For Harshil (FDW Engineering):**

* [PostgreSQL Official Docs: Writing a Foreign Data Wrapper](https://www.postgresql.org/docs/current/fdwhandler.html) \- The primary reference for the C structures expected by the query planner.  
* [PostgreSQL Official Docs: Extension Building Infrastructure (PGXS)](https://www.postgresql.org/docs/current/extend-pgxs.html) \- For writing the Makefile.  
* Reference Source Code: The file\_fdw extension located in the contrib/file\_fdw directory of the PostgreSQL source tree (acts as the perfect template for reading local files).

***PHASE 2***

| Team Member | Phase 2 Role | Core Responsibilities for Final Deadline |
| :---- | :---- | :---- |
| **Kunj** | **Query Planner Engineer** | • Hook into Postgres's GetForeignPaths and GetForeignPlan functions. • Extract the WHERE clause (e.g., user\_id \= X) from the user's query. • Write the C logic that queries the global\_index table to retrieve the exact {File\_Path, Row\_Group\_ID}. |
| **Dhruvraj** | **Targeted Execution Engineer** | • Shift focus from the background watcher into the Postgres FDW codebase. • Rewrite the FDW's IterateForeignScan using C++ Arrow APIs. • Implement the logic to jump directly to the specific Row\_Group\_ID passed by Kunj's planner, completely bypassing decompression for the rest of the file. |
| **Dipen** | **System Robustness & Compaction** | • Upgrade the background indexer to handle edge cases (e.g., what if a file is deleted from the lake?). • Implement periodic compaction to remove outdated or redundant mappings from the B-Tree index, as outlined in your architecture. • Ensure thread safety (handling cases where Postgres queries the index at the exact millisecond the watcher is updating it). |
| **Harshil** | **Performance & Benchmarking Lead** | • Scale up the data generator to create a multi-gigabyte data lake. • Write a benchmarking suite that runs hundreds of queries, toggling the index ON and OFF. • Generate the final visualizations (Latency vs. Selectivity, Disk I/O vs. Dataset Size) that prove the 10x-100x speedup for the final presentation. |

