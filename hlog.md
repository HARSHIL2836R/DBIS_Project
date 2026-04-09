- Create environment using venv
- Data generation
  - `python generate_data.py`
  - Output:
```
Target: medium  |  Output: ./data_lake
Customers    :    1,000,000 rows
Products     :      200,000 rows
Transactions :   10,000,000 rows

[1/3] Generating 1,000,000 customers ...
  chunk 0000/0009  5.8 MB
  chunk 0009/0009  5.7 MB
  ✓ Done in 3.1s

[2/3] Generating 200,000 products ...
  chunk 0000/0001  5.5 MB
  chunk 0001/0001  5.5 MB
  ✓ Done in 0.9s

[3/3] Generating 10,000,000 transactions ...
  chunk 0000/0024    4.0%  45.2 MB  speed=0.21M rows/s  ETA=46s
  chunk 0001/0024    8.0%  45.2 MB  speed=0.22M rows/s  ETA=42s
  chunk 0002/0024   12.0%  45.2 MB  speed=0.22M rows/s  ETA=40s
  chunk 0003/0024   16.0%  45.2 MB  speed=0.22M rows/s  ETA=39s
  chunk 0004/0024   20.0%  45.2 MB  speed=0.22M rows/s  ETA=37s
  chunk 0005/0024   24.0%  45.2 MB  speed=0.22M rows/s  ETA=35s
  chunk 0006/0024   28.0%  45.2 MB  speed=0.21M rows/s  ETA=34s
  chunk 0007/0024   32.0%  45.2 MB  speed=0.21M rows/s  ETA=32s
  chunk 0008/0024   36.0%  45.2 MB  speed=0.21M rows/s  ETA=30s
  chunk 0009/0024   40.0%  45.2 MB  speed=0.21M rows/s  ETA=28s
  chunk 0010/0024   44.0%  45.2 MB  speed=0.21M rows/s  ETA=26s
  chunk 0011/0024   48.0%  45.2 MB  speed=0.21M rows/s  ETA=24s
  chunk 0012/0024   52.0%  45.2 MB  speed=0.21M rows/s  ETA=22s
  chunk 0013/0024   56.0%  45.2 MB  speed=0.22M rows/s  ETA=20s
  chunk 0014/0024   60.0%  45.2 MB  speed=0.22M rows/s  ETA=19s
  chunk 0015/0024   64.0%  45.2 MB  speed=0.22M rows/s  ETA=17s
  chunk 0016/0024   68.0%  45.2 MB  speed=0.22M rows/s  ETA=15s
  chunk 0017/0024   72.0%  45.2 MB  speed=0.22M rows/s  ETA=13s
  chunk 0018/0024   76.0%  45.2 MB  speed=0.22M rows/s  ETA=11s
  chunk 0019/0024   80.0%  45.3 MB  speed=0.22M rows/s  ETA=9s
  chunk 0020/0024   84.0%  45.2 MB  speed=0.22M rows/s  ETA=7s
  chunk 0021/0024   88.0%  45.2 MB  speed=0.22M rows/s  ETA=5s
  chunk 0022/0024   92.0%  45.2 MB  speed=0.22M rows/s  ETA=4s
  chunk 0023/0024   96.0%  45.2 MB  speed=0.22M rows/s  ETA=2s
  chunk 0024/0024  100.0%  45.2 MB  speed=0.22M rows/s  ETA=0s
  ✓ Done in 46.0s

=================================================================
DATA LAKE SUMMARY
=================================================================
  customers      :   10 files  0.056 GB
  products       :    2 files  0.011 GB
  transactions   :   25 files  1.105 GB
  ─────────────────────────────────────────────
  TOTAL          :        1.171 GB

Transaction file schema  : ['order_id', 'customer_id', 'product_id', 'amount', 'quantity', 'status', 'region', 'timestamp', 'payment_method', 'is_returned', 'warehouse_id', 'shipping_days']
Row groups per file      : 4
Rows per file            : 400,000
```

- WSL has PostgreSQL 18.3 and pg_config
- `make clean && make PG_CONFIG=/usr/bin/pg_config` to build the extension
- `make install PG_CONFIG=/usr/bin/pg_config` to install the extension

Installation look like:
```
/bin/mkdir -p '/usr/lib/postgresql/18/lib' # Create PostgreSQL v18 library dir for shared objects; -p creates parent dirs and ignores if already present.
/bin/mkdir -p '/usr/share/postgresql/18/extension' # Create extension metadata dir (control + SQL files); -p means idempotent directory creation.
/bin/mkdir -p '/usr/share/postgresql/18/extension' # Repeated safety call to ensure extension dir exists before copy (same -p behavior).
/usr/bin/install -c -m 755 parquet_gsi_fdw.so '/usr/lib/postgresql/18/lib/parquet_gsi_fdw.so' # Copy FDW shared library into PostgreSQL lib dir; -c copies file, -m 755 sets rwxr-xr-x so server can load it.
/usr/bin/install -c -m 644 .//parquet_gsi_fdw.control '/usr/share/postgresql/18/extension/' # Install extension control file; -m 644 sets rw-r--r-- (readable by postgres, not executable).
/usr/bin/install -c -m 644 .//parquet_gsi_fdw--1.0.sql '/usr/share/postgresql/18/extension/' # Install extension SQL script used by CREATE EXTENSION; mode 644 is standard for SQL metadata.
/bin/mkdir -p '/usr/lib/postgresql/18/lib/bitcode/parquet_gsi_fdw' # Create bitcode output dir for LLVM/LTO artifacts; needed when PostgreSQL is built with LLVM support.
/bin/mkdir -p '/usr/lib/postgresql/18/lib/bitcode'/parquet_gsi_fdw/ # Same destination path via split quoting; ensures nested bitcode folder exists.
/usr/bin/install -c -m 644 parquet_gsi_fdw.bc '/usr/lib/postgresql/18/lib/bitcode'/parquet_gsi_fdw/./ # Copy LLVM bitcode (.bc) file; mode 644, target /./ resolves to same directory.
cd '/usr/lib/postgresql/18/lib/bitcode' && /usr/lib/llvm-21/bin/llvm-lto -thinlto -thinlto-action=thinlink -o parquet_gsi_fdw.index.bc parquet_gsi_fdw/parquet_gsi_fdw.bc # Change into bitcode dir, then run llvm-lto to create ThinLTO index; -thinlto enables ThinLTO, -thinlto-action=thinlink performs index/link step, -o sets output file.
```

- `sudo service postgresql start`
- `sudo -u postgres psql -d postgres`