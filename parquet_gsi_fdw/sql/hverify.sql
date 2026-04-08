-- 1) Ensure extension exists
CREATE EXTENSION IF NOT EXISTS parquet_gsi_fdw;

-- 2) Recreate server pointing to your data lake root
DROP SERVER IF EXISTS parquet_gsi_server CASCADE;
CREATE SERVER parquet_gsi_server
FOREIGN DATA WRAPPER parquet_gsi_fdw
OPTIONS (data_lake_path '/mnt/c/Users/Dell/Documents/IIT_academics/Sem 6/CS349 DIS Lab/DBIS Project/data/data_lake');

-- 3) One foreign table per folder (baseline FDW returns file_path)
CREATE FOREIGN TABLE customers_files (
    file_path text
)
SERVER parquet_gsi_server
OPTIONS (data_lake_path '/mnt/c/Users/Dell/Documents/IIT_academics/Sem 6/CS349 DIS Lab/DBIS Project/data/data_lake/customers');

CREATE FOREIGN TABLE products_files (
    file_path text
)
SERVER parquet_gsi_server
OPTIONS (data_lake_path '/mnt/c/Users/Dell/Documents/IIT_academics/Sem 6/CS349 DIS Lab/DBIS Project/data/data_lake/products');

CREATE FOREIGN TABLE transactions_files (
    file_path text
)
SERVER parquet_gsi_server
OPTIONS (data_lake_path '/mnt/c/Users/Dell/Documents/IIT_academics/Sem 6/CS349 DIS Lab/DBIS Project/data/data_lake/transactions');

-- 4) Verification queries
SELECT 'customers' AS table_name, COUNT(*) AS parquet_files FROM customers_files
UNION ALL
SELECT 'products', COUNT(*) FROM products_files
UNION ALL
SELECT 'transactions', COUNT(*) FROM transactions_files;

SELECT * FROM customers_files LIMIT 5;
SELECT * FROM products_files LIMIT 5;
SELECT * FROM transactions_files LIMIT 5;