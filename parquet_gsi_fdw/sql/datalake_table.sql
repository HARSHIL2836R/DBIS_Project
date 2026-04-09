CREATE EXTENSION IF NOT EXISTS parquet_gsi_fdw;

CREATE SERVER parquet_gsi_srv
FOREIGN DATA WRAPPER parquet_gsi_fdw
OPTIONS (data_lake_path '/mnt/c/Users/Dell/Documents/IIT_academics/Sem 6/CS349 DIS Lab/DBIS Project/data/data_lake');

CREATE FOREIGN TABLE datalake_table (
    file_path text
)
SERVER parquet_gsi_srv;

SELECT * FROM datalake_table LIMIT 20;