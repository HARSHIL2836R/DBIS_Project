CREATE EXTENSION parquet_gsi_fdw;

DROP SERVER IF EXISTS parquet_gsi_srv CASCADE;
CREATE SERVER parquet_gsi_srv
FOREIGN DATA WRAPPER parquet_gsi_fdw
OPTIONS (data_lake_path '/absolute/path/to/data_lake');

DROP FOREIGN TABLE IF EXISTS datalake_table;
CREATE FOREIGN TABLE datalake_table (
    file_path text
)
SERVER parquet_gsi_srv;

SELECT * FROM datalake_table;
