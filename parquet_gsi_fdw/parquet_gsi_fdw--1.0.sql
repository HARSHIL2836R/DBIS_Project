CREATE FUNCTION parquet_gsi_fdw_handler()
RETURNS fdw_handler
AS 'MODULE_PATHNAME', 'parquet_gsi_fdw_handler'
LANGUAGE C STRICT;

CREATE FUNCTION parquet_gsi_fdw_validator(text[], oid)
RETURNS void
AS 'MODULE_PATHNAME', 'parquet_gsi_fdw_validator'
LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER parquet_gsi_fdw
HANDLER parquet_gsi_fdw_handler
VALIDATOR parquet_gsi_fdw_validator;
