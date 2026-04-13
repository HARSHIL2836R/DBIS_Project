EXTENSION = parquet_gsi_fdw
MODULE_big = parquet_gsi_fdw
OBJS = parquet_gsi_fdw.o
DATA = parquet_gsi_fdw--1.0.sql

PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
