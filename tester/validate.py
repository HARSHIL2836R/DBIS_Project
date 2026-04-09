'''
testing script using pytest and psycopg2.  
  * Connect to the local PostgreSQL instance and execute SELECT queries.  
  * Independently open the raw Parquet files via Python.  
  * Assert that the database output perfectly matches the raw file data to ensure the FDW is not dropping or corrupting rows.  
'''
import os
import psycopg2 as pg
import pyarrow.parquet as pq
