-- Comments added using AI

-- ============================================================================
-- PARQUET_FDW EXTENSION AND SERVER SETUP
-- ============================================================================
-- Initialize the parquet_fdw extension which allows PostgreSQL to read
-- Apache Parquet files from external data lakes as foreign tables
CREATE EXTENSION IF NOT EXISTS parquet_fdw;

-- Create a foreign data server that will be used to connect to parquet files
CREATE SERVER IF NOT EXISTS parquet_srv
FOREIGN DATA WRAPPER parquet_fdw;

-- ============================================================================
-- HELPER FUNCTION: Recursive Parquet File Discovery
-- ============================================================================
-- Purpose: Recursively find all .parquet files in a given directory
-- and its subdirectories. Used to support nested data lake structures
-- where parquet files may be organized in multiple levels of directories.
CREATE OR REPLACE FUNCTION public.list_parquet_files_recursive_jsonb(args jsonb)
RETURNS text[]
LANGUAGE plpgsql
AS $$
DECLARE
    dir text := args->>'dir';
    entry text;
    full_path text;
    files text[] := ARRAY[]::text[];
    stat record;
BEGIN
    IF dir IS NULL OR dir = '' THEN
        RAISE EXCEPTION 'missing dir in args';
    END IF;

    FOR entry IN SELECT * FROM pg_ls_dir(dir)
    LOOP
        full_path := dir || '/' || entry;
        stat := pg_stat_file(full_path, true);

        IF stat IS NULL THEN
            CONTINUE;
        ELSIF stat.isdir THEN
            files := files || list_parquet_files_recursive_jsonb(
                jsonb_build_object('dir', full_path)
            );
        ELSIF entry ILIKE '%.parquet' THEN
            files := array_append(files, full_path);
        END IF;
    END LOOP;

    RETURN files;
END;
$$;

-- ============================================================================
-- HELPER FUNCTION: Refresh Foreign Table File References
-- ============================================================================
-- Purpose: Updates a foreign table's file list when new parquet files are
-- added to the data lake. Ensures the FDW always knows about all available
-- files without manual intervention.
CREATE OR REPLACE FUNCTION public.refresh_parquet_foreign_table_files(
    p_foreign_table regclass,
    p_args jsonb
)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    resolved_files text[];
    qualified_name text;
BEGIN
    SELECT array_agg(file_path ORDER BY file_path)
      INTO resolved_files
      FROM unnest(public.list_parquet_files_recursive_jsonb(p_args)) AS t(file_path);

    IF COALESCE(array_length(resolved_files, 1), 0) = 0 THEN
        RAISE EXCEPTION 'no parquet files found for %', p_args;
    END IF;

    SELECT format('%I.%I', n.nspname, c.relname)
      INTO qualified_name
      FROM pg_class c
      JOIN pg_namespace n
        ON n.oid = c.relnamespace
     WHERE c.oid = p_foreign_table;

    IF qualified_name IS NULL THEN
        RAISE EXCEPTION 'foreign table % does not exist', p_foreign_table::text;
    END IF;

    -- import_parquet_explicit stores a static filename list, so reruns need
    -- to refresh the option when the data lake file set changes.
    EXECUTE format(
        'ALTER FOREIGN TABLE %s OPTIONS (SET filename %L)',
        qualified_name,
        array_to_string(resolved_files, ' ')
    );
END;
$$;

-- ============================================================================
-- INITIALIZE FOREIGN TABLES FROM PARQUET DATA LAKE
-- ============================================================================
-- This block creates three foreign tables that map to parquet files in the
-- data lake. Each table represents a different data domain:
-- - customers: Customer master data (ID, email, region, demographics, etc.)
-- - products: Product catalog (ID, name, category, price, stock, etc.)
-- - transactions: Orders/transactions (order ID, amounts, dates, status, etc.)
DO $$
BEGIN
    IF to_regclass('public.customers') IS NULL THEN
        PERFORM import_parquet_explicit(
            'customers',
            'public',
            'parquet_srv',
            ARRAY[
                'customer_id',
                'email',
                'region',
                'age',
                'loyalty_tier',
                'signup_date',
                'total_orders',
                'lifetime_value'
            ],
            ARRAY[
                'varchar(32)'::regtype,
                'varchar(255)'::regtype,
                'varchar(100)'::regtype,
                'int4'::regtype,
                'varchar(50)'::regtype,
                'date'::regtype,
                'int4'::regtype,
                'numeric(12,2)'::regtype
            ],
            'public.list_parquet_files_recursive_jsonb(jsonb)'::regprocedure::regproc,
            '{"dir": "/tmp/data_lake/customers"}'::jsonb,
            NULL
        );
    END IF;
    PERFORM public.refresh_parquet_foreign_table_files(
        'public.customers'::regclass,
        '{"dir": "/tmp/data_lake/customers"}'::jsonb
    );

    IF to_regclass('public.products') IS NULL THEN
        PERFORM import_parquet_explicit(
            'products',
            'public',
            'parquet_srv',
            ARRAY[
                'product_id',
                'name',
                'category',
                'price',
                'stock',
                'brand',
                'weight_kg',
                'rating',
                'reviews'
            ],
            ARRAY[
                'varchar(32)'::regtype,
                'varchar(255)'::regtype,
                'varchar(100)'::regtype,
                'numeric(10,2)'::regtype,
                'int4'::regtype,
                'varchar(100)'::regtype,
                'numeric(5,2)'::regtype,
                'numeric(3,2)'::regtype,
                'int4'::regtype
            ],
            'public.list_parquet_files_recursive_jsonb(jsonb)'::regprocedure::regproc,
            '{"dir": "/tmp/data_lake/products"}'::jsonb,
            NULL
        );
    END IF;
    PERFORM public.refresh_parquet_foreign_table_files(
        'public.products'::regclass,
        '{"dir": "/tmp/data_lake/products"}'::jsonb
    );

    IF to_regclass('public.transactions') IS NULL THEN
        PERFORM import_parquet_explicit(
            'transactions',
            'public',
            'parquet_srv',
            ARRAY[
                'order_id',
                'customer_id',
                'product_id',
                'amount',
                'quantity',
                'status',
                'region',
                'timestamp',
                'payment_method',
                'is_returned',
                'warehouse_id',
                'shipping_days'
            ],
            ARRAY[
                'varchar(32)'::regtype,
                'varchar(32)'::regtype,
                'varchar(32)'::regtype,
                'numeric(10,2)'::regtype,
                'int4'::regtype,
                'varchar(50)'::regtype,
                'varchar(100)'::regtype,
                'timestamp'::regtype,
                'varchar(50)'::regtype,
                'boolean'::regtype,
                'int4'::regtype,
                'int4'::regtype
            ],
            'public.list_parquet_files_recursive_jsonb(jsonb)'::regprocedure::regproc,
            '{"dir": "/tmp/data_lake/transactions"}'::jsonb,
            NULL
        );
    END IF;
    PERFORM public.refresh_parquet_foreign_table_files(
        'public.transactions'::regclass,
        '{"dir": "/tmp/data_lake/transactions"}'::jsonb
    );
END;
$$;

-- ============================================================================
-- METADATA TABLES FOR GSI (GENERIC SECONDARY INDEX) MANAGEMENT
-- ============================================================================
-- These tables track the state and management of secondary indexes built
-- on foreign table columns. GSIs enable fast lookups on specific columns
-- from parquet files without scanning entire files.

-- gsi_registry: Stores metadata about registered indexes (what to index)
CREATE TABLE IF NOT EXISTS public.gsi_registry (
    index_name text PRIMARY KEY,
    foreigntable_oid oid NOT NULL,
    table_name text NOT NULL,
    column_name text NOT NULL,
    column_type regtype NOT NULL,
    storage_table text NOT NULL,
    data_lake_path text NOT NULL,
    status text NOT NULL DEFAULT 'building'
        CHECK (status IN ('building', 'ready', 'dropping', 'dropped', 'failed')),
    created_at timestamptz NOT NULL DEFAULT now(),
    last_synced_at timestamptz
);

CREATE UNIQUE INDEX IF NOT EXISTS gsi_registry_table_column_key
ON public.gsi_registry (foreigntable_oid, column_name);

-- gsi_file_catalog: Tracks all parquet files in the data lake and their metadata
CREATE TABLE IF NOT EXISTS public.gsi_file_catalog (
    file_id bigserial PRIMARY KEY,
    foreigntable_oid oid NOT NULL,
    table_name text NOT NULL,
    data_lake_path text NOT NULL,
    file_path text NOT NULL UNIQUE,
    file_size bigint NOT NULL,
    file_mtime timestamptz NOT NULL,
    is_active boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT now(),
    last_seen_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS gsi_file_catalog_table_active_idx
ON public.gsi_file_catalog (foreigntable_oid, is_active);

-- gsi_index_file_state: Tracks the indexing status of each file for each index
-- (which files have been indexed, which are pending, failed, etc.)
CREATE TABLE IF NOT EXISTS public.gsi_index_file_state (
    index_name text NOT NULL REFERENCES public.gsi_registry(index_name) ON DELETE CASCADE,
    file_id bigint NOT NULL REFERENCES public.gsi_file_catalog(file_id) ON DELETE CASCADE,
    status text NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'indexing', 'indexed', 'failed', 'deleted')),
    last_indexed_at timestamptz,
    last_error text,
    PRIMARY KEY (index_name, file_id)
);

DELETE from public.gsi_index_file_state;

CREATE INDEX IF NOT EXISTS gsi_index_file_state_status_idx
ON public.gsi_index_file_state (index_name, status);

-- ============================================================================
-- FUNCTION: Register a New Generic Secondary Index (GSI)
-- ============================================================================
-- Purpose: Creates a new secondary index on a specified column of a foreign
-- table. The index will store unique values and their corresponding file IDs
-- and row group IDs for efficient lookups. Registers the index in the GSI
-- registry for tracking and maintenance.
DROP FUNCTION IF EXISTS public.register_gsi(regclass, text, text, text);

CREATE OR REPLACE FUNCTION public.register_gsi(
    foreign_table regclass,
    p_index_name text,
    p_column_name text,
    p_data_lake_path text
)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    target_type regtype;
    target_table text;
BEGIN
    SELECT a.atttypid::regtype,
           c.relname
      INTO target_type,
           target_table
      FROM pg_attribute a
      JOIN pg_class c
        ON c.oid = a.attrelid
     WHERE a.attrelid = foreign_table
       AND a.attname = p_column_name
       AND a.attnum > 0
       AND NOT a.attisdropped;

    IF target_type IS NULL THEN
        RAISE EXCEPTION 'column % does not exist on %', p_column_name, foreign_table::text;
    END IF;

    EXECUTE format(
        'CREATE TABLE IF NOT EXISTS public.%I (
            indexed_val %s NOT NULL,
            file_id bigint NOT NULL REFERENCES public.gsi_file_catalog(file_id) ON DELETE CASCADE,
            rowgroup_ids int[] NOT NULL,
            PRIMARY KEY (indexed_val, file_id)
        )',
        p_index_name,
        target_type::text
    );

    EXECUTE format(
        'CREATE INDEX IF NOT EXISTS %I ON public.%I (file_id)',
        p_index_name || '_file_id_idx',
        p_index_name
    );

    INSERT INTO public.gsi_registry (
        index_name,
        foreigntable_oid,
        table_name,
        column_name,
        column_type,
        storage_table,
        data_lake_path,
        status
    )
    VALUES (
        p_index_name,
        foreign_table::oid,
        target_table,
        p_column_name,
        target_type,
        p_index_name,
        p_data_lake_path,
        'building'
    )
    ON CONFLICT (index_name) DO UPDATE
        SET foreigntable_oid = EXCLUDED.foreigntable_oid,
            table_name = EXCLUDED.table_name,
            column_name = EXCLUDED.column_name,
            column_type = EXCLUDED.column_type,
            storage_table = EXCLUDED.storage_table,
            data_lake_path = EXCLUDED.data_lake_path,
            status = CASE
                WHEN public.gsi_registry.status = 'dropping' THEN 'dropping'
                ELSE 'building'
            END;
END;
$$;

-- ============================================================================
-- REGISTER SECONDARY INDEXES ON KEY COLUMNS
-- ============================================================================
-- Create GSI indexes on frequently-queried columns. These indexes will
-- enable fast lookups and filtering on these columns instead of full scans.

-- TEST 1: Index on customer age for demographic filtering
SELECT public.register_gsi(
    'public.customers'::regclass,
    'gsi_customers_age',
    'age',
    '/tmp/data_lake/customers'
);

-- TEST 2: Index on customer_id in transactions for customer lookup
SELECT public.register_gsi(
    'public.transactions' :: regclass,
    'gsi_transactions_customer_id',
    'customer_id',
    '/tmp/data_lake/transactions'
);

-- TEST 3: Index on order_id in transactions for order lookup
SELECT public.register_gsi(
    'public.transactions'::regclass,
    'gsi_transactions_order_id',
    'order_id',
    '/tmp/data_lake/transactions'
);

-- TEST 4: Index on product_id in transactions for product lookup
SELECT public.register_gsi(
    'public.transactions'::regclass,
    'gsi_transactions_product_id',
    'product_id',
    '/tmp/data_lake/transactions'
);

-- ============================================================================
-- VERIFY REGISTERED INDEXES
-- ============================================================================
-- Display all registered GSI indexes and their current status (should be 'building')
SELECT index_name, table_name, column_name, status
FROM public.gsi_registry
ORDER BY index_name;

-- ============================================================================
-- PERFORMANCE TESTING: Query Execution Plans with EXPLAIN ANALYZE
-- ============================================================================
-- These queries test the effectiveness of the secondary indexes by showing
-- the query execution plan and actual execution statistics.

-- TEST 5: Query customers by age (using gsi_customers_age index)
-- Expected: Should use the age index for faster filtering instead of full scan
EXPLAIN ANALYZE SELECT * FROM public.customers WHERE age = 30;

-- TEST 6: Query transactions by customer ID (using gsi_transactions_customer_id index)
-- Expected: Should use the customer_id index to quickly find all orders for a customer
EXPLAIN ANALYZE
SELECT * FROM public.transactions
WHERE customer_id = '21a6607b1790f22eba088795b6c54e57';

-- TEST 7: Query transactions by order ID (using gsi_transactions_order_id index)
-- Expected: Should use the order_id index to retrieve a specific order quickly
EXPLAIN ANALYZE
SELECT * FROM public.transactions
WHERE order_id = '1f6f92afdb4baa8a09a733ebd0616fea';
