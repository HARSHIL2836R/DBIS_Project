CREATE EXTENSION IF NOT EXISTS parquet_fdw;

CREATE SERVER IF NOT EXISTS parquet_srv
FOREIGN DATA WRAPPER parquet_fdw;

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

        IF stat.isdir THEN
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
END;
$$;

-- 2. Create the GSI Metadata Catalog 
CREATE TABLE IF NOT EXISTS parquet_gsi_catalog (
    foreigntable_oid oid,
    table_name text,
    column_name text
);

-- 3. Register a dummy GSI for the customers table on the 'age' column
DELETE FROM parquet_gsi_catalog WHERE foreigntable_oid = 'public.customers'::regclass::oid;

INSERT INTO parquet_gsi_catalog (foreigntable_oid, table_name, column_name)
VALUES ('public.customers'::regclass::oid, 'gsi_customers_age', 'age');

CREATE TABLE IF NOT EXISTS gsi_customers_age (
    parquet_file_path text,
    rowgroup_id int,
    indexed_val int,   -- Matches the type of the 'age' column
    primary key (indexed_val,parquet_file_path, rowgroup_id)
);

-- Insert a dummy payload to prove it works
INSERT INTO gsi_customers_age VALUES ('/tmp/data_lake/customers/customers_chunk_0000.parquet', 0, 30);
-- Now run the explain
EXPLAIN SELECT * FROM public.customers WHERE age=30;
