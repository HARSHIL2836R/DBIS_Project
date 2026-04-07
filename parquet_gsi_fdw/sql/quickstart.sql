CREATE EXTENSION parquet_gsi_fdw;

DROP SERVER IF EXISTS parquet_gsi_server CASCADE;
CREATE SERVER parquet_gsi_server
FOREIGN DATA WRAPPER parquet_gsi_fdw
OPTIONS (data_lake_path '/tmp/data_lake');

DROP FOREIGN TABLE IF EXISTS customers;
DROP FOREIGN TABLE IF EXISTS products;
DROP FOREIGN TABLE IF EXISTS transactions;
--  Customers: (customer_id, email, region, age, loyalty_tier, signup_date, total_orders, lifetime_value)
-- # Products: (product_id, name, category, price, stock, brand, weight_kg, rating, reviews)
-- #Transactions: (order_id, customer_id, product_id, amount, quantity, status, region, timestamp, payment_method, is_returned, warehouse_id, shipping_days)
CREATE FOREIGN TABLE customers (
    customer_id INT,
    email VARCHAR(255),
    region VARCHAR(100),
    age INT,
    loyalty_tier VARCHAR(50),
    signup_date DATE,
    total_orders INT,
    lifetime_value DECIMAL(12, 2)
) 
SERVER parquet_gsi_server 
OPTIONS (data_lake_path '/tmp/data_lake/customers/');

CREATE FOREIGN TABLE products (
    product_id INT,
    name VARCHAR(255),
    category VARCHAR(100),
    price DECIMAL(10, 2),
    stock INT,
    brand VARCHAR(100),
    weight_kg DECIMAL(5, 2),
    rating DECIMAL(3, 2),
    reviews INT
)
SERVER parquet_gsi_server
OPTIONS (data_lake_path '/tmp/data_lake/products/');

CREATE FOREIGN TABLE transactions (
    order_id INT,
    customer_id INT,
    product_id INT,
    amount DECIMAL(10, 2),
    quantity INT,
    status VARCHAR(50),
    region VARCHAR(100),
    timestamp TIMESTAMP,
    payment_method VARCHAR(50),
    is_returned BOOLEAN,
    warehouse_id INT,
    shipping_days INT
)
SERVER parquet_gsi_server
OPTIONS (data_lake_path '/tmp/data_lake/transactions/');


SELECT * FROM transactions LIMIT 1000;
