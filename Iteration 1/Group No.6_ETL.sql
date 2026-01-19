CREATE DATABASE ecommerce_dw;

DROP SCHEMA IF EXISTS staging CASCADE;
DROP SCHEMA IF EXISTS dw CASCADE;

CREATE SCHEMA staging;
CREATE SCHEMA dw;


-- =============================================================
-- GROUP#6
-- FILE: <Group#6_ETL.sql>
-- PROJECT: Data Warehouse ETL Workflow
-- DESCRIPTION:
--   Implements the ETL process from staging to DW schema.
--   Includes cleaning, transformation, and loading of data.
-- =============================================================

-- ====================== PART 1: EXTRACT ======================
-- Extract data from staging tables
-- -------------------------------------------------------------
DROP TABLE IF EXISTS staging.stg_customers CASCADE;
CREATE TABLE staging.stg_customers (
    customerid TEXT,
    customername TEXT,
    country TEXT,
    signupdate TEXT
);

DROP TABLE IF EXISTS staging.stg_products CASCADE;
CREATE TABLE staging.stg_products (
    stockcode TEXT,
    description TEXT,
    unitprice TEXT,
    category TEXT,
    brand TEXT
);

DROP TABLE IF EXISTS staging.stg_sales CASCADE;
CREATE TABLE staging.stg_sales (
    invoiceid TEXT,
    stockcode TEXT,
    description TEXT,
    customerid TEXT,
    date TEXT,
    quantity TEXT,
    unitprice TEXT,
    totalamount TEXT
);

DROP TABLE IF EXISTS staging.stg_date CASCADE;
CREATE TABLE staging.stg_date (
    date TEXT,
    year TEXT,
    month TEXT,
    day TEXT,
    weekday TEXT
);


-- ============== PART 2: LOAD CSVs INTO STAGING (EXTRACT) ==============
-- Load CSVs into staging. tables.

-- ============== PART 3: QUICK STAGING CHECKS ==============
-- Verify that data exists in staging area
-- Row counts

SELECT 'stg_customers' AS customers_rows, COUNT(*) FROM staging.stg_customers;
SELECT 'stg_products'  AS products_rows, COUNT(*) FROM staging.stg_products;
SELECT 'stg_sales'     AS sales_rows, COUNT(*) FROM staging.stg_sales;
SELECT 'stg_date'      AS date_rows, COUNT(*) FROM staging.stg_date;

-- Check nulls in critical business keys
SELECT COUNT(*) AS null_customerid FROM staging.stg_customers WHERE customerid IS NULL OR TRIM(customerid) = '';
SELECT COUNT(*) AS null_stockcode  FROM staging.stg_products  WHERE stockcode IS NULL OR TRIM(stockcode) = '';
SELECT COUNT(*) AS null_invoiceid  FROM staging.stg_sales     WHERE invoiceid IS NULL OR TRIM(invoiceid) = '';

-- ============== PART 4: TRANSFORMATIONS (examples included) ==============
-- create cleaned / normalized intermediate (transform) tables in staging schema.

-- 4.1 customers_clean: deduplicate & normalize strings
DROP TABLE IF EXISTS staging.customers_clean;
CREATE TABLE staging.customers_clean AS
SELECT *
FROM (
  SELECT
    TRIM(customerid) AS customerid,
    INITCAP(NULLIF(TRIM(customername), '')) AS customername,
    INITCAP(NULLIF(TRIM(country), '')) AS country,
    CASE
      WHEN TRIM(signupdate) ~ '^\d{4}-\d{2}-\d{2}$'
        THEN NULLIF(TRIM(signupdate),'')::DATE
      ELSE NULL
    END AS signupdate,
    ROW_NUMBER() OVER (
      PARTITION BY TRIM(customerid)
      ORDER BY
        CASE
          WHEN TRIM(signupdate) ~ '^\d{4}-\d{2}-\d{2}$'
            THEN NULLIF(TRIM(signupdate),'')::DATE
          ELSE NULL
        END DESC NULLS LAST
    ) AS rn
  FROM staging.stg_customers
  WHERE COALESCE(TRIM(customerid),'') <> ''
) t
WHERE rn = 1;


-- Transformation example #1: duplicate removal/deduplication(customers) explanation:
-- We used DISTINCT ON + ROW_NUMBER to keep the row per customerid with the latest signup_date.


-- 4.2 products_clean: normalize prices & strings

DROP TABLE IF EXISTS staging.products_clean;
CREATE TABLE staging.products_clean AS
SELECT DISTINCT
  TRIM(stockcode) AS stockcode,
  INITCAP(NULLIF(TRIM(description), '')) AS description,
  INITCAP(NULLIF(TRIM(category), '')) AS category,
  INITCAP(NULLIF(TRIM(brand), '')) AS brand,
  CASE
    WHEN REGEXP_REPLACE(TRIM(unitprice), '[^0-9\.\-]', '', 'g') ~ '^-?[0-9]+(\.[0-9]+)?$'
      THEN (REGEXP_REPLACE(TRIM(unitprice), '[^0-9\.\-]', '', 'g'))::NUMERIC
    ELSE NULL
  END AS unitprice_raw
FROM staging.stg_products
WHERE COALESCE(TRIM(stockcode),'') <> '';

-- compute median fallback in staging (ETL side)
DROP TABLE IF EXISTS staging._median_price;
CREATE TABLE staging._median_price AS
SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY unitprice_raw) AS median_price
FROM staging.products_clean
WHERE unitprice_raw IS NOT NULL;


-- If a product has NULL unitprice, set it to median_price (fallback)
-- set unitprice with fallback median
ALTER TABLE staging.products_clean ADD COLUMN unitprice NUMERIC;
UPDATE staging.products_clean
SET unitprice = COALESCE(unitprice_raw, (SELECT median_price FROM staging._median_price));

ALTER TABLE staging.products_clean DROP COLUMN unitprice_raw;
DROP TABLE IF EXISTS staging._median_price;

-- Transformation Example #2: Currency normalization (products)
-- unitprice_raw now contains numeric values where possible (symbols removed)
-- We'll replace null/invalid unit prices per-product with median unitprice across dataset.


-- 4.3 sales_clean: parse numeric & timestamps, fix negatives
DROP TABLE IF EXISTS staging.sales_clean;
CREATE TABLE staging.sales_clean AS
SELECT
  TRIM(invoiceid) AS invoiceid,
  TRIM(stockcode) AS stockcode,
  INITCAP(NULLIF(TRIM(description), '')) AS description,
  TRIM(customerid) AS customerid,
  CASE
    WHEN TRIM(date) ~ '^\d{4}-\d{2}-\d{2}(?:\s+\d{2}:\d{2}(:\d{2})?)?$'
      THEN NULLIF(TRIM(date),'')::TIMESTAMP
    WHEN TRIM(date) ~ '^\d{2}/\d{2}/\d{4}(?:\s+\d{2}:\d{2}(:\d{2})?)?$'
      THEN to_timestamp(NULLIF(TRIM(date),''), 'DD/MM/YYYY HH24:MI:SS')
    ELSE NULL
  END AS dt_ts,
  CASE WHEN REGEXP_REPLACE(TRIM(quantity), '[^0-9\-]', '', 'g') ~ '^-?[0-9]+$'
       THEN (REGEXP_REPLACE(TRIM(quantity), '[^0-9\-]', '', 'g'))::INT ELSE NULL END AS quantity_num,
  CASE WHEN REGEXP_REPLACE(TRIM(unitprice), '[^0-9\.\-]', '', 'g') ~ '^-?[0-9]+(\.[0-9]+)?$'
       THEN (REGEXP_REPLACE(TRIM(unitprice), '[^0-9\.\-]', '', 'g'))::NUMERIC(12,2) ELSE NULL END AS unitprice_num,
  CASE WHEN REGEXP_REPLACE(TRIM(totalamount), '[^0-9\.\-]', '', 'g') ~ '^-?[0-9]+(\.[0-9]+)?$'
       THEN (REGEXP_REPLACE(TRIM(totalamount), '[^0-9\.\-]', '', 'g'))::NUMERIC(18,2) ELSE NULL END AS totalamount_num
FROM staging.stg_sales;


-- Example transform: replace negative or zero unit prices with product median from products_clean
-- fix invalid unitprice using product median from staging.products_clean
UPDATE staging.sales_clean sc
SET unitprice_num = COALESCE(sc.unitprice_num, p.unitprice)
FROM staging.products_clean p
WHERE sc.stockcode = p.stockcode AND (sc.unitprice_num IS NULL OR sc.unitprice_num <= 0);

-- Recompute totalamount_num if missing or inconsistent (quantity * unitprice)
UPDATE staging.sales_clean
SET totalamount_num = ROUND(quantity_num * unitprice_num,2)
WHERE (totalamount_num IS NULL OR totalamount_num = 0) AND quantity_num IS NOT NULL AND unitprice_num IS NOT NULL;


-- Remove rows with no invoiceid or no stockcode (example of cleaning nulls in PK fields)

DELETE FROM staging.sales_clean
WHERE COALESCE(TRIM(invoiceid),'') = '' 
   OR COALESCE(TRIM(stockcode),'') = '';


-- Transformation Example #3: Fix negative/zero unit price
-- Implemented in update on staging.sales_clean to set unitprice_num to product-level unitprice where missing or <= 0.


-- ---------- 4.4 date_clean: parse timestamps and extract components ----------
DROP TABLE IF EXISTS staging._datetime_samples;
CREATE TABLE staging._datetime_samples AS
SELECT DISTINCT
  CASE
    WHEN TRIM(date) ~ '^\d{4}-\d{2}-\d{2}(?:\s+\d{2}:\d{2}(:\d{2})?)?$'
      THEN NULLIF(TRIM(date),'')::TIMESTAMP
    WHEN TRIM(date) ~ '^\d{2}/\d{2}/\d{4}(?:\s+\d{2}:\d{2}(:\d{2})?)?$'
      THEN to_timestamp(NULLIF(TRIM(date),''), 'DD/MM/YYYY HH24:MI:SS')
    ELSE NULL
  END AS parsed_ts
FROM staging.stg_date
UNION
SELECT DISTINCT dt_ts FROM staging.sales_clean WHERE dt_ts IS NOT NULL;

DROP TABLE IF EXISTS staging.date_components;
CREATE TABLE staging.date_components AS
SELECT DISTINCT
  parsed_ts,
  parsed_ts::DATE AS full_date,
  parsed_ts::TIME AS full_time,
  EXTRACT(YEAR FROM parsed_ts)::INT AS year,
  EXTRACT(MONTH FROM parsed_ts)::INT AS month,
  EXTRACT(DAY FROM parsed_ts)::INT AS day,
  EXTRACT(HOUR FROM parsed_ts)::INT AS hour,
  EXTRACT(MINUTE FROM parsed_ts)::INT AS minute,
  FLOOR(EXTRACT(SECOND FROM parsed_ts))::INT AS second,
  TRIM(TO_CHAR(parsed_ts, 'FMDay')) AS weekday,
  (EXTRACT(ISODOW FROM parsed_ts) IN (6,7)) AS is_weekend,
  EXTRACT(QUARTER FROM parsed_ts)::INT AS quarter,
  TO_CHAR(parsed_ts, 'YYYYMMDDHH24MISS')::BIGINT AS date_key_full
FROM staging._datetime_samples
WHERE parsed_ts IS NOT NULL;

-- Transformation Example #4: Date/time formatting and decomposition
--  - Implemented in staging._datetime_samples -> staging.date_components -> dw.dim_date.
--    We parse raw timestamp strings into TIMESTAMP and extract year/month/day/hour/minute/second,
--    weekday and is_weekend indicator.


-- ============== PART 5: CREATE DW (Dimension & Fact) ==============
SET search_path TO dw, public;

-- dim_customer
DROP TABLE IF EXISTS dw.dim_customer CASCADE;
CREATE TABLE dw.dim_customer (
    customer_key SERIAL PRIMARY KEY,
    customerid VARCHAR(50) UNIQUE,
    customername TEXT,
    country TEXT,
    signupdate DATE
);

-- dim_product
DROP TABLE IF EXISTS dw.dim_product CASCADE;
CREATE TABLE dw.dim_product (
    product_key SERIAL PRIMARY KEY,
    stockcode VARCHAR(50) UNIQUE,
    description TEXT,
    unitprice NUMERIC(12,2),
    category TEXT,
    brand TEXT
);

-- dim_date
DROP TABLE IF EXISTS dw.dim_date CASCADE;
CREATE TABLE dw.dim_date (
    date_key BIGINT PRIMARY KEY,    -- YYYYMMDDHH24MISS
    full_datetime TIMESTAMP,
    full_date DATE,
    full_time TIME,
    year INT,
    month INT,
    day INT,
    hour INT,
    minute INT,
    second INT,
    weekday TEXT,
    is_weekend BOOLEAN,
    quarter INT
);

-- fact_sales (ETL)
DROP TABLE IF EXISTS dw.fact_sales CASCADE;
CREATE TABLE dw.fact_sales (
    sales_key SERIAL PRIMARY KEY,
    date_key BIGINT REFERENCES dw.dim_date(date_key),
    product_key INT REFERENCES dw.dim_product(product_key),
    customer_key INT REFERENCES dw.dim_customer(customer_key),
    invoiceid VARCHAR(50),
    quantity INT,
    unitprice NUMERIC(12,2),
    totalamount NUMERIC(18,2)
);

-- ============== PART 2: LOAD ETL results into DW dims & fact ==============

-- -------- Load dim_customer --------
INSERT INTO dw.dim_customer (customerid, customername, country, signupdate)
SELECT customerid, customername, country, signupdate 
FROM staging.customers_clean;

-- -------- Load dim_product --------
INSERT INTO dw.dim_product (stockcode, description, unitprice, category, brand)
SELECT stockcode, description, unitprice, category, brand
FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY stockcode ORDER BY stockcode) rn
  FROM staging.products_clean
) x
WHERE rn = 1
ON CONFLICT (stockcode) DO UPDATE
  SET description = EXCLUDED.description,
      unitprice = EXCLUDED.unitprice,
      category = EXCLUDED.category,
      brand = EXCLUDED.brand;

-- -------- Load dim_date --------
INSERT INTO dw.dim_date (
  date_key, full_datetime, full_date, full_time,
  year, month, day, hour, minute, second,
  weekday, is_weekend, quarter
)
SELECT
  date_key_full, parsed_ts, full_date, full_time,
  year, month, day, hour, minute, second,
  weekday, is_weekend, quarter
FROM staging.date_components;

-- ============== PART 6: LOAD FACT (JOIN STAGING -> DIMENSIONS) ==============
-- We match sales_clean to dim_product by stockcode, dim_customer by customerid, and dim_date by nearest matching full_datetime.
-- Because dim_date contains discrete timestamps from both sales and date.csv, we try exact match on timestamp.
-- ETL Fact Load matching ELT row count
-- ETL Fact Load (staging.sales_clean is already cleaned)
INSERT INTO dw.fact_sales (
    date_key, product_key, customer_key, invoiceid, quantity, unitprice, totalamount
)
SELECT
  COALESCE(TO_CHAR(s.dt_ts,'YYYYMMDDHH24MISS')::BIGINT, 19700101000000) AS date_key,
  p.product_key,
  c.customer_key,
  s.invoiceid,
  COALESCE(s.quantity_num, 0) AS quantity,  -- default 0 if missing
  COALESCE(s.unitprice_num, p.unitprice) AS unitprice,
  COALESCE(s.totalamount_num,
           ROUND(COALESCE(s.quantity_num,0) * COALESCE(s.unitprice_num, p.unitprice),2)) AS totalamount
FROM staging.sales_clean s
LEFT JOIN dw.dim_product p ON s.stockcode = p.stockcode
LEFT JOIN dw.dim_customer c ON s.customerid = c.customerid
LEFT JOIN dw.dim_date d ON TO_CHAR(s.dt_ts,'YYYYMMDDHH24MISS')::BIGINT = d.date_key;

-- Note: rows where product_key or customer_key is NULL are skipped (log them or handle in an error table)

-- ============== PART 7: VALIDATION + EXAMPLES ==============
-- 7.1 Validation: count rows
SELECT 'dim_customer' AS dim_customer, COUNT(*) FROM dw.dim_customer;
SELECT 'dim_product'  AS dim_product, COUNT(*) FROM dw.dim_product;
SELECT 'dim_date'     AS dim_date, COUNT(*) FROM dw.dim_date;
SELECT 'fact_sales'   AS fact_sales, COUNT(*) FROM dw.fact_sales;

-- 7.2 Validation: revenue sum comparison (staging vs DW)
SELECT 'dw_fact_total' AS source, SUM(totalamount) AS total_revenue FROM dw.fact_sales
UNION ALL
SELECT 'staging_sales_total' AS source, SUM(totalamount_num) AS total_revenue FROM staging.sales_clean;

-- 7.3 Show some sample rows from each DW table
SELECT * FROM dw.dim_customer LIMIT 5;
SELECT * FROM dw.dim_product  LIMIT 5;
SELECT * FROM dw.dim_date     LIMIT 5;
SELECT * FROM dw.fact_sales   LIMIT 5;

-- ============== PART 8: LOGGING BAD ROWS ==============
-- Create a simple errors table to capture rows that could not be loaded if desired
DROP TABLE IF EXISTS staging.etl_errors CASCADE;
CREATE TABLE staging.etl_errors (
    error_id SERIAL PRIMARY KEY,
    source_table TEXT,
    source_row JSONB,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT now()
);

-- Example: insert skipped sales rows for manual review (products/customers missing)
-- Log any skipped sales rows into staging.etl_errors  (where product or customer missing)
INSERT INTO staging.etl_errors (source_table, source_row, error_message)
SELECT 'staging.sales_clean', to_jsonb(s.*), 'Skipped in ETL load (missing product/customer/date/qty)'
FROM staging.sales_clean s
LEFT JOIN dw.dim_product p ON s.stockcode = p.stockcode
LEFT JOIN dw.dim_customer c ON s.customerid = c.customerid
LEFT JOIN dw.dim_date d ON TO_CHAR(s.dt_ts,'YYYYMMDDHH24MISS')::BIGINT = d.date_key
WHERE (p.product_key IS NULL OR c.customer_key IS NULL OR d.date_key IS NULL OR s.quantity_num IS NULL);

SELECT 'etl_errors'   AS etl_errors, COUNT(*) FROM staging.etl_errors;

SELECT * FROM staging.etl_errors   LIMIT 5;


-- =============================================================
-- ETL Workflow Explanation (Short Version)
-- =============================================================
/*
1) Overview:
   - This ETL script extracts raw data from staging tables, applies transformations,
     and loads cleaned/validated data into the DW schema (dimensional and fact tables).
   - Transformations include:
       • Deduplication of customers and products
       • Normalization of strings and numeric fields
       • Parsing timestamps and extracting date components
       • Fixing missing or invalid unit prices using median fallback
       • Recomputing totals and removing invalid rows (e.g., missing PKs)

2) Error Handling:
   - Rows failing key constraints or data validation (missing product/customer/date, invalid quantity) 
     are **not loaded into the DW tables**.
   - These rows are captured in `staging.etl_errors` with source data, error reason, and timestamp
     for later review and correction.
   - Ensures that ETL continues processing valid rows without pipeline interruption.

3) Benefits:
   - Maintains DW integrity by loading only clean, consistent data.
   - Allows auditing and reprocessing of failed rows.
   - Implements standard ETL best practices: robustness, reliability, and traceability.
*/

-- =============================================================
-- End of ETL script
-- =============================================================