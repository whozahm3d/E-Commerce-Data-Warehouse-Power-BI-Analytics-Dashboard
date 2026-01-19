-- ============================================================
-- File: Group6_ELT.sql
-- Project: E-Commerce Data Warehouse (ELT phase)
-- Purpose: Load raw data into dw.raw_* tables, transform inside dw,
--          create fact_sales_elt and compare results with ETL pipeline.
-- ============================================================


-- ============================================================
-- Step 0: Ensure schemas exist and set search path
-- ============================================================

CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS dw;

SET search_path = dw, public;

-- ============================================================
-- PART A: EXTRACT & LOAD (copy raw staging -> dw.raw_*)
-- - We keep raw data untouched in the DW schema so transformations happen inside DW
-- ============================================================

-- Drop existing raw tables if present (so script is idempotent)
DROP TABLE IF EXISTS dw.raw_customers CASCADE;
DROP TABLE IF EXISTS dw.raw_products CASCADE;
DROP TABLE IF EXISTS dw.raw_sales_raw CASCADE;
DROP TABLE IF EXISTS dw.raw_date CASCADE;

-- Create raw tables in dw schema by copying staging structure
CREATE TABLE dw.raw_customers AS
SELECT * FROM staging.stg_customers WITH NO DATA;

CREATE TABLE dw.raw_products AS
SELECT * FROM staging.stg_products WITH NO DATA;

CREATE TABLE dw.raw_sales_raw AS
SELECT * FROM staging.stg_sales WITH NO DATA;

CREATE TABLE dw.raw_date AS
SELECT * FROM staging.stg_date WITH NO DATA;

-- Load data: copy from staging -> dw.raw_* (if staging already has rows)
INSERT INTO dw.raw_customers
SELECT * FROM staging.stg_customers;

INSERT INTO dw.raw_products
SELECT * FROM staging.stg_products;

INSERT INTO dw.raw_sales_raw
SELECT * FROM staging.stg_sales;

INSERT INTO dw.raw_date
SELECT * FROM staging.stg_date;

-- Quick counts to confirm raw load (optional debug)
SELECT 'raw_customers' AS raw_customers, COUNT(*) FROM dw.raw_customers;
SELECT 'raw_products'  AS raw_products, COUNT(*) FROM dw.raw_products;
SELECT 'raw_sales_raw' AS raw_sales_raw, COUNT(*) FROM dw.raw_sales_raw;
SELECT 'raw_date'      AS raw_dates, COUNT(*) FROM dw.raw_date;

-- Show some sample rows from each DW table
SELECT * FROM dw.raw_customers 	LIMIT 5;
SELECT * FROM dw.raw_products 	LIMIT 5;
SELECT * FROM dw.raw_date       LIMIT 5;
SELECT * FROM dw.raw_sales_raw  LIMIT 5;

-- ============================================================
-- PART B: TRANSFORM INSIDE DW (create elt dimensions)
-- - We'll create dim_customer_elt, dim_product_elt, dim_date_elt
-- - Then create fact_sales_elt
-- ============================================================

-- 1) dim_customer_elt
DROP TABLE IF EXISTS dw.dim_customer_elt CASCADE;
CREATE TABLE dw.dim_customer_elt (
    customer_key SERIAL PRIMARY KEY,
    customerid VARCHAR(50) UNIQUE,
    customername TEXT,
    country TEXT,
    signupdate DATE
);

INSERT INTO dw.dim_customer_elt (customerid, customername, country, signupdate)
SELECT customerid, customername, country, signupdate FROM (
  SELECT
    TRIM(customerid) AS customerid,
    INITCAP(NULLIF(TRIM(customername), '')) AS customername,
    INITCAP(NULLIF(TRIM(country), '')) AS country,
    -- safe parsing: accept ISO YYYY-MM-DD only (extend patterns if needed)
    CASE
      WHEN TRIM(signupdate) ~ '^\d{4}-\d{2}-\d{2}$' THEN NULLIF(TRIM(signupdate),'')::DATE
      ELSE NULL
    END AS signupdate,
    ROW_NUMBER() OVER (
      PARTITION BY TRIM(customerid)
      ORDER BY
        CASE WHEN TRIM(signupdate) ~ '^\d{4}-\d{2}-\d{2}$' THEN NULLIF(TRIM(signupdate),'')::DATE ELSE NULL END DESC NULLS LAST
    ) AS rn
  FROM dw.raw_customers
  WHERE COALESCE(TRIM(customerid),'') <> ''
) t
WHERE rn = 1;


-- 2) dim_product_elt
-- compute median first from raw_products (ELT side)
DROP TABLE IF EXISTS dw._median_price_elt;
CREATE TABLE dw._median_price_elt AS
SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY
    CASE WHEN REGEXP_REPLACE(TRIM(unitprice), '[^0-9\.\-]', '', 'g') ~ '^-?[0-9]+(\.[0-9]+)?$'
         THEN (REGEXP_REPLACE(TRIM(unitprice), '[^0-9\.\-]', '', 'g'))::NUMERIC
         ELSE NULL END
) AS median_price
FROM dw.raw_products
WHERE COALESCE(TRIM(unitprice),'') <> '';

DROP TABLE IF EXISTS dw.dim_product_elt CASCADE;
CREATE TABLE dw.dim_product_elt (
    product_key SERIAL PRIMARY KEY,
    stockcode VARCHAR(50) UNIQUE,
    description TEXT,
    unitprice NUMERIC(12,2),
    category TEXT,
    brand TEXT
);

-- Normalize unitprice inside DW (remove non-numeric chars, fallback to median)
INSERT INTO dw.dim_product_elt (stockcode, description, unitprice, category, brand)
SELECT DISTINCT ON (TRIM(stockcode))
  TRIM(stockcode) AS stockcode,
  INITCAP(NULLIF(TRIM(description), '')) AS description,
  COALESCE(
    (CASE WHEN REGEXP_REPLACE(TRIM(unitprice), '[^0-9\.\-]', '', 'g') ~ '^-?[0-9]+(\.[0-9]+)?$'
          THEN (REGEXP_REPLACE(TRIM(unitprice), '[^0-9\.\-]', '', 'g'))::NUMERIC
          ELSE NULL END),
    (SELECT median_price FROM dw._median_price_elt)
  )::NUMERIC(12,2) AS unitprice,
  INITCAP(NULLIF(TRIM(category), '')) AS category,
  INITCAP(NULLIF(TRIM(brand), '')) AS brand
FROM dw.raw_products
WHERE COALESCE(TRIM(stockcode),'') <> ''
ORDER BY TRIM(stockcode);

-- remove helper
DROP TABLE IF EXISTS dw._median_price_elt;


-- 3) dim_date_elt
DROP TABLE IF EXISTS dw.dim_date_elt CASCADE;
CREATE TABLE dw.dim_date_elt (
    date_key BIGINT PRIMARY KEY,
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

-- Build a combined set of timestamps from raw date CSV and raw sales timestamps
DROP TABLE IF EXISTS dw._datetime_samples_elt;
CREATE TABLE dw._datetime_samples_elt AS
SELECT DISTINCT
  CASE
    WHEN TRIM(date) ~ '^\d{4}-\d{2}-\d{2}(?:\s+\d{2}:\d{2}(:\d{2})?)?$'
      THEN NULLIF(TRIM(date),'')::TIMESTAMP
    WHEN TRIM(date) ~ '^\d{2}/\d{2}/\d{4}(?:\s+\d{2}:\d{2}(:\d{2})?)?$'
      THEN to_timestamp(NULLIF(TRIM(date),''), 'DD/MM/YYYY HH24:MI:SS')
    ELSE NULL
  END AS parsed_ts
FROM dw.raw_date
UNION
SELECT DISTINCT
  CASE
    WHEN TRIM(date) ~ '^\d{4}-\d{2}-\d{2}(?:\s+\d{2}:\d{2}(:\d{2})?)?$'
      THEN NULLIF(TRIM(date),'')::TIMESTAMP
    WHEN TRIM(date) ~ '^\d{2}/\d{2}/\d{4}(?:\s+\d{2}:\d{2}(:\d{2})?)?$'
      THEN to_timestamp(NULLIF(TRIM(date),''), 'DD/MM/YYYY HH24:MI:SS')
    ELSE NULL
  END AS parsed_ts
FROM dw.raw_sales_raw
WHERE COALESCE(TRIM(date),'') <> '';

INSERT INTO dw.dim_date_elt (
  date_key, full_datetime, full_date, full_time, year, month, day, hour, minute, second, weekday, is_weekend, quarter
)
SELECT
  TO_CHAR(parsed_ts,'YYYYMMDDHH24MISS')::BIGINT AS date_key,
  parsed_ts AS full_datetime,
  parsed_ts::DATE AS full_date,
  parsed_ts::TIME AS full_time,
  EXTRACT(YEAR FROM parsed_ts)::INT AS year,
  EXTRACT(MONTH FROM parsed_ts)::INT AS month,
  EXTRACT(DAY FROM parsed_ts)::INT AS day,
  EXTRACT(HOUR FROM parsed_ts)::INT AS hour,
  EXTRACT(MINUTE FROM parsed_ts)::INT AS minute,
  FLOOR(EXTRACT(SECOND FROM parsed_ts))::INT AS second,
  TRIM(TO_CHAR(parsed_ts,'FMDay')) AS weekday,
  (EXTRACT(ISODOW FROM parsed_ts) IN (6,7)) AS is_weekend,
  EXTRACT(QUARTER FROM parsed_ts)::INT AS quarter
FROM dw._datetime_samples_elt
WHERE parsed_ts IS NOT NULL;

--DROP TABLE IF EXISTS dw._datetime_samples_elt;

-- ============================================================
-- PART C: CREATE fact_sales_elt (transform inside DW)
-- - Parse, normalize, deduplicate, join to *_elt dims and build fact_sales_elt
-- ============================================================

DROP TABLE IF EXISTS dw.fact_sales_elt CASCADE;
CREATE TABLE dw.fact_sales_elt (
    sales_key SERIAL PRIMARY KEY,
    date_key BIGINT REFERENCES dw.dim_date_elt(date_key),
    product_key INT REFERENCES dw.dim_product_elt(product_key),
    customer_key INT REFERENCES dw.dim_customer_elt(customer_key),
    invoiceid VARCHAR(50),
    quantity INT,
    unitprice NUMERIC(12,2),
    totalamount NUMERIC(18,2),
    load_ts TIMESTAMP DEFAULT now()
);

-- Insert transformed rows into fact_sales_elt
-- We perform the same transformations as ETL but here all inside DW schema using raw copies
WITH cleaned AS (
  SELECT
    TRIM(invoiceid) AS invoiceid,
    TRIM(stockcode) AS stockcode,
    TRIM(description) AS description,
    TRIM(customerid) AS customerid,
    CASE WHEN TRIM(date) = '' THEN NULL ELSE NULLIF(TRIM(date),'')::TIMESTAMP END AS dt_ts,
    CASE WHEN REGEXP_REPLACE(TRIM(quantity), '[^0-9\.\-]', '', 'g') ~ '^-?[0-9]+$'
         THEN (REGEXP_REPLACE(TRIM(quantity), '[^0-9\.\-]', '', 'g'))::INT ELSE 0 END AS quantity_num,
    CASE WHEN REGEXP_REPLACE(TRIM(unitprice), '[^0-9\.\-]', '', 'g') ~ '^-?[0-9]+(\.[0-9]+)?$'
         THEN (REGEXP_REPLACE(TRIM(unitprice), '[^0-9\.\-]', '', 'g'))::NUMERIC(12,2) ELSE NULL END AS unitprice_num,
    CASE WHEN REGEXP_REPLACE(TRIM(totalamount), '[^0-9\.\-]', '', 'g') ~ '^-?[0-9]+(\.[0-9]+)?$'
         THEN (REGEXP_REPLACE(TRIM(totalamount), '[^0-9\.\-]', '', 'g'))::NUMERIC(18,2) ELSE NULL END AS totalamount_num
  FROM dw.raw_sales_raw
  WHERE COALESCE(TRIM(invoiceid),'') <> ''
)

INSERT INTO dw.fact_sales_elt (
    date_key, product_key, customer_key, invoiceid, quantity, unitprice, totalamount, load_ts
)
SELECT
  COALESCE(TO_CHAR(c2.dt_ts,'YYYYMMDDHH24MISS')::BIGINT, 19700101000000) AS date_key,
  p.product_key,
  c.customer_key,
  c2.invoiceid,
  c2.quantity_num,
  COALESCE(c2.unitprice_num, p.unitprice) AS unitprice,
  COALESCE(c2.totalamount_num,
           ROUND(c2.quantity_num * COALESCE(c2.unitprice_num, p.unitprice),2)) AS totalamount,
  now() AS load_ts
FROM cleaned c2
LEFT JOIN dw.dim_product_elt p ON c2.stockcode = p.stockcode
LEFT JOIN dw.dim_customer_elt c ON c2.customerid = c.customerid;


-- ============== PART D: VALIDATION + EXAMPLES ==============
-- 7.1 Validation: count rows
SELECT 'dim_customer_elt' AS dim_customer_elt, COUNT(*) FROM dw.dim_customer_elt;
SELECT 'dim_product_elt'  AS dim_product_elt, COUNT(*) FROM dw.dim_product_elt;
SELECT 'dim_date_elt'     AS dim_date_elt, COUNT(*) FROM dw.dim_date_elt;
SELECT 'fact_sales_elt'   AS fact_sales_elt, COUNT(*) FROM dw.fact_sales_elt;

-- 7.2 Validation: revenue sum comparison (staging vs DW)
SELECT 'dw_fact_total_elt' AS source, SUM(totalamount) AS total_revenue FROM dw.fact_sales_elt
UNION ALL
SELECT 'staging_sales_total' AS source, SUM(totalamount_num) AS total_revenue FROM staging.sales_clean;

-- 7.3 Show some sample rows from each DW table
SELECT * FROM dw.dim_customer_elt LIMIT 5;
SELECT * FROM dw.dim_product_elt  LIMIT 5;
SELECT * FROM dw.dim_date_elt     LIMIT 5;
SELECT * FROM dw.fact_sales_elt   LIMIT 5;

-- error log for ETL/ELT runs
DROP TABLE IF EXISTS staging.elt_errors CASCADE;
CREATE TABLE staging.elt_errors (
    error_id SERIAL PRIMARY KEY,
    source_table TEXT,
    source_row JSONB,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT now()
);


-- Log skipped rows (where product or customer missing)
INSERT INTO staging.elt_errors (source_table, source_row, error_message)
SELECT 'dw.raw_sales_raw' AS source_table, to_jsonb(rs.*) AS source_row,
       CASE
         WHEN p.product_key IS NULL AND c.customer_key IS NULL THEN 'Missing product AND customer mapping'
         WHEN p.product_key IS NULL THEN 'Missing product mapping'
         WHEN c.customer_key IS NULL THEN 'Missing customer mapping'
         WHEN ( (CASE WHEN TRIM(rs.date) ~ '^\d{4}-' OR TRIM(rs.date) ~ '^\d{2}/' THEN 1 ELSE 0 END) = 1
                AND (TO_CHAR( (CASE WHEN TRIM(rs.date) ~ '^\d{4}-' THEN NULLIF(TRIM(rs.date),'')::TIMESTAMP
                                      WHEN TRIM(rs.date) ~ '^\d{2}/' THEN to_timestamp(NULLIF(TRIM(rs.date),''),'DD/MM/YYYY HH24:MI:SS')
                                      ELSE NULL END ), 'YYYYMMDDHH24MISS')::BIGINT) IS NULL)
             THEN 'Missing date mapping (no matching dim_date_elt)'
         ELSE 'Other mapping issue'
       END AS error_message
FROM dw.raw_sales_raw rs
LEFT JOIN dw.dim_product_elt p ON TRIM(rs.stockcode) = p.stockcode
LEFT JOIN dw.dim_customer_elt c ON TRIM(rs.customerid) = c.customerid
LEFT JOIN dw.dim_date_elt d ON
  CASE
    WHEN TRIM(rs.date) ~ '^\d{4}-' THEN TO_CHAR(NULLIF(TRIM(rs.date),'')::TIMESTAMP,'YYYYMMDDHH24MISS')::BIGINT
    WHEN TRIM(rs.date) ~ '^\d{2}/' THEN TO_CHAR(to_timestamp(NULLIF(TRIM(rs.date),''),'DD/MM/YYYY HH24:MI:SS'),'YYYYMMDDHH24MISS')::BIGINT
    ELSE NULL
  END = d.date_key
WHERE COALESCE(TRIM(rs.invoiceid),'') <> ''
  AND (p.product_key IS NULL OR c.customer_key IS NULL OR d.date_key IS NULL);

SELECT 'elt_errors'   AS fact_sales, COUNT(*) FROM staging.elt_errors;

SELECT * FROM staging.elt_errors   LIMIT 5;

-- =============================================================
-- ELT Workflow Explanation (Short Version)
-- =============================================================
/*
1) Overview:
   - This ELT script loads raw data from staging directly into DW raw tables (dw.raw_*),
     then performs all cleaning, transformation, and deduplication **inside the DW**.
   - Key steps:
       • Raw staging tables are preserved in DW for auditability
       • Transformations are applied in-database to build *_elt dimension tables:
         dim_customer_elt, dim_product_elt, dim_date_elt
       • fact_sales_elt is populated by joining transformed dimensions with raw sales data
       • Normalization includes string trimming, case formatting, numeric parsing, timestamp parsing, 
         and median-based fallbacks for invalid or missing unit prices

2) Error Handling:
   - Rows with missing product, customer, or date mapping are **not loaded into fact_sales_elt**
   - These skipped rows are captured in `staging.elt_errors` for review and reprocessing
   - Ensures ELT maintains DW integrity without losing traceability of problematic data

3) Benefits / Differences from ETL:
   - ELT keeps raw data untouched in the DW, performing transformations **after load**
   - Enables audit trail, replayability, and easier performance tuning inside the DW
   - Side-by-side comparison queries with ETL allow validation of data consistency
*/


-- ============================================================
-- PART D: VALIDATION / COMPARISON (ETL vs ELT)
-- - Show counts and sums for fact_sales (ETL) vs fact_sales_elt (ELT)
-- ============================================================

-- ---------- D1. Basic row counts ----------
-- Compare how many records exist in each fact table
SELECT 'fact_sales_etl_count' AS metric, COUNT(*) AS value FROM dw.fact_sales
UNION ALL
SELECT 'fact_sales_elt_count' AS metric, COUNT(*) AS value FROM dw.fact_sales_elt;

-- ---------- D2. Compare total revenue (SUM of totalamount) ----------
SELECT 'fact_sales_etl_sum' AS metric, COALESCE(SUM(totalamount),0) AS value FROM dw.fact_sales
UNION ALL
SELECT 'fact_sales_elt_sum' AS metric, COALESCE(SUM(totalamount),0) AS value FROM dw.fact_sales_elt;

-- ---------- D3. Compare average unit price ----------
SELECT 'ETL Avg Unit Price' AS metric, ROUND(AVG(unitprice),2) AS avg_unitprice
FROM dw.fact_sales
UNION ALL
SELECT 'ELT Avg Unit Price' AS metric, ROUND(AVG(unitprice),2) AS avg_unitprice
FROM dw.fact_sales_elt;

-- ---------- D4. Compare number of unique customers ----------
SELECT 'ETL Unique Customers' AS metric, COUNT(DISTINCT customer_key) AS unique_customers
FROM dw.fact_sales
UNION ALL
SELECT 'ELT Unique Customers' AS metric, COUNT(DISTINCT customer_key) AS unique_customers
FROM dw.fact_sales_elt;

-- ---------- D5. Compare number of unique products sold ----------
SELECT 'ETL Unique Products' AS metric, COUNT(DISTINCT product_key) AS unique_products
FROM dw.fact_sales
UNION ALL
SELECT 'ELT Unique Products' AS metric, COUNT(DISTINCT product_key) AS unique_products
FROM dw.fact_sales_elt;


-- ---------- D6. Compare total quantity sold ----------
SELECT 'ETL Total Quantity' AS metric, COALESCE(SUM(quantity),0) AS total_quantity
FROM dw.fact_sales
UNION ALL
SELECT 'ELT Total Quantity' AS metric, COALESCE(SUM(quantity),0) AS total_quantity
FROM dw.fact_sales_elt;


-- ---------- D7. Revenue by Country (Top 10) ----------
SELECT 'ETL' AS pipeline, c.country, ROUND(SUM(f.totalamount),2) AS total_revenue
FROM dw.fact_sales f
JOIN dw.dim_customer c ON f.customer_key = c.customer_key
GROUP BY c.country
ORDER BY total_revenue DESC
LIMIT 10;

SELECT 'ELT' AS pipeline, c.country, ROUND(SUM(f.totalamount),2) AS total_revenue
FROM dw.fact_sales_elt f
JOIN dw.dim_customer_elt c ON f.customer_key = c.customer_key
GROUP BY c.country
ORDER BY total_revenue DESC
LIMIT 10;


-- ---------- D8. Product-Level Comparison ----------
-- Compare product-level total sales between ETL and ELT
SELECT
    p.stockcode,
    COALESCE(etl_sum, 0) AS etl_total,
    COALESCE(elt_sum, 0) AS elt_total,
    (COALESCE(elt_sum, 0) - COALESCE(etl_sum, 0)) AS difference
FROM dw.dim_product_elt p
LEFT JOIN (
    SELECT product_key, SUM(totalamount) AS etl_sum
    FROM dw.fact_sales
    GROUP BY product_key
) etl ON etl.product_key = p.product_key
LEFT JOIN (
    SELECT product_key, SUM(totalamount) AS elt_sum
    FROM dw.fact_sales_elt
    GROUP BY product_key
) elt ON elt.product_key = p.product_key
ORDER BY ABS(COALESCE(elt_sum,0) - COALESCE(etl_sum,0)) DESC
LIMIT 20;


-- ---------- D9. Daily Revenue Trend (Optional visualization query) ----------
-- You can export this to Excel or Power BI for validation plots
SELECT 'ETL' AS pipeline, d.full_date, SUM(f.totalamount) AS daily_revenue
FROM dw.fact_sales f
JOIN dw.dim_date d ON f.date_key = d.date_key
GROUP BY d.full_date
ORDER BY d.full_date;

SELECT 'ELT' AS pipeline, d.full_date, SUM(f.totalamount) AS daily_revenue
FROM dw.fact_sales_elt f
JOIN dw.dim_date_elt d ON f.date_key = d.date_key
GROUP BY d.full_date
ORDER BY d.full_date;


-- ---------- D10. Summary Metrics ----------
-- Quick side-by-side KPIs for ETL vs ELT
SELECT
    'ETL' AS pipeline,
    COUNT(*) AS total_rows,
    ROUND(SUM(totalamount),2) AS total_revenue,
    ROUND(AVG(unitprice),2) AS avg_price,
    COUNT(DISTINCT customer_key) AS unique_customers,
    COUNT(DISTINCT product_key) AS unique_products
FROM dw.fact_sales
UNION ALL
SELECT
    'ELT' AS pipeline,
    COUNT(*) AS total_rows,
    ROUND(SUM(totalamount),2) AS total_revenue,
    ROUND(AVG(unitprice),2) AS avg_price,
    COUNT(DISTINCT customer_key) AS unique_customers,
    COUNT(DISTINCT product_key) AS unique_products
FROM dw.fact_sales_elt;


-- ============================================================
-- End of ELT script
-- ============================================================
