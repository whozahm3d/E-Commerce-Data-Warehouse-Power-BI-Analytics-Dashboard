CREATE DATABASE ecommerce_dw;


-- =============================================================
-- File: Group6_schema.sql
-- Purpose: E-Commerce dw
--          Online Retail dataset (customers, products, sales, date)
-- =============================================================

-- ============== PART 0: Prep (drop + recreate schemas) ==============
DROP SCHEMA IF EXISTS staging CASCADE;
DROP SCHEMA IF EXISTS dw CASCADE;

CREATE SCHEMA staging;
CREATE SCHEMA dw;

SET search_path TO dw, public;
-- =============== PART 1: DIMENSION TABLES ====================

-- ---------- dim_customer ----------
CREATE TABLE dw.dim_customer (
    customer_key SERIAL PRIMARY KEY,
    customerid VARCHAR(50) UNIQUE,
    customername TEXT,
    country TEXT,
    signupdate DATE
);

-- ---------- dim_product ----------
CREATE TABLE dw.dim_product (
    product_key SERIAL PRIMARY KEY,
    stockcode VARCHAR(50) UNIQUE,
    description TEXT,
    unitprice NUMERIC(12,2),
    category TEXT,
    brand TEXT
);

-- ---------- dim_date ----------
CREATE TABLE dw.dim_date (
    date_key BIGINT PRIMARY KEY,    -- YYYYMMDDHHMMSS (bigint)
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

-- ==================== FACT TABLE ====================

-- ---------- fact_sales ----------
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

-- =============================================================
-- End of Data Warehouse Schema Implementation
-- =============================================================