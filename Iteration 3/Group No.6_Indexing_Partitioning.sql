-- ============================================================
-- File: Group6_Indexing_partitioning.sql
-- Project: E-Commerce DW — Performance Optimizations (Iteration 3)
-- Group 6
-- Purpose: Apply indexing, partitioning, materialized views, and
--          index-type comparisons for DW performance tuning.
-- ============================================================
-- Section E –

-- Q10

-- Before indexes (use the Hash Join query as example)
EXPLAIN ANALYZE
SELECT a.author_name, SUM(fs.revenue_usd) AS total_revenue
FROM bookverse_dw.fact_sales fs
JOIN bookverse_dw.dim_book b ON fs.book_key = b.book_key
JOIN bookverse_dw.dim_author a ON b.author_key = a.author_key
GROUP BY a.author_name;

-- Add indexes
CREATE INDEX idx_fact_book_key ON bookverse_dw.fact_sales(book_key);
CREATE INDEX idx_book_author_key ON bookverse_dw.dim_book(author_key);
CREATE INDEX idx_author_key ON bookverse_dw.dim_author(author_key);

-- After indexes
EXPLAIN ANALYZE
SELECT a.author_name, SUM(fs.revenue_usd) AS total_revenue
FROM bookverse_dw.fact_sales fs
JOIN bookverse_dw.dim_book b ON fs.book_key = b.book_key
JOIN bookverse_dw.dim_author a ON b.author_key = a.author_key
GROUP BY a.author_name;


-- The above is not part of the project

SET search_path = dw, public;

-- ============================================================
-- Full Table Scan Demonstration: Before and After Indexing
-- ============================================================

-- Drop index if exists
DROP INDEX IF EXISTS idx_fact_sales_date_btree;
-- Before indexing: Full table scan
EXPLAIN ANALYZE
SELECT SUM(totalamount) AS total_revenue
FROM fact_sales_elt;
-- Create B-Tree index on date_key
CREATE INDEX idx_fact_sales_date_btree
ON fact_sales_elt (date_key);
-- After indexing: Query with range filter (index used)
EXPLAIN ANALYZE
SELECT SUM(totalamount) AS total_revenue
FROM fact_sales_elt
WHERE date_key BETWEEN 20200101000000 AND 20201231235959;

-- ============================================================
-- 1. INDEXING TECHNIQUES FOR DW
-- ============================================================

-- A. B-TREE INDEX (High-cardinality / range query)
EXPLAIN ANALYZE
SELECT SUM(totalamount) AS total_revenue
FROM dw.fact_sales_elt
WHERE date_key BETWEEN 20200101000000 AND 20201231235959;

-- Create B-Tree index
DROP INDEX IF EXISTS idx_fact_sales_date_btree;
CREATE INDEX idx_fact_sales_date_btree
ON dw.fact_sales_elt (date_key);

-- After B-Tree index
EXPLAIN ANALYZE
SELECT SUM(totalamount) AS total_revenue
FROM dw.fact_sales_elt
WHERE date_key BETWEEN 20200101000000 AND 20201231235959;

-- B. Bitmap index (Low-cardinality column)
-- Category in dim_product_elt (few distinct values)
EXPLAIN ANALYZE
SELECT p.category, SUM(p.unitprice*100) AS dummy_sales
FROM dw.fact_sales_elt f
JOIN dw.dim_product_elt p ON f.product_key = p.product_key
GROUP BY p.category;

DROP INDEX IF EXISTS idx_product_category_bitmap;
-- PostgreSQL does not have 'bitmap' access method directly
-- Use standard B-Tree, which bitmap scan can internally use
DROP INDEX IF EXISTS idx_product_category_btree;
CREATE INDEX idx_product_category_btree
ON dw.dim_product_elt (category);

EXPLAIN ANALYZE
SELECT p.category, SUM(p.unitprice*100) AS dummy_sales
FROM dw.fact_sales_elt f
JOIN dw.dim_product_elt p ON f.product_key = p.product_key
GROUP BY p.category;


-- ============================================================
-- 2. PARTITIONING STRATEGY (Horizontal Partitioning by Year)
-- ============================================================

DROP TABLE IF EXISTS fact_sales_elt_partitioned CASCADE;

CREATE TABLE fact_sales_elt_partitioned (
    sales_key SERIAL,
    date_key BIGINT,
    product_key INT,
    customer_key INT,
    invoiceid VARCHAR(50),
    quantity INT,
    unitprice NUMERIC(12,2),
    totalamount NUMERIC(18,2),
    load_ts TIMESTAMP DEFAULT now()
)
PARTITION BY RANGE (date_key);

-- Analyze before adding Partitions
EXPLAIN ANALYZE
SELECT SUM(totalamount) AS revenue_2012
FROM fact_sales_elt_partitioned
WHERE date_key BETWEEN 20120101000000 AND 20121231235959;

-- Yearly partitions (example 2010–2013)
CREATE TABLE fact_sales_elt_y2010 PARTITION OF fact_sales_elt_partitioned
FOR VALUES FROM (20100101000000) TO (20101231235959);

CREATE TABLE fact_sales_elt_y2011 PARTITION OF fact_sales_elt_partitioned
FOR VALUES FROM (20110101000000) TO (20111231235959);

CREATE TABLE fact_sales_elt_y2012 PARTITION OF fact_sales_elt_partitioned
FOR VALUES FROM (20120101000000) TO (20121231235959);

CREATE TABLE fact_sales_elt_y2013 PARTITION OF fact_sales_elt_partitioned
FOR VALUES FROM (20130101000000) TO (20131231235959);

-- Load fact data
INSERT INTO fact_sales_elt_partitioned
SELECT * FROM dw.fact_sales_elt;

-- Partition pruning query
EXPLAIN ANALYZE
SELECT SUM(totalamount) AS revenue_2012
FROM fact_sales_elt_partitioned
WHERE date_key BETWEEN 20120101000000 AND 20121231235959;

-- ============================================================
-- 3. MATERIALIZED VIEW / CACHED AGGREGATIONS
-- ============================================================

-- Monthly revenue by product category
-- Before Materialized views

EXPLAIN ANALYZE
SELECT 
    DATE_TRUNC('month', d.full_datetime) AS month,
    p.category,
    SUM(f.totalamount) AS monthly_revenue,
    AVG(f.unitprice) AS avg_unitprice,
    COUNT(DISTINCT f.customer_key) AS unique_customers
FROM dw.fact_sales_elt_partitioned f
JOIN dw.dim_product_elt p 
    ON f.product_key = p.product_key
JOIN dw.dim_date_elt d 
    ON f.date_key = d.date_key
WHERE p.category = 'Electronics'
GROUP BY DATE_TRUNC('month', d.full_datetime), p.category
ORDER BY month DESC
LIMIT 12;


DROP MATERIALIZED VIEW IF EXISTS mv_monthly_category_sales;

CREATE MATERIALIZED VIEW mv_monthly_category_sales AS
SELECT 
    DATE_TRUNC('month', d.full_datetime) AS month,
    p.category,
    SUM(f.totalamount) AS monthly_revenue,
    AVG(f.unitprice) AS avg_unitprice,
    COUNT(DISTINCT f.customer_key) AS unique_customers
FROM dw.fact_sales_elt_partitioned f
JOIN dw.dim_product_elt p ON f.product_key = p.product_key
JOIN dw.dim_date_elt d ON f.date_key = d.date_key
GROUP BY DATE_TRUNC('month', d.full_datetime), p.category;

-- Query materialized view
EXPLAIN ANALYZE
SELECT *
FROM mv_monthly_category_sales
WHERE category = 'Electronics'
ORDER BY month DESC
LIMIT 12;

-- ============================================================
-- 4. INDEX TYPE COMPARISON
-- ============================================================

-- A. Primary vs Secondary (on dim_customer_elt)
-- ------------------------------------------------------------
-- Primary key scan (customer_key)
-- BEFORE: no index or relying only on sequential scan
EXPLAIN ANALYZE
SELECT *
FROM dw.dim_customer_elt
WHERE customer_key = 5000;
-- Expected: very fast lookup due to primary key

DROP INDEX IF EXISTS idx_dim_customer_key;

CREATE INDEX idx_dim_customer_key
ON dw.dim_customer_elt (customer_key);

EXPLAIN ANALYZE
SELECT *
FROM dw.dim_customer_elt
WHERE customer_key = 5000;

-- Secondary index (customername)
-- BEFORE: sequential scan on customername
EXPLAIN ANALYZE
SELECT *
FROM dw.dim_customer_elt
WHERE customername = 'John Doe';

-- Create secondary index
DROP INDEX IF EXISTS idx_customer_email;
CREATE INDEX idx_customer_email
ON dw.dim_customer_elt (customername);

-- AFTER: secondary index scan
EXPLAIN ANALYZE
SELECT *
FROM dw.dim_customer_elt
WHERE customername = 'John Doe';
-- Analysis:
-- Primary key: optimized for unique lookups
-- Secondary index: optimized for filtering on non-unique attributes
-- Runtime improvement depends on table size and selectivity

-- B. Composite index (product_key + date_key)
-- ------------------------------------------------------------
-- BEFORE: sequential scan or single-column index
EXPLAIN ANALYZE
SELECT *
FROM fact_sales_elt_partitioned
WHERE product_key = 25
  AND date_key BETWEEN 20120101000000 AND 20121231235959;

-- AFTER: using composite index
DROP INDEX IF EXISTS idx_fact_product_date;
CREATE INDEX idx_fact_product_date
ON fact_sales_elt_partitioned (product_key, date_key);

EXPLAIN ANALYZE
SELECT *
FROM fact_sales_elt_partitioned
WHERE product_key = 25
  AND date_key BETWEEN 20120101000000 AND 20121231235959;

-- C. Sequential scan vs B-Tree / Composite index for aggregations
-- Sequential scan (baseline)
EXPLAIN ANALYZE
SELECT SUM(totalamount)
FROM dw.fact_sales_elt_partitioned
WHERE customer_key = 101;

-- Using secondary index (B-tree)
EXPLAIN ANALYZE
SELECT SUM(totalamount)
FROM dw.fact_sales_elt_partitioned
WHERE customer_key = 101
AND date_key BETWEEN 20190101000000 AND 20191231235959;

-- Using composite index (customer_key + date_key + unitprice)
EXPLAIN ANALYZE
SELECT SUM(totalamount)
FROM dw.fact_sales_elt_partitioned
WHERE customer_key = 101
AND date_key BETWEEN 20190101000000 AND 20191231235959
AND unitprice > 50;

-- Key Points:
-- 1) Primary key indexes are ideal for unique lookups (OLTP style) but also fast in DW.
-- 2) Secondary indexes help filter non-unique columns in analytical queries.
-- 3) Composite indexes are powerful for DW workloads where multi-column filters are common.
-- 4) Sequential scans are okay for small datasets, but indexing drastically reduces query time on large fact tables.
-- 5) Performance gain depends on selectivity, data size, and query patterns; DW workloads benefit from carefully chosen B-Tree and composite indexes.


-- ============================================================
-- 5. COMPLEX INSIGHTS QUERIES
-- ============================================================

-- Top 5 customers by total revenue
EXPLAIN ANALYZE
SELECT c.customername, SUM(f.totalamount) AS revenue
FROM fact_sales_elt_partitioned f
JOIN dw.dim_customer_elt c ON f.customer_key = c.customer_key
GROUP BY c.customername
ORDER BY revenue DESC
LIMIT 5;

-- Top 5 products by monthly revenue trend
EXPLAIN ANALYZE
SELECT 
    p.description, 
    DATE_TRUNC('month', d.full_datetime) AS month,
    SUM(f.totalamount) AS monthly_revenue
FROM fact_sales_elt_partitioned f
JOIN dw.dim_product_elt p ON f.product_key = p.product_key
JOIN dw.dim_date_elt d ON f.date_key = d.date_key
GROUP BY 
    p.description, 
    DATE_TRUNC('month', d.full_datetime)
ORDER BY 
    month DESC, 
    monthly_revenue DESC
LIMIT 10;


-- Revenue share by category
EXPLAIN ANALYZE
SELECT p.category, ROUND(SUM(f.totalamount)/SUM(SUM(f.totalamount)) OVER (),2) AS revenue_share
FROM fact_sales_elt_partitioned f
JOIN dw.dim_product_elt p ON f.product_key = p.product_key
GROUP BY p.category
ORDER BY revenue_share DESC;




-- These below queries are not the part of the project
-- 1. Create parent table partitioned by range on year extracted from date_key
CREATE TABLE fact_sales_horz (
    sales_id SERIAL,
    date_key date NOT NULL,
    channel VARCHAR(20),
    customer_key INT,
    product_key INT,
    sales_amount NUMERIC(10,2),
    quantity INT
) PARTITION BY RANGE (EXTRACT(YEAR FROM date_key));

-- 2. Create child partitions for 2023 and 2024
CREATE TABLE fact_sales_2023 PARTITION OF fact_sales_horz
    FOR VALUES FROM (2023) TO (2024);

CREATE TABLE fact_sales_2024 PARTITION OF fact_sales_horz
    FOR VALUES FROM (2024) TO (2025);

-- 3. Insert data from base table
INSERT INTO fact_sales_horz(date_key, channel, customer_key, product_key, sales_amount, quantity)
SELECT TO_DATE(CAST(date_key AS TEXT), 'YYYYMMDD'), channel, customer_key, product_key, revenue_usd, quantity FROM fact_sales_base;

-- 4. Validate Partition Pruning
-- Run query to check only 2023 partition scanned
EXPLAIN ANALYZE
SELECT * FROM fact_sales_horz WHERE EXTRACT(YEAR FROM date_key) = 2023;

-- Create list partitioned table by channel
CREATE TABLE fact_sales_list_partition (
    sales_id SERIAL,
    date_key DATE NOT NULL,
    channel VARCHAR(20),
    customer_key INT,
    product_key INT,
    sales_amount NUMERIC(10,2),
    quantity INT
) PARTITION BY LIST (channel);

-- Create partitions for 'Online' and 'InStore'
CREATE TABLE fact_sales_online PARTITION OF fact_sales_list_partition
    FOR VALUES IN ('Online');

CREATE TABLE fact_sales_instore PARTITION OF fact_sales_list_partition
    FOR VALUES IN ('In-Store');

-- Insert data
INSERT INTO fact_sales_list_partition(date_key, channel, customer_key, product_key, sales_amount, quantity)
SELECT TO_DATE(CAST(date_key AS TEXT), 'YYYYMMDD'), channel, customer_key, product_key, revenue_usd, quantity FROM fact_sales_base;

-- Query partition specific data
SELECT * FROM fact_sales_list_partition WHERE channel = 'Online';

-- Create hash partitioned table with 4 buckets
CREATE TABLE fact_sales_hash_partition (
    sales_id SERIAL,
    date_key DATE NOT NULL,
    channel VARCHAR(20),
    customer_key INT,
    product_key INT,
    sales_amount NUMERIC(10,2),
    quantity INT
) PARTITION BY HASH (customer_key);

-- Create 4 hash partitions
CREATE TABLE fact_sales_hash_p0 PARTITION OF fact_sales_hash_partition FOR VALUES WITH (MODULUS 4, REMAINDER 0);
CREATE TABLE fact_sales_hash_p1 PARTITION OF fact_sales_hash_partition FOR VALUES WITH (MODULUS 4, REMAINDER 1);
CREATE TABLE fact_sales_hash_p2 PARTITION OF fact_sales_hash_partition FOR VALUES WITH (MODULUS 4, REMAINDER 2);
CREATE TABLE fact_sales_hash_p3 PARTITION OF fact_sales_hash_partition FOR VALUES WITH (MODULUS 4, REMAINDER 3);

-- Insert data
INSERT INTO fact_sales_hash_partition(date_key, channel, customer_key, product_key, sales_amount, quantity)
SELECT TO_DATE(CAST(date_key AS TEXT), 'YYYYMMDD'), channel, customer_key, product_key, revenue_usd, quantity FROM fact_sales_base;

-- Validate hash distribution (sample validation query)
SELECT customer_key, count(*) FROM fact_sales_hash_partition GROUP BY customer_key ORDER BY customer_key;

select 'P0', count(*) from fact_sales_hash_p0 union all
select 'P1', count(*) from fact_sales_hash_p1 union all
select 'P2', count(*) from fact_sales_hash_p2 union all
select 'P3', count(*) from fact_sales_hash_p3 


create table product (
	product_key int primary key,
	product_name text,
	category text,
	price_usd int
)

-- Create hot table (frequently queried columns)
CREATE TABLE product_hot (
    product_key INT PRIMARY KEY,
    category text,
    price_usd int
);

-- Create cold table (rarely queried columns)
CREATE TABLE product_cold (
    product_key INT PRIMARY KEY references product_hot(product_key),
    product_name text
);

-- Insert from CSV or from original product table
insert into product_hot
select product_key, category, price_usd
from product

insert into product_cold
select product_key, product_name
from product

-- Compare query performance
-- Query only hot columns
EXPLAIN ANALYZE
SELECT category, avg(price_usd)
from product_hot
group by category

-- Query needing cold column join
EXPLAIN ANALYZE
SELECT ph.category, pc.product_name
from product_hot ph
join product_cold pc using (product_key)

--SECTION D
explain analyze
select * from  fact_sales_horz
where sales_id = 155

explain analyze
select customer_key, sum(sales_amount)
from fact_sales_horz
group by customer_key

--SECTION E
EXPLAIN ANALYZE
SELECT * FROM fact_sales_horz WHERE EXTRACT(YEAR FROM date_key) = 2023;

-- Force heap scan (assumes table has indexes)
SET enable_seqscan = OFF;

EXPLAIN ANALYZE
SELECT * FROM fact_sales_base WHERE customer_key = 12345;

-- Force sequential scan
SET enable_seqscan = ON;

EXPLAIN ANALYZE
SELECT * FROM fact_sales_base WHERE customer_key = 12345;

-- Simple query on non-partitioned table (sequential scan expected)
EXPLAIN ANALYZE
SELECT SUM(revenue_usd) FROM fact_sales_base WHERE EXTRACT(YEAR FROM TO_DATE(CAST(date_key AS TEXT), 'YYYYMMDD')) = 2023;

-- Same query on range partitioned table with pruning
EXPLAIN ANALYZE
SELECT SUM(sales_amount) FROM fact_sales_horz WHERE EXTRACT(YEAR FROM date_key) = 2023;
-- ============================================================
-- KEY POINTS / SUMMARY
-- ============================================================
/*
1) Indexing Techniques:
   • B-Tree index on date_key improves range queries on fact table.
   • Bitmap (via B-Tree) on low-cardinality category allows faster grouping queries.
2) Partitioning Strategy:
   • Horizontal partitioning by year enables partition pruning.
   • Queries automatically scan only relevant partitions.
3) Materialized View:
   • Pre-aggregated monthly revenue by category accelerates repeated reporting.
   • Reduces computational overhead on large fact table.
4) Index Type Comparison:
   • Primary key: efficient for lookups by unique key.
   • Secondary index: good for non-unique search filters.
   • Composite index: effective for multi-column filters (product + date).
5) Insights Queries:
   • Top customers, top products, revenue share by category demonstrate analytical usage.
   • Complex joins and aggregations benefit from indexing, partitioning, and materialized views.
*/

-- ============================================================
-- END OF INDEXING PARTITIONING
-- ============================================================
