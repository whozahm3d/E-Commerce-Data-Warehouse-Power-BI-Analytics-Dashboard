-- =============================================================
-- FILE: Group6_MOLAP.sql
-- PURPOSE: MOLAP analytical queries for E-commerce DW (ELT tables)
-- =============================================================
SET search_path = dw, public;

------------------------------------------------------------
-- Step 0: Helper table for months (precomputed)
------------------------------------------------------------
DROP TABLE IF EXISTS date_month_elt CASCADE;
CREATE TEMP TABLE date_month_elt AS
SELECT date_key, DATE_TRUNC('month', full_date)::DATE AS month
FROM dw.dim_date_elt;

------------------------------------------------------------
-- MOLAP 1: Monthly revenue by country
------------------------------------------------------------
DROP TABLE IF EXISTS molap_month_country CASCADE;
CREATE TABLE molap_month_country AS
SELECT
    dm.month,
    COALESCE(c.country,'UNKNOWN') AS country,
    SUM(f.totalamount) AS total_revenue,
    SUM(f.quantity)::BIGINT AS total_quantity,
    COUNT(DISTINCT f.invoiceid) AS order_count
FROM dw.fact_sales_elt f
JOIN date_month_elt dm ON f.date_key = dm.date_key
LEFT JOIN dw.dim_customer_elt c ON f.customer_key = c.customer_key
GROUP BY dm.month, COALESCE(c.country,'UNKNOWN')
ORDER BY dm.month ASC, country ASC;

-- Quick check
SELECT * FROM molap_month_country LIMIT 10;

------------------------------------------------------------
-- MOLAP 2: Monthly product summary
------------------------------------------------------------
DROP TABLE IF EXISTS molap_month_product CASCADE;
CREATE TABLE molap_month_product AS
SELECT
    dm.month,
    p.product_key,
    p.stockcode,
    p.description,
    SUM(f.totalamount) AS revenue,
    SUM(f.quantity)::BIGINT AS total_qty,
    AVG(f.unitprice) AS avg_unitprice
FROM dw.fact_sales_elt f
JOIN date_month_elt dm ON f.date_key = dm.date_key
JOIN dw.dim_product_elt p ON f.product_key = p.product_key
GROUP BY dm.month, p.product_key, p.stockcode, p.description
ORDER BY dm.month ASC, revenue DESC;

-- Quick check
SELECT * FROM molap_month_product LIMIT 10;

------------------------------------------------------------
-- MOLAP 3: Customer monthly revenue (only months with sales)
------------------------------------------------------------
DROP TABLE IF EXISTS molap_customer_month CASCADE;
CREATE TABLE molap_customer_month AS
SELECT
    dm.month,
    c.customer_key,
    c.customerid,
    COALESCE(SUM(f.totalamount),0) AS revenue,
    COUNT(DISTINCT f.invoiceid) AS orders,
    SUM(f.quantity)::BIGINT AS total_quantity
FROM dw.fact_sales_elt f
JOIN date_month_elt dm ON f.date_key = dm.date_key
JOIN dw.dim_customer_elt c ON f.customer_key = c.customer_key
GROUP BY dm.month, c.customer_key, c.customerid
ORDER BY dm.month ASC, revenue DESC;

-- Quick check
SELECT * FROM molap_customer_month LIMIT 10;

------------------------------------------------------------
-- MOLAP 4: Top 10 products last 3 months
------------------------------------------------------------
-- Top 10 products in the last 3 months based on your ELT DW
WITH last3 AS (
    SELECT f.*, d.full_date
    FROM dw.fact_sales_elt f
    JOIN dw.dim_date_elt d ON f.date_key = d.date_key
    -- Use latest date in your fact table to calculate 3-month window
    WHERE d.full_date >= (
        (SELECT MAX(d2.full_date) FROM dw.dim_date_elt d2)
        - INTERVAL '3 months'
    )
)
SELECT 
    p.stockcode,
    p.description,
    ROUND(SUM(f.totalamount),2) AS revenue,
    SUM(f.quantity)::BIGINT AS qty_sold
FROM last3 f
JOIN dw.dim_product_elt p ON f.product_key = p.product_key
GROUP BY p.stockcode, p.description
ORDER BY revenue DESC
LIMIT 10;


------------------------------------------------------------
-- MOLAP 5: Daily revenue trend (last 90 days)
------------------------------------------------------------
WITH last90 AS (
    SELECT f.*, d.full_date
    FROM dw.fact_sales_elt f
    JOIN dw.dim_date_elt d ON f.date_key = d.date_key
    WHERE d.full_date >= (
        (SELECT MAX(d2.full_date) FROM dw.dim_date_elt d2)
        - INTERVAL '90 days'
    )
)
SELECT 
    d.full_date,
    ROUND(SUM(f.totalamount),2) AS daily_revenue
FROM last90 f
JOIN dw.dim_date_elt d ON f.date_key = d.date_key
GROUP BY d.full_date
ORDER BY d.full_date ASC
LIMIT 30;


------------------------------------------------------------
-- MOLAP 6: Cohort analysis (signup month vs 3-month revenue)
------------------------------------------------------------
WITH customer_signup AS (
    SELECT customer_key, DATE_TRUNC('month', signupdate)::DATE AS signup_month
    FROM dw.dim_customer_elt
    WHERE signupdate IS NOT NULL
),
customer_revenue AS (
    SELECT c.customer_key,
           DATE_TRUNC('month', d.full_date)::DATE AS sale_month,
           SUM(f.totalamount) AS revenue
    FROM dw.fact_sales_elt f
    JOIN dw.dim_date_elt d ON f.date_key = d.date_key
    JOIN dw.dim_customer_elt c ON f.customer_key = c.customer_key
    GROUP BY c.customer_key, sale_month
)
SELECT s.signup_month,
       SUM(CASE WHEN r.sale_month = s.signup_month THEN r.revenue ELSE 0 END) AS month0_revenue,
       SUM(CASE WHEN r.sale_month = s.signup_month + INTERVAL '1 month' THEN r.revenue ELSE 0 END) AS month1_revenue,
       SUM(CASE WHEN r.sale_month = s.signup_month + INTERVAL '2 month' THEN r.revenue ELSE 0 END) AS month2_revenue
FROM customer_signup s
LEFT JOIN customer_revenue r ON r.customer_key = s.customer_key
GROUP BY s.signup_month
ORDER BY s.signup_month DESC
LIMIT 12;

------------------------------------------------------------
-- MOLAP 7: Verification MOLAP vs OLAP for monthly sales by country
------------------------------------------------------------
WITH olap AS (
    SELECT
        DATE_TRUNC('month', d.full_date)::DATE AS month,
        COALESCE(c.country,'UNKNOWN') AS country,
        SUM(f.totalamount) AS revenue
    FROM dw.fact_sales_elt f
    JOIN dw.dim_date_elt d ON f.date_key = d.date_key
    LEFT JOIN dw.dim_customer_elt c ON f.customer_key = c.customer_key
    GROUP BY DATE_TRUNC('month', d.full_date)::DATE, COALESCE(c.country,'UNKNOWN')
)
SELECT
    olap.month,
    olap.country,
    ROUND(olap.revenue,2) AS olap_revenue,
    ROUND(COALESCE(m.total_revenue,0),2) AS molap_revenue,
    ROUND(COALESCE(m.total_revenue,0) - olap.revenue,2) AS diff
FROM olap
LEFT JOIN molap_month_country m
  ON m.month = olap.month
 AND m.country = olap.country
ORDER BY olap.month DESC, olap.country ASC
LIMIT 20;

