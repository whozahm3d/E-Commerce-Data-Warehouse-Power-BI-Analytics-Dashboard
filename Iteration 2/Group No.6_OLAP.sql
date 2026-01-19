-- =============================================================
-- FILE: Group6_OLAP.sql (For ELT DW schema)
-- PURPOSE: OLAP analytical queries for E-commerce DW (ELT tables)
-- =============================================================
SET search_path = dw, public;

------------------------------------------------------------
-- 1) Monthly sales by country
------------------------------------------------------------
WITH sales_with_date AS (
  SELECT f.*, d.year, d.month, d.full_date
  FROM dw.fact_sales_elt f
  JOIN dw.dim_date_elt d ON f.date_key = d.date_key
)
SELECT
  year,
  month,
  COALESCE(c.country, 'UNKNOWN') AS country,
  ROUND(SUM(totalamount), 2) AS total_revenue,
  SUM(quantity) AS total_quantity
FROM sales_with_date f
LEFT JOIN dw.dim_customer_elt c ON f.customer_key = c.customer_key
GROUP BY year, month, c.country
ORDER BY year, month, total_revenue DESC;

-- ================================================
-- Top 10 products by revenue (last 3 months, ELT-safe)
-- ================================================
WITH last3months AS (
    SELECT MAX(full_date) AS max_date,
           MAX(full_date) - INTERVAL '3 months' AS min_date
    FROM dw.dim_date_elt
)
SELECT p.stockcode,
       p.description,
       ROUND(SUM(f.totalamount),2) AS revenue,
       SUM(f.quantity) AS qty_sold
FROM dw.fact_sales_elt f
JOIN dw.dim_product_elt p ON f.product_key = p.product_key
JOIN dw.dim_date_elt d ON f.date_key = d.date_key
JOIN last3months m ON d.full_date BETWEEN m.min_date AND m.max_date
GROUP BY p.stockcode, p.description
ORDER BY revenue DESC
LIMIT 10;


------------------------------------------------------------
-- 3) Customer Lifetime Value (CLTV)
------------------------------------------------------------
SELECT
  c.customer_key, c.customerid, c.customername,
  COALESCE(SUM(f.totalamount), 0) AS lifetime_revenue,
  MAX(d.full_date) AS last_purchase_date,
  COUNT(DISTINCT f.invoiceid) AS order_count
FROM dw.dim_customer_elt c
LEFT JOIN dw.fact_sales_elt f ON c.customer_key = f.customer_key
LEFT JOIN dw.dim_date_elt d ON f.date_key = d.date_key
GROUP BY c.customer_key, c.customerid, c.customername
ORDER BY lifetime_revenue DESC;

------------------------------------------------------------
-- 4) Daily revenue trend (last 90 days)
------------------------------------------------------------
WITH last90days AS (
    SELECT MAX(full_date) AS max_date,
           MAX(full_date) - INTERVAL '90 days' AS min_date
    FROM dw.dim_date_elt
)
SELECT d.full_date, ROUND(SUM(f.totalamount),2) AS daily_revenue
FROM dw.fact_sales_elt f
JOIN dw.dim_date_elt d ON f.date_key = d.date_key
JOIN last90days r ON d.full_date BETWEEN r.min_date AND r.max_date
GROUP BY d.full_date
ORDER BY d.full_date;


------------------------------------------------------------
-- 5) Product price vs revenue analysis
------------------------------------------------------------
SELECT
  p.stockcode, p.description,
  ROUND(AVG(f.unitprice),2) AS avg_unitprice,
  ROUND(SUM(f.totalamount),2) AS total_revenue,
  SUM(f.quantity) AS total_quantity
FROM dw.fact_sales_elt f
JOIN dw.dim_product_elt p ON f.product_key = p.product_key
GROUP BY p.stockcode, p.description
ORDER BY total_revenue DESC;

------------------------------------------------------------
-- 6) Cohort analysis (signup month vs 3-month revenue)
------------------------------------------------------------
WITH customer_signup AS (
    SELECT 
        customer_key, 
        DATE_TRUNC('month', signupdate)::DATE AS signup_month
    FROM dw.dim_customer_elt
    WHERE signupdate IS NOT NULL
),
customer_revenue AS (
    SELECT 
        f.customer_key,
        DATE_TRUNC('month', d.full_date)::DATE AS sale_month,
        SUM(f.totalamount) AS revenue
    FROM dw.fact_sales_elt f
    JOIN dw.dim_date_elt d ON f.date_key = d.date_key
    GROUP BY f.customer_key, DATE_TRUNC('month', d.full_date)::DATE
)
SELECT 
    s.signup_month,
    SUM(CASE WHEN r.sale_month = s.signup_month THEN r.revenue ELSE 0 END) AS month0_revenue,
    SUM(CASE WHEN r.sale_month = s.signup_month + INTERVAL '1 month' THEN r.revenue ELSE 0 END) AS month1_revenue,
    SUM(CASE WHEN r.sale_month = s.signup_month + INTERVAL '2 month' THEN r.revenue ELSE 0 END) AS month2_revenue
FROM customer_signup s
LEFT JOIN customer_revenue r 
       ON r.customer_key = s.customer_key
      AND r.sale_month >= s.signup_month
      AND r.sale_month < s.signup_month + INTERVAL '3 month'
GROUP BY s.signup_month
ORDER BY s.signup_month DESC;


------------------------------------------------------------
-- 7) OLAP Verification Query (monthly revenue by country)
------------------------------------------------------------
SELECT
  DATE_TRUNC('month', d.full_date)::DATE AS month,
  COALESCE(c.country, 'UNKNOWN') AS country,
  ROUND(SUM(f.totalamount),2) AS revenue
FROM dw.fact_sales_elt f
JOIN dw.dim_date_elt d ON f.date_key = d.date_key
LEFT JOIN dw.dim_customer_elt c ON f.customer_key = c.customer_key
GROUP BY
  DATE_TRUNC('month', d.full_date)::DATE,
  COALESCE(c.country, 'UNKNOWN')
ORDER BY month DESC, revenue DESC;


