/*
==========================================================================================
   GROUP 6 — SCHEMA IMPLEMENTATION JUSTIFICATION
   (This file documents and justifies design choices for our Data Warehouse)
==========================================================================================

Our DW architecture contains:

   DIMENSION TABLES:
      • dim_customer, dim_customer_elt
      • dim_product,  dim_product_elt
      • dim_date,     dim_date_elt

   FACT TABLES:
      • fact_sales     (ETL-loaded)
      • fact_sales_elt (ELT-loaded)

Our justification follows the design of ETL/ELT workflows that:
   • Clean raw CSV data (TRIM() strings, fix nulls, validate numeric values)
   • Deduplicate customers & products
   • Derive surrogate keys
   • Generate consistent date_key for all dates
   • Load star schema for OLAP queries (DSS) and MOLAP materialization
==========================================================================================
*/


/*****************************************************************************************
1. PRIMARY KEY JUSTIFICATION (Aligned with our ETL + ELT Workflows)
*****************************************************************************************/

-- ==========================
-- Dimension Tables
-- ==========================

/*
dim_customer:
   PK: customer_key (SERIAL surrogate)
   Justification based on ETL workflow:
      - Raw files contain customerid with missing or malformed values.
      - ETL normalizes customer names, trims spaces, and removes invalid IDs.
      - Using customer_key avoids relying on inconsistent CSV customerid.
      - Surrogate keys ensure stable joins from fact tables → customer dimension.
      - Supports deduplication performed during ETL and ELT.

dim_product:
   PK: product_key (SERIAL surrogate)
   Justification based on ETL workflow:
      - stockcode often contains spaces, inconsistent casing, or NULLs.
      - ETL trims, cleans, and validates stockcode before loading.
      - Surrogate keys prevent DW breakage if stockcode formats change.
      - Required for product lookup in both ETL and ELT fact loading.

dim_date:
   PK: date_key (BIGINT: YYYYMMDDHH24MISS)
   Justification based on date transformations:
      - ETL parses invoice_date into full timestamp.
      - Derived BIGINT avoids composite keys and accelerates joins.
      - Ensures consistent mapping for MOLAP table and OLAP CUBE queries.
      - Sorts chronologically without extra conversion.
*/


/*****************************************************************************************
2. FOREIGN KEY JUSTIFICATION (Reflecting ETL/ELT Matching Logic)
*****************************************************************************************/

/*
fact_sales / fact_sales_elt:
   FKs:
      - date_key → dim_date.date_key
      - product_key → dim_product.product_key
      - customer_key → dim_customer.customer_key

Justification:
   - ETL ensures all fact rows reference cleaned dimensional values.
   - Rows with missing customerid or stockcode are skipped during load,
     so referential integrity is guaranteed.
   - Ensures accurate slicing by date, customer, and product.
   - Supports star-schema design for OLAP (analytical) workloads.

All joins in ETL:
   • customer matched via TRIM(customerid)
   • product matched via TRIM(stockcode)
   • date matched via derived date_key
*/


/*****************************************************************************************
3. INDEXING RECOMMENDATIONS (Based on OLAP, ELT, MOLAP, and DSS Queries)
*****************************************************************************************/

/*
We selected the 3 most important columns based on:

   • OLAP rollups (GROUP BY month, product, customer)
   • ELT joins during fact loading
   • DSS queries performing SUM(), DATE_TRUNC(), CUBE operations
   • MOLAP materialized tables used for fast BI queries
*/

-- ========================================================================================
-- Recommended Index 1: fact_sales.date_key
-- ========================================================================================

/*
Why:
   - Most analytical queries filter by date range:
        WHERE d.full_date >= CURRENT_DATE - INTERVAL '12 months'
   - MOLAP comparisons use DATE_TRUNC('month', full_date)
   - Improves time-series revenue queries and rolling aggregates.

Based on our workflow:
   - Both ETL and ELT compute date_key using:
         TO_CHAR(full_date, 'YYYYMMDDHH24MISS')
   - Index speeds up fact→date dimension joins during loading.
*/


-- ========================================================================================
-- Recommended Index 2: fact_sales.product_key
-- ========================================================================================

/*
Why:
   - ELT fact loading repeatedly joins fact_sales_elt → dim_product_elt
   - OLAP queries measure revenue by product/category
   - High selectivity when filtering specific stockcodes

Workload evidence:
   - "Top 10 selling products"
   - Aggregation used in MOLAP tables: revenue_by_month_product
*/


-- ========================================================================================
-- Recommended Index 3: dim_product.stockcode (Unique Index)
-- ========================================================================================

/*
Why:
   - In ETL, stockcode is the primary natural key for product matching.
   - During loading:
        TRIM(stockcode) = TRIM(raw stockcode from sales)
   - Without this index, product lookups slow down significantly.

This index directly supports:
   • fact_sales loading
   • fact_sales_elt loading
   • deduplication queries in ELT pipeline
*/


/*****************************************************************************************
4. OPTIONAL INDEXES (Useful for Large DWs, Optional for Assignment)
*****************************************************************************************/

/*
1. fact_sales.customer_key
   - Helps segmentation: "Revenue by country", "Revenue by customer".

2. dim_date.full_date
   - BI tools often use full timestamp instead of date_key.

3. fact_sales(totalamount)
   - Useful for heavy anomaly detection or fraud-likeness analysis.

4. dim_customer(customerid)
   - Speeds up matching during ETL deduping operations.
*/


/*****************************************************************************************
5. SURROGATE KEYS JUSTIFICATION (Based on Raw → Staging → Cleansed Workflow)
*****************************************************************************************/

/*
Why surrogate keys were required:

Raw CSV issues:
   - Missing customer IDs
   - Inconsistent stockcodes
   - Duplicate customer records
   - Mixed date formats

ETL fixes:
   - Correcting values (TRIM, CAST, COALESCE)
   - Removing NULL or invalid rows
   - Standardizing product and customer records

Surrogate key benefits:
   - Integer join keys in fact tables = faster OLAP performance
   - Stable primary keys even if raw data changes
   - Allow changes without breaking relationships
*/


/*****************************************************************************************
6. BIGINT date_key JUSTIFICATION (Aligned with OLAP + MOLAP queries)
*****************************************************************************************/

/*
Our ETL/ELT workflows generate date_key using:

      TO_CHAR(full_date, 'YYYYMMDDHH24MISS')::BIGINT

Justification:
   - Consistent across ETL + ELT + MOLAP workflows
   - Supports BETWEEN filters without extra casting
   - Faster than TIMESTAMP comparisons in OLAP cubes
   - Enables correct ordering and partitioning
*/


/*****************************************************************************************
7. SCHEMA SUMMARY (Aligned with our actual implementation)
*****************************************************************************************/

/*
DIM TABLE PRIMARY KEYS:
   - dim_customer.customer_key (SERIAL surrogate)
   - dim_product.product_key  (SERIAL surrogate)
   - dim_date.date_key        (BIGINT derived)

FACT TABLE KEY:
   - fact_sales.sales_key     (SERIAL surrogate)
   - fact_sales_elt.sales_key (SERIAL surrogate)

FOREIGN KEYS:
   - fact_sales.date_key     → dim_date.date_key
   - fact_sales.product_key  → dim_product.product_key
   - fact_sales.customer_key → dim_customer.customer_key

RECOMMENDED INDEXES:
   1. fact_sales.date_key
   2. fact_sales.product_key
   3. dim_product.stockcode
*/


/*****************************************************************************************
END OF FILE
*****************************************************************************************/
