# 🛒 E-Commerce Data Warehouse & Power BI Analytics

![PostgreSQL](https://img.shields.io/badge/PostgreSQL-Data%20Warehouse-blue)
![Power BI](https://img.shields.io/badge/Power%20BI-Dashboard-yellow)
![SQL](https://img.shields.io/badge/SQL-ETL%20%7C%20ELT%20%7C%20OLAP%20%7C%20MOLAP-lightgrey)

## 🎓 Academic Project Overview

This project was developed as part of a **Data Warehousing & Business Intelligence** university course.

The objective was to design and implement a complete analytical system including:

- Data warehouse schema design (fact & dimension tables)
- End-to-end ETL and ELT workflows
- Data validation and consistency checks
- OLAP query implementation
- MOLAP pre-aggregation tables
- Performance comparison of join algorithms
- Interactive Power BI dashboard using DAX

---

## 🧱 System Architecture

The system follows a modern layered BI architecture:

Source Data (CSV / Raw Tables)
        │
        ▼
Staging Layer (PostgreSQL)
        │
        ▼
ETL / ELT Processing Layer
        │
        ▼
Data Warehouse (Star Schema)
        │
        ├── OLAP Queries
        ├── MOLAP Aggregation Tables
        │
        ▼
Power BI Semantic Model & Dashboard

This architecture ensures:

  - Data quality and validation
  - Analytical performance optimization
  - Scalability for future enhancements

---

## 🗃️ Data Warehouse Design

- Star schema implementation
- Fact tables: sales transactions
- Dimensions: customer, product, date
- Surrogate keys and indexing applied
- Referential integrity enforced

---

## 🔄 Data Processing Pipelines

### ETL Workflow
Transformations applied before loading into DW.

### ELT Workflow (Primary)
Raw data loaded first → transformations performed inside PostgreSQL.

Validation performed using:
- Row count checks
- Revenue reconciliation
- Aggregation verification

---

## 📊 Analytics

### OLAP
- Monthly revenue trends
- Regional performance
- Product/category analysis
- Customer metrics

### MOLAP
- Monthly summary tables for faster querying
- Monthly revenue by region
- Monthly revenue by product category
- Customer lifetime revenue summary
- Order volume by country
- Product performance rankings
- Yearly and quarterly revenue summaries

⚙️ Query Optimization & Performance Engineering
Join Techniques Analysis

The project includes a dedicated performance evaluation of PostgreSQL join algorithms using:
Nested Loop Join
Hash Join
Sort-Merge Join

Each join type was tested using EXPLAIN ANALYZE on analytical queries involving:
fact_sales
dim_customer
dim_product
dim_date
Metrics evaluated:
Execution time
Cost estimation
Memory usage
Join order
Scan methods (sequential vs index scan)
This analysis helped identify the most efficient join strategy for large fact-dimension joins.

Indexing & Performance Optimization

To support fast analytical workloads, the following optimizations were applied:
Indexes on all foreign keys in the fact table
date_key
product_key
customer_key

Indexes on dimension natural keys:
customer_id
stock_code
full_date

Composite indexes for frequent filtering:
(date_key, product_key)
(customer_key, date_key)

Query planner statistics maintained using:
ANALYZE
VACUUM
These optimizations significantly reduced:
Full table scans
Query latency for dashboard visuals
ETL and ELT processing time

---

## 📈 Power BI Dashboard

Includes:

- KPI cards (Revenue, Orders, ARPO, YoY Growth)
- Monthly revenue line chart
- Top regions/products bar chart
- Customer performance matrix
- Slicers & drill-down hierarchy

---

## 🛠 Technologies Used

- PostgreSQL
- SQL
- Power BI
- DAX
- ETL / ELT
- OLAP / MOLAP

---

## 📁 Project Structure

E-Commerce-Data-Warehouse-Power-BI-Analytics/
│
├── sql/
│   ├── schema/              # DW schema creation scripts
│   ├── etl/                 # ETL workflow scripts
│   ├── elt/                 # ELT workflow scripts
│   ├── olap/                # Analytical queries
│   ├── molap/               # Aggregation tables & views
│   └── joins/               # Join performance analysis
│
├── datasets/                # Raw and cleaned CSV files
│
├── powerbi/
│   └── dashboard.pbix       # Final Power BI dashboard
│
├── screenshots/             # Dashboard & model screenshots
│
├── docs/                    # Project report and documentation
│
├── .github/workflows/       # GitHub Actions (SQL validation)
│
└── README.md


---

## 🚀 How to Run

1. Create PostgreSQL database
2. Run scripts in order:
3. Open Power BI → Connect to PostgreSQL → Import DW tables
4. Load `dashboard.pbix`

---

## 📌 Future Improvements

- Incremental ETL
- Real-time data ingestion
- Partitioned fact tables
- Cloud deployment
- Automated scheduling

---

## 👤 Author

**Ali Ahmad**  
BS Data Science  
Data Warehousing & Business Intelligence Enthusiast

---

⭐ If you find this project useful, consider starring the repository!
