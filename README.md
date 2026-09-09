# Cricket-big-data-project
In this project we will be using cricket api and getting final insights. Using Medallion Architecture.

---

## 🔹 What is Medallion Architecture?
Medallion architecture is a data engineering design pattern for organizing data into progressively cleaner and more trustworthy layers. It's commonly used in modern data lakehouses, especially with platforms like Databricks, but the concept is broadly applicable.

---

## 🔹 Data Lakehouses
A Data Lakehouse is an architecture that combines the best parts of a **Data Lake** and a **Data Warehouse**.

![Data Lakehouse Image](https://github.com/Sinhaaz/Cricket-big-data-project/blob/main/Data%20Lakehouse.png)

---

### 🔹 Data Lakehouse Layers

![Medallion Architecture](https://github.com/Sinhaaz/Cricket-big-data-project/blob/main/Medallion%20Arch.png)

---

#### 🥉 Bronze: Raw data

This layer stores data exactly as it arrives from source systems.

**Characteristics:**

Minimal or no transformations
Preserves original schema
Supports replay and auditing
Often append-only

**Example:**

- Raw application logs
- CSV files from vendors
- CDC (Change Data Capture) events
- IoT sensor readings
```
{
  "customer_id": "123",
  "name": " Alice ",
  "signup": "01/02/2025",
  "status": "A"
}
```
---

#### 🥈 Silver: Cleaned and standardized

This layer improves data quality and makes it suitable for analytics.

**Typical transformations:**

- Remove duplicates
- Handle missing values
- Standardize formats
- Parse timestamps
- Validate records
- Join reference data
- Apply business rules

The same record might become:
```
{
  "customer_id": 123,
  "name": "Alice",
  "signup_date": "2025-01-02",
  "status": "Active"
}
```
Silver tables are usually the foundation for machine learning and downstream transformations.

---

#### 🥇 Gold: Business-ready

Gold contains curated datasets designed for reporting and decision-making.

**Examples:**

- Daily sales by region
- Customer lifetime value
- Product performance
- Executive dashboards
- Feature tables for ML

Instead of individual transactions, Gold might contain:

| Region | Revenue | Customers |
|--------|---------|-----------|
| East   | $2.4M   | 14,200    |
| West   | $1.8M   | 11,900    |

These datasets are optimized for BI tools like Microsoft, Tableau, or SQL queries.

---

### 🔹 End-to-End Example

Suppose you're building an e-commerce analytics platform.

**1.Bronze Layer**

- orders_raw
- customers_raw
- payments_raw

**2.Silver Layer**

- orders_clean
- customers_clean
- payments_clean
- Invalid orders removed
- Currency standardized
- Dates parsed
- Duplicate customers merged

**3.Gold Layer**

- daily_sales
- customer_360
- top_products
- revenue_by_country

Business users query directly to **Gold Layer datasets**.

---

### 🔹 Why use Medallion architecture?

It provides several benefits:

- Improved data quality: Errors are caught and corrected before data reaches analysts.
- Traceability: You can always trace a Gold metric back to the original raw records.
- Reproducibility: If transformation logic changes, you can rebuild downstream layers from Bronze.
- Separation of concerns: Each layer has a clear purpose, making pipelines easier to maintain.
- Scalability: Teams can work independently on ingestion, cleaning, and business modeling.


Although Medallion architecture is conceptually platform-agnostic, it's commonly implemented with:

- Data lakehouse storage (e.g., object storage)
- Table formats like Delta Lake, Apache Iceberg, or Apache Hudi
- Processing engines such as Apache Spark or SQL engines
- Workflow orchestrators like Apache Airflow or Dagster

**When to use it -**

Medallion architecture works well when you:

- Ingest data from multiple operational systems.
- Need reliable analytics and reporting.
- Build machine learning pipelines.
- Require auditability and lineage.
- Manage large-scale ETL/ELT workflows.

In essence, the pattern is about progressively increasing trust in your data: 
- Bronze preserves the source 
- Silver makes it reliable 
- Gold makes it actionable