# Cricket-big-data-project
In this project we will be using cricket api and getting final insights. Using Medallion Architecture.

---

## 🔹 1_API Ingestion and Bronze Layer

**Data Source:** https://cricketdata.org

**BRONZE Layer Data Flow**

```text
Cricket API
     ↓
Raw JSON
     ↓
Databricks Volume
     ↓
Bronze DataFrame
     ↓
Delta Bronze Table
```


**🥉 BRONZE Layer**

This Databricks notebook ingests cricket match data from a REST API and stores it in the **Bronze layer** of the Medallion Architecture.

**Import Libraries**

```python
import requests
import json
from pyspark.sql.functions import *
from pyspark.sql.types import *
```

Imports the required libraries:

* `requests` → calls the Cricket API
* `json` → handles JSON data
* `pyspark.sql.functions` → provides Spark functions such as `current_timestamp()`
* `pyspark.sql.types` → defines the DataFrame schema

**Create Catalog, Schema and Volume**

```python
spark.sql("CREATE CATALOG IF NOT EXISTS workspace")
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.default")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.default.cricket_api_project")

base_path = '/Volumes/workspace/default/cricket_api_project'
```

Creates the Databricks **Catalog, Schema, and Volume** required for the project.

The `base_path` specifies where the raw API data will be stored.

**Call Cricket API**

```python
API_KEY = '...'
api_url = f"https://api.cricapi.com/v1/currentMatches?apikey={API_KEY}&offset=0"

response = requests.get(api_url)
response.raise_for_status()

api_data = response.json()
```
Once you sign up in https://cricketdata.org , you will get your API_KEY and sign up is free.

Calls the Cricket API and retrieves the current cricket match data.

* `requests.get()` → sends the API request
* `raise_for_status()` → checks whether the request was successful
* `response.json()` → converts the API response into JSON/Python data

**Inspect API Response**

```python
print(api_data)
print(api_data.keys())
print(json.dumps(api_data, indent=2)[:2000])
```

Displays the **complete API response, keys and first part of the API response** to understand the structure of the incoming data.

**Save Raw API Response**

```python
raw_file_path = f'{base_path}/current_matches_raw.json'

with open(raw_file_path,'w') as file:
    json.dump(api_data, file)
```

Saves the original API response as a **JSON file** in the Databricks Volume.

This represents the **raw data** before transformations.

**Create Bronze DataFrame**

```python
bronze_data = [{
    "source_api": api_url,
    "raw_json": json.dumps(api_data),
    "ingestion_time": None
}]
```

Creates the data structure for the Bronze layer.

It stores:

* `source_api` → API from which the data came
* `raw_json` → complete API response
* `ingestion_time` → time when data was ingested

**Define Bronze Schema**

```python
bronze_schema = StructType([
    StructField("source_api", StringType(), True),
    StructField("raw_json", StringType(), True),
    StructField("ingestion_time", TimestampType(), True)
])
```

Defines the **schema and data types** for the Bronze DataFrame.

**Create Bronze DataFrame**

```python
bronze_df = spark.createDataFrame(
    bronze_data,
    schema=bronze_schema
).withColumn(
    "ingestion_time",
    current_timestamp()
)
```

Creates a Spark DataFrame using the Bronze data and schema.

`current_timestamp()` records the time when the data was ingested.

**Display Bronze DataFrame**

```python
display(bronze_df)
```

Displays the Bronze DataFrame in Databricks for verification.

**Save Bronze Table**

```python
bronze_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.cricket_bronze_current_matches")
```

Saves the Bronze DataFrame as a **Delta table**.

**Table created:**

```text
workspace.default.cricket_bronze_current_matches
```

This becomes the **Bronze layer** of the Medallion Architecture.

**Success Message**

```python
display("BRONZE TABLE CREATED SUCCESSFULLY")
```

Displays a confirmation message that the Bronze table was created successfully.

---

## 🔹 2_BronzetoSilverCleanMatchTable

**Bronze → Silver Layer Data Flow**

```text
🥉 Bronze
Raw JSON
    │
    ▼
Parse JSON
    │
    ▼
Extract Required Fields
    │
    ▼
Format & Transform Data
    │
    ▼
Convert Data Types
    │
    ▼
🥈 Silver
Structured Cricket Data
```
**🥈 Silver Layer**

The Silver layer reads the raw JSON data from the Bronze table, extracts useful cricket match fields, transforms the data into a structured format, and saves it as a Delta table.

**Read Bronze Table**

```sql
select * from workspace.default.cricket_bronze_current_matches
```

Reads and displays the data stored in the Bronze table.

**Import Libraries**

```python
import json
from pyspark.sql.functions import *
from pyspark.sql.types import *
```

Imports the required libraries for:

* Parsing JSON data
* Creating Spark DataFrames
* Applying Spark transformations
* Defining DataFrame schemas

**Read Bronze Data and Parse JSON**

```python
bronze_df = spark.table(
    'workspace.default.cricket_bronze_current_matches'
)

raw_json = bronze_df.select("raw_json").collect()[0]['raw_json']

api_data = json.loads(raw_json)

matches = api_data.get("data", [])
```

- Reads the Bronze Delta table and extracts the `raw_json` column.
- The JSON string is converted into a Python dictionary using `json.loads()`.
- The `data` field containing the cricket matches is then extracted.
- `collect()` brings the selected data into the **driver memory**, so it should only be used when the amount of data is small.

**Extract Useful Fields**

Loops through each cricket match and extracts only the required fields.

```python
for match in matches:
```

The code extracts:

* Match ID
* Match name
* Match type
* Status
* Venue
* Match date
* Match time
* Teams
* Scores
* Match started status
* Match ended status

It also formats the scores into a readable format such as:

```text
180/5 in 20 overs
```

The extracted records are stored in `silver_rows`.
<br>This is the main **transformation step from Bronze to Silver**.

**Create Silver DataFrame**

Defines the schema for the Silver data and creates a Spark DataFrame.

```python
silver_schema = StructType([
    ...
])
```

The code then performs two transformations:

```python
.withColumn("match_date", to_date(col("match_date")))
.withColumn("loaded_at", current_timestamp())
```

* Converts `match_date` from string to a proper **date** type.
* Adds `loaded_at` to record when the Silver data was loaded.

The resulting DataFrame is displayed using:

```python
display(silver_df)
```

**Save Silver Table**

```python
silver_df.write \
    .format('delta') \
    .mode('overwrite') \
    .option("overwriteSchema", "true") \
    .saveAsTable(
        'workspace.default.cricket_silver_current_matches'
    )
```

Saves the transformed DataFrame as a **Delta table**.

**Table created:**

```text
workspace.default.cricket_silver_current_matches
```

The Silver layer now contains **cleaned and structured cricket match data** ready for further analysis or transformation into the Gold layer.

---
## 🔹 3_GoldLayerAnalytics

**Silver → Gold Layer Data Flow**
```text
🥈 Silver
Cleaned & Structured Data
        │
        ▼
   Aggregations
        │
   ┌────┼──────────────┐
   ▼    ▼              ▼
Match  Venue         Team
Type   Analysis      Analysis
   │    │              │
   └────┼──────────────┘
        ▼
🥇 Gold
Business Analytics
```


**🥇 Gold Layer**

The Gold layer reads the cleaned Silver data and creates **business-level analytics** that can be used for reporting and dashboards.

**Import Libraries**

```python
from pyspark.sql.functions import *
```

Imports Spark SQL functions required for aggregation operations such as `count()` and `groupBy()`.

**Read the Silver Layer**

```python
silver_df = spark.table(
    'workspace.default.cricket_silver_current_matches'
)

display(silver_df)
```

Reads the Silver Delta table and displays the cleaned and structured cricket match data.

**Match Type Distribution**

```python
gold_match_type_df = silver_df.groupBy('match_type') \
    .agg(count('*').alias('Total_Matches'))
```

Groups matches by their **match type** and calculates the total number of matches for each type.

Example categories could include:

* Test
* ODI
* T20

This creates a Gold-level analytical dataset.

**Venue-wise Match Count**

```python
gold_venue_df = silver_df.groupBy('venue') \
    .agg(count('*').alias('Total_Matches'))
```

Groups the matches by **venue** and calculates how many matches have been played at each venue.

**Team-wise Match Count**

```python
team_1_df = silver_df.select(col("team_1").alias("Team"))
team_2_df = silver_df.select(col("team_2").alias("Team"))

all_teams_df = team_1_df.union(team_2_df)

gold_team_df = all_teams_df.groupBy('Team') \
    .agg(count('*').alias("matches_played"))
```

Combines both `team_1` and `team_2` columns into a single `Team` column.

Then it groups the teams and calculates the **number of matches played by each team**.

```text
team_1 ──┐
         ├── UNION ──> Team ──> GROUP BY ──> Matches Played
team_2 ──┘
```

**Final Analytics Queries**

```python
display(spark.sql("""select count(*) as Total_matches,
count(distinct match_type) AS total_matches_types,
count(distinct venue) as Total_venues
FROM workspace.default.cricket_silver_current_matches"""))
```

Calculates overall summary metrics:

* **Total matches**
* **Number of different match types**
* **Number of different venues**

These metrics provide a high-level summary of the cricket data.

---

## 🔹 What is Medallion Architecture?
Medallion architecture is a data engineering design pattern for organizing data into progressively cleaner and more trustworthy layers. It's commonly used in modern data lakehouses, especially with platforms like Databricks, but the concept is broadly applicable.

---

### 🔹 Data Lakehouses
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