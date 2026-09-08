# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Importing libraries
import requests
import json
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# DBTITLE 1,Create Catalog, Schema and Volume
spark.sql("CREATE CATALOG IF NOT EXISTS workspace")
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.default")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.default.cricket_api_project")

base_path = '/Volumes/workspace/default/cricket_api_project'

# COMMAND ----------

# DBTITLE 1,Calling Cricket API
API_KEY = 'f634e5fb-96fb-431e-af32-28b0a0fb1919'
api_url = f"https://api.cricapi.com/v1/currentMatches?apikey={API_KEY}&offset=0"

response = requests.get(api_url)
response.raise_for_status()

api_data = response.json()
# JSON gives in key value format
print(api_data.keys())
print(json.dumps(api_data, indent=2)[:2000])

# COMMAND ----------

print(api_data)

# COMMAND ----------

# DBTITLE 1,Save Raw API response in Volumes
raw_file_path = f'{base_path}/current_matches_raw.json'

with open(raw_file_path,'w') as file:
    json.dump(api_data, file)

print("Raw API data is save to file",raw_file_path)

# COMMAND ----------

# DBTITLE 1,Create Bronze Layer DF or Table
bronze_data = [{
    "source_api":api_url,
    "raw_json":json.dumps(api_data),
    "ingestion_time":None
}]

bronze_schema=StructType([
    StructField("source_api",StringType(),True),
    StructField("raw_json",StringType(),True),
    StructField("ingestion_time",TimestampType(),True)
])

bronze_df = spark.createDataFrame(bronze_data,schema=bronze_schema)\
    .withColumn("ingestion_time",current_timestamp())

display(bronze_df)

# COMMAND ----------

bronze_data

# COMMAND ----------

bronze_schema

# COMMAND ----------

# DBTITLE 1,Saving the Bronze Table
bronze_df.write\
    .format("delta")\
    .mode("overwrite")\
    .saveAsTable("workspace.default.cricket_bronze_current_matches")

display("BRONZE TABLE CREATED SUCCESSFULLY")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.default.cricket_bronze_current_matches;