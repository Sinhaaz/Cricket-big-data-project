# Databricks notebook source
# DBTITLE 1,Importing Libraries
from pyspark.sql.functions import *

# COMMAND ----------

# DBTITLE 1,Read the Silver Layer
silver_df=spark.table('workspace.default.cricket_silver_current_matches')
display(silver_df)

# COMMAND ----------

# DBTITLE 1,Gold Analytics 1:Match type Distribution
gold_match_type_df=silver_df.groupBy('match_type').agg(count('*').alias('Total_Matches'))
display(gold_match_type_df)

# COMMAND ----------

# DBTITLE 1,Gold Analytics 2:Venue wise Match Count
gold_venue_df=silver_df.groupBy('venue').agg(count('*').alias('Total_Matches'))
display(gold_venue_df)

# COMMAND ----------

# DBTITLE 1,Gold Analytics 3:Team Wise Match Count
team_1_df=silver_df.select(col("team_1").alias("Team"))
team_2_df=silver_df.select(col("team_2").alias("Team"))

all_teams_df=team_1_df.union(team_2_df)
gold_team_df=all_teams_df.groupBy('Team')\
    .agg(count('*').alias("matches_played"))

display(gold_team_df)

# COMMAND ----------

# DBTITLE 1,Gold Analytics 4:Final Analytics Queries
display(spark.sql("""select count(*) as Total_matches,
count(distinct match_type) AS total_matches_types,
count(distinct venue) as Total_venues
FROM workspace.default.cricket_silver_current_matches"""))

# COMMAND ----------

# MAGIC %sql 
# MAGIC select count(*) as Total_matches,
# MAGIC count(distinct match_type) AS total_matches_types,
# MAGIC count(distinct venue) as Total_venues
# MAGIC FROM workspace.default.cricket_silver_current_matches