# Databricks notebook source
# DBTITLE 1,Prototype Summary
# MAGIC %md
# MAGIC 
# MAGIC We have 2 streams:
# MAGIC   1. **User click stream** from website
# MAGIC   2. CDC stream containing **user profile changes** (considered 'slowly changing')
# MAGIC   
# MAGIC Goal is to aggregate these sources into one table, which will be used for:
# MAGIC   1. Building **new recommendation models** (i.e. what to recommend next for the user)
# MAGIC   2. **Scoring new activity** against existing models

# COMMAND ----------

# DBTITLE 1,Architecture
# MAGIC %md
# MAGIC 
# MAGIC <h2><img src="/files/tables/Screen_Shot_2019_06_25_at_5_42_19_PM-bfe74.png"></h2>

# COMMAND ----------

# DBTITLE 1,What is Databricks Delta?
# MAGIC %md
# MAGIC 
# MAGIC ##**Delta** = **Spark** +  **Performance and Reliability Optimizations\***
# MAGIC 
# MAGIC \* e.g. dynamic partition pruning, proprietary code-gen, faster window-functions, faster skew joins, Z-ordering based on space filling curves...
# MAGIC 
# MAGIC ... and introduces a new table format, built on top of Parquet, which **significantly simplifies streaming pipelines**

# COMMAND ----------

# DBTITLE 1,Looks Easy... Why? ... Databricks Delta Simplifies Streaming Pipelines
# MAGIC %md
# MAGIC 
# MAGIC - **Upsert** semantics allow CDC stream to be applied to a Spark table
# MAGIC - Delta table used in join step is **automatically refreshed** to include up-to-date (CDC stream aware) data
# MAGIC - Delta table optimization (compaction & indexing) **enhances query performance** of signal store
# MAGIC - Introduces **ACID compliance** for simulataneous reads and writes
# MAGIC 
# MAGIC Without these Delta capabilities, the pipeline would be significantly more complex and have greater latency

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ###Step 0: [Prep](#notebook/1494166000783585/command/1494166000783662)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 1: [Read Streams From EventHub](#notebook/1494166000783585/command/1494166000783612)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 2: [Upsert CDC Stream into Delta Table ](#notebook/1494166000783585/command/1494166000783654)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 3: [Enrich Click Stream Data with User Profiles](#notebook/1494166000783585/command/1494166000783660)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 4: [Aggregate into Signal Store](#notebook/1494166000783585/command/1494166000783670)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5: [Query Signal Store & Optimize Tables](#notebook/1494166000783585/command/1494166000783691)