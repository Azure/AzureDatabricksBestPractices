# Databricks notebook source
# DBTITLE 1,Last Time...
# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC <img src="https://s3-us-west-1.amazonaws.com/paul-databricks/IMG_20190214_114842 (1).jpg" alt="workflow" width="800">

# COMMAND ----------

# DBTITLE 1,Prototype Assumptions
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

# DBTITLE 1,Prototype Design
# MAGIC %md
# MAGIC 
# MAGIC <img src="https://s3-us-west-1.amazonaws.com/paul-databricks/Screen+Shot+2019-03-13+at+4.15.55+PM.png" alt="workflow" width="1000">

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
# MAGIC ###Step 0: [Prep / Clean Up](https://demo.cloud.databricks.com/#notebook/2429899/command/2429900)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 1: [Read Streams From Kinesis](https://demo.cloud.databricks.com/#notebook/2429906/command/2429907)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 2: [Upsert CDC Stream into Delta Table ](https://demo.cloud.databricks.com/#notebook/2429906/command/2430311)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 3: [Enrich Click Stream Data with User Profiles](https://demo.cloud.databricks.com/#notebook/2429906/command/2430314)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 4: [Aggregate into Signal Store](https://demo.cloud.databricks.com/#notebook/2429906/command/2430780)

# COMMAND ----------

# MAGIC %md
# MAGIC ### [Query Signal Store & Optimize Tables](https://demo.cloud.databricks.com/#notebook/2429906/command/2430840)