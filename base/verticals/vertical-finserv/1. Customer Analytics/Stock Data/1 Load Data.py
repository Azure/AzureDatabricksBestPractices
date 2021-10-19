# Databricks notebook source
# MAGIC %md
# MAGIC **Business case:**  
# MAGIC Exploratory analytics on market orders to determine market trends.
# MAGIC   
# MAGIC **Problem Statement:**  
# MAGIC Data is stored in different systems and its difficult to build analytics using multiple data sources. Copying data into a single platform is time consuming.  
# MAGIC   
# MAGIC **Business solution:**  
# MAGIC Use S3 as a data lake to store different sources of data in a single platform. This allows data scientists / analysis to quickly analyze the data and generate reports to predict market trends and/or make financial decisions.
# MAGIC   
# MAGIC **Technical Solution:**  
# MAGIC Use Databricks as a single platform to pull various sources of data from API endpoints, or batch dumps into S3 for further processing. ETL the CSV datasets into efficient Parquet formats for performant processing. 

# COMMAND ----------

# MAGIC %md
# MAGIC **Metadata**  
# MAGIC Owner: Vida  
# MAGIC Runnable: True  
# MAGIC Last Spark Version:  Spark 2.1

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Import Stock Data from Quandl

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Step 1: Download the data

# COMMAND ----------

import quandl

# COMMAND ----------

dbutils.widgets.text("quandl_api_key", "", "Quandl API Key:")

# COMMAND ----------

quandl.ApiConfig.api_key = dbutils.widgets.get("quandl_api_key")

# COMMAND ----------

quandl.bulkdownload("WIKI", download_type="complete", filename="/tmp/WIKI.zip")

# COMMAND ----------

# MAGIC %sh unzip /tmp/WIKI.zip -d /tmp

# COMMAND ----------

dbutils.fs.rm("/DemoData/stock_data", True)

# COMMAND ----------

# MAGIC %sh mkdir -p /dbfs/DemoData/stock_data

# COMMAND ----------

# MAGIC %sh cp /tmp/WIKI_*.csv /dbfs/DemoData/stock_data/

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Step 2: Load the CSV file as a Spark SQL Table

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC CREATE TEMPORARY TABLE stock_data_csv (
# MAGIC   ticker String,
# MAGIC   date   Date,
# MAGIC   open   Float,
# MAGIC   high   Float,
# MAGIC   low    Float,
# MAGIC   close  Float,
# MAGIC   volume Float,
# MAGIC   ex_dividend Float,
# MAGIC   split_ratio Float,
# MAGIC   adj_open Float,
# MAGIC   adj_high Float,
# MAGIC   adj_low Float,
# MAGIC   adj_close Float,
# MAGIC   adj_volume Float)
# MAGIC USING CSV
# MAGIC OPTIONS (
# MAGIC   path "/DemoData/stock_data/WIKI_*.csv"
# MAGIC )

# COMMAND ----------

# MAGIC %sql select * from stock_data_csv

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Let's count the number of lines.  Note the amount of time it takes to parse the CSV file.

# COMMAND ----------

# MAGIC %sql select count(*) from stock_data_csv

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Step 3: Optimize by saving this table as parquet.

# COMMAND ----------

# MAGIC %sql DROP TABLE IF EXISTS stock_data

# COMMAND ----------

sqlContext.table("stock_data_csv").write.saveAsTable("stock_data")

# COMMAND ----------

# MAGIC %sql select * from stock_data

# COMMAND ----------

# MAGIC %sql select count(*) as num_of_records from stock_data

# COMMAND ----------

