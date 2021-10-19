-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://cdn2.hubspot.net/hubfs/438089/docs/training/dblearning-banner.png" alt="Databricks Learning" width="555" height="64">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2018 Databricks, Inc. All rights reserved.<br/>

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### Challenges
-- MAGIC * Larger Data
-- MAGIC * Faster data and decisions - seconds, minutes, hours not days or weeks after it is created
-- MAGIC * Streaming Pipelines can be hard
-- MAGIC * Realtime Dashboards and alerts - for the holiday season, promotional campaigns, track falling or rising trends
-- MAGIC 
-- MAGIC ### Azure Databricks Solutions
-- MAGIC * Deploy Event Hubs with a click of button
-- MAGIC * Connect Azure Databricks with a click of a button
-- MAGIC * Easy streaming pipelines almost the same as batch - SQL, Python, Scala, Java & R
-- MAGIC * Make this data avialable on Storage or ADL to end users in minutes not days or weeks. 
-- MAGIC 
-- MAGIC ### Why Initech Needs Streaming
-- MAGIC * Sales up or down (rolling 24 hours, 1 hour), to identify trends that are good or bad
-- MAGIC * Holidays and promotions - how are the performing in real time

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## What is Structured Streaming?
-- MAGIC 
-- MAGIC <div style="width: 100%">
-- MAGIC   <div style="margin: auto; width: 800px">
-- MAGIC     <img src="http://spark.apache.org/docs/latest/img/structured-streaming-stream-as-a-table.png"/>
-- MAGIC   </div>
-- MAGIC </div>
-- MAGIC 
-- MAGIC Data is appended to the Input Table every _trigger interval_. For instance, if the trigger interval is 1 second, then new data is appended to the Input Table every seconds. (The trigger interval is analogous to the _batch interval_ in the legacy RDD-based Streaming API.) 

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ####Azure Databricks for Streaming Analytics, Alerts, ETL & Data Engineers 
-- MAGIC 
-- MAGIC ![arch](https://kpistoropen.blob.core.windows.net/collateral/roadshow/azure_roadshow_sde.png)

-- COMMAND ----------

SET spark.sql.shuffle.partitions = 4

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ##![Spark Logo Tiny](https://kpistoropen.blob.core.windows.net/collateral/roadshow/logo_spark_tiny.png) *Part-1:* Create Streaming DataFrame

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC 
-- MAGIC #schema for our streaming DataFrame
-- MAGIC 
-- MAGIC from pyspark.sql.types import *
-- MAGIC schema = StructType([ \
-- MAGIC   StructField("orderUUID", StringType(), True), \
-- MAGIC   StructField("productId", IntegerType(), True), \
-- MAGIC   StructField("userId", IntegerType(), True), \
-- MAGIC   StructField("quantity", IntegerType(), True), \
-- MAGIC   StructField("discount", DoubleType(), True), \
-- MAGIC   StructField("orderTimestamp", TimestampType(), True)])
-- MAGIC 
-- MAGIC 
-- MAGIC streaming_df = spark.readStream \
-- MAGIC     .schema(schema) \
-- MAGIC     .option("maxFilesPerTrigger", 1) \
-- MAGIC     .csv("dbfs:/mnt/training-sources/initech/streaming/orders/data/part-*")
-- MAGIC     
-- MAGIC streaming_df.createOrReplaceTempView("orders")

-- COMMAND ----------

-- DBTITLE 0,TO-DO
SELECT * FROM orders

-- COMMAND ----------

-- MAGIC %md ##![Spark Logo Tiny](https://kpistoropen.blob.core.windows.net/collateral/roadshow/logo_spark_tiny.png) *Part-2:* Transform Streaming Table 

-- COMMAND ----------

-- MAGIC %md ### It's just a SparkSQL Table
-- MAGIC 
-- MAGIC We can use normal Spark SQL transformations on our streaming DataFrame. For example, let's group the number of orders by productId
-- MAGIC 
-- MAGIC <img src="https://spark.apache.org/docs/latest/img/structured-streaming-example-model.png"/>

-- COMMAND ----------

SELECT 
  sum(quantity) AS total_units_by_product,
  productId 
  FROM orders 
  GROUP BY productId 
  ORDER BY total_units_by_product DESC

-- COMMAND ----------

-- MAGIC %md ##![Spark Logo Tiny](https://kpistoropen.blob.core.windows.net/collateral/roadshow/logo_spark_tiny.png) *Part-3:* Streaming Joins

-- COMMAND ----------

-- MAGIC %md ### Streaming Joins
-- MAGIC 
-- MAGIC Grouping by unkown product IDs is not that that exciting. Let's join the stream with the product lookup data set

-- COMMAND ----------

-- DBTITLE 1,TO-DO
CREATE OR REPLACE TEMPORARY VIEW products 
USING parquet
OPTIONS ("path" "/mnt/training-sources/initech/productsFull/")

-- COMMAND ----------

-- DBTITLE 1,TO-DO
CREATE OR REPLACE TEMPORARY VIEW orders_joined 
AS (
  SELECT 
    products.*, 
    orders.orderUUID,
    orders.userId,
    orders.quantity,
    orders.discount,
    orders.orderTimestamp
  FROM 
    orders
  JOIN 
    products
  ON (products.productid = orders.productid)
  )

-- COMMAND ----------

-- DBTITLE 1,TO-DO
SELECT * FROM orders_joined

-- COMMAND ----------

-- MAGIC %md ##![Spark Logo Tiny](https://kpistoropen.blob.core.windows.net/collateral/roadshow/logo_spark_tiny.png) *Part-4:* Streaming Reports & KPIs - Revenue by Product Name 

-- COMMAND ----------

-- MAGIC %md ### Calculate the Total Revenue by Product Name
-- MAGIC 
-- MAGIC * Now that we have the product `Name` let's use that instead of the `productId` to `groupBy`
-- MAGIC * Also let's calculate the total revenue instead of just units sold
-- MAGIC   * Use the `quanity` column and the `StandardCost` column 

-- COMMAND ----------

-- DBTITLE 1,TO-DO
SELECT 
  sum(quantity*StandardCost) AS revenue_by_product,
  Name 
  FROM orders_joined 
  GROUP BY Name 
  ORDER BY revenue_by_product DESC

-- COMMAND ----------

