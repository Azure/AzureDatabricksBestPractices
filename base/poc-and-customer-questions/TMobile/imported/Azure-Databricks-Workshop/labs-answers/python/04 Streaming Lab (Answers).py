# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://cdn2.hubspot.net/hubfs/438089/docs/training/dblearning-banner.png" alt="Databricks Learning" width="555" height="64">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2018 Databricks, Inc. All rights reserved.<br/>

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Challenges
# MAGIC * Larger Data
# MAGIC * Faster data and decisions - seconds, minutes, hours not days or weeks after it is created
# MAGIC * Streaming Pipelines can be hard
# MAGIC * Realtime Dashboards and alerts - for the holiday season, promotional campaigns, track falling or rising trends
# MAGIC 
# MAGIC ### Azure Databricks Solutions
# MAGIC * Deploy Event Hubs with a click of button
# MAGIC * Connect Azure Databricks with a click of a button
# MAGIC * Easy streaming pipelines almost the same as batch - SQL, Python, Scala, Java & R
# MAGIC * Make this data avialable on Storage or ADL to end users in minutes not days or weeks. 
# MAGIC 
# MAGIC ### Why Initech Needs Streaming
# MAGIC * Sales up or down (rolling 24 hours, 1 hour), to identify trends that are good or bad
# MAGIC * Holidays and promotions - how are the performing in real time

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## What is Structured Streaming?
# MAGIC 
# MAGIC <div style="width: 100%">
# MAGIC   <div style="margin: auto; width: 800px">
# MAGIC     <img src="http://spark.apache.org/docs/latest/img/structured-streaming-stream-as-a-table.png"/>
# MAGIC   </div>
# MAGIC </div>
# MAGIC 
# MAGIC Data is appended to the Input Table every _trigger interval_. For instance, if the trigger interval is 1 second, then new data is appended to the Input Table every seconds. (The trigger interval is analogous to the _batch interval_ in the legacy RDD-based Streaming API.) 

# COMMAND ----------

# MAGIC %md 
# MAGIC ####Azure Databricks for Streaming Analytics, Alerts, ETL & Data Engineers 
# MAGIC 
# MAGIC ![arch](https://kpistoropen.blob.core.windows.net/collateral/roadshow/azure_roadshow_sde.png)

# COMMAND ----------

# MAGIC %md 
# MAGIC ##![Spark Logo Tiny](https://kpistoropen.blob.core.windows.net/collateral/roadshow/logo_spark_tiny.png) *Part-1:* Create Streaming DataFrame

# COMMAND ----------

#schema for our streaming DataFrame

from pyspark.sql.types import *
schema = StructType([ \
  StructField("orderUUID", StringType(), True), \
  StructField("productId", IntegerType(), True), \
  StructField("userId", IntegerType(), True), \
  StructField("quantity", IntegerType(), True), \
  StructField("discount", DoubleType(), True), \
  StructField("orderTimestamp", TimestampType(), True)])

# COMMAND ----------

# MAGIC %fs ls /mnt/training-sources/initech/streaming/orders/data/

# COMMAND ----------

#streaming DataFrame reader for data on Azure Storage

streaming_df = spark.readStream \
    .schema(schema) \
    .option("maxFilesPerTrigger", 1) \
    .csv("dbfs:/mnt/training-sources/initech/streaming/orders/data/part-*")

# COMMAND ----------

# MAGIC %md
# MAGIC ## What can we do with this Streaming DataFrame?
# MAGIC 
# MAGIC If you run the following cell, you'll get a continuously updating display of the number of records read from the stream so far. Note that we're just calling `display()` on our DataFrame, _exactly_ as if it were a DataFrame reading from a static data source.
# MAGIC 
# MAGIC To stop the continuous update, just cancel the query.

# COMMAND ----------

display(streaming_df)

# COMMAND ----------

# MAGIC %md ##![Spark Logo Tiny](https://kpistoropen.blob.core.windows.net/collateral/roadshow/logo_spark_tiny.png) *Part-2:* Transform Streaming DataFrame 

# COMMAND ----------

# MAGIC %md ### It's just a DataFrame
# MAGIC 
# MAGIC We can use normal DataFrame transformations on our streaming DataFrame. For example, let's group the number of orders by productId
# MAGIC 
# MAGIC <img src="https://spark.apache.org/docs/latest/img/structured-streaming-example-model.png"/>

# COMMAND ----------

from pyspark.sql.functions import *
top_products = streaming_df.groupBy("productId").agg(sum(col("quantity")).alias("total_units_by_product")).orderBy(desc("total_units_by_product"))

# COMMAND ----------

# MAGIC %md 
# MAGIC * Call `display` on `top_products`
# MAGIC * Turn the streaming table into a streaming bar chart

# COMMAND ----------

display(top_products)

# COMMAND ----------

# MAGIC %md ##![Spark Logo Tiny](https://kpistoropen.blob.core.windows.net/collateral/roadshow/logo_spark_tiny.png) *Part-3:* Streaming Joins

# COMMAND ----------

# MAGIC %md ### Streaming Joins
# MAGIC 
# MAGIC Grouping by unkown product IDs is not that that exciting. Let's join the stream with the product lookup data set
# MAGIC * Use the join key productId
# MAGIC * Hint: Since both DataFrames have the same column name `productId`
# MAGIC * Use the duplicated columns trick documented here: https://docs.azuredatabricks.net/spark/latest/faq/join-two-dataframes-duplicated-column.html

# COMMAND ----------

# MAGIC %md Load the product lookup data from Azure Storage

# COMMAND ----------

product_lookup = spark.read.parquet("/mnt/training-sources/initech/productsFull/")

# COMMAND ----------

# MAGIC %md Join the `streaming_df` with `product_lookup` on `productId`
# MAGIC * Hint: https://docs.azuredatabricks.net/spark/latest/faq/join-two-dataframes-duplicated-column.html

# COMMAND ----------

#TO-DO
joined_df = streaming_df.join(product_lookup, "ProductID")

# COMMAND ----------

display(joined_df)

# COMMAND ----------

# MAGIC %md ##![Spark Logo Tiny](https://kpistoropen.blob.core.windows.net/collateral/roadshow/logo_spark_tiny.png) *Part-4:* Calculate a Streaming Dashboard - Revenue by Product Name 

# COMMAND ----------

# MAGIC %md ### Calculate the Total Revenue by Product Name
# MAGIC 
# MAGIC * Now that we have the product `Name` let's use that instead of the `productId` to `groupBy`
# MAGIC * Also let's calculate the total revenue instead of just units sold
# MAGIC   * Use the `quanity` column and the `StandardCost` column 

# COMMAND ----------

#TO-DO
top_products = joined_df.groupBy("Name").agg(sum(col("quantity")*col("StandardCost")).alias("total_revenue_by_product")).orderBy(desc("total_revenue_by_product"))

# COMMAND ----------

display(top_products)

# COMMAND ----------

