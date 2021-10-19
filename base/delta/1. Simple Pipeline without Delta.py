# Databricks notebook source
# MAGIC %md ## Without Delta Pipeline, with Spark and Parquet
# MAGIC 
# MAGIC ![stream](https://kpistoropen.blob.core.windows.net/collateral/delta/non-delta-new.png)

# COMMAND ----------

#quick clean up of demo folder
dbutils.fs.rm("/generic/iot-pipeline/", True)

# COMMAND ----------

# MAGIC %md #### Historical and new data is often written in very small files and very small directories (such as eventhub capture): 
# MAGIC + This data is also partitioned by arrival time not event time!
# MAGIC 
# MAGIC ![stream](https://docs.microsoft.com/en-us/azure/data-lake-store/media/data-lake-store-archive-eventhub-capture/data-lake-store-eventhub-data-sample.png)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Step 0: Read data

# COMMAND ----------

# MAGIC %fs ls /databricks-datasets/structured-streaming/events/

# COMMAND ----------

from pyspark.sql.functions import expr

rawData = spark.read \
  .option("inferSchema", "true") \
  .json("/databricks-datasets/structured-streaming/events/") \
  .drop("time") \
  .withColumn("date", expr("cast(concat('2019-01-', cast(rand(5) * 30 as int) + 1) as date)")) \
  .withColumn("deviceId", expr("cast(rand(5) * 100 as int)"))
  # add a couple of columns for demo purposes

# COMMAND ----------

display(rawData)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Step 1: Write out raw data and create staging table

# COMMAND ----------

rawData.write.format("parquet").partitionBy("date").save("/generic/iot-pipeline/")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE IF EXISTS demo_iot_data;
# MAGIC CREATE TABLE demo_iot_data (action STRING, deviceId INTEGER, date DATE)
# MAGIC USING parquet
# MAGIC OPTIONS (path = "/generic/iot-pipeline/")
# MAGIC PARTITIONED BY (date)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Step 2: Query the data

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM demo_iot_data

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Wait, no results? That's strange. Let's repair the table then.

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW PARTITIONS demo_iot_data

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC MSCK REPAIR TABLE demo_iot_data

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW PARTITIONS demo_iot_data

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT count(*) FROM demo_iot_data

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Step 3: Appending new data

# COMMAND ----------

new_data = spark.range(100000) \
  .selectExpr("'Open' as action", "'2019-01-30' date") \
  .withColumn("deviceId", expr("cast(rand(5) * 500 as int)"))

# COMMAND ----------

# MAGIC %md ##Note: This is dangerous to simply append to the production table. 

# COMMAND ----------

new_data.write.format("parquet").partitionBy("date").mode("append").save("/generic/iot-pipeline/")

# COMMAND ----------

# MAGIC %fs ls /generic/iot-pipeline/

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Step 4: Query should show new results

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT count(*) FROM demo_iot_data

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC That's strange, well, we can repair the table again right.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC MSCK REPAIR TABLE demo_iot_data

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT count(*) FROM demo_iot_data

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Step 5: Upserts / Changes (on previously written data)

# COMMAND ----------

new_data.drop("date").write.format("parquet").mode("overwrite").save("/generic/iot-pipeline/date=2019-01-30/")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Step 6: Query should reflect new data

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT count(*) FROM demo_iot_data

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC That's strange, guess we need to refresh the metadata.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC REFRESH TABLE demo_iot_data

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT count(*) FROM demo_iot_data

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Step 7: Add historical data

# COMMAND ----------

from pyspark.sql.functions import expr
  
old_batch_data = spark.range(100000) \
  .repartition(200) \
  .selectExpr("'Open' as action", "cast(concat('2019-01-', cast(rand(5) * 15 as int) + 1) as date) as date") \
  .withColumn("deviceId", expr("cast(rand(5) * 100 as int)"))

old_batch_data.write.format("parquet").partitionBy("date").mode("append").save("/generic/iot-pipeline/")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT count(*) FROM demo_iot_data

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Won't be up to date until we call refresh

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC REFRESH TABLE demo_iot_data

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT count(*) FROM demo_iot_data

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Performance Improvements
# MAGIC 
# MAGIC Now we want to build other pipelines with this information, we want to write out to our data lake and allow data scientists to query it quickly. The above query took 7 seconds, there's not much data there - it's probably just not well formatted.
# MAGIC 
# MAGIC In order to get reasonable performance you're going to have to build a whole pipeline just to manage the file sizes and trying to optimize it for querying later on.

# COMMAND ----------

# MAGIC %fs ls /generic/iot-pipeline/

# COMMAND ----------

# MAGIC %fs ls dbfs:/generic/iot-pipeline/date=2019-01-01/

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT count(*) FROM demo_iot_data

# COMMAND ----------

