# Databricks notebook source
# MAGIC %md ## Delta Pipeline, with Azure Databricks
# MAGIC 
# MAGIC ![stream](https://kpistoropen.blob.core.windows.net/collateral/delta/Delta.png)

# COMMAND ----------

dbutils.fs.rm("/delta/iot-pipeline/", True)
# clear out current version to walk through tutorial

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

# MAGIC %md
# MAGIC 
# MAGIC # Step 1: Write out raw data and create our table

# COMMAND ----------

rawData.write.format("delta").partitionBy("date").save("/delta/iot-pipeline/")

# COMMAND ----------

# MAGIC %fs ls dbfs:/delta/iot-pipeline	

# COMMAND ----------

# MAGIC %fs ls dbfs:/delta/iot-pipeline/_delta_log/

# COMMAND ----------

display(spark.read.parquet("dbfs:/delta/iot-pipeline/"))

# COMMAND ----------

# MAGIC %fs ls dbfs:/delta/iot-pipeline/date=2019-01-01/

# COMMAND ----------

spark.read.parquet("dbfs:/delta/iot-pipeline/date=2019-01-01/part-00000-64c2a4c2-329b-4114-8f95-68b418c683fb.c000.snappy.parquet")

# COMMAND ----------

# MAGIC %fs ls dbfs:/delta/iot-pipeline/date=2019-01-01/

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS demo_iot_data_delta;
# MAGIC CREATE TABLE demo_iot_data_delta
# MAGIC USING DELTA
# MAGIC LOCATION "/delta/iot-pipeline/"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Step 2: Query the data

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT count(*) FROM demo_iot_data_delta

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Worked great, no repair table necessary, since Delta automatically handles the metadata

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Step 3: Adding new data

# COMMAND ----------

new_data = spark.range(100000) \
  .selectExpr("'Open' as action", "cast('2019-01-30' as date) as date") \
  .withColumn("deviceId", expr("cast(rand(5) * 500 as int)"))

# COMMAND ----------

display(new_data)

# COMMAND ----------

new_data.write.format("delta").partitionBy("date").mode("append").save("/delta/iot-pipeline/")

# COMMAND ----------

new_data.write.format("delta").partitionBy("date").mode("append").save("/delta/iot-pipeline/")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Step 4: Query should show new results

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT count(*) FROM demo_iot_data_delta

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Again, no update necessary.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Step 5: Updating previous data

# COMMAND ----------

new_data.write.format("delta").mode("overwrite") \
  .option("replaceWhere", "date = cast('2019-01-30' as date)") \
  .save("/delta/iot-pipeline/")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Step 6: Query should reflect new data

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT count(*) FROM demo_iot_data_delta

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

old_batch_data.write.format("delta").partitionBy("date").mode("append").save("/delta/iot-pipeline/")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM demo_iot_data_delta

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Performance Improvements
# MAGIC 
# MAGIC Now we want to build other pipelines with this information, we want to write out to our data warehouse and allow data scientists to query it quickly. The above query took 7 seconds, there's not much data there - it's probably just not well formatted.
# MAGIC 
# MAGIC With Delta, fixing this is simple.

# COMMAND ----------

# MAGIC %fs ls /delta/iot-pipeline/date=2019-01-01/

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE "/delta/iot-pipeline/"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Now the table is optimized for querying. This is going to be an order of magnitude faster.

# COMMAND ----------

# MAGIC %fs ls /delta/iot-pipeline/date=2019-01-01/

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT count(*) FROM demo_iot_data_delta

# COMMAND ----------

