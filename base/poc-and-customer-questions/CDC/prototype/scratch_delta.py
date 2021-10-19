# Databricks notebook source
spark.conf.get("spark.databricks.delta.optimize.minFileSize")

# COMMAND ----------

spark.conf.get("spark.databricks.delta.optimize.maxFileSize")

# COMMAND ----------

# MAGIC %run ./creds_python

# COMMAND ----------

# DBTITLE 1,Read Kinesis Stream
from pyspark.sql.types import *
from pyspark.sql.functions import *

click_schema= StructType() \
          .add("uid", StringType()) \
          .add("clickTimestamp", TimestampType()) \
          .add("exchangeID", IntegerType()) \
          .add ("publisher", StringType()) \
          .add ("creativeID", IntegerType()) \
          .add("click", StringType()) \
          .add ("advertiserID", IntegerType()) \
          .add("browser", StringType()) \
          .add("geo", StringType()) \
          .add("bidAmount", DoubleType())

click = spark \
  .readStream \
  .format("kinesis") \
  .option("streamName", "ecommerce-clicks") \
  .option("initialPosition", "latest") \
  .option("region", kinesisRegion) \
  .option("awsAccessKey", awsAccessKeyId) \
  .option("awsSecretKey", awsSecretKey) \
  .load()

# COMMAND ----------

# DBTITLE 1,Convert JSON to Columns, and add Partitioning Columns
click_raw = click.selectExpr("cast (data as STRING) jsonData")
click_transformed = click_raw.select(from_json("jsonData", click_schema).alias("fields")).select("fields.*")
click_transformed = click_transformed.withColumn("date",to_date(col("clickTimestamp"))).withColumn("hour",hour("clickTimestamp")).withColumn("min",minute("clickTimestamp"))

# COMMAND ----------

display(click_transformed)

# COMMAND ----------

# DBTITLE 1,For tidying up / Start From Scratch
dbutils.fs.rm("/mnt/databricks-paul/sfdc-delta", True)
dbutils.fs.rm("/mnt/databricks-paul/sfdc-delta-checkpoints", True)

# COMMAND ----------

# DBTITLE 1,Write Append Only, Partition By Columns
click_transformed.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation","/mnt/databricks-paul/sfdc-delta-checkpoints") \
    .partitionBy(["geo","date","hour","min"]) \
    .trigger(processingTime='30 seconds') \
    .start("/mnt/databricks-paul/sfdc-delta")

# COMMAND ----------

# DBTITLE 1,Create Table Reference in Metastore
# MAGIC %sql
# MAGIC CREATE TABLE paul.append_only_table
# MAGIC USING delta
# MAGIC LOCATION "/mnt/databricks-paul/sfdc-delta"

# COMMAND ----------

spark.conf.set("spark.databricks.delta.autoOptimize.capacity", 0)

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE paul.append_only_table SET TBLPROPERTIES (delta.autoOptimize=true)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM paul.append_only_table

# COMMAND ----------

# DBTITLE 1,Create a 'Mutated Stream', where Browser Column is Altered for Subset of Stream
click_mutated = click_transformed.where(col("browser")=="Safari").withColumn("browser",lit("Paul"))

# COMMAND ----------

display(click_mutated)

# COMMAND ----------

# DBTITLE 1,Define Function to Upsert Mutated Stream
#Function to upsert `microBatchOutputDF` into Delta table using MERGE
def upsertToDelta(microBatchOutputDF, batch):
  #Set the dataframe to view name
  spark.catalog.dropGlobalTempView("stream_updates")
  microBatchOutputDF.drop_duplicates(["uid"]).createGlobalTempView("stream_updates")
  
  #Use the view name to apply MERGE
  #NOTE: You have to use the SparkSession that has been used to define the `updates` dataframe
  spark.sql("""
    MERGE INTO paul.append_only_table t
    USING global_temp.stream_updates s
    ON s.uid = t.uid AND s.date = t.date AND s.hour = t.hour AND s.min = t.min
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
  """)

# COMMAND ----------

# DBTITLE 1,Apply Upserts to Delta Table  
click_mutated.writeStream \
  .format("delta") \
  .foreachBatch(upsertToDelta) \
  .outputMode("update") \
  .option("checkpointLocation","/mnt/databricks-paul/update-checkpoints") \
  .start()

# COMMAND ----------

# DBTITLE 1,Can see mutations are being applied...
# MAGIC %sql
# MAGIC SELECT count(*) FROM paul.append_only_table WHERE browser=="Paul"

# COMMAND ----------

display(dbutils.fs.ls("/mnt/databricks-paul/sfdc-delta"))

# COMMAND ----------

display(dbutils.fs.ls("/mnt/databricks-paul/sfdc-delta/geo=Atlanta/date=2019-05-22/hour=19/min=58/"))

# COMMAND ----------

import datetime
import time
ts = time.time()
datenow = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d')
hournow = datetime.datetime.fromtimestamp(ts).strftime('%H')
minnow = datetime.datetime.fromtimestamp(ts).strftime('%M')

spark.sql("OPTIMIZE paul.append_only_table WHERE date <= '{0}' AND hour <= {1} AND min < {2}".format(datenow, hournow, minnow))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM paul.append_only_table WHERE date <= '2019-05-21'

# COMMAND ----------

display(dbutils.fs.ls("/mnt/databricks-paul/sfdc-delta/geo=Atlanta/date=2019-05-21/hour=22/min=45/"))