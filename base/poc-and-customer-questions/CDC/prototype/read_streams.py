# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %run ./creds_python

# COMMAND ----------

# DBTITLE 1,Read Stream 1: User Click Stream From Kenesis
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

# DBTITLE 1,View Raw Click Stream Data
click_raw = click.selectExpr("cast (data as STRING) jsonData")

display(click_raw)

# COMMAND ----------

# DBTITLE 1,Read Stream 2: User Profile CDC Stream From Kenesis
from pyspark.sql.types import *

cdc_schema= StructType() \
          .add("timestamp", TimestampType()) \
          .add("uid", StringType()) \
          .add("street", StringType()) \
          .add ("city", StringType()) \
          .add ("state", StringType()) \
          .add("zipcode", StringType()) \
          .add("firstname", StringType()) \
          .add("lastname", StringType())
          
cdc = spark \
  .readStream \
  .format("kinesis") \
  .option("streamName", "ecommerce-profile") \
  .option("initialPosition", "latest") \
  .option("region", kinesisRegion) \
  .option("awsAccessKey", awsAccessKeyId) \
  .option("awsSecretKey", awsSecretKey) \
  .load()


# COMMAND ----------

# DBTITLE 1,View Raw CDC Stream Data
cdc_raw = cdc.selectExpr("cast (data as STRING) jsonData")

display(cdc_raw)

# COMMAND ----------

# DBTITLE 1,Parse JSON Data Using Defined Schema
click_transformed = click_raw.select(from_json("jsonData", click_schema).alias("fields")).select("fields.*")
cdc_transformed = cdc_raw.select(from_json("jsonData", cdc_schema).alias("fields")).select("fields.*") \
                                   .withColumnRenamed("timestamp","cdcTimestamp")

display(click_transformed)

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ###[Back to Diagram](https://demo.cloud.databricks.com/#notebook/2504942/command/2504945)

# COMMAND ----------

# DBTITLE 1,Create Initial Delta Table for User Profile Data
dbutils.fs.rm("/mnt/databricks-paul/profile-data", True)
schema = StructType().add("cdcTimestamp", TimestampType()) \
                    .add("uid", StringType()) \
                    .add("street", StringType()) \
                    .add ("city", StringType()) \
                    .add ("state", StringType()) \
                    .add("zipcode", StringType()) \
                    .add("firstname", StringType()) \
                    .add("lastname", StringType())
profile_data = sqlContext.createDataFrame(sc.emptyRDD(), schema)
profile_data.write.format("delta").save("/mnt/databricks-paul/profile-data")
spark.sql("CREATE TABLE IF NOT EXISTS paul.profile_data USING DELTA LOCATION '/mnt/databricks-paul/profile-data/'")

# COMMAND ----------

# DBTITLE 1,Upsert CDC Stream to User Profile Delta Table
#Function to upsert `microBatchOutputDF` into Delta table using MERGE
def upsertToDelta(microBatchOutputDF, batch):
  #Set the dataframe to view name
  spark.catalog.dropGlobalTempView("stream_updates")
  microBatchOutputDF.drop_duplicates(["uid"]).createGlobalTempView("stream_updates")
  #microBatchOutputDF.write.saveAsTable("paul.updates", mode='overwrite')
  
  #Use the view name to apply MERGE
  #NOTE: You have to use the SparkSession that has been used to define the `updates` dataframe
  spark.sql("""
    MERGE INTO paul.profile_data t
    USING global_temp.stream_updates s
    ON s.uid = t.uid
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
  """)

# COMMAND ----------

cdc_transformed.writeStream \
  .format("delta") \
  .foreachBatch(upsertToDelta) \
  .outputMode("update") \
  .option("mergeSchema", True) \
  .option("checkpointLocation","/mnt/databricks-paul/cdc_transformed-checkpoints") \
  .start()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM paul.profile_data

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ###[Back to Diagram](https://demo.cloud.databricks.com/#notebook/2504942/command/2504945)

# COMMAND ----------

# DBTITLE 1,Enrich Click Stream with Profile
profile_data_delta = spark.read.format("delta").load("/mnt/databricks-paul/profile-data/")
click_with_profile = click_transformed.join(profile_data_delta, on="uid", how="left")

# COMMAND ----------

click_with_profile.writeStream \
  .format("delta") \
  .outputMode("append") \
  .option("path","/mnt/databricks-paul/click_with_profile") \
  .option("checkpointLocation","/mnt/databricks-paul/click_with_profile-checkpoints") \
  .option("mergeSchema", True) \
  .start()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS paul.click_with_profile USING DELTA LOCATION "/mnt/databricks-paul/click_with_profile";

# COMMAND ----------

# MAGIC %sql
# MAGIC select * FROM paul.click_with_profile

# COMMAND ----------

# MAGIC %sql
# MAGIC select geo, sum(bidAmount) as sum_bidAmount FROM paul.click_with_profile group by geo order by sum_bidAmount desc

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ###[Back to Diagram](https://demo.cloud.databricks.com/#notebook/2504942/command/2504945)

# COMMAND ----------

# DBTITLE 1,Aggregate into Signal Store
click_with_profile_stream = spark.readStream.format("delta").load("/mnt/databricks-paul/click_with_profile")

#Specific calculations / aggregations
click_with_profile_stream = click_with_profile_stream.withColumn("geo_not_equal_city", ((col("geo")!=col("city")) & (col("city").isNotNull())).cast('integer'))
signal_store = click_with_profile_stream.groupBy("uid").agg(approx_count_distinct("geo").alias("count_geo"),sum("geo_not_equal_city").alias("sum_geo_not_equal_city"))

# COMMAND ----------

display(signal_store)

# COMMAND ----------

# DBTITLE 1,Write into Delta Table
signal_store.writeStream \
  .format("delta") \
  .outputMode("complete") \
  .option("path","/mnt/databricks-paul/signal_store") \
  .option("checkpointLocation","/mnt/databricks-paul/signal_store-checkpoints") \
  .start()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS paul.signal_store USING DELTA LOCATION "/mnt/databricks-paul/signal_store";

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ###[Back to Diagram](https://demo.cloud.databricks.com/#notebook/2504942/command/2504945)

# COMMAND ----------

# DBTITLE 1,Query Signal Store
# MAGIC %sql
# MAGIC SELECT * FROM paul.signal_store ORDER BY sum_geo_not_equal_city DESC limit 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM paul.click_with_profile WHERE uid="hd2714-f115-688fe-07g302036";

# COMMAND ----------

# DBTITLE 1,Optimize Delta Tables
display(dbutils.fs.ls("/mnt/databricks-paul/click_with_profile/"))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Streaming jobs typically lead to many small files being created a.k.a. **'The Small File Problem'**
# MAGIC 
# MAGIC Delta can automatically **compact** these to significantly improve performance

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE paul.click_with_profile ZORDER BY (uid)

# COMMAND ----------

# DBTITLE 1,Delta Compacts into Larger Files...
display(dbutils.fs.ls("/mnt/databricks-paul/click_with_profile/"))

# COMMAND ----------

# DBTITLE 1,Significantly Increasing Performance...
# MAGIC %sql
# MAGIC SELECT * FROM paul.click_with_profile WHERE uid="hd2714-f115-688fe-07g302036";

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ###[Back to Diagram](https://demo.cloud.databricks.com/#notebook/2504942/command/2504945)