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

#display(click_raw)

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

#display(cdc_raw)

# COMMAND ----------

# DBTITLE 1,Parse JSON Data Using Defined Schema
click_transformed = click_raw.select(from_json("jsonData", click_schema).alias("fields")).select("fields.*")
cdc_transformed = cdc_raw.select(from_json("jsonData", cdc_schema).alias("fields")).select("fields.*") \
                                   .withColumnRenamed("timestamp","cdcTimestamp")

#display(cdc_transformed)

# COMMAND ----------

dbutils.fs.rm("/mnt/databricks-paul/union-data-raw", True)
dbutils.fs.rm("/mnt/databricks-paul/click-data-raw-checkpoints", True)

click_transformed.writeStream \
  .format("delta") \
  .outputMode("append") \
  .option("path","/mnt/databricks-paul/union-data-raw") \
  .option("checkpointLocation","/mnt/databricks-paul/click-data-raw-checkpoints") \
  .option("mergeSchema", True) \
  .start()

# COMMAND ----------

dbutils.fs.rm("/mnt/databricks-paul/cdc-data-raw-checkpoints", True)

cdc_transformed.writeStream \
  .format("delta") \
  .outputMode("append") \
  .option("path","/mnt/databricks-paul/union-data-raw") \
  .option("checkpointLocation","/mnt/databricks-paul/cdc-data-raw-checkpoints") \
  .option("mergeSchema", True) \
  .start()

# COMMAND ----------

# DBTITLE 1,Create initial Delta Table - CDC
dbutils.fs.rm("/mnt/databricks-paul/cdc-data", True)
schema = StructType().add("cdcTimestamp", TimestampType()) \
                    .add("uid", StringType()) \
                    .add("street", StringType()) \
                    .add ("city", StringType()) \
                    .add ("state", StringType()) \
                    .add("zipcode", StringType()) \
                    .add("firstname", StringType()) \
                    .add("lastname", StringType())
cdc_delta = sqlContext.createDataFrame(sc.emptyRDD(), schema)
cdc_delta.write.format("delta").save("/mnt/databricks-paul/cdc-data")
spark.sql("CREATE TABLE IF NOT EXISTS paul.cdc_delta USING DELTA LOCATION '/mnt/databricks-paul/cdc-data/'")

# COMMAND ----------

# DBTITLE 1,Create initial Delta Table - Click Data
dbutils.fs.rm("/mnt/databricks-paul/click-data", True)
schema = StructType().add("clickTimestamp", TimestampType()) \
                    .add("exchangeID", IntegerType()) \
                    .add("publisher", StringType()) \
                    .add("creativeID", IntegerType()) \
                    .add("click", StringType()) \
                    .add("advertiserID", IntegerType()) \
                    .add("browser", StringType()) \
                    .add("geo", StringType()) \
                    .add("bidAmount", DoubleType()) \
                    .add("uid", StringType())
click_delta = sqlContext.createDataFrame(sc.emptyRDD(), schema)
click_delta.write.format("delta").save("/mnt/databricks-paul/click-data")
spark.sql("CREATE TABLE IF NOT EXISTS paul.click_delta USING DELTA LOCATION '/mnt/databricks-paul/click-data/'")

# COMMAND ----------

dbutils.fs.rm("/mnt/databricks-paul/click-changes", True)
schema = StructType().add("clickTimestamp", TimestampType()) \
                    .add("uid", StringType())
click_delta = sqlContext.createDataFrame(sc.emptyRDD(), schema)
click_delta.write.format("delta").save("/mnt/databricks-paul/click-changes")
spark.sql("CREATE TABLE IF NOT EXISTS paul.click_changes USING DELTA LOCATION '/mnt/databricks-paul/click-changes/'")

# COMMAND ----------

#Function to upsert `microBatchOutputDF` into Delta table using MERGE
def upsertToDeltaCDC(microBatchOutputDF, batch):
  #Set the dataframe to view name
  spark.catalog.dropGlobalTempView("stream_updates_cdc")
  microBatchOutputDF.drop_duplicates(["uid"]).createGlobalTempView("stream_updates_cdc")
  #microBatchOutputDF.write.saveAsTable("paul.updates", mode='overwrite')
  
  #Use the view name to apply MERGE
  #NOTE: You have to use the SparkSession that has been used to define the `updates` dataframe
  spark.sql("""
    MERGE INTO paul.cdc_delta t
    USING global_temp.stream_updates_cdc s
    ON s.uid = t.uid
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
  """)

# COMMAND ----------

#Function to upsert `microBatchOutputDF` into Delta table using MERGE
def upsertToDeltaClick(microBatchOutputDF, batch):
  #Set the dataframe to view name
  spark.catalog.dropGlobalTempView("stream_updates_click")
  microBatchOutputDF.drop_duplicates(["uid"]).createGlobalTempView("stream_updates_click")
  #microBatchOutputDF.write.saveAsTable("paul.updates", mode='overwrite')
  
  #Use the view name to apply MERGE
  #NOTE: You have to use the SparkSession that has been used to define the `updates` dataframe
  spark.sql("""
    MERGE INTO paul.click_delta t
    USING global_temp.stream_updates_click s
    ON s.uid = t.uid
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
  """)

# COMMAND ----------

# DBTITLE 1,Upsert
cdc_transformed.writeStream \
  .format("delta") \
  .foreachBatch(upsertToDeltaCDC) \
  .outputMode("update") \
  .start()

# COMMAND ----------

click_transformed.writeStream \
  .format("delta") \
  .foreachBatch(upsertToDeltaClick) \
  .outputMode("update") \
  .start()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VIEW IF NOT EXISTS signal_union AS SELECT a.*,b.street FROM paul.click_delta a JOIN paul.cdc_delta b ON a.uid = b.uid;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM signal_union

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS paul.cdc_streams USING DELTA LOCATION "/mnt/databricks-paul/union-data-raw";
# MAGIC CREATE TABLE IF NOT EXISTS paul.click_streams USING DELTA LOCATION "/mnt/databricks-paul/union-data-raw";

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM paul.union_streams;

# COMMAND ----------

union_stream = spark.readStream.format("delta").load("/mnt/databricks-paul/union-data-raw")

# COMMAND ----------

dbutils.fs.rm("/mnt/databricks-paul/signal-union", True)
schema = StructType().add("clickTimestamp", TimestampType()) \
                    .add("exchangeID", IntegerType()) \
                    .add("publisher", StringType()) \
                    .add("creativeID", IntegerType()) \
                    .add("click", StringType()) \
                    .add("advertiserID", IntegerType()) \
                    .add("browser", StringType()) \
                    .add("geo", StringType()) \
                    .add("bidAmount", DoubleType()) \
                    .add("cdcTimestamp", TimestampType()) \
                    .add("uid", StringType()) \
                    .add("street", StringType()) \
                    .add ("city", StringType()) \
                    .add ("state", StringType()) \
                    .add("zipcode", StringType()) \
                    .add("firstname", StringType()) \
                    .add("lastname", StringType())
signal_union = sqlContext.createDataFrame(sc.emptyRDD(), schema)
signal_union.write.format("delta").save("/mnt/databricks-paul/signal-union")
spark.sql("CREATE TABLE IF NOT EXISTS paul.signal_union USING DELTA LOCATION '/mnt/databricks-paul/signal-union/'")

# COMMAND ----------

#Function to upsert `microBatchOutputDF` into Delta table using MERGE
def upsertToDelta(microBatchOutputDF, batch):
  #Set the dataframe to view name
  spark.catalog.dropGlobalTempView("stream_updates")
  microBatchOutputDF.drop_duplicates(["uid"]).createGlobalTempView("stream_updates")
  
  #Use the view name to apply MERGE
  #NOTE: You have to use the SparkSession that has been used to define the `updates` dataframe
  spark.sql("""
    MERGE INTO paul.signal_union t
    USING global_temp.stream_updates s
    ON s.uid = t.uid AND s.street is not null
    WHEN MATCHED THEN
       UPDATE SET street = s.street,
         city = s.city,
         state = s.state
    WHEN NOT MATCHED THEN INSERT (street, city, state) VALUES (s.street, s.city, s.state)
  """)

# COMMAND ----------

dbutils.fs.rm("/mnt/databricks-paul/upsert-checkpoints", True)

union_stream.writeStream \
  .format("delta") \
  .foreachBatch(upsertToDelta) \
  .outputMode("update") \
  .option("checkpointLocation","/mnt/databricks-paul/upsert-checkpoints") \
  .start()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM paul.signal_union

# COMMAND ----------

datetime_batch = "2019-04-29T20:35:38.512+0000"

changes = display(spark.sql("SELECT * FROM paul.signal_union WHERE clickTimestamp<to_date('2019-05-29T20:35:38.512+0000')"))

#changes = spark.sql("SELECT * FROM paul.signal_union WHERE clickTimestamp<to_date(" + datetime_batch + ")")

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
  .trigger(once=True) \
  .start()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM paul.profile_data

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