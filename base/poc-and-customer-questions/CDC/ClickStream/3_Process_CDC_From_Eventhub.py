# Databricks notebook source
# MAGIC %run ./0_Set_Credentials

# COMMAND ----------

import json
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Click data from Event Hub

# COMMAND ----------

click_connectionString = "Endpoint=sb://joel-eh.servicebus.windows.net/;SharedAccessKeyName=click-sas;SharedAccessKey=jxsJlIV8SKxoaTT9rvUidoiVVZtuXrhWZZdd5/ZevAA=;EntityPath=click"
click_ehConf = {}
click_ehConf['eventhubs.connectionString'] = click_connectionString

# startOffset = "-1"
startOffset = "@latest"
startingEventPosition = {
  "offset": startOffset,  
  "seqNo": -1,            #not in use
  "enqueuedTime": None,   #not in use
  "isInclusive": True
}
click_ehConf["eventhubs.startingPosition"] = json.dumps(startingEventPosition)

# COMMAND ----------

click = spark \
  .readStream \
  .format("eventhubs") \
  .options(**click_ehConf) \
  .load() \
  .selectExpr("cast (body as STRING) jsonData")

display(click)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read People Profile data from Event Hub

# COMMAND ----------

people_connectionString = "Endpoint=sb://joel-eh.servicebus.windows.net/;SharedAccessKeyName=people-sas;SharedAccessKey=tqE3mJUDmukYkKy81+RIcd1bFCl3lY6Se4Ql6QFzEyw=;EntityPath=people"
people_ehConf = {}
people_ehConf['eventhubs.connectionString'] = people_connectionString

# startOffset = "-1"
startOffset = "@latest"
startingEventPosition = {
  "offset": startOffset,  
  "seqNo": -1,            #not in use
  "enqueuedTime": None,   #not in use
  "isInclusive": True
}
people_ehConf["eventhubs.startingPosition"] = json.dumps(startingEventPosition)

# COMMAND ----------

people_profile = spark \
  .readStream \
  .format("eventhubs") \
  .options(**people_ehConf) \
  .load() \
  .selectExpr("cast (body as STRING) jsonData")

display(people_profile)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parse and Put Structure to Click and People Streams

# COMMAND ----------

click_schema = (StructType([
  StructField("exchangeID",IntegerType(),True),
  StructField("publisher",StringType(),True),
  StructField("creativeID",IntegerType(),True),
  StructField("click",StringType(),True),
  StructField("advertiserID",IntegerType(),True),
  StructField("uid",StringType(),True),
  StructField("browser",StringType(),True),
  StructField("geo",StringType(),True),
  StructField("bidAmount",DoubleType(),True),
  StructField("impTimestamp",TimestampType(),True),
  StructField("date",DateType(),True)])
)
people_cdc_schema = (StructType([
  StructField("uid",StringType(),True),
  StructField("street",StringType(),True),
  StructField("city",StringType(),True),
  StructField("state",StringType(),True),
  StructField("zipcode",StringType(),True),
  StructField("firstname",StringType(),True),
  StructField("lastname",StringType(),True),
  StructField("cdctime",TimestampType(),True)])
)

# COMMAND ----------

click_transformed = click.select(from_json("jsonData", click_schema).alias("fields")).select("fields.*")
display(click_transformed)

# COMMAND ----------

people_cdc_transformed = people_profile.select(from_json("jsonData", people_cdc_schema).alias("fields")).select("fields.*")
display(people_cdc_transformed)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Initial Delta Table for People Profile Data

# COMMAND ----------

people_delta_path = "wasbs://clickstream@joelsimpleblobstore.blob.core.windows.net/people_delta"
people_delta_checkpoint = "wasbs://clickstream@joelsimpleblobstore.blob.core.windows.net/people_delta_checkpoints"

dbutils.fs.rm(people_delta_path, True)

people_data = sqlContext.createDataFrame(sc.emptyRDD(), people_cdc_schema)
people_data.write.format("delta").save(people_delta_path)
spark.sql("CREATE TABLE IF NOT EXISTS joel.people_data USING DELTA LOCATION '"+people_delta_path+"'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Upsert CDC Stream to People Profile Data

# COMMAND ----------

#Function to upsert `microBatchOutputDF` into Delta table using MERGE
def upsertToDelta(microBatchOutputDF, batch):
  #Set the dataframe to view name
  spark.catalog.dropGlobalTempView("stream_updates")
  microBatchOutputDF.drop_duplicates(["uid"]).createGlobalTempView("stream_updates")
  #microBatchOutputDF.write.saveAsTable("joel.updates", mode='overwrite')
  
  #Use the view name to apply MERGE
  #NOTE: You have to use the SparkSession that has been used to define the `updates` dataframe
  spark.sql("""
    MERGE INTO joel.people_data t
    USING global_temp.stream_updates s
    ON s.uid = t.uid 
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
  """)

# COMMAND ----------

people_cdc_transformed.writeStream \
  .format("delta") \
  .foreachBatch(upsertToDelta) \
  .outputMode("update") \
  .option("mergeSchema", True) \
  .option("checkpointLocation", people_delta_checkpoint) \
  .start()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM joel.people_data

# COMMAND ----------

# MAGIC %md
# MAGIC ## Enrich Click Stream with Profile

# COMMAND ----------

profile_data_delta = spark.read.format("delta").load(people_delta_path)
click_with_profile = click_transformed.join(profile_data_delta, on="uid", how="left")

# COMMAND ----------

click_profile_path = "wasbs://clickstream@joelsimpleblobstore.blob.core.windows.net/click_profile_delta"
click_profile_checkpoint = "wasbs://clickstream@joelsimpleblobstore.blob.core.windows.net/click_profile_checkpoints"

click_with_profile.writeStream \
  .format("delta") \
  .outputMode("append") \
  .option("path",click_profile_path) \
  .option("checkpointLocation",click_profile_checkpoint) \
  .option("mergeSchema", True) \
  .start()

# COMMAND ----------

spark.sql("CREATE TABLE IF NOT EXISTS joel.click_with_profile USING DELTA LOCATION '"+click_profile_path+"'")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * FROM joel.click_with_profile

# COMMAND ----------

# MAGIC %sql
# MAGIC select geo, sum(bidAmount) as sum_bidAmount FROM joel.click_with_profile group by geo order by sum_bidAmount desc

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aggregate into Signal Store and Write to Delta

# COMMAND ----------

click_with_profile_stream = spark.readStream.format("delta").load(click_profile_path)

#Specific calculations / aggregations
click_with_profile_stream = click_with_profile_stream.withColumn("geo_not_equal_city", ((col("geo")!=col("city")) & (col("city").isNotNull())).cast('integer'))
signal_store = click_with_profile_stream.groupBy("uid").agg(approx_count_distinct("geo").alias("count_geo"),sum("geo_not_equal_city").alias("sum_geo_not_equal_city"))

# COMMAND ----------

display(signal_store)

# COMMAND ----------

signal_store_path = "wasbs://clickstream@joelsimpleblobstore.blob.core.windows.net/signal_store_delta"
signal_store_checkpoint = "wasbs://clickstream@joelsimpleblobstore.blob.core.windows.net/signal_store_checkpoints"

signal_store.writeStream \
  .format("delta") \
  .outputMode("complete") \
  .option("path",signal_store_path) \
  .option("checkpointLocation",signal_store_checkpoint) \
  .start()

# COMMAND ----------

spark.sql("CREATE TABLE IF NOT EXISTS joel.signal_store USING DELTA LOCATION '"+signal_store_path+"'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query Signal Store

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM joel.signal_store ORDER BY sum_geo_not_equal_city DESC limit 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM joel.click_with_profile WHERE uid="hd2714-f115-688fe-07g302036";

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optimize Delta Tables

# COMMAND ----------

  display(dbutils.fs.ls(click_profile_path))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Streaming jobs typically lead to many small files being created a.k.a. **'The Small File Problem'**
# MAGIC 
# MAGIC Delta can automatically **compact** these to significantly improve performance

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE joel.click_with_profile ZORDER BY (uid)

# COMMAND ----------

# DBTITLE 1,Delta Compacts into Larger Files...
display(dbutils.fs.ls(click_profile_path))

# COMMAND ----------

# DBTITLE 1,Significantly Increasing Performance...
# MAGIC %sql
# MAGIC SELECT * FROM joel.click_with_profile WHERE uid="hd2714-f115-688fe-07g302036";

# COMMAND ----------

