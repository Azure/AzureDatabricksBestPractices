# Databricks notebook source
# MAGIC %run ./0_Set_Credentials

# COMMAND ----------

from pyspark.sql.types  import *
from pyspark.sql.functions import to_json, struct, current_timestamp
import json

# COMMAND ----------

# MAGIC %md
# MAGIC ## Send Click data to Event Hub

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

# COMMAND ----------

# Remove checkpoint so that we can emulate resending same data
click_checkpoint = "wasbs://clickstream@joelsimpleblobstore.blob.core.windows.net/click_checkpoint"
dbutils.fs.rm(click_checkpoint, True)

# COMMAND ----------

click = spark.readStream.schema(click_schema).option("maxFilesPerTrigger", 1).parquet("wasbs://clickstream@joelsimpleblobstore.blob.core.windows.net/clicks")
display(click)

# COMMAND ----------

click_connectionString = "Endpoint=sb://joel-eh.servicebus.windows.net/;SharedAccessKeyName=click-sas;SharedAccessKey=jxsJlIV8SKxoaTT9rvUidoiVVZtuXrhWZZdd5/ZevAA=;EntityPath=click"
click_ehWriteConf = {}
click_ehWriteConf['eventhubs.connectionString'] = click_connectionString

click_ds = click \
  .select(to_json(struct([click[x] for x in click.columns])).alias("body")) \
  .writeStream \
  .format("eventhubs") \
  .options(**click_ehWriteConf) \
  .option("checkpointLocation", click_checkpoint) \
  .start()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Send People Profile data to Event Hub

# COMMAND ----------

people_profile_schema = (StructType([
  StructField("uid",StringType(),True),
  StructField("street",StringType(),True),
  StructField("city",StringType(),True),
  StructField("state",StringType(),True),
  StructField("zipcode",StringType(),True),
  StructField("firstname",StringType(),True),
  StructField("lastname",StringType(),True)])
)

# COMMAND ----------

# Remove checkpoint so that we can emulate resending same data
people_checkpoint = "wasbs://clickstream@joelsimpleblobstore.blob.core.windows.net/people_profile_checkpoint"
dbutils.fs.rm(people_checkpoint, True)

# COMMAND ----------

people_profile = spark.readStream.schema(people_profile_schema).option("maxFilesPerTrigger", 1).parquet("wasbs://clickstream@joelsimpleblobstore.blob.core.windows.net/people_profiles")
people_profile = people_profile.withColumn('cdctime',current_timestamp())

display(people_profile)

# COMMAND ----------

people_connectionString = "Endpoint=sb://joel-eh.servicebus.windows.net/;SharedAccessKeyName=people-sas;SharedAccessKey=tqE3mJUDmukYkKy81+RIcd1bFCl3lY6Se4Ql6QFzEyw=;EntityPath=people"
people_ehWriteConf = {}
people_ehWriteConf['eventhubs.connectionString'] = people_connectionString

people_profile = people_profile \
  .select(to_json(struct([people_profile[x] for x in people_profile.columns])).alias("body")) \
  .writeStream \
  .format("eventhubs") \
  .options(**people_ehWriteConf) \
  .option("checkpointLocation", people_checkpoint) \
  .start()

# COMMAND ----------

