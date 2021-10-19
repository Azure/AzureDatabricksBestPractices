# Databricks notebook source
import sys
import logging
import datetime
import time
import os

from azure.eventhub import EventHubClient, Sender, EventData

logger = logging.getLogger("azure")

# Address can be in either of these formats:
# "amqps://<URL-encoded-SAS-policy>:<URL-encoded-SAS-key>@<mynamespace>.servicebus.windows.net/myeventhub"
# "amqps://<mynamespace>.servicebus.windows.net/myeventhub"
# For example:
ADDRESS = "amqps://joel-eh.servicebus.windows.net/test"

# SAS policy and key are not required if they are encoded in the URL
USER = "test-sa"
KEY = "kfLkdN7eHhJe+9lS1lzCaOpM52dmmh5ybkdJ+wiT334="

connectionString = "Endpoint=sb://joel-eh.servicebus.windows.net/;SharedAccessKeyName=test-sa;SharedAccessKey=kfLkdN7eHhJe+9lS1lzCaOpM52dmmh5ybkdJ+wiT334=;EntityPath=test"

# COMMAND ----------

# Create Event Hubs client
client = EventHubClient(ADDRESS, debug=False, username=USER, password=KEY)
sender = client.add_sender(partition="0")
client.run()
try:
    start_time = time.time()
    for i in range(100):
        print("Sending message: {}".format(i))
        sender.send(EventData(str(i)))
except:
    raise
finally:
    end_time = time.time()
    client.stop()
    run_time = end_time - start_time
    logger.info("Runtime: {} seconds".format(run_time))

# COMMAND ----------

storage_account_name = "joelsimpleblobstore"
storage_account_access_key = "XuKm58WHURoiBqbxGuKFvLDQwDu4ctbjNgphY14qhRsUmsiFMdBmw2UbIIBYQvawsydVSiqZVaRPaWv/v2l/7g=="

# COMMAND ----------

spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
  storage_account_access_key)

# COMMAND ----------

from pyspark.sql.types  import *
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

click = spark.readStream.schema(click_schema).option("maxFilesPerTrigger", 1).parquet("wasbs://clickstream@joelsimpleblobstore.blob.core.windows.net/clicks")

# COMMAND ----------

display(click)

# COMMAND ----------

from pyspark.sql.functions import to_json, struct

# COMMAND ----------

ehWriteConf = {}
ehWriteConf['eventhubs.connectionString'] = connectionString

ds = click \
  .select(to_json(struct([click[x] for x in click.columns])).alias("body")) \
  .writeStream \
  .format("eventhubs") \
  .options(**ehWriteConf) \
  .option("checkpointLocation", "wasbs://clickstream@joelsimpleblobstore.blob.core.windows.net/checkpoint_tmp2") \
  .start()

# COMMAND ----------

import json

# COMMAND ----------

ehConf = {}
ehConf['eventhubs.connectionString'] = connectionString

# startOffset = "-1"
startOffset = "@latest"
startingEventPosition = {
  "offset": startOffset,  
  "seqNo": -1,            #not in use
  "enqueuedTime": None,   #not in use
  "isInclusive": True
}
ehConf["eventhubs.startingPosition"] = json.dumps(startingEventPosition)

# COMMAND ----------

readdf = spark \
  .readStream \
  .format("eventhubs") \
  .options(**ehConf) \
  .load()

# COMMAND ----------

display(readdf)

# COMMAND ----------

readdf_raw = readdf.selectExpr("cast (body as STRING) jsonData")

display(readdf_raw)

# COMMAND ----------

