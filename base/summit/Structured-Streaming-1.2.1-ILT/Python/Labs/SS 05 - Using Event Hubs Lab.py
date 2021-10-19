# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <img src="https://files.training.databricks.com/images/Apache-Spark-Logo_TM_200px.png" style="float: left: margin: 20px"/>
# MAGIC 
# MAGIC # Structured Streaming with Azure Event Hubs Lab 
# MAGIC 
# MAGIC ## Instructions
# MAGIC 
# MAGIC * Insert solutions wherever it says `FILL_IN`
# MAGIC * Feel free to copy/paste code from the previous notebook, where applicable
# MAGIC * Run test cells to verify that your solution is correct
# MAGIC 
# MAGIC ## Library Requirements
# MAGIC 
# MAGIC 1. the Maven library with coordinate `com.microsoft.azure:azure-eventhubs-spark_2.11:2.3.7`
# MAGIC    - this allows Databricks `spark` session to communicate with an Event Hub
# MAGIC 2. the Python library `azure-eventhub`
# MAGIC    - this is allows the Python kernel to stream content to an Event Hub
# MAGIC 3. the Python library `sseclient`
# MAGIC    - this is used to create a streaming client to an existing streaming server
# MAGIC 
# MAGIC Documentation on how to install Python libraries:
# MAGIC https://docs.azuredatabricks.net/user-guide/libraries.html#pypi-libraries
# MAGIC 
# MAGIC Documentation on how to install Maven libraries:
# MAGIC https://docs.azuredatabricks.net/user-guide/libraries.html#maven-or-spark-package

# COMMAND ----------

# MAGIC %md
# MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Getting Started</h2>
# MAGIC 
# MAGIC Run the following cell to configure our "classroom."

# COMMAND ----------

# MAGIC %run "../Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC Initialize widgets at the top.

# COMMAND ----------

dbutils.widgets.text("CONNECTION_STRING", "", "Connection String")
dbutils.widgets.text("EVENT_HUB_NAME", "fake-log-server", "Event Hub")

# COMMAND ----------

# To remove widgets at the top, uncomment the line below
dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Exercise 1: Configure Event Hubs</h2>
# MAGIC 
# MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> The connection string will be provided to you by your instructor.
# MAGIC 
# MAGIC The function below reads connection strings that you've entered in those widget boxes at the top.
# MAGIC 
# MAGIC It also ensures that you do not have empty connection strings.

# COMMAND ----------

pcString = dbutils.widgets.get("CONNECTION_STRING")
uniqueEHName = dbutils.widgets.get("EVENT_HUB_NAME")

# check to make sure it is not an empty string
assert pcString != "", ": The Primary Connection String must be non-empty"
assert uniqueEHName != "", ": The Unique Event Hubs Name must be non-empty"

fullPCString = pcString.replace(".net/;", ".net/{}/;".format(uniqueEHName))
connectionString = "{};EntityPath={}".format(pcString, uniqueEHName)

# COMMAND ----------

# MAGIC %md
# MAGIC Assemble the following:
# MAGIC * A `startingEventPosition` as a JSON string
# MAGIC * An `EventHubsConf` 
# MAGIC   * to include a string with connection credentials
# MAGIC   * to set a starting position for the stream read
# MAGIC   * to throttle Event Hubs' processing of the streams

# COMMAND ----------

# TODO
import json

Create the starting position Dictionary as a JSON string
startingEventPosition = {
 "offset": "-1",         # Set "offset" to "-1"
 "seqNo": -1,            # Set "seqNo" to -1
 "enqueuedTime": None,   # Set "equeuedTime" to None
 "isInclusive": True     # Set "isInclusive" to True
}

eventHubsConf  = {
 FILL_IN : FILL_IN,  # Pass connectionString parameter to EventHubsConf object
 FILL_IN : FILL_IN,  # Define starting position from start of stream as a JSON string
 FILL_IN : FILL_IN   # Throttle Event Hubs' processing of the streams to, say, 100
}

# COMMAND ----------

# TEST - Run this cell to test your solution.
import re
csRegex = "Endpoint.*SharedAccessKeyName.*"

dbTest("SS-05-partition",     True, re.match(csRegex, connectionString) != None)

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Exercise 2: Use Event Hubs to Read a Stream</h2>
# MAGIC 
# MAGIC In this example, we are looking at a series of `ERROR`, `WARNING` and `INFO` log messages that are coming in.
# MAGIC 
# MAGIC We want to analyze how many log messages are coming from each IP address?
# MAGIC 
# MAGIC Create `initialDF` with the following Event Hubs parameters:
# MAGIC 
# MAGIC When you are done, run the TEST cell that follows to verify your results.

# COMMAND ----------

# TODO
from pyspark.sql.functions import col

spark.conf.set("spark.sql.shuffle.partitions", sc.defaultParallelism)

initialDF = (spark.readStream     # Get the DataStreamReader
  .FILL_IN                        # Specify the source format as "eventhubs"
  .FILL_IN                        # Event Hubs options as a map
  .FILL_IN                        # Load the DataFrame
)

# COMMAND ----------

# TEST - Run this cell to test your solution.
initSchemaStr = str(initialDF.schema)

dbTest("SS-05-partition",        True, "(partition,StringType,true)" in initSchemaStr)
dbTest("SS-05-offset",           True, "(offset,StringType,true)" in initSchemaStr)
dbTest("SS-05-sequenceNumber",   True, "(sequenceNumber,LongType,true)" in initSchemaStr)
dbTest("SS-05-enqueuedTime",     True, "(enqueuedTime,TimestampType,true)" in initSchemaStr)
dbTest("SS-05-publisher",        True, "(publisher,StringType,true)" in initSchemaStr)
dbTest("SS-05-partitionKey",     True, "(partitionKey,StringType,true)" in initSchemaStr)
dbTest("SS-05-properties",       True, "(properties,MapType(StringType,StringType,true),true)" in initSchemaStr)
dbTest("SS-05-systemProperties", True, "(systemProperties,MapType(StringType,StringType,true),true)" in initSchemaStr)

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Exercise 3: Do Some ETL Processing</h2>
# MAGIC 
# MAGIC Perform the following ETL steps:
# MAGIC 
# MAGIC 0. Cast `value` column to STRING
# MAGIC 0. `ts_string` is derived from `value` at positions 14 to 24,
# MAGIC 0. `epoc` is derived from `unix_timestamp` of `ts_string` using format "yyyy/MM/dd HH:mm:ss.SSS"
# MAGIC 0. `capturedAt` is derived from casting `epoc` to `timestamp` format
# MAGIC 0. `logData` is created by applying `regexp_extract` on `value`.. use this string `"""^.*\]\s+(.*)$"""`
# MAGIC 
# MAGIC For additional information on`regexp_extract()`, `unix_timestamp()` or any other functions see:
# MAGIC 
# MAGIC <a href="http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.functions$" target="_blank">org.apache.spark.sql.functions</a>
# MAGIC 
# MAGIC When you are done, run the TEST cell that follows to verify your results.

# COMMAND ----------

# TODO
from pyspark.sql.functions import col, unix_timestamp, regexp_extract, substring

cleanDF = (initialDF
 .withColumn(FILL_IN)  # Cast "body" column to STRING
 .withColumn(FILL_IN)  # Select the "body" column, pull substring(2,23) from it and rename to "ts_string"
 .withColumn(FILL_IN)  # Select the "ts_string" column, apply unix_timestamp to it and rename to "epoc"
 .withColumn(FILL_IN)  # Select the "epoc" column and cast to a timestamp and rename it to "capturedAt"
 .withColumn(FILL_IN)  # Select the "body" column and apply the regexp `"""^.*\]\s+(.*)$"""` and rename to "logData"
)

# COMMAND ----------

# TEST - Run this cell to test your solution.
schemaStr = str(cleanDF.schema)

dbTest("SS-05-schema-body",       True, "(body,StringType,true)" in schemaStr)
dbTest("SS-05-schema-ts_string",  True, "(ts_string,StringType,true)" in schemaStr)
dbTest("SS-05-schema-epoc",       True, "(epoc,LongType,true)" in schemaStr)
dbTest("SS-05-schema-capturedAt", True, "(capturedAt,TimestampType,true" in schemaStr)
dbTest("SS-05-schema-logData",    True, "(logData,StringType,true)" in schemaStr)

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Exercise 4: Classify and Count IP Addresses Over a 10s Window</h2>
# MAGIC 
# MAGIC To solve this problem, you need to:
# MAGIC 
# MAGIC 0. Parse the first part of an IP address from the column `logData` with the `regexp_extract()` function
# MAGIC   * You will need a regular expression which we have already provided below as `IP_REG_EX`
# MAGIC   * Hint: take the 1st matching value
# MAGIC 0. Filter out the records that don't contain IP addresses
# MAGIC 0. Form another column called `ipClass` that classifies IP addresses based on the first part of an IP address 
# MAGIC   * 1 to 126: "Class A"
# MAGIC   * 127: "Loopback"
# MAGIC   * 128 to 191: "Class B"
# MAGIC   * 192 to 223: "Class C"
# MAGIC   * 224 to 239: "Class D"
# MAGIC   * 240 to 256: "Class E"
# MAGIC   * anything else is invalid
# MAGIC 0. Perform an aggregation over a window of time, grouping by the `capturedAt` window and `ipClass`
# MAGIC   * For this lab, use a 10-second window
# MAGIC 0. Count the number of IP values that belong to a specific `ipClass`
# MAGIC 0. Sort by `ipClass`

# COMMAND ----------

# TODO
from pyspark.sql.functions import col,length, window, when

This is the regular expression pattern that we will use 
IP_REG_EX = """^.*\s+(\d{1,3})\.\d{1,3}\.\d{1,3}\.\d{1,3}.*$"""

ipDF = (cleanDF
 .withColumn(FILL_IN)                                # apply regexp_extract on IP_REG_EX with value of 1 to "logData" and rename it "ip"
 .FILL_IN                                            # keep only "ip" that have non-zero length
 .withColumn("ipClass", when(FILL_IN)                # figure out class of IP address based on first two octets
    .FILL_IN                                         # add rest of when/otherwise clauses
 .groupBy(window(FILL_IN), FILL_IN)                  # gather in 10 second windows of "capturedAt", call them "time" and "ipClass" 
 .FILL_IN                                            # add up total
 .FILL_IN                                            # sort by IP class

# COMMAND ----------

# TEST - Run this cell to test your solution.
schemaStr = str(ipDF.schema)

dbTest("SS-05-schema-ipClass", True, "(ipClass,StringType,false)" in schemaStr)
dbTest("SS-05-schema-count",   True, "(count,LongType,false)" in schemaStr)
dbTest("SS-05-schema-start",   True, "(start,TimestampType,true)" in schemaStr)
dbTest("SS-05-schema-end",     True, "(end,TimestampType,true)" in schemaStr)

print("Tests passed!")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Exercise 5: Display a LIVE Plot</h2>
# MAGIC 
# MAGIC The `DataFrame` that you pass to `display()` should have three columns:
# MAGIC 
# MAGIC * `time`: The time window structure
# MAGIC * `ipClass`: The class the first part of the IP address belongs to
# MAGIC * `count`: The number of times that said class of IP address appeared in the window
# MAGIC 
# MAGIC Under <b>Plot Options</b>, use the following:
# MAGIC * <b>Keys:</b> `ipClass`
# MAGIC * <b>Values:</b> `count`
# MAGIC 
# MAGIC <b>Display type:</b> is 
# MAGIC * <b>Pie Chart</b>
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/eLearning/Structured-Streaming/plot-options-pie.png"/>
# MAGIC 
# MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> Make sure the Fake Log stream server is running at this point.

# COMMAND ----------

# TODO
display(FILL_IN)

# COMMAND ----------

# TEST - Run this cell to test your solution.
dbTest("SS-05-numActiveStreams", True, len(spark.streams.active) > 0)

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC Wait until stream is done initializing...

# COMMAND ----------

untilStreamIsReady("SS05-ipDF-p")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Exercise 6: Stop streaming jobs</h2>
# MAGIC 
# MAGIC Before we can conclude, we need to shut down all active streams.

# COMMAND ----------

# TODO
FILL_IN  # Iterate over all the active streams
FILL_IN  # Stop the stream

# COMMAND ----------

# TEST - Run this cell to test your solution.
dbTest("SS-05-numActiveStreams", 0, len(spark.streams.active))

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC Clean up widgets!

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Next Steps</h2>
# MAGIC 
# MAGIC Start the next lesson, [Twitter Capstone]($../SS 99 - Twitter Capstone).

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>