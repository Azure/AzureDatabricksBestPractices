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
# MAGIC # Structured Streaming with Kafka Lab
# MAGIC 
# MAGIC ## Instructions
# MAGIC * Insert solutions wherever it says `FILL_IN`
# MAGIC * Feel free to copy/paste code from the previous notebook, where applicable
# MAGIC * Run test cells to verify that your solution is correct

# COMMAND ----------

# MAGIC %md
# MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Getting Started</h2>
# MAGIC 
# MAGIC Run the following cell to configure our "classroom."

# COMMAND ----------

# MAGIC %run "../Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Exercise 1: Use Kafka to read a stream</h2>
# MAGIC 
# MAGIC In this example, we are looking at a series of `ERROR`, `WARNING` and `INFO` log messages that are coming in via our Kafka server.
# MAGIC 
# MAGIC We want to analyze how many log messages are coming from each IP address?
# MAGIC 
# MAGIC Create `initialDF` with the following Kafka parameters:
# MAGIC 
# MAGIC 0. `format` is `kafka`
# MAGIC 0. `kafka.bootstrap.server` (pick the server closest to you)
# MAGIC   * is `server1.databricks.training:9092` US (Oregon)
# MAGIC   * or `server2.databricks.training:9092` Singapore
# MAGIC 0. `subscribe` is `logdata`
# MAGIC 
# MAGIC When you are done, run the TEST cell that follows to verify your results.

# COMMAND ----------

# ANSWER
spark.conf.set("spark.sql.shuffle.partitions", sc.defaultParallelism)

kafkaServer = "server1.databricks.training:9092"   # US (Oregon)
# kafkaServer = "server2.databricks.training:9092" # Singapore

# Create our initial DataFrame
initialDF = (spark.readStream
  .format("kafka")                                                             # Specify "kafka" as the type of the stream
  .option("kafka.bootstrap.servers", kafkaServer)                              # Set the location of the kafka serve
  .option("subscribe", "logdata")                                              # Indicate which topics to listen to
  .option("startingOffsets", "earliest")                                       # Rewind stream to beginning when we restart notebook
  .option("maxOffsetsPerTrigger", 1000)                                        # Throttle Kafka's processing of the streams
  .load()                                                                      # Load the input data stream in as a DataFrame
)

# COMMAND ----------

# TEST - Run this cell to test your solution.
initSchemaStr = str(initialDF.schema)

dbTest("SS-04-key",       True, "(key,BinaryType,true)" in initSchemaStr)
dbTest("SS-04-value",     True, "(value,BinaryType,true)" in initSchemaStr)
dbTest("SS-04-topic",     True, "(topic,StringType,true)" in initSchemaStr)
dbTest("SS-04-partition", True, "(partition,IntegerType,true)" in initSchemaStr)
dbTest("SS-04-offset",    True, "(offset,LongType,true)" in initSchemaStr)
dbTest("SS-04-timestamp", True, "(timestamp,TimestampType,true)" in initSchemaStr)
dbTest("SS-04-timestampType", True, "(timestampType,IntegerType,true)" in initSchemaStr)

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Exercise 2: Do Some ETL Processing</h2>
# MAGIC 
# MAGIC Perform the following ETL steps:
# MAGIC 
# MAGIC 0. Cast `value` column to STRING
# MAGIC 0. `ts_string` is derived from `value` at positions 14 to 24,
# MAGIC 0. `epoc` is derived from `unix_timestamp` of `ts_string` using format "yyyy/MM/dd HH:mm:ss.SSS"
# MAGIC 0. `capturedAt` is derived from casting `epoc` to `timestamp` format
# MAGIC 0. `logData` is created by applying `regexp_extract` on `value`.. use this string `"""^.*\]\s+(.*)$"""`
# MAGIC 
# MAGIC When you are done, run the TEST cell that follows to verify your results.

# COMMAND ----------

# ANSWER
from pyspark.sql.functions import col, unix_timestamp, regexp_extract

cleanDF = (initialDF
  .withColumn("value", col("value").cast("STRING"))                            # cast "value" column to STRING
  .withColumn("ts_string", col("value").substr(14, 24))                        # Select the "value" column, pull substring(2,23) from it and rename to "ts_string"
  .withColumn("epoc", unix_timestamp("ts_string", "yyyy/MM/dd HH:mm:ss.SSS"))  # Select the "ts_string" column, apply unix_timestamp to it and rename to "epoc"
  .withColumn("capturedAt", col("epoc").cast("timestamp"))                     # Select the "epoc" column and cast to a timestamp and rename it to "capturedAt"               
  .withColumn("logData", regexp_extract("value", """^.*\]\s+(.*)$""", 1))      # Select the "logData" column and apply the regexp `"""^.*\]\s+(.*)$"""`
)

# COMMAND ----------

# TEST - Run this cell to test your solution.
schemaStr = str(cleanDF.schema)

dbTest("SS-04-schema-value",     True, "(value,StringType,true)" in schemaStr)
dbTest("SS-04-schema-ts_string",  True, "(ts_string,StringType,true)" in schemaStr)
dbTest("SS-04-schema-epoc",   True, "(epoc,LongType,true)" in schemaStr)
dbTest("SS-04-schema-capturedAt", True, "(capturedAt,TimestampType,true)" in schemaStr)
dbTest("SS-04-schema-logData",  True, "(logData,StringType,true)" in schemaStr)

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Exercise 3: Classify and Count IP Addresses Over a 10s Window</h2>
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

# ANSWER
from pyspark.sql.functions import col,length, window, when

# This is the regular expression pattern that we will use 
IP_REG_EX = """^.*\s+(\d{1,3})\.\d{1,3}\.\d{1,3}\.\d{1,3}.*$"""

ipDF = (cleanDF
  .withColumn("ip", regexp_extract(col("logData"), IP_REG_EX, 1))  # apply regexp_extract on IP_REG_EX with value of 1 to "logData" and rename it "ip"
  .filter(length(col("ip"))> 0)                                    # keep only "ip" that have non-zero length
  .withColumn("ipClass"                                            # figure out class of IP address based on first two octets
     ,when(col("ip") < 127, "Class A")
     .when(col("ip") == 127, "Loopback")
     .when((col("ip") > 127) & (col("ip") < 192), "Class B")
     .when((col("ip") >= 192) & (col("ip") < 224), "Class C")
     .when((col("ip") >= 224) & (col("ip") < 240), "Class D")
     .when((col("ip") >= 240) & (col("ip") < 256), "Class C")
     .otherwise("Invalid"))
  .groupBy( window(col("capturedAt"), "10 seconds").alias("time"), col("ipClass"))    # form 10 second windows of "capturedAt", call them "time" 
  .count()                                                          # add up total
  .orderBy("ipClass")                                               # sort by IP class
)

# COMMAND ----------

# TEST - Run this cell to test your solution.
schemaStr = str(ipDF.schema)

dbTest("SS-04-schema-ipClass", True, "(ipClass,StringType,false)" in schemaStr)
dbTest("SS-04-schema-count",   True, "(count,LongType,false)" in schemaStr)
dbTest("SS-04-schema-start",   True, "(start,TimestampType,true)" in schemaStr)
dbTest("SS-04-schema-end",     True, "(end,TimestampType,true)" in schemaStr)

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Exercise 4: Display a LIVE Plot</h2>
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

# COMMAND ----------

# ANSWER
display(ipDF)

# COMMAND ----------

# TEST - Run this cell to test your solution.
dbTest("SS-04-numActiveStreams", True, len(spark.streams.active) > 0)

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC Wait until stream is done initializing...

# COMMAND ----------

untilStreamIsReady("delta_1p")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Exercise 5: Stop streaming jobs</h2>
# MAGIC 
# MAGIC Before we can conclude, we need to shut down all active streams.

# COMMAND ----------

# ANSWER
for s in spark.streams.active: # Iterate over all the active streams
  s.stop()                     # Stop the stream

# COMMAND ----------

# TEST - Run this cell to test your solution.
dbTest("SS-04-numActiveStreams", 0, len(spark.streams.active))

print("Tests passed!")

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