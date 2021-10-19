// Databricks notebook source
// MAGIC 
// MAGIC %md-sandbox
// MAGIC 
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC # Gain Actionable Insights from a Data Lake, Satisfy GDPR
// MAGIC 
// MAGIC In this capstone project, use Databricks Delta to manage a data lake consisting of a lot of historical data plus incoming streaming data.
// MAGIC 
// MAGIC A video gaming company stores historical data in a data lake, which is growing exponentially. 
// MAGIC 
// MAGIC The data isn't sorted in any particular way (actually, it's quite a mess).
// MAGIC 
// MAGIC It is proving to be _very_ difficult to query and manage this data because there is so much of it.
// MAGIC 
// MAGIC To further complicate issues, a regulatory agency has decreed you be able to identify and delete all data associated with a specific user (i.e. GDPR). 
// MAGIC 
// MAGIC In other words, you must delete data associated with a specific `deviceId`.
// MAGIC 
// MAGIC ## Audience
// MAGIC * Primary Audience: Data Engineers
// MAGIC * Additional Audiences: Data Analysts and Data Scientists
// MAGIC 
// MAGIC ## Prerequisites
// MAGIC * Web browser: current versions of Google Chrome, Firefox, Safari, Microsoft Edge and 
// MAGIC Internet Explorer 11 on Windows 7, 8, or 10 (see <a href="https://docs.databricks.com/user-guide/supported-browsers.html#supported-browsers#" target="_blank">Supported Web Browsers</a>)
// MAGIC * Databricks Runtime 4.2 or greater
// MAGIC * Completed courses Spark-SQL, DataFrames or ETL-Part 1 from <a href="https://academy.databricks.com/" target="_blank">Databricks Academy</a>, or have similar knowledge
// MAGIC * Have done the rest of this course
// MAGIC 
// MAGIC ## Instructions
// MAGIC 0. Read in streaming data into Databricks Delta raw tables
// MAGIC 0. Create Databricks Delta query table
// MAGIC 0. Compute aggregate statistics about data i.e. create summary table
// MAGIC 0. Identify events associated with specific `deviceId` 
// MAGIC 0. Do data cleanup using Databricks Delta advanced features
// MAGIC 
// MAGIC ## CAUTION
// MAGIC * Do not use <b>RunAll</b> mode (next to <b>Permissions</b>). 

// COMMAND ----------

// MAGIC %md
// MAGIC ### Getting Started
// MAGIC 
// MAGIC Run the following cell to configure our "classroom."

// COMMAND ----------

// MAGIC %run "./Includes/Classroom-Setup"

// COMMAND ----------

// MAGIC %md
// MAGIC Set up relevant paths.

// COMMAND ----------

val inputPath = "/mnt/training/gaming_data/mobile_streaming_events_b"

val basePath         = userhome + "/delta/scala"
val outputPathBronze = basePath + "/gaming/bronze.delta"
val outputPathSilver = basePath + "/gaming/silver.delta"
val outputPathGold   = basePath + "/gaming/gold.delta"
val checkpointPath   = basePath + "/08/checkpoints"

// Configure our shuffle partitions for these exercises
spark.conf.set("spark.sql.shuffle.partitions", 8)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Step 1: Prepare Schema and Read Streaming Data from input source
// MAGIC 
// MAGIC The input source is a folder containing files of around 100,000 bytes each and is set up to stream slowly.
// MAGIC 
// MAGIC Run this code to read streaming data in.

// COMMAND ----------

import org.apache.spark.sql.types.{StructType, StructField, StringType, DoubleType, TimestampType, IntegerType}

lazy val eventSchema = StructType(List(
  StructField("eventName", StringType, true),
  StructField("eventParams", StructType(List(
    StructField("game_keyword", StringType, true),
    StructField("app_name", StringType, true),
    StructField("scoreAdjustment", IntegerType, true),
    StructField("platform", StringType, true),
    StructField("app_version", StringType, true),
    StructField("device_id", StringType, true),
    StructField("client_event_time", TimestampType, true),
    StructField("amount", DoubleType, true)
  )), true)
))

val gamingEventDF = (spark
  .readStream
  .schema(eventSchema) 
  .option("streamName","mobilestreaming_demo") 
  .option("maxFilesPerTrigger", 1)                // treat each file as Trigger event
  .json(inputPath) 
) 

// COMMAND ----------

// MAGIC %md
// MAGIC ### Step 2: Write Stream
// MAGIC 
// MAGIC The instructions here are to:
// MAGIC 
// MAGIC * Write the stream from `gamingEventDF` to the Databricks Delta data lake in path defined by `outputPathBronze`.
// MAGIC * Convert `client_event_time` to a date format and rename to `eventDate`
// MAGIC * Filter out null `eventDate` 

// COMMAND ----------

// TODO

import org.apache.spark.sql.functions.to_date

eventsStream = gamingEventDF
  .withColumn(FILL_IN) 
  .filter(FILL_IN) 

   FILL_IN  

  .option("checkpointLocation", checkpointPath) 
  .outputMode("append") 
  .queryName("stream_1s")
  .start(outputPathBronze)

// COMMAND ----------

// Wait until the stream is initialized...
untilStreamIsReady("stream_1s")

// COMMAND ----------

// MAGIC %md
// MAGIC ...then create table `mobile_events_delta_raw`.

// COMMAND ----------

// TODO
spark.sql(s"""
   DROP TABLE IF EXISTS mobile_events_delta_raw
 """)
spark.sql(s"""
   CREATE TABLE mobile_events_delta_raw
   FILL_IN

// COMMAND ----------

// TEST - Run this cell to test your solution.
lazy val tableExists = spark.catalog.tableExists("mobile_events_delta_raw")
lazy val firstRow = spark.sql("SELECT * FROM mobile_events_delta_raw").take(1)

dbTest("Delta-08-mobileEventsRawTableExists", true, tableExists, "Table does not exist!")  
dbTest("Delta-08-firstRow", true, firstRow.nonEmpty, "Not enough data has been streamed!") 

// COMMAND ----------

// MAGIC %md
// MAGIC ### Step 3a: Create a Databricks Delta table
// MAGIC 
// MAGIC Create `device_id_type_table` from data in `/mnt/training/gaming_data/dimensionData`.
// MAGIC 
// MAGIC This table associates `deviceId` with `deviceType` = `{android, ios}`.

// COMMAND ----------

// TODO
val tablePath = 
spark.sql(s"""
   DROP TABLE IF EXISTS device_id_type_table
 """)
spark.sql(s"""
   CREATE TABLE device_id_type_table
   FILL_IN

// COMMAND ----------

// TEST - Run this cell to test your solution.
val tableExists = spark.catalog.tableExists("device_id_type_table")

dbTest("Delta-08-tableExists", true, tableExists)  

println("Tests passed!")

// COMMAND ----------

// MAGIC %md
// MAGIC ### Step 3b: Create a query table
// MAGIC 
// MAGIC Create table `mobile_events_delta_query` by joining `device_id_type_table` with `mobile_events_delta_raw` on `deviceId`.
// MAGIC * Your fields should be `eventName`, `deviceId`, `eventTime`, `eventDate` and `deviceType`.
// MAGIC * Make sure to `PARTITION BY (eventDate)`
// MAGIC * Write to `outputPathSilver`

// COMMAND ----------

// TODO
spark.sql(s"""
   DROP TABLE IF EXISTS mobile_events_delta_query
""")

spark.sql(s"""
   CREATE TABLE mobile_events_delta_query 
   FILL_IN

// COMMAND ----------

// TEST - Run this cell to test your solution.
import org.apache.spark.sql.types.{StructType, StructField, StringType, TimestampType, DateType}
lazy val schema = spark.table("mobile_events_delta_query").schema.mkString(",")

dbTest("test-1", true, schema.contains("eventName,StringType"))
dbTest("test-2", true, schema.contains("deviceId,StringType"))
dbTest("test-3", true, schema.contains("eventTime,TimestampType"))
dbTest("test-4", true, schema.contains("eventDate,DateType"))
dbTest("test-5", true, schema.contains("deviceType,StringType"))

lazy val firstRowQuery = spark.sql("SELECT * FROM mobile_events_delta_raw").take(1)
dbTest("Delta-08-firstRowQuery", true, firstRowQuery.nonEmpty, "There are no rows in the query table!") 

println("Tests passed!")

// COMMAND ----------

// MAGIC %md
// MAGIC ### Step 4a: Create a Delta summary table out of query table
// MAGIC 
// MAGIC The company executives want to look at the number of active users by week.
// MAGIC 
// MAGIC Count number of events in the by week.

// COMMAND ----------

// TODO
spark.sql(s"""
   DROP TABLE IF EXISTS mobile_events_delta_summary
""")

spark.sql(s"""
    CREATE TABLE mobile_events_delta_summary  
    FILL_IN

// COMMAND ----------

// TEST - Run this cell to test your solution.
lazy val schema = spark.table("mobile_events_delta_summary").schema.mkString(",")

dbTest("test-1", true, schema.contains("WAU,LongType"))
dbTest("test-2", true, schema.contains("week,IntegerType"))

println("Tests passed!")

// COMMAND ----------

// MAGIC %md
// MAGIC ### Step 4b: Visualization
// MAGIC 
// MAGIC The company executives are visual people: they like pretty charts.
// MAGIC 
// MAGIC Create a bar chart out of `mobile_events_delta_summary` where the horizontal axis is month and the vertical axis is WAU.

// COMMAND ----------

// MAGIC %sql
// MAGIC -- TODO
// MAGIC FILL_IN

// COMMAND ----------

// MAGIC %md
// MAGIC ### Step 5: Isolate a specific `deviceId`
// MAGIC 
// MAGIC Identify all the events associated with a specific user, rougly proxied by the first `deviceId` we encounter in our query. 
// MAGIC 
// MAGIC Use the `mobile_events_delta_query` table.
// MAGIC 
// MAGIC The `deviceId` you come up with should be a string.

// COMMAND ----------

// TODO 
val deviceId = spark.sql("FILL_IN").collect()(0)(0).toString

// COMMAND ----------

// TEST - Run this cell to test your solution.
val deviceIdexists = deviceId.length > 0

dbTest("Delta-L8-lenDeviceId", true, deviceIdexists)

println("Tests passed!")

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ### Step 6: ZORDER 
// MAGIC 
// MAGIC Since the events are implicitly ordered by `eventTime`, implicitly re-order by `deviceId`. 
// MAGIC 
// MAGIC The data pertaining to this `deviceId` is spread out all over the data lake. (It's definitely _not_ co-located!).
// MAGIC 
// MAGIC Pass in the `deviceId` variable you defined above.
// MAGIC 
// MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> `ZORDER` may take a few minutes.

// COMMAND ----------

// MAGIC %sql
// MAGIC -- TODO
// MAGIC OPTIMIZE FILL_IN
// MAGIC ZORDER BY FILL_IN

// COMMAND ----------

// MAGIC %md
// MAGIC ### Step 7: Delete Specific `deviceId`
// MAGIC 
// MAGIC 0. Delete rows with that particular `deviceId` from `mobile_events_delta_query`.
// MAGIC 0. Make sure that `deviceId` is no longer in the table!

// COMMAND ----------

// TODO
spark.sql("FILL_IN")
val noDeviceId = spark.sql("FILL_IN").collect()

// COMMAND ----------

// TEST - Run this cell to test your solution.
dbTest("Delta-08-noDeviceId", true , noDeviceId.isEmpty)

println("Tests passed!")

// COMMAND ----------

// MAGIC %md
// MAGIC ### Step 8: Stop the streaming process

// COMMAND ----------

// TODO

for (s <- FILL_IN)
  s.FILL_IN

// COMMAND ----------

// TEST - Run this cell to test your solution.
val numActiveStreams = spark.streams.active.length
dbTest("Delta-08-numActiveStreams", 0, numActiveStreams)

println("Tests passed!")

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ### Step 9: Clean Up
// MAGIC 
// MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> Do not use a retention of 0 hours in production, as this may affect queries that are currently in flight. 
// MAGIC By default this value is 7 days. 
// MAGIC 
// MAGIC We use 0 hours here for purposes of demonstration only.
// MAGIC 
// MAGIC Recall, we use `VACUUM` to reduce the number of files in each partition directory to 1.

// COMMAND ----------

// Disable the safety check for a retention of 0 hours
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", false)

// COMMAND ----------

// MAGIC %sql
// MAGIC -- TODO
// MAGIC VACUUM FILL_IN

// COMMAND ----------

// MAGIC %md
// MAGIC If the `../eventDate=2018-05-20` directory does not exist, try a different `eventDate` directory.

// COMMAND ----------

// TEST - Run this cell to test your solution.
val numFilesOne  = dbutils.fs.ls(outputPathSilver + "/eventDate=2018-05-20/").length

dbTest("Delta-08-numFilesOne", 1, numFilesOne, "Try another eventDate= directory")

println("Tests passed!")

// COMMAND ----------

// MAGIC %md
// MAGIC Congratulations: ALL DONE!!

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>