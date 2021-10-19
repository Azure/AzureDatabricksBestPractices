# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Gain Actionable Insights from a Data Lake, Satisfy GDPR
# MAGIC 
# MAGIC In this capstone project, use Databricks Delta to manage a data lake consisting of a lot of historical data plus incoming streaming data.
# MAGIC 
# MAGIC A video gaming company stores historical data in a data lake, which is growing exponentially. 
# MAGIC 
# MAGIC The data isn't sorted in any particular way (actually, it's quite a mess).
# MAGIC 
# MAGIC It is proving to be _very_ difficult to query and manage this data because there is so much of it.
# MAGIC 
# MAGIC To further complicate issues, a regulatory agency has decreed you be able to identify and delete all data associated with a specific user (i.e. GDPR). 
# MAGIC 
# MAGIC In other words, you must delete data associated with a specific `deviceId`.
# MAGIC 
# MAGIC ## Audience
# MAGIC * Primary Audience: Data Engineers
# MAGIC * Additional Audiences: Data Analysts and Data Scientists
# MAGIC 
# MAGIC ## Prerequisites
# MAGIC * Web browser: current versions of Google Chrome, Firefox, Safari, Microsoft Edge and 
# MAGIC Internet Explorer 11 on Windows 7, 8, or 10 (see <a href="https://docs.databricks.com/user-guide/supported-browsers.html#supported-browsers#" target="_blank">Supported Web Browsers</a>)
# MAGIC * Databricks Runtime 4.2 or greater
# MAGIC * Completed courses Spark-SQL, DataFrames or ETL-Part 1 from <a href="https://academy.databricks.com/" target="_blank">Databricks Academy</a>, or have similar knowledge
# MAGIC * Have done the rest of this course
# MAGIC 
# MAGIC ## Instructions
# MAGIC 0. Read in streaming data into Databricks Delta raw tables
# MAGIC 0. Create Databricks Delta query table
# MAGIC 0. Compute aggregate statistics about data i.e. create summary table
# MAGIC 0. Identify events associated with specific `deviceId` 
# MAGIC 0. Do data cleanup using Databricks Delta advanced features
# MAGIC 
# MAGIC ## CAUTION
# MAGIC * Do not use <b>RunAll</b> mode (next to <b>Permissions</b>). 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Getting Started
# MAGIC 
# MAGIC Run the following cell to configure our "classroom."

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC Set up relevant paths.

# COMMAND ----------

inputPath = "/mnt/training/gaming_data/mobile_streaming_events_b"

basePath         = userhome + "/delta/python"
outputPathBronze = basePath + "/gaming/bronze.delta"
outputPathSilver = basePath + "/gaming/silver.delta"
outputPathGold   = basePath + "/gaming/gold.delta"
checkpointPath   = basePath + "/08/checkpoints"

# Configure our shuffle partitions for these exercises
spark.conf.set("spark.sql.shuffle.partitions", 8)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Prepare Schema and Read Streaming Data from input source
# MAGIC 
# MAGIC The input source is a folder containing files of around 100,000 bytes each and is set up to stream slowly.
# MAGIC 
# MAGIC Run this code to read streaming data in.

# COMMAND ----------

from pyspark.sql.types import StructType, StringType, IntegerType, TimestampType, DoubleType

eventSchema = ( StructType()
  .add('eventName', StringType()) 
  .add('eventParams', StructType() 
    .add('game_keyword', StringType()) 
    .add('app_name', StringType()) 
    .add('scoreAdjustment', IntegerType()) 
    .add('platform', StringType()) 
    .add('app_version', StringType()) 
    .add('device_id', StringType()) 
    .add('client_event_time', TimestampType()) 
    .add('amount', DoubleType()) 
  )     
)

gamingEventDF = (spark
  .readStream
  .schema(eventSchema) 
  .option('streamName','mobilestreaming_demo') 
  .option("maxFilesPerTrigger", 1)                # treat each file as Trigger event
  .json(inputPath) 
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Write Stream
# MAGIC 
# MAGIC The instructions here are to:
# MAGIC 
# MAGIC * Write the stream from `gamingEventDF` to the Databricks Delta data lake in path defined by `outputPathBronze`.
# MAGIC * Convert `client_event_time` to a date format and rename to `eventDate`
# MAGIC * Filter out null `eventDate` 

# COMMAND ----------

# ANSWER
from pyspark.sql.functions import col, to_date

eventsStream = (gamingEventDF
  .withColumn('eventDate', to_date(gamingEventDF.eventParams.client_event_time))
  .filter(col('eventDate').isNotNull())
  .writeStream 
  .partitionBy('eventDate') 
  .format('delta') 
  .option('checkpointLocation', checkpointPath) 
  .queryName("stream_1p")
  .start(outputPathBronze)
)

# COMMAND ----------

# Wait until the stream is initialized...
untilStreamIsReady("stream_1p")

# COMMAND ----------

# MAGIC %md
# MAGIC ...then create table `mobile_events_delta_raw`.

# COMMAND ----------

# ANSWER
spark.sql("""
    DROP TABLE IF EXISTS mobile_events_delta_raw
  """)
spark.sql("""
    CREATE TABLE mobile_events_delta_raw
    USING DELTA 
    LOCATION '{}' 
  """.format(outputPathBronze))

# COMMAND ----------

# TEST - Run this cell to test your solution.
try:
  rawTableExists = (spark.table("mobile_events_delta_raw") is not None)
except:
  rawTableExists = False
  
firstRow = spark.sql("SELECT * FROM mobile_events_delta_raw").take(1)

dbTest("Delta-08-rawTableExists", True, rawTableExists)  
dbTest("Delta-08-rowsExist", False, not firstRow) 

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3a: Create a Databricks Delta table
# MAGIC 
# MAGIC Create `device_id_type_table` from data in `/mnt/training/gaming_data/dimensionData`.
# MAGIC 
# MAGIC This table associates `deviceId` with `deviceType` = `{android, ios}`.

# COMMAND ----------

# ANSWER
tablePath = "/mnt/training/gaming_data/dimensionData"
spark.sql("""
    DROP TABLE IF EXISTS device_id_type_table
  """)
spark.sql("""
    CREATE TABLE device_id_type_table
    USING DELTA 
    LOCATION '{}' 
  """.format(tablePath))

# COMMAND ----------

# TEST - Run this cell to test your solution.
try:
  tableExists = (spark.table("device_id_type_table") is not None)
except:
  tableExists = False
  
dbTest("Delta-08-tableExists", True, tableExists)  

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3b: Create a query table
# MAGIC 
# MAGIC Create table `mobile_events_delta_query` by joining `device_id_type_table` with `mobile_events_delta_raw` on `deviceId`.
# MAGIC * Your fields should be `eventName`, `deviceId`, `eventTime`, `eventDate` and `deviceType`.
# MAGIC * Make sure to `PARTITION BY (eventDate)`
# MAGIC * Write to `outputPathSilver`

# COMMAND ----------

# ANSWER
spark.sql("""
    DROP TABLE IF EXISTS mobile_events_delta_query
""")

spark.sql("""
    CREATE TABLE mobile_events_delta_query
    USING DELTA
    PARTITIONED BY (eventDate)
    LOCATION '{}'
    AS 
      SELECT eventName, eventParams.device_id AS deviceId, eventParams.client_event_time AS eventTime, eventDate, device_id_type_table.deviceType AS deviceType
      FROM mobile_events_delta_raw
      INNER JOIN device_id_type_table ON device_id_type_table.device_id = mobile_events_delta_raw.eventParams.device_id
""".format(outputPathSilver))             

# COMMAND ----------

# TEST - Run this cell to test your solution.
from pyspark.sql.types import StructField, StructType, StringType, TimestampType, DateType
schema = spark.table("mobile_events_delta_query").schema

firstRowQuery = spark.sql("SELECT * FROM mobile_events_delta_query limit 1").collect()[0][0]

dbTest("test-1", True, "eventName,StringType" in str(schema))
dbTest("test-2", True, "deviceId,StringType" in str(schema))
dbTest("test-3", True, "eventTime,TimestampType" in str(schema))
dbTest("test-4", True, "eventDate,DateType" in str(schema))
dbTest("test-5", True, "deviceType,StringType" in str(schema))

dbTest("Delta-08-queryRowsExist", False, not firstRowQuery) 

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4a: Create a Delta summary table out of query table
# MAGIC 
# MAGIC The company executives want to look at the number of active users by week.
# MAGIC 
# MAGIC Count number of events in the by week.

# COMMAND ----------

# ANSWER
spark.sql("""
    DROP TABLE IF EXISTS mobile_events_delta_summary
""")

spark.sql("""
    CREATE TABLE mobile_events_delta_summary 
    USING DELTA
    LOCATION '{}'
    AS
      SELECT count(DISTINCT deviceId) AS WAU, weekofyear(eventTime) as week
      FROM mobile_events_delta_query
      GROUP BY weekofyear(eventTime)
      ORDER BY weekofyear(eventTime)
""".format(outputPathGold))   

# COMMAND ----------

# TEST - Run this cell to test your solution.
schema = str(spark.table("mobile_events_delta_summary").schema)

dbTest("test-1", True, "WAU,LongType" in str(schema))
dbTest("test-2", True, "week,IntegerType" in str(schema))

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4b: Visualization
# MAGIC 
# MAGIC The company executives are visual people: they like pretty charts.
# MAGIC 
# MAGIC Create a bar chart out of `mobile_events_delta_summary` where the horizontal axis is month and the vertical axis is WAU.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ANSWER
# MAGIC SELECT * FROM mobile_events_delta_summary

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5: Isolate a specific `deviceId`
# MAGIC 
# MAGIC Identify all the events associated with a specific user, rougly proxied by the first `deviceId` we encounter in our query. 
# MAGIC 
# MAGIC Use the `mobile_events_delta_query` table.
# MAGIC 
# MAGIC The `deviceId` you come up with should be a string.

# COMMAND ----------

# ANSWER  
deviceId = str(spark.sql("SELECT deviceId FROM mobile_events_delta_query limit 1").collect()[0][0])

# COMMAND ----------

# TEST - Run this cell to test your solution.
deviceIdexists = len(deviceId) > 0

dbTest("Delta-L8-lenDeviceId", True, deviceIdexists)

print("Tests passed!")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Step 6: ZORDER 
# MAGIC 
# MAGIC Since the events are implicitly ordered by `eventTime`, implicitly re-order by `deviceId`. 
# MAGIC 
# MAGIC The data pertaining to this `deviceId` is spread out all over the data lake. (It's definitely _not_ co-located!).
# MAGIC 
# MAGIC Pass in the `deviceId` variable you defined above.
# MAGIC 
# MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> `ZORDER` may take a few minutes.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ANSWER
# MAGIC OPTIMIZE mobile_events_delta_query
# MAGIC ZORDER BY (deviceId)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 7: Delete Specific `deviceId`
# MAGIC 
# MAGIC 0. Delete rows with that particular `deviceId` from `mobile_events_delta_query`.
# MAGIC 0. Make sure that `deviceId` is no longer in the table!

# COMMAND ----------

# ANSWER
spark.sql("DELETE FROM mobile_events_delta_query WHERE deviceId='{}' ".format(deviceId))
noDeviceId = spark.sql(("SELECT * FROM mobile_events_delta_query WHERE deviceId='{}' ").format(deviceId)).collect()

# COMMAND ----------

# TEST - Run this cell to test your solution.
dbTest("Delta-08-noDeviceId", True , not noDeviceId)

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 8: Stop the streaming process

# COMMAND ----------

# ANSWER

for s in spark.streams.active:
  s.stop()

# COMMAND ----------

# TEST - Run this cell to test your solution.
numActiveStreams = len(spark.streams.active)
dbTest("Delta-08-numActiveStreams", 0, numActiveStreams)

print("Tests passed!")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Step 9: Clean Up
# MAGIC 
# MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> Do not use a retention of 0 hours in production, as this may affect queries that are currently in flight. 
# MAGIC By default this value is 7 days. 
# MAGIC 
# MAGIC We use 0 hours here for purposes of demonstration only.
# MAGIC 
# MAGIC Recall, we use `VACUUM` to reduce the number of files in each partition directory to 1.

# COMMAND ----------

# Disable the safety check for a retention of 0 hours
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", False)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ANSWER
# MAGIC VACUUM mobile_events_delta_query RETAIN 0 HOURS;

# COMMAND ----------

# MAGIC %md
# MAGIC If the `../eventDate=2018-05-20` directory does not exist, try a different `eventDate` directory.

# COMMAND ----------

# TEST - Run this cell to test your solution.
numFilesOne = len(dbutils.fs.ls(("{}/eventDate=2018-05-20").format(outputPathSilver)))

dbTest("Delta-08-numFilesOne", 1, numFilesOne)

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC Congratulations: ALL DONE!!

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>