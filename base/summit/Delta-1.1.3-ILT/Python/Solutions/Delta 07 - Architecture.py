# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Databricks Delta Architecture
# MAGIC Databricks&reg; Delta simplifies data pipelines and eliminates the need for the traditional Lambda architecture.
# MAGIC 
# MAGIC ## In this lesson you:
# MAGIC * Get streaming Wikipedia data into a data lake via Kafka broker
# MAGIC * Write streaming data into a <b>raw</b> table
# MAGIC * Clean up bronze data and generate normalized <b>query</b> tables
# MAGIC * Create <b>summary</b> tables of key business metrics
# MAGIC * Create plots/dashboards of business metrics
# MAGIC 
# MAGIC ## Audience
# MAGIC * Primary Audience: Data Engineers 
# MAGIC * Secondary Audience: Data Analyst and Data Scientists
# MAGIC 
# MAGIC ## Prerequisites
# MAGIC * Web browser: current versions of Google Chrome, Firefox, Safari, Microsoft Edge and 
# MAGIC Internet Explorer 11 on Windows 7, 8, or 10 (see <a href="https://docs.databricks.com/user-guide/supported-browsers.html#supported-browsers#" target="_blank">Supported Web Browsers</a>)
# MAGIC * Databricks Runtime 4.2 or greater
# MAGIC * Completed courses Spark-SQL, DataFrames or ETL-Part 1 from <a href="https://academy.databricks.com/" target="_blank">Databricks Academy</a>, or have similar knowledge
# MAGIC * Lesson: <a href="$./02-Create">Create</a>
# MAGIC 
# MAGIC ## Datasets Used
# MAGIC * Read Wikipedia edits in real time, with a multitude of different languages. 
# MAGIC * Aggregate the anonymous edits by country, over a window, to see who's editing the English Wikipedia over time.
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

# MAGIC %md-sandbox
# MAGIC ## Lambda Architecture
# MAGIC 
# MAGIC The Lambda architecture is a big data processing architecture that combines both batch- and real-time processing methods.
# MAGIC It features an append-only immutable data source that serves as system of record. Timestamped events are appended to 
# MAGIC existing events (nothing is overwritten). Data is implicitly ordered by time of arrival. 
# MAGIC 
# MAGIC Notice how there are really two pipelines here, one batch and one streaming, hence the name <i>lambda</i> architecture.
# MAGIC 
# MAGIC It is very difficult to combine processing of batch and real-time data as is evidenced by the diagram below.
# MAGIC 
# MAGIC 
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/Delta/lambda.png" style="height: 400px"/></div><br/>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Databricks Delta Architecture
# MAGIC 
# MAGIC The Databricks Delta Architecture is a vast improvmemt upon the traditional Lambda architecture.
# MAGIC 
# MAGIC Text files, RDBMS data and streaming data is all collected into a <b>raw</b> table (also known as "bronze" tables at Databricks).
# MAGIC 
# MAGIC A Raw table is then parsed into <b>query</b> tables (also known as "silver" tables at Databricks). They may be joined with dimension tables.
# MAGIC 
# MAGIC <b>Summary</b> tables (also known as "gold" tables at Databricks) are business level aggregates often used for reporting and dashboarding. 
# MAGIC This would include aggregations such as daily active website users.
# MAGIC 
# MAGIC The end outputs are actionable insights, dashboards and reports of business metrics.
# MAGIC 
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/Delta/delta.png" style="height: 350px"/></div><br/>

# COMMAND ----------

# MAGIC %md
# MAGIC Set up relevant paths.

# COMMAND ----------

basePath       = userhome + "/delta/python"
bronzePath     = basePath + "/wikipedia/bronze.delta"
silverPath     = basePath + "/wikipedia/silver.delta"
checkpointPath = basePath + "/07/checkpoints"

# Configure our shuffle partitions for these exercises
spark.conf.set("spark.sql.shuffle.partitions", 8)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save to RAW table (aka "bronze table")
# MAGIC 
# MAGIC <b>Raw data</b> is unaltered data that is collected into a data lake, either via bulk upload or through streaming sources.
# MAGIC 
# MAGIC The following function reads the Wikipedia IRC channels that has been dumped into our Kafka server.
# MAGIC 
# MAGIC The Kafka server acts as a sort of "firehose" and dumps raw data into our data lake.
# MAGIC 
# MAGIC Since raw data coming in from a stream is transient, we'd like to save it to a more permanent data structure.
# MAGIC 
# MAGIC Below, the first step is to set up schema. The fields we use further down in the notebook are commented.

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType

schema = StructType([
  StructField("channel", StringType(), True),
  StructField("comment", StringType(), True),
  StructField("delta", IntegerType(), True),
  StructField("flag", StringType(), True),
  StructField("geocoding", StructType([                 # (OBJECT): Added by the server, field contains IP address geocoding information for anonymous edit.
    StructField("city", StringType(), True),
    StructField("country", StringType(), True),
    StructField("countryCode2", StringType(), True),
    StructField("countryCode3", StringType(), True),
    StructField("stateProvince", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
  ]), True),
  StructField("isAnonymous", BooleanType(), True),      # (BOOLEAN): Whether or not the change was made by an anonymous user
  StructField("isNewPage", BooleanType(), True),
  StructField("isRobot", BooleanType(), True),
  StructField("isUnpatrolled", BooleanType(), True),
  StructField("namespace", StringType(), True),         # (STRING): Page's namespace. See https://en.wikipedia.org/wiki/Wikipedia:Namespace 
  StructField("page", StringType(), True),              # (STRING): Printable name of the page that was edited
  StructField("pageURL", StringType(), True),           # (STRING): URL of the page that was edited
  StructField("timestamp", StringType(), True),         # (STRING): Time the edit occurred, in ISO-8601 format
  StructField("url", StringType(), True),
  StructField("user", StringType(), True),              # (STRING): User who made the edit or the IP address associated with the anonymous editor
  StructField("userURL", StringType(), True),
  StructField("wikipediaURL", StringType(), True),
  StructField("wikipedia", StringType(), True),         # (STRING): Short name of the Wikipedia that was edited (e.g., "en" for the English)
])

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC Next, stream into bronze Databricks Delta directory.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Notice how we are invoking the `.start(path)` method. 
# MAGIC 
# MAGIC This is so that the data is streamed into the path we want (and not a default directory).

# COMMAND ----------

from pyspark.sql.functions import from_json, col
(spark.readStream
  .format("kafka")  
  .option("kafka.bootstrap.servers", "server1.databricks.training:9092")  # Oregon
  #.option("kafka.bootstrap.servers", "server2.databricks.training:9092") # Singapore
  .option("subscribe", "en")
  .load()
  .withColumn("json", from_json(col("value").cast("string"), schema))
  .select(col("timestamp").alias("kafka_timestamp"), col("json.*"))
  .writeStream
  .format("delta")
  .option("checkpointLocation", checkpointPath + "/bronze")
  .outputMode("append")
  .queryName("stream_1p")
  .start(bronzePath)
)

# COMMAND ----------

# MAGIC %md
# MAGIC Wait until stream is done initializing...

# COMMAND ----------

untilStreamIsReady("stream_1p")

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS WikipediaEditsRaw")

spark.sql("""
  CREATE TABLE WikipediaEditsRaw
  USING Delta
  LOCATION '{}'
""".format(bronzePath))

# COMMAND ----------

# MAGIC %md
# MAGIC Take a look at the raw table.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM WikipediaEditsRaw LIMIT 5

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create QUERY tables (aka "silver tables")
# MAGIC 
# MAGIC Notice how `WikipediaEditsRaw` has JSON encoding. For example `{"city":null,"country":null,"countryCode2":null,"c..`
# MAGIC 
# MAGIC In order to be able parse the data in human-readable form, create query tables out of the raw data using columns<br>
# MAGIC `wikipedia`, `isAnonymous`, `namespace`, `page`, `pageURL`, `geocoding`, `timestamp` and `user`.
# MAGIC 
# MAGIC Stream into a Databricks Delta query directory.

# COMMAND ----------

from pyspark.sql.functions import unix_timestamp, col

(spark.readStream
  .format("delta")
  .load(str(bronzePath))
  .select(col("wikipedia"),
          col("isAnonymous"),
          col("namespace"),
          col("page"),
          col("pageURL"),
          col("geocoding"),
          unix_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSX").cast("timestamp").alias("timestamp"),
          col("user"))
  .writeStream
  .format("delta")
  .option("checkpointLocation", checkpointPath + "/silver")
  .outputMode("append")
  .queryName("stream_2p")
  .start(silverPath)
)

# COMMAND ----------

# MAGIC %md
# MAGIC Wait until the stream is done initializing...

# COMMAND ----------

untilStreamIsReady("stream_2p")

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS WikipediaEdits")

spark.sql("""
  CREATE TABLE WikipediaEdits
  USING Delta
  LOCATION '{}'
""".format(silverPath))

# COMMAND ----------

# MAGIC %md
# MAGIC Take a peek at the streaming query view.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM WikipediaEdits

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create SUMMARY (aka "gold") level data 
# MAGIC 
# MAGIC Summary queries can take a long time.
# MAGIC 
# MAGIC Instead of running the below query off `WikipediaEdits`, let's create a summary query.
# MAGIC 
# MAGIC We are interested in a breakdown of what countries anonymous edits are coming from.

# COMMAND ----------

from pyspark.sql.functions import col, desc, count

goldDF = (spark.readStream
  .format("delta")
  .load(str(silverPath))
  .withColumn("countryCode", col("geocoding.countryCode3"))
  .filter(col("namespace") == "article")
  .filter(col("countryCode") != "null")
  .filter(col("isAnonymous") == True)
  .groupBy(col("countryCode"))
  .count() 
  .withColumnRenamed("count", "total")
  .orderBy(col("total").desc())
)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ## Creating Visualizations (aka "platinum" level) 
# MAGIC 
# MAGIC #### Mapping Anonymous Editors' Locations
# MAGIC 
# MAGIC Use that geocoding information to figure out the countries associated with the editors.
# MAGIC 
# MAGIC When you run the query, the default is a (live) html table.
# MAGIC 
# MAGIC In order to create a slick world map visualization of the data, you'll need to click on the item below.
# MAGIC 
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/Delta/plot-options-1.png" style="height: 200px"/></div><br/>
# MAGIC 
# MAGIC Then go to <b>Plot Options...</b> and drag `countryCode` into the <b>Keys:</b> box and `total` into the <b>Values:</b> box and click <b>Apply</b>.
# MAGIC 
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/Delta/plot-options-2.png" style="height: 200px"/></div><br/> 
# MAGIC 
# MAGIC By invoking a `display` action on a DataFrame created from a `readStream` transformation, we can generate a LIVE visualization!
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Keep an eye on the plot for a minute or two and watch the colors change.

# COMMAND ----------

display(goldDF, streamName = "stream_3p")

# COMMAND ----------

# MAGIC %md
# MAGIC Wait for the streams initialize

# COMMAND ----------

untilStreamIsReady("stream_3p")

# COMMAND ----------

# MAGIC %md
# MAGIC Make sure all streams are stopped.

# COMMAND ----------

for s in spark.streams.active:
    s.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC 
# MAGIC Use the Databricks Delta architecture to craft raw, query and summary tables to produce beautiful visualizations of key business metrics.
# MAGIC 
# MAGIC Use these concepts to implement a Delta architecture in the Capstone project.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Topics & Resources
# MAGIC 
# MAGIC * <a href="http://lambda-architecture.net/#" target="_blank">Lambda Architecture</a>
# MAGIC * <a href="https://bennyaustin.wordpress.com/2010/05/02/kimball-and-inmon-dw-models/#" target="_blank">Data Warehouse Models</a>
# MAGIC * <a href="https://people.apache.org//~pwendell/spark-nightly/spark-branch-2.1-docs/latest/structured-streaming-kafka-integration.html#" target="_blank">Reading structured streams from Kafka</a>
# MAGIC * <a href="http://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html#creating-a-kafka-source-stream#" target="_blank">Create a Kafka Source Stream</a>
# MAGIC * <a href="https://docs.databricks.com/delta/delta-intro.html#case-study-multi-hop-pipelines#" target="_blank">Multi Hop Pipelines</a>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>