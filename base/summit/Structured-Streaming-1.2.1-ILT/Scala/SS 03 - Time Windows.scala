// Databricks notebook source
// MAGIC 
// MAGIC %md-sandbox
// MAGIC 
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC <img src="https://files.training.databricks.com/images/Apache-Spark-Logo_TM_200px.png" style="float: left: margin: 20px"/>
// MAGIC 
// MAGIC # Working with Time Windows
// MAGIC 
// MAGIC ## In this lesson you:
// MAGIC * Use sliding windows to aggregate over chunks of data rather than all data
// MAGIC * Apply watermarking to throw away stale old data that you do not have space to keep
// MAGIC * Plot live graphs using `display`
// MAGIC 
// MAGIC ## Audience
// MAGIC * Primary Audience: Data Engineers
// MAGIC * Secondary Audience: Data Scientists, Software Engineers

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Getting Started</h2>
// MAGIC 
// MAGIC Run the following cell to configure our "classroom."

// COMMAND ----------

// MAGIC %run "./Includes/Classroom-Setup"

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Streaming Aggregations</h2>
// MAGIC 
// MAGIC Continuous applications often require near real-time decisions on real-time, aggregated statistics.
// MAGIC 
// MAGIC Some examples include 
// MAGIC * Aggregating errors in data from IoT devices by type 
// MAGIC * Detecting anomalous behavior in a server's log file by aggregating by country. 
// MAGIC * Doing behavior analysis on instant messages via hash tags.
// MAGIC 
// MAGIC However, in the case of streams, you generally don't want to run aggregations over the entire dataset. 

// COMMAND ----------

// MAGIC %md
// MAGIC ### What problems might you encounter if you aggregate over a stream's entire dataset?

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Windowing</h2>
// MAGIC 
// MAGIC If we were using a static DataFrame to produce an aggregate count, we could use `groupBy()` and `count()`. 
// MAGIC 
// MAGIC Instead we accumulate counts within a sliding window, answering questions like "How many records are we getting every second?"
// MAGIC 
// MAGIC The following illustration, from the <a href="https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html" target="_blank">Structured Streaming Programming Guide</a> guide, helps us understanding how it works:

// COMMAND ----------

// MAGIC %md
// MAGIC <img src="http://spark.apache.org/docs/latest/img/structured-streaming-window.png">

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Event Time vs Receipt Time</h2>
// MAGIC 
// MAGIC **Event Time** is the time at which the event occurred in the real world.
// MAGIC 
// MAGIC **Event Time** is **NOT** something maintained by the Structured Streaming framework. 
// MAGIC 
// MAGIC At best, Streams only knows about **Receipt Time** - the time a piece of data arrived in Spark.

// COMMAND ----------

// MAGIC %md
// MAGIC ### What are some examples of **Event Time**? **of Receipt Time**?

// COMMAND ----------

// MAGIC %md
// MAGIC ### What are some of the inherent problems with using **Receipt Time**?

// COMMAND ----------

// MAGIC %md
// MAGIC ### When might it be OK to use **Receipt Time** instead of **Event Time**?

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Windowed Streaming Example</h2>
// MAGIC 
// MAGIC For this example, we will examine the files in `/mnt/training/sensor-data/accelerometer/time-series-stream.json/`.
// MAGIC 
// MAGIC Each line in the file contains a JSON record with two fields: `time` and `action`
// MAGIC 
// MAGIC New files are being written to this directory continuously (aka streaming).
// MAGIC 
// MAGIC Theoretically, there is no end to this process.
// MAGIC 
// MAGIC Let's start by looking at the head of one such file:

// COMMAND ----------

// MAGIC %fs head dbfs:/mnt/training/sensor-data/accelerometer/time-series-stream.json/file-0.json

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC Let's try to analyze these files interactively. 
// MAGIC 
// MAGIC First configure a schema.
// MAGIC 
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> The schema must be specified for file-based Structured Streams. 
// MAGIC Because of the simplicity of the schema, we can use the simpler, DDL-formatted, string representation of the schema.

// COMMAND ----------

val inputPath = "dbfs:/mnt/training/sensor-data/accelerometer/time-series-stream.json/"

val jsonSchema = "time timestamp, action string"

// COMMAND ----------

// MAGIC %md
// MAGIC With the schema defined, we can create the initial DataFrame `inputDf` and then `countsDF` which represents our aggregation:

// COMMAND ----------

import org.apache.spark.sql.functions.window
import org.apache.spark.sql.types.StructType

val inputDF = spark
  .readStream                           // Returns an instance of DataStreamReader   
  .schema(jsonSchema)                   // Set the schema of the JSON data
  .option("maxFilesPerTrigger", 1)      // Treat a sequence of files as a stream, one file at a time
  .json(inputPath)                      // Specifies the format, path and returns a DataFrame

val countsDF = inputDF
  .groupBy($"action",                   // Aggregate by action...
           window($"time", "1 hour"))   // ...then by a 1 hour window
  .count()                              // For the aggregate, produce a count
  .select($"window.start".as("start"),  // Elevate field to column
          $"count",                     // Include count
          $"action")                    // Include action
  .orderBy($"start")                    // Sort by the start time

// COMMAND ----------

// MAGIC %md
// MAGIC To view the results of our query, pass the DataFrame `countsDF` to the `display()` function.

// COMMAND ----------

display(countsDF)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### What's the cause of the delay?
// MAGIC * `groupBy()` causes a **shuffle**
// MAGIC * By default, this produces **200 partitions**
// MAGIC * Plus a **stateful aggregation** to be maintained **over time**
// MAGIC 
// MAGIC This results in :
// MAGIC * Maintenance of an **in-memory state map** for **each window** within **each partition**
// MAGIC * Writing of the state map to a fault-tolerant store 
// MAGIC   * On some clusters, that will be HDFS 
// MAGIC   * Databricks uses the DBFS
// MAGIC * Around 1 to 2 seconds overhead

// COMMAND ----------

// MAGIC %md
// MAGIC One way to reduce this overhead is to reduce the number of partitions Spark shuffles to.
// MAGIC 
// MAGIC In most cases, you want a 1-to-1 mapping of partitions to cores for streaming applications.

// COMMAND ----------

// MAGIC %md
// MAGIC Rerun the query below and notice the performance improvement.
// MAGIC 
// MAGIC Once the data is loaded, render a line graph with 
// MAGIC * **Keys** is set to `start`
// MAGIC * **Series groupings** is set to `action`
// MAGIC * **Values** is set to `count`

// COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", sc.defaultParallelism)

display(countsDF)

// COMMAND ----------

// MAGIC %md
// MAGIC Wait until stream is done initializing...

// COMMAND ----------

untilStreamIsReady("SS03-countsDF-s")

// COMMAND ----------

// MAGIC %md
// MAGIC When you are done, stop all the streaming jobs.

// COMMAND ----------

for (s <- spark.streams.active) // Iterate over all active streams
  s.stop()                      // Stop the stream

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Problem with Generating Many Windows</h2>
// MAGIC 
// MAGIC We are generating a window for every 1 hour aggregate. 
// MAGIC 
// MAGIC _Every window_ has to be separately persisted and maintained.
// MAGIC 
// MAGIC Over time, this aggregated data will build up in the driver.
// MAGIC 
// MAGIC The end result being a massive slowdown if not an OOM Error.
// MAGIC 
// MAGIC ### How do we fix that problem?

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Watermarking</h2>
// MAGIC 
// MAGIC A better solution to the problem is to define a cut-off.
// MAGIC 
// MAGIC A point after which Structured Streaming is allowed to throw saved windows away.
// MAGIC 
// MAGIC That's what _watermarking_ allows us to do.

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### Refining our previous example
// MAGIC 
// MAGIC Below is our previous example with watermarking. 
// MAGIC 
// MAGIC We're telling Structured Streaming to keep no more than 2 hours of aggregated data.

// COMMAND ----------

val watermarkedDF = inputDF
  .withWatermark("time", "2 hours")     // Specify a 2-hour watermark
  .groupBy($"action",                   // Aggregate by action...
           window($"time", "1 hour"))   // ...then by a 1 hour window
  .count()                              // For each aggregate, produce a count
  .select($"window.start".as("start"),  // Elevate field to column
          $"count",                     // Include count
          $"action")                    // Include action
  .orderBy($"start")                    // Sort by the start time

display(watermarkedDF)                  // Start the stream and display it 

// COMMAND ----------

// MAGIC %md
// MAGIC In the example above,   
// MAGIC * Data received 2 hour _past_ the watermark will be dropped. 
// MAGIC * Data received within 2 hours of the watermark will never be dropped.
// MAGIC 
// MAGIC More specifically, any data less than 2 hours behind the latest data processed till then is guaranteed to be aggregated.
// MAGIC 
// MAGIC However, the guarantee is strict only in one direction. 
// MAGIC 
// MAGIC Data delayed by more than 2 hours is not guaranteed to be dropped; it may or may not get aggregated. 
// MAGIC 
// MAGIC The more delayed the data is, the less likely the engine is going to process it.

// COMMAND ----------

// MAGIC %md
// MAGIC Wait until stream is done initializing...

// COMMAND ----------

untilStreamIsReady("SS03-watermarkDF-s")

// COMMAND ----------

// MAGIC %md
// MAGIC Stop all the streams

// COMMAND ----------

for (s <- spark.streams.active) // Iterate over all active streams
  s.stop()                      // Stop the stream

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Next Steps</h2>
// MAGIC 
// MAGIC Start the next lab, [Time Windows Lab]($./Labs/SS 03 - Time Windows Lab).

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>