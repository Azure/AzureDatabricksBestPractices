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
// MAGIC # Structured Streaming Concepts
// MAGIC 
// MAGIC ## In this lesson you:
// MAGIC * Stream data from a file and write it out to a distributed file system
// MAGIC * List active streams
// MAGIC * Stop active streams
// MAGIC 
// MAGIC ## Datasets Used
// MAGIC Data from 
// MAGIC `/mnt/training/definitive-guide/data/activity-data-stream.json/`
// MAGIC contains smartphone accelerometer samples from devices and users. 
// MAGIC 
// MAGIC The file consists of the following columns:
// MAGIC 
// MAGIC | Field          | Description |
// MAGIC | ------------- | ----------- |
// MAGIC | `Index` | unique identifier of event |
// MAGIC | `x` | acceleration in x-dir |
// MAGIC | `y` | acceleration in y-dir |
// MAGIC | `z` | acceleration in z-dir |
// MAGIC | `User` | unique user identifier |
// MAGIC | `Model` | i.e Nexus4 |
// MAGIC | `Device` | type of Model |
// MAGIC | `gt` | transportation mode |
// MAGIC 
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> The last column is named `gt`. This is an initialism for "ground truth". Wikipedia: <a href="https://en.wikipedia.org/wiki/Ground_truth" target="_blank">Ground Truth</a>

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
// MAGIC ### The Schema
// MAGIC 
// MAGIC Every streaming DataFrame must have a schema - the definition of column names and data types.
// MAGIC 
// MAGIC Some sources such as Kafka define the schema for you.
// MAGIC 
// MAGIC In file-based streaming sources, for example, the schema is user-defined.

// COMMAND ----------

// MAGIC %md
// MAGIC ### Why must a schema be specified for a streaming DataFrame?
// MAGIC 
// MAGIC To say that another way... 
// MAGIC 
// MAGIC ### Why are streaming DataFrames unable to infer/read a schema?

// COMMAND ----------

// Define the schema using a DDL-formatted string.
val dataSchema = "Recorded_At timestamp, Device string, Index long, Model string, User string, _corrupt_record String, gt string, x double, y double, z double"

// COMMAND ----------

// MAGIC %md
// MAGIC ### Configuring a File Stream
// MAGIC 
// MAGIC In our example below, we will be consuming files written continuously to a pre-defined directory. 
// MAGIC 
// MAGIC To control how much data is pulled into Spark at once, we can specify the option `maxFilesPerTrigger`.
// MAGIC 
// MAGIC In our example below, we will be reading in only one file for every trigger interval:
// MAGIC 
// MAGIC `dsr.option("maxFilesPerTrigger", 1)`
// MAGIC 
// MAGIC Both the location and file type are specified with the following call, which itself returns a `DataFrame`:
// MAGIC 
// MAGIC `df = dsr.json(dataPath)`

// COMMAND ----------

val dataPath = "dbfs:/mnt/training/definitive-guide/data/activity-data-stream.json"

val initialDF = spark
  .readStream                             // Returns DataStreamReader
  .option("maxFilesPerTrigger", 1)        // Force processing of only 1 file per trigger 
  .schema(dataSchema)                     // Required for all streaming DataFrames
  .json(dataPath)                         // The stream's source directory and file type

// COMMAND ----------

// MAGIC %md
// MAGIC And with the initial `DataFrame`, we can apply some transformations:

// COMMAND ----------

val streamingDF = initialDF
  .withColumnRenamed("Index", "User_ID")  // Pick a "better" column name
  .drop("_corrupt_record")                // Remove an unnecessary column

// COMMAND ----------

// MAGIC %md
// MAGIC ### Streaming DataFrames
// MAGIC 
// MAGIC Other than the call to `spark.readStream`, it looks just like any other `DataFrame`.
// MAGIC 
// MAGIC But is it a "streaming" `DataFrame`?
// MAGIC 
// MAGIC You can differentiate between a "static" and "streaming" `DataFrame` with the following call:

// COMMAND ----------

// Static vs Streaming?
streamingDF.isStreaming

// COMMAND ----------

// MAGIC %md
// MAGIC ### Unsupported Operations
// MAGIC 
// MAGIC Most operations on a "streaming" DataFrame are identical to a "static" DataFrame.
// MAGIC 
// MAGIC There are some exceptions to this.
// MAGIC 
// MAGIC One such example would be to sort our never-ending stream by `Recorded_At`.

// COMMAND ----------

try {
  val sortedDF = streamingDF.orderBy($"Recorded_At".desc)
  display(sortedDF)
} catch {
  case _: Throwable => println("Sorting is not supported on an unaggregated stream")
}

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC Sorting is one of a handful of operations that is either too complex or logically not possible to do with a stream.
// MAGIC 
// MAGIC For more information on this topic, see the <a href="https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#unsupported-operations" target="_blank">Structured Streaming Programming Guide / Unsupported Operations</a>.
// MAGIC 
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> We will see in the following module how we can sort an **aggregated** stream.

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Writing a Stream</h2>
// MAGIC 
// MAGIC The method `DataFrame.writeStream` returns a `DataStreamWriter` used to configure the output of the stream.
// MAGIC 
// MAGIC There are a number of parameters to the `DataStreamWriter` configuration:
// MAGIC * Query's name (optional) - This name must be unique among all the currently active queries in the associated SQLContext.
// MAGIC * Trigger (optional) - Default value is `ProcessingTime(0`) and it will run the query as fast as possible.
// MAGIC * Checkpointing directory (optional)
// MAGIC * Output mode
// MAGIC * Output sink
// MAGIC * Configuration specific to the output sink, such as:
// MAGIC   * The host, port and topic of the receiving Kafka server
// MAGIC   * The file format and final destination of files
// MAGIC   * A custom sink via `dsw.foreach(...)`
// MAGIC 
// MAGIC Once the configuration is completed, we can trigger the job with a call to `dsw.start()`

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### Triggers
// MAGIC 
// MAGIC The trigger specifies when the system should process the next set of data.
// MAGIC 
// MAGIC | Trigger Type                           | Example | Notes |
// MAGIC |----------------------------------------|-----------|-------------|
// MAGIC | Unspecified                            |  | _DEFAULT_- The query will be executed as soon as the system has completed processing the previous query |
// MAGIC | Fixed interval micro-batches           | `dsw.trigger(Trigger.ProcessingTime("6 hours"))` | The query will be executed in micro-batches and kicked off at the user-specified intervals |
// MAGIC | One-time micro-batch                   | `dsw.trigger(Trigger.Once())` | The query will execute _only one_ micro-batch to process all the available data and then stop on its own |
// MAGIC | Continuous w/fixed checkpoint interval | `dsw.trigger(Trigger.Continuous("1 second"))` | The query will be executed in a low-latency, continuous processing mode. _EXPERIMENTAL_ in 2.3.2 |
// MAGIC 
// MAGIC In the example below, you will be using a fixed interval of 3 seconds:
// MAGIC 
// MAGIC `dsw.trigger(Trigger.ProcessingTime("3 seconds"))`

// COMMAND ----------

// MAGIC %md
// MAGIC ### Checkpointing
// MAGIC 
// MAGIC A <b>checkpoint</b> stores the current state of your streaming job to a reliable storage system such as Azure Blob Storageor HDFS. It does not store the state of your streaming job to the local file system of any node in your cluster. 
// MAGIC 
// MAGIC Together with write ahead logs, a terminated stream can be restarted and it will continue from where it left off.
// MAGIC 
// MAGIC To enable this feature, you only need to specify the location of a checkpoint directory:
// MAGIC 
// MAGIC `dsw.option("checkpointLocation", checkpointPath)`

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Points to consider:
// MAGIC * Without checkpointing you lose all state
// MAGIC * Required for some sinks (e.g. Parquet files)
// MAGIC * Every job should use a unique checkpointing location

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### Output Modes
// MAGIC 
// MAGIC | Mode   | Example | Notes |
// MAGIC | ------------- | ----------- |
// MAGIC | **Complete** | `dsw.outputMode("complete")` | The entire updated Result Table is written to the sink. The individual sink implementation decides how to handle writing the entire table. |
// MAGIC | **Append** | `dsw.outputMode("append")`     | Only the new rows appended to the Result Table since the last trigger are written to the sink. |
// MAGIC | **Update** | `dsw.outputMode("update")`     | Only the rows in the Result Table that were updated since the last trigger will be outputted to the sink. Since Spark 2.1.1 |
// MAGIC 
// MAGIC In the example below, we are writing to a Parquet directory which only supports the `append` mode: 
// MAGIC 
// MAGIC `dsw.outputMode("append")`

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### Output Sinks
// MAGIC 
// MAGIC `DataStreamWriter.format` accepts the following values, among others:
// MAGIC 
// MAGIC | Output Sink | Example                                          | Notes |
// MAGIC | ----------- | ------------------------------------------------ | ----- |
// MAGIC | **File**    | `dsw.format("parquet")`, `dsw.format("csv")`...  | Dumps the Result Table to a file. Supports Parquet, json, csv, etc.|
// MAGIC | **Kafka**   | `dsw.format("kafka")`      | Writes the output to one or more topics in Kafka |
// MAGIC | **Console** | `dsw.format("console")`    | Prints data to the console (useful for debugging) |
// MAGIC | **Memory**  | `dsw.format("memory")`     | Updates an in-memory table, which can be queried through Spark SQL or the DataFrame API |
// MAGIC | **foreach** | `dsw.foreach(writer: ForeachWriter)` | This is your "escape hatch", allowing you to write your own type of sink. |
// MAGIC | **Delta**    | `dsw.format("delta")`     | A proprietary sink |
// MAGIC 
// MAGIC In the example below, we will be appending files to a Parquet file and specifying its location with this call:
// MAGIC 
// MAGIC `dsw.format("parquet").start(outputPathDir)`

// COMMAND ----------

// MAGIC %md
// MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Let's Do Some Streaming</h2>
// MAGIC 
// MAGIC In the cell below, we write data from a streaming query to `outputPathDir`. 
// MAGIC 
// MAGIC There are a couple of things to note below:
// MAGIC 0. We are giving the query a name via the call to `queryName` 
// MAGIC 0. Spark begins running jobs once we call `start`
// MAGIC 0. The call to `start` returns a `StreamingQuery` object

// COMMAND ----------

import org.apache.spark.sql.streaming.Trigger

val basePath = userhome + "/structured-streaming-concepts/scala" // A working directory for our streaming app
dbutils.fs.mkdirs(basePath)                                      // Make sure that our working directory exists
val outputPathDir = basePath + "/output.parquet"                 // A subdirectory for our output
val checkpointPath = basePath + "/checkpoint"                    // A subdirectory for our checkpoint & W-A logs

val streamingQuery = streamingDF                                 // Start with our "streaming" DataFrame
  .writeStream                                                   // Get the DataStreamWriter
  .queryName("stream_1s")                                        // Name the query
  .trigger(Trigger.ProcessingTime("3 seconds"))                  // Configure for a 3-second micro-batch
  .format("parquet")                                             // Specify the sink type, a Parquet file
  .option("checkpointLocation", checkpointPath)                  // Specify the location of checkpoint files & W-A logs
  .outputMode("append")                                          // Write only new data to the "file"
  .start(outputPathDir)                                          // Start the job, writing to the specified directory

// COMMAND ----------

// Wait until stream is done initializing...
untilStreamIsReady("stream_1s")

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Managing Streaming Queries</h2>
// MAGIC 
// MAGIC When a query is started, the `StreamingQuery` object can be used to monitor and manage the query.
// MAGIC 
// MAGIC | Method    |  Description |
// MAGIC | ----------- | ------------------------------- |
// MAGIC |`id`| get unique identifier of the running query that persists across restarts from checkpoint data |
// MAGIC |`runId`| get unique id of this run of the query, which will be generated at every start/restart |
// MAGIC |`name`| get name of the auto-generated or user-specified name |
// MAGIC |`explain()`| print detailed explanations of the query |
// MAGIC |`stop()`| stop query |
// MAGIC |`awaitTermination()`| block until query is terminated, with stop() or with error |
// MAGIC |`exception`| exception if query terminated with error |
// MAGIC |`recentProgress`| array of most recent progress updates for this query |
// MAGIC |`lastProgress`| most recent progress update of this streaming query |

// COMMAND ----------

streamingQuery.recentProgress

// COMMAND ----------

// MAGIC %md
// MAGIC Additionally, we can iterate over a list of active streams:

// COMMAND ----------

for (s <- spark.streams.active)          // Iterate over all streams 
  println("%s: %s".format(s.id, s.name)) // Print the stream's id and name

// COMMAND ----------

// MAGIC %md
// MAGIC The code below stops the `streamingQuery` defined above and introduces `awaitTermination()`
// MAGIC 
// MAGIC `awaitTermination()` will block the current thread
// MAGIC * Until the stream stops or 
// MAGIC * Until the specified timeout elapses

// COMMAND ----------

streamingQuery.awaitTermination(5*1000) // Stream for another 5 seconds while the current thread blocks
streamingQuery.stop()                   // Stop the stream

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC 
// MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> The Display function</h2>
// MAGIC 
// MAGIC Within the Databricks notebooks, we can use the `display()` function to render a live plot
// MAGIC 
// MAGIC #### One Gotcha!
// MAGIC When you pass a "streaming" `DataFrame` to `display`:
// MAGIC * A "memory" sink is being used
// MAGIC * The output mode is complete
// MAGIC * The query name is specified with the `streamName` parameter
// MAGIC * The trigger is specified with the `trigger` parameter
// MAGIC * The checkpointing location is specified with the `checkpointLocation`
// MAGIC 
// MAGIC `display(myDF, streamName = "myQuery")`
// MAGIC 
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> We just programmatically stopped our only streaming query in the previous cell. In the cell below, `display` will automatically start our streaming DataFrame, `streamingDF`.  We are passing `stream_2p` as the name for this newly started stream.  

// COMMAND ----------

val myStream = "stream_2s"
display(streamingDF, streamName = myStream)

// COMMAND ----------

// Wait until stream is done initializing...
untilStreamIsReady(myStream)

// COMMAND ----------

// MAGIC %md
// MAGIC Using the value passed to `streamName` in the call to `display`, we can programatically access this specific stream:

// COMMAND ----------

println("Looking for %s".format(myStream))

for (stream <- spark.streams.active)       // Loop over all active streams
  if (stream.name == myStream)             // Single out "streamWithTimestamp"
    println("Found %s (%s)".format(stream.name, stream.id)) 

// COMMAND ----------

// MAGIC %md
// MAGIC Stop all remaining streams.

// COMMAND ----------

for (s <- spark.streams.active)
  s.stop

// COMMAND ----------

// MAGIC %md
// MAGIC Clean up our directories

// COMMAND ----------

dbutils.fs.rm(basePath, true)

// COMMAND ----------

// MAGIC %md
// MAGIC ## End-to-end Fault Tolerance
// MAGIC 
// MAGIC Structured Streaming ensures end-to-end exactly-once fault-tolerance guarantees through _checkpointing_ and <a href="https://en.wikipedia.org/wiki/Write-ahead_logging" target="_blank">Write Ahead Logs</a>.
// MAGIC 
// MAGIC Structured Streaming sources, sinks, and the underlying execution engine work together to track the progress of stream processing. If a failure occurs, the streaming engine attempts to restart and/or reprocess the data.
// MAGIC 
// MAGIC This approach _only_ works if the streaming source is replayable. To ensure fault-tolerance, Structured Streaming assumes that every streaming source has offsets, akin to:
// MAGIC 
// MAGIC * <a target="_blank" href="https://kafka.apache.org/documentation/#intro_topics">Kafka message offsets</a>
// MAGIC 
// MAGIC At a high level, the underlying streaming mechanism relies on a couple approaches:
// MAGIC 
// MAGIC * First, Structured Streaming uses checkpointing and write-ahead logs to record the offset range of data being processed during each trigger interval.
// MAGIC * Next, the streaming sinks are designed to be _idempotent_—that is, multiple writes of the same data (as identified by the offset) do _not_ result in duplicates being written to the sink.
// MAGIC 
// MAGIC Taken together, replayable data sources and idempotent sinks allow Structured Streaming to ensure **end-to-end, exactly-once semantics** under any failure condition.

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Summary</h2>
// MAGIC 
// MAGIC We use `readStream` to read streaming input from a variety of input sources and create a DataFrame.
// MAGIC 
// MAGIC Nothing happens until we invoke `writeStream` or `display`. 
// MAGIC 
// MAGIC Using `writeStream` we can write to a variety of output sinks. Using `display` we draw LIVE bar graphs, charts and other plot types in the notebook.

// COMMAND ----------

// MAGIC %md
// MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Next Steps</h2>
// MAGIC 
// MAGIC Start the next lab, [Streaming Concepts Lab]($./Labs/SS 02 - Streaming Concepts Lab).

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Additional Topics &amp; Resources</h2>
// MAGIC * <a href="https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#" target="_blank">Structured Streaming Programming Guide</a>
// MAGIC * <a href="https://www.youtube.com/watch?v=rl8dIzTpxrI" target="_blank">A Deep Dive into Structured Streaming</a> by Tathagata Das. This is an excellent video describing how Structured Streaming works.

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>