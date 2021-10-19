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
// MAGIC # Structured Streaming with Kafka 
// MAGIC 
// MAGIC We have another server that reads Wikipedia edits in real time, with a multitude of different languages. 
// MAGIC 
// MAGIC **What you will learn:**
// MAGIC * About Kafka
// MAGIC * How to establish a connection with Kafka
// MAGIC * More examples 
// MAGIC * More visualizations
// MAGIC 
// MAGIC ## Audience
// MAGIC * Primary Audience: Data Engineers
// MAGIC * Additional Audiences: Data Scientists and Software Engineers

// COMMAND ----------

// MAGIC %md
// MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Getting Started</h2>
// MAGIC 
// MAGIC Run the following cell to configure our "classroom."

// COMMAND ----------

// MAGIC %run "./Includes/Classroom-Setup"

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC 
// MAGIC <img style="float:right" src="https://files.training.databricks.com/images/eLearning/Structured-Streaming/kafka.png"/>
// MAGIC 
// MAGIC <div>
// MAGIC   <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> The Kafka Ecosystem</h2>
// MAGIC 
// MAGIC   <p>Based on the publish/subscribe messaging pattern</p>
// MAGIC 
// MAGIC   <p>The **publisher** creates the **message**</p>
// MAGIC 
// MAGIC   <p>The message may contain a **key** for partitioning</p>
// MAGIC 
// MAGIC   <p>The message is published to a **topic**</p>
// MAGIC 
// MAGIC   <p>Topics are managed by the **broker**</p>
// MAGIC 
// MAGIC   <p>The broker is the central point for all messages</p>
// MAGIC 
// MAGIC   <p>The **consumer** subscribes to a certain topic</p>
// MAGIC 
// MAGIC   <p>The broker ensures delivery of a topic-specific message to a consumer</p>
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> The Kafka Server</h2>
// MAGIC 
// MAGIC Each edit to Wikipedia is **published** to Kafka as a JSON **message**.
// MAGIC 
// MAGIC Each message is segregated by language into distinct **topics**.
// MAGIC 
// MAGIC For example, the Kafka topic "en" corresponds to edits for **en.wikipedia.org**.

// COMMAND ----------

// MAGIC %md
// MAGIC ### Required Options
// MAGIC 
// MAGIC When consuming from a Kafka source, you **must** specify at least two options:
// MAGIC 
// MAGIC <p>1. The Kafka bootstrap servers, for example:</p>
// MAGIC <p>`dsr.option("kafka.bootstrap.servers", "server1.databricks.training:9092")`</p>
// MAGIC <p>2. Some indication of the topics you want to consume.</p>

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC #### Specifying a topic
// MAGIC 
// MAGIC There are three, mutually-exclusive, ways to specify the topics for consumption:
// MAGIC 
// MAGIC | Option        | Value                                          | Example |
// MAGIC | ------------- | ---------------------------------------------- | ------- |
// MAGIC | **subscribe** | A comma-separated list of topics               | `dsr.option("subscribe", "topic1")` <br/> `dsr.option("subscribe", "topic1,topic2,topic3")` |
// MAGIC | **assign**    | A JSON string indicating topics and partitions | `dsr.option("assign", "{'topic1': [1,3], 'topic2': [2,5]}")`
// MAGIC | **subscribePattern**   | A (Java) regular expression           | `dsr.option("subscribePattern", "e[ns]")` <br/> `dsr.option("subscribePattern", "topic[123]")`

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC 
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> In the example to follow, we're using the "subscribe" option to select the topics we're interested in consuming. 
// MAGIC We've selected only the "en" topic, corresponding to edits for the English Wikipedia. 
// MAGIC If we wanted to consume multiple topics (multiple Wikipedia languages, in our case), we could just specify them as a comma-separate list:
// MAGIC 
// MAGIC ```dsr.option("subscribe", "en,es,it,fr,de,eo")```

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC #### Other options
// MAGIC 
// MAGIC There are other, optional, arguments you can give the Kafka source. 
// MAGIC 
// MAGIC For more information, see the <a href="https://people.apache.org//~pwendell/spark-nightly/spark-branch-2.1-docs/latest/structured-streaming-kafka-integration.html#" target="_blank">Structured Streaming and Kafka Integration Guide</a>

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> The Kafka Schema</h2>
// MAGIC 
// MAGIC Reading from Kafka returns a `DataFrame` with the following fields:
// MAGIC 
// MAGIC | Field             | Type   | Description |
// MAGIC |------------------ | ------ |------------ |
// MAGIC | **key**           | binary | The key of the record (not needed) |
// MAGIC | **value**         | binary | Our JSON payload |
// MAGIC | **topic**         | string | The topic this record is received from (not needed) |
// MAGIC | **partition**     | int    | The Kafka topic partition from which this record is received (not needed) |
// MAGIC | **offset**        | long   | The position of this record in the corresponding Kafka topic partition (not needed) |
// MAGIC | **timestamp**     | long   | The timestamp of this record  |
// MAGIC | **timestampType** | int    | The timestamp type of a record (not needed) |
// MAGIC 
// MAGIC In the example below, the only column we want to keep is `value`.
// MAGIC 
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> The default of `spark.sql.shuffle.partitions` is 200.
// MAGIC This setting is used in operations like `groupBy`.
// MAGIC In this case, we should be setting this value to match the current number of cores.

// COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", sc.defaultParallelism)

val kafkaServer = "server1.databricks.training:9092"  // US (Oregon)
// kafkaServer = "server2.databricks.training:9092"   // Singapore

val editsDF = spark.readStream                        // Get the DataStreamReader
  .format("kafka")                                    // Specify the source format as "kafka"
  .option("kafka.bootstrap.servers", kafkaServer)     // Configure the Kafka server name and port
  .option("subscribe", "en")                          // Subscribe to the "en" Kafka topic 
  .option("startingOffsets", "earliest")              // Rewind stream to beginning when we restart notebook
  .option("maxOffsetsPerTrigger", 1000)               // Throttle Kafka's processing of the streams
  .load()                                             // Load the DataFrame
  .select($"value".cast("STRING"))                    // Cast the "value" column to STRING

// COMMAND ----------

// MAGIC %md
// MAGIC Let's display some data.

// COMMAND ----------

val myStream = "my_scala_stream"
display(editsDF,  streamName = myStream)

// COMMAND ----------

// MAGIC %md
// MAGIC Wait until stream is done initializing...

// COMMAND ----------

untilStreamIsReady("my_scala_stream")

// COMMAND ----------

// MAGIC %md
// MAGIC Make sure to stop the stream before continuing.

// COMMAND ----------

for (s <- spark.streams.active) { // Iterate over all active streams
  if (s.name == myStream) {       // Look for our specific stream
    println("Stopping "+s.name)   // A little extra feedback
    s.stop                        // Stop the stream
  }
}

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Use Kafka to display the raw data</h2>
// MAGIC 
// MAGIC The Kafka server acts as a sort of "firehose" (or asynchronous buffer) and displays raw data.
// MAGIC 
// MAGIC Since raw data coming in from a stream is transient, we'd like to save it to a more permanent data structure.
// MAGIC 
// MAGIC The first step is to define the schema for the JSON payload.
// MAGIC 
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Only those fields of future interest are commented below.
// MAGIC 
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Use of the `lazy` keyword suppresses gobs of useless output

// COMMAND ----------

import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, TimestampType}

lazy val schema = StructType(List(
  StructField("channel", StringType, true),
  StructField("comment", StringType, true),
  StructField("delta", IntegerType, true),
  StructField("flag", StringType, true),
  StructField("geocoding", StructType(List(            //  (OBJECT): Added by the server, field contains IP address geocoding information for anonymous edit.
    StructField("city", StringType, true),
    StructField("country", StringType, true),
    StructField("countryCode2", StringType, true),
    StructField("countryCode3", StringType, true),
    StructField("stateProvince", StringType, true),
    StructField("latitude", DoubleType, true),
    StructField("longitude", DoubleType, true)
  )), true),
  StructField("isAnonymous", BooleanType, true),
  StructField("isNewPage", BooleanType, true),
  StructField("isRobot", BooleanType, true),
  StructField("isUnpatrolled", BooleanType, true),
  StructField("namespace", StringType, true),           //   (STRING): Page's namespace. See https://en.wikipedia.org/wiki/Wikipedia:Namespace 
  StructField("page", StringType, true),                //   (STRING): Printable name of the page that was edited
  StructField("pageURL", StringType, true),             //   (STRING): URL of the page that was edited
  StructField("timestamp", TimestampType, true),        //   (STRING): Time the edit occurred, in ISO-8601 format
  StructField("url", StringType, true),
  StructField("user", StringType, true),                //   (STRING): User who made the edit or the IP address associated with the anonymous editor
  StructField("userURL", StringType, true),
  StructField("wikipediaURL", StringType, true),
  StructField("wikipedia", StringType, true)            //   (STRING): Short name of the Wikipedia that was edited (e.g., "en" for the English)
))

// COMMAND ----------

// MAGIC %md
// MAGIC Next we can use the function `from_json` to parse out the full message with the schema specified above.

// COMMAND ----------

import org.apache.spark.sql.functions.from_json

val jsonEdits = editsDF.select(
  from_json($"value", schema).as("json"))   // Parse the column "value" and name it "json"

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC When parsing a value from JSON, we end up with a single column containing a complex object.
// MAGIC 
// MAGIC We can clearly see this by simply printing the schema.

// COMMAND ----------

jsonEdits.printSchema()

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC The fields of a complex object can be referenced with a "dot" notation as in:
// MAGIC 
// MAGIC  
// MAGIC `$"json.geocoding.countryCode3"` 
// MAGIC 
// MAGIC A large number of these fields/columns can become unwieldy.
// MAGIC 
// MAGIC For that reason, it is common to extract the sub-fields and represent them as first-level columns as seen below:

// COMMAND ----------

import org.apache.spark.sql.functions.{unix_timestamp}

val anonDF = jsonEdits
  .select($"json.wikipedia".as("wikipedia"),      // Promoting from sub-field to column
          $"json.isAnonymous".as("isAnonymous"),  //     "       "      "      "    "
          $"json.namespace".as("namespace"),      //     "       "      "      "    "
          $"json.page".as("page"),                //     "       "      "      "    "
          $"json.pageURL".as("pageURL"),          //     "       "      "      "    "
          $"json.geocoding".as("geocoding"),      //     "       "      "      "    "
          $"json.user".as("user"),                //     "       "      "      "    "
          $"json.timestamp".cast("timestamp"))    // Promoting and converting to a timestamp
  .filter($"namespace" === "article")             // Limit result to just articles
  .filter($"geocoding.countryCode3".isNotNull)    // We only want results that are geocoded

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Mapping Anonymous Editors' Locations</h2>
// MAGIC 
// MAGIC When you run the query, the default is a [live] html table.
// MAGIC 
// MAGIC The geocoded information allows us to associate an anonymous edit with a country.
// MAGIC 
// MAGIC We can then use that geocoded information to plot edits on a [live] world map.
// MAGIC 
// MAGIC 
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Keep an eye on the plot for a minute or two and watch the colors change.

// COMMAND ----------

val mappedDF = anonDF
  .groupBy("geocoding.countryCode3") // Aggregate by country (code)
  .count()                           // Produce a count of each aggregate

display(mappedDF)

// COMMAND ----------

// MAGIC %md
// MAGIC Wait until stream is done initializing...

// COMMAND ----------

untilStreamIsReady("SS04-mapped-s")

// COMMAND ----------

// MAGIC %md
// MAGIC Stop the streams.

// COMMAND ----------

for (s <- spark.streams.active)  // Iterate over all active streams
  s.stop()                       // Stop the stream

// COMMAND ----------

// MAGIC %md
// MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Next Steps</h2>
// MAGIC 
// MAGIC Start the next lab, [Using Kafka Lab]($./Labs/SS 04 - Using Kafka Lab).

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Additional Topics &amp; Resources</h2>
// MAGIC 
// MAGIC * <a href="http://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html#creating-a-kafka-source-stream#" target="_blank">Create a Kafka Source Stream</a>
// MAGIC * <a href="https://kafka.apache.org/documentation/" target="_blank">Official Kafka Documentation</a>
// MAGIC * <a href="https://www.confluent.io/blog/okay-store-data-apache-kafka/" target="_blank">Use Kafka to store data</a>

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>