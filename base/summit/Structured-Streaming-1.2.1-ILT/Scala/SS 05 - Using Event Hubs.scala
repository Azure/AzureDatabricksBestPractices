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
// MAGIC # Structured Streaming with Azure Event Hubs
// MAGIC 
// MAGIC We have another server that reads Wikipedia edits in real time, with a multitude of different languages. 
// MAGIC 
// MAGIC **What you will learn:**
// MAGIC * About EventHub
// MAGIC * How to establish a connection with EventHub
// MAGIC * More examples 
// MAGIC * More visualizations
// MAGIC 
// MAGIC ## Audience
// MAGIC * Primary Audience: Data Engineers
// MAGIC * Additional Audiences: Data Scientists and Software Engineers
// MAGIC 
// MAGIC ## Library Requirements
// MAGIC 
// MAGIC 1. the Maven library with coordinate `com.microsoft.azure:azure-eventhubs-spark_2.11:2.3.7`
// MAGIC    - this allows Databricks `spark` session to communicate with an Event Hub
// MAGIC 2. the Python library `azure-eventhub`
// MAGIC    - this is allows the Python kernel to stream content to an Event Hub
// MAGIC 3. the Python library `sseclient`
// MAGIC    - this is used to create a streaming client to an existing streaming server
// MAGIC 
// MAGIC Documentation on how to install Python libraries:
// MAGIC https://docs.azuredatabricks.net/user-guide/libraries.html#pypi-libraries
// MAGIC 
// MAGIC Documentation on how to install Maven libraries:
// MAGIC https://docs.azuredatabricks.net/user-guide/libraries.html#maven-or-spark-package

// COMMAND ----------

// MAGIC %md
// MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Getting Started</h2>
// MAGIC 
// MAGIC Run the following cell to configure our "classroom."

// COMMAND ----------

// MAGIC %run "./Includes/Classroom-Setup"

// COMMAND ----------

// MAGIC %md
// MAGIC Set up fill-in text boxes at the top.
// MAGIC 
// MAGIC Your instructor should tell you what the connection string is. You can also copy it from Azure portal.
// MAGIC 
// MAGIC Your `EVENT_HUB_NAME` should be specific to your namespace.

// COMMAND ----------

dbutils.widgets.text("CONNECTION_STRING", "", "Connection String")
dbutils.widgets.text("EVENT_HUB_NAME", "wiki-changes", "Event Hub")

// COMMAND ----------

// MAGIC %md
// MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Azure Event Hubs</h2>
// MAGIC 
// MAGIC Microsoft Azure Event Hubs is a fully managed, real-time data ingestion service.
// MAGIC You can stream millions of events per second from any source to build dynamic data pipelines and immediately respond to business challenges.
// MAGIC It integrates seamlessly with a host of other Azure services.
// MAGIC 
// MAGIC Event Hubs can be used in a variety of applications such as
// MAGIC * Anomaly detection (fraud/outliers)
// MAGIC * Application logging
// MAGIC * Analytics pipelines, such as clickstreams
// MAGIC * Archiving data
// MAGIC * Transaction processing
// MAGIC * User telemetry processing
// MAGIC * Device telemetry streaming
// MAGIC * <b>Live dashboarding</b>
// MAGIC 
// MAGIC In this notebook, we will show you how to use Event Hubs to produce LIVE Dashboards.

// COMMAND ----------

// MAGIC %md
// MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Configure Authentication</h2>
// MAGIC 
// MAGIC We will need to define these two variables:
// MAGIC 
// MAGIC * `CONNECTION_STRING`
// MAGIC * `EVENT_HUB_NAME`

// COMMAND ----------

lazy val pcString = dbutils.widgets.get("CONNECTION_STRING")
lazy val uniqueEHName = dbutils.widgets.get("EVENT_HUB_NAME")

// check to make sure it is not an empty string
assert(!pcString.isEmpty, ": The Primary Connection String must be non-empty")
assert(!uniqueEHName.isEmpty, ": The Unique Event Hubs Name must be non-empty")

lazy val fullPCString = pcString.replace(".net/;", s".net/$uniqueEHName/;")
lazy val connectionString = s"$fullPCString;EntityPath=$uniqueEHName"

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Event Hubs Configuration</h2>
// MAGIC 
// MAGIC Assemble the following:
// MAGIC * A `startingEventPosition` as a JSON string
// MAGIC * An `EventHubsConf` 
// MAGIC   * to include a string with connection credentials
// MAGIC   * to set a starting position for the stream read
// MAGIC   * to throttle Event Hubs' processing of the streams
// MAGIC   

// COMMAND ----------

// Create the starting position Map as a JSON string 
lazy val startingEventPosition = """{
  "offset": "-1",
  "seqNo": -1,              
  "enqueuedTime": null,
  "isInclusive": true}"""

// Have to cast this to a regular (immutable) Map of [String, String]
lazy val eventHubsConf = Map(
  "eventhubs.connectionString" -> connectionString,
  "eventhubs.startingposition" -> startingEventPosition,
  "setMaxEventsPerTrigger" -> "100"
)

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> The Event Hubs Schema</h2>
// MAGIC 
// MAGIC Reading from Event Hubs returns a `DataFrame` with the following fields:
// MAGIC 
// MAGIC | Field             | Type   | Description |
// MAGIC |------------------ | ------ |------------ |
// MAGIC | **body**          | binary | Our JSON payload |
// MAGIC | **partition**     | string | The partition from which this record is received  |
// MAGIC | **offset**        | string | The position of this record in the corresponding EventHubs partition|
// MAGIC | **sequenceNumber**     | long   | A unique identifier for a packet of data (alternative to a timestamp) |
// MAGIC | **enqueuedTime** 	| timestamp | Time when data arrives |
// MAGIC | **publisher**     | string | Who produced the message |
// MAGIC | **partitionKey**  | string | A mechanism to access partition by key |
// MAGIC | **properties**    | map[string, json] | Extra properties |
// MAGIC 
// MAGIC In the example below, the only column we want to keep is `body`.
// MAGIC 
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> The default of `spark.sql.shuffle.partitions` is 200.
// MAGIC This setting is used in operations like `groupBy`.
// MAGIC In this case, we should be setting this value to match the current number of cores.

// COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", sc.defaultParallelism)

val editsDF = spark.readStream     // Get the DataStreamReader
  .format("eventhubs")             // Specify the source format as "eventhubs"
  .options(eventHubsConf)          // Event Hubs options as a map
  .load()                          // Load the DataFrame
  .select($"body".cast("STRING"))  // Cast the "body" column to STRING

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC Let's display some data.
// MAGIC 
// MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> Make sure the Wikipedia stream server is running at this point.

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
// MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Use Event Hubs to Display the Raw Data</h2>
// MAGIC 
// MAGIC The Event Hubs server acts as a sort of "firehose" (or asynchronous buffer) and displays raw data.
// MAGIC 
// MAGIC Please use the Event Hub Stream Server notebook to add content to the stream. 
// MAGIC 
// MAGIC Since raw data coming in from a stream is transient, we'd like to save it to a more permanent data structure.
// MAGIC 
// MAGIC The first step is to define the schema for the JSON payload.
// MAGIC 
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Use of the `lazy` keyword suppresses gobs of useless output

// COMMAND ----------

import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, TimestampType}

lazy val schema = StructType(List(
  StructField("bot", BooleanType, true),
  StructField("comment", StringType, true),
  StructField("id", IntegerType, true),                  // ID of the recentchange event 
  StructField("length",  StructType(List( 
    StructField("new", IntegerType, true),               // Length of new change
    StructField("old", IntegerType, true)                // Length of old change
  )), true), 
  StructField("meta", StructType(List(  
	StructField("domain", StringType, true),
	StructField("dt", StringType, true),
	StructField("id", StringType, true),
	StructField("request_id", StringType, true),
	StructField("schema_uri", StringType, true),
	StructField("topic", StringType, true),
	StructField("uri", StringType, true),
	StructField("partition", StringType, true),
	StructField("offset", StringType, true)
  )), true),
  StructField("minor", BooleanType, true),                 // Is it a minor revision?
  StructField("namespace", IntegerType, true),             // ID of relevant namespace of affected page
  StructField("parsedcomment", StringType, true),          // The comment parsed into simple HTML
  StructField("revision", StructType(List(                 
    StructField("new", IntegerType, true),                 // New revision ID
    StructField("old", IntegerType, true)                  // Old revision ID
  )), true),
  StructField("server_name", StringType, true),
  StructField("server_script_path", StringType, true),
  StructField("server_url", StringType, true),
  StructField("timestamp", IntegerType, true),             // Unix timestamp 
  StructField("title", StringType, true),                  // Full page name
  StructField("type", StringType, true),                   // Type of recentchange event (rc_type). One of "edit", "new", "log", "categorize", or "external".
  StructField("geolocation", StructType(List(              // Geo location info structure
    StructField("PostalCode", StringType, true),
    StructField("StateProvince", StringType, true),
    StructField("city", StringType, true), 
    StructField("country", StringType, true),
    StructField("countrycode3", StringType, true)          // Really, we only need the three-letter country code in this exercise
   )), true),
  StructField("user", StringType, true),                   // User ID of person who wrote article
  StructField("wiki", StringType, true)                    // wfWikiID
))

// COMMAND ----------

// MAGIC %md
// MAGIC Next we can use the function `from_json` to parse out the full message with the schema specified above.

// COMMAND ----------

import org.apache.spark.sql.functions.from_json

val jsonEdits = editsDF.select(
  from_json($"body", schema).as("json"))   // Parse the column "value" and name it "json"

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
// MAGIC `$"json.wiki"` 
// MAGIC 
// MAGIC A large number of these fields/columns can become unwieldy.
// MAGIC 
// MAGIC For that reason, it is common to extract the sub-fields and represent them as first-level columns as seen below:

// COMMAND ----------

val wikiDF = jsonEdits
  .select($"json.wiki".as("wikipedia"),                         // Promoting from sub-field to column
          $"json.namespace".as("namespace"),                    //     "       "      "      "    "
          $"json.title".as("page"),                             //     "       "      "      "    "
          $"json.server_name".as("pageURL"),                    //     "       "      "      "    "
          $"json.user".as("user"),                              //     "       "      "      "    "
          $"json.geolocation.countrycode3".as("countryCode3"),  //     "       "      "      "    "
          $"json.timestamp".cast("timestamp"))                  // Promoting and converting to a timestamp
  .filter($"wikipedia".isNotNull)

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
// MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> Make sure the Wikipedia stream server is running at this point.
// MAGIC 
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Keep an eye on the plot for a minute or two and watch the colors change.

// COMMAND ----------

val mappedDF = wikiDF
  .groupBy("countryCode3")   // Aggregate by country (code)
  .count()                   // Produce a count of each aggregate

display(mappedDF)

// COMMAND ----------

// MAGIC %md
// MAGIC Wait until stream is done initializing...

// COMMAND ----------

untilStreamIsReady("SS05-mappedDF_p")

// COMMAND ----------

// MAGIC %md
// MAGIC Stop the streams.

// COMMAND ----------

for (s <- spark.streams.active)  // Iterate over all active streams
  s.stop()          

// COMMAND ----------

// MAGIC %md
// MAGIC Clean up widgets.

// COMMAND ----------

dbutils.widgets.removeAll()

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Next Steps</h2>
// MAGIC 
// MAGIC Start the next lab, [Using Event Hubs Lab]($./Labs/SS 05 - Using Event Hubs Lab).

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Additional Topics &amp; Resources</h2>
// MAGIC 
// MAGIC 
// MAGIC * <a href="https://docs.databricks.com/spark/latest/structured-streaming/streaming-event-hubs.html" target="_blank">Databricks documentation on Azure Event Hubs</a>
// MAGIC * <a href="https://docs.microsoft.com/en-us/azure/event-hubs/event-hubs-about" target="_blank">Microsoft documentation on Azure Event Hubs</a>

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>