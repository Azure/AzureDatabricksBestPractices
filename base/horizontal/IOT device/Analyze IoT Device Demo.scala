// Databricks notebook source
displayHTML(s"""
<h3>
  <img width="200px" src="https://spark.apache.org/images/spark-logo-trademark.png"/> 
  + 
  <img src="http://training.databricks.com/databricks_guide/databricks_logo_400px.png"/>
</h3>
""")

// COMMAND ----------

// MAGIC %md
// MAGIC **Click on ![](http://training.databricks.com/databricks_guide/ImportNotebookIcon3.png) at the top if you want to import this notebook and run the code in your Databricks account.** 

// COMMAND ----------

// MAGIC %md ## How to Analyze IoT Device Data Using Dataset and Spark SQL

// COMMAND ----------

// MAGIC %md Datasets in Apache Spark 2.0 provide high-level domain specific APIs as well as provide structure and compile-time type-safety. You can read your
// MAGIC JSON data file into a DataFrame, a generic row of JVM objects, and convert them into type-specific collection of JVM objects. 
// MAGIC 
// MAGIC In this notebook, we show you how you read a JSON file, convert your semi-structured JSON data into a collection of Datasets[T], and introduce some high-level Spark 2.0 Dataset APIs.

// COMMAND ----------

// MAGIC %md Use the Scala case class *DeviceIoTData* to convert the JSON device data into a Scala object. Of note here is GeoIP information for each device entry:
// MAGIC * IP address
// MAGIC * ISO-3166-1 two and three letter codes
// MAGIC * Country Name
// MAGIC * Latitude and longitude
// MAGIC 
// MAGIC With these attributes as part of the device data, we can map and visualize them as needed. For each IP associated with a *device_id*, I optained the above attributes from a webservice at http://freegeoip.net/csv/ip
// MAGIC 
// MAGIC *{"device_id": 198164, "device_name": "sensor-pad-198164owomcJZ", "ip": "80.55.20.25", "cca2": "PL", "cca3": "POL", "cn": "Poland", "latitude": 53.080000, "longitude": 18.620000, "scale": "Celsius", "temp": 21, "humidity": 65, "battery_level": 8, "c02_level": 1408, "lcd": "red", "timestamp" :1458081226051 }*
// MAGIC 
// MAGIC This dataset is available from Public [S3 bucket](//databricks-public-datasets/data/iot) or
// MAGIC 
// MAGIC from my [Gitbub](https://github.com/dmatrix/examples/blob/master/spark/databricks/notebooks/py/data/iot_devices.json)

// COMMAND ----------

// MAGIC %run ./data_Setup

// COMMAND ----------

// MAGIC %fs ls wasbs://dataset@wesdias.blob.core.windows.net/Azure/dataset/horizontal/iot/

// COMMAND ----------

// MAGIC %fs head wasbs://dataset@wesdias.blob.core.windows.net/Azure/dataset/horizontal/iot/devices.json

// COMMAND ----------

// MAGIC %md ## PART 1: Reading JSON files

// COMMAND ----------

// MAGIC %md Let's create a schema so Spark doesn't have to infer it and reading it is a lot faster

// COMMAND ----------

import org.apache.spark.sql.types._

val jsonSchema = new StructType()
        .add("battery_level", LongType)
        .add("c02_level", LongType)
        .add("cca2", StringType)
        .add("cca3",StringType)
        .add("cn", StringType)
        .add("device_id", LongType)
        .add("device_name", StringType)
        .add("humidity", LongType)
        .add("ip", StringType)
        .add("latitude", DoubleType)
        .add("lcd", StringType)
        .add("longitude", DoubleType)
        .add("scale", StringType)
        .add("temp", LongType)
        .add("timestamp", TimestampType)

// COMMAND ----------

// MAGIC %md Create a Scalal case class to represent your IoT Device Data

// COMMAND ----------

case class DeviceIoTData (battery_level: Long, c02_level: Long, cca2: String, cca3: String, cn: String, device_id: Long, device_name: String, humidity: Long, ip: String, latitude: Double, lcd: String, longitude: Double, scale:String, temp: Long, timestamp: Long)

// COMMAND ----------

// MAGIC %md Import the necessary Encorders for the Tungsten to generate off-heap memory for the Dataset objects of type DeviceIoTData

// COMMAND ----------

import org.apache.spark.sql._

// COMMAND ----------

//fetch the JSON device information uploaded on our S3, now mounted on /mnt/datariders
val jsonFile = "wasbs://dataset@wesdias.blob.core.windows.net/Azure/dataset/horizontal/iot/devices.json"
//read the json file and create the dataset from the case class DeviceIoTData
// ds is now a collection of JVM Scala objects DeviccIoTData
val ds = spark
          .read
          .schema(jsonSchema)
          .json(jsonFile)
          .as[DeviceIoTData]

// COMMAND ----------

//lets cache the dataset
ds.cache()
ds.count()

// COMMAND ----------

// MAGIC %md Displaying your Dataset

// COMMAND ----------

//display Dataset's 
display(ds)

// COMMAND ----------

// MAGIC %md ##PART 2: Analysing IOT data with Dataset API methods

// COMMAND ----------

// MAGIC %md Let's iterate over the first 10 entries with the foreach() method and print them. Notice this is quite similar to RDD but a lot easier.
// MAGIC 
// MAGIC Do keep this tab open in your browser. [Dataset API](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.Dataset)

// COMMAND ----------

ds.take(10).foreach(println(_))

// COMMAND ----------

// MAGIC %md 
// MAGIC 
// MAGIC For all relational expressions, the [Catalyst Optimizer](https://databricks.com/blog/2015/04/13/deep-dive-into-spark-sqls-catalyst-optimizer.html) will formulate an optimized logical and physical plan for execution, and [Tungsten](https://databricks.com/blog/2015/04/28/project-tungsten-bringing-spark-closer-to-bare-metal.html) engine will optimize the generated code. For our *DeviceIoTData*, it will use its standard encoders to optimize its binary internal representation, hence decrease the size of generated code, minimize the bytes transfered over the networks between nodes, and execute faster.
// MAGIC 
// MAGIC For instance, let's first filter the device dataset on *temp* and *humidity* attributes with a predicate and display the first 10 items.
// MAGIC 
// MAGIC Note that filter(), as in RDD APis, returns an immutable Dataset.

// COMMAND ----------

// MAGIC %md ####Find out all devices with temperatures excedding 30 and humidity greater than 70

// COMMAND ----------

// issue select, map, filter, foreach operations on the datasets, just as you would for DataFrames
// it returns back a Dataset. Notice how we use DeviceIoTData's fields
val dsTempDS = ds.filter(d => {d.temp > 30 && d.humidity > 70})


// COMMAND ----------

display(dsTempDS)

// COMMAND ----------

// MAGIC %md #### Display the first 10 items from the Datasets in sorted device_id descending order Q1

// COMMAND ----------

display(ds.filter(d => {d.temp > 30 && d.humidity > 70})
  .sort($"device_id".desc)
  .limit(10))

// COMMAND ----------

// MAGIC %md #### use the filter() used in Q2 and select the most hot and humid countries' sensors

// COMMAND ----------

val dsTemp = ds.filter(d=> d.temp > 30 && d.humidity > 70)
             .select("temp", "humidity", "cca3","device_name", "device_id")
             .orderBy("temp", "humidity", "cca3")

// COMMAND ----------

display(dsTemp)

// COMMAND ----------

// MAGIC %md Select individual fields using the Dataset method select() where battery_level is greater than 6, sort in descending order on C02_level and battery_level. Note that this high-level domain specific language API reads like a SQL query

// COMMAND ----------

val sortedDS = ds.select($"battery_level", $"c02_level", $"device_name", $"cca3")
               .where($"battery_level" > 6)
               .sort($"c02_level".desc, $"battery_level".desc)
sortedDS.printSchema

// COMMAND ----------

display(sortedDS)

// COMMAND ----------

// MAGIC %md #### Generate an optimized RDDs based physical plan of execution.

// COMMAND ----------

sortedDS.explain (true)

// COMMAND ----------

// MAGIC %md Apply higher-level Dataset API methods such as groupBy() and avg(). In other words, take all temperatures readings > 25, along with their corresponding devices' humidity, groupBy cca3 country code, and compute averages. 

// COMMAND ----------

// MAGIC %md #### Compute the average humidity and temperature of all the devices in each country whose temperature and humidity are greater than 25 and 75 resepctively

// COMMAND ----------

val dsAvgTmpDS = ds.filter(d => {d.temp > 25 && d.humidity > 75})
                .select("temp", "humidity", "cca3")
                .groupBy($"cca3")
                .avg()
                .sort($"avg(temp)".desc, $"avg(humidity)".desc)
display(dsAvgTmpDS)

// COMMAND ----------

dsAvgTmpDS.explain(true)

// COMMAND ----------

// MAGIC %md ##PART 3: Visualizaing datasets using various display methods and graphs

// COMMAND ----------

// MAGIC %md #### Visualizing datasets

// COMMAND ----------

// MAGIC %md By saving our Dataset as a table and caching it, I can issue complex SQL queries against it and visualize the results, using notebook's myriad plotting options.

// COMMAND ----------

ds.write.saveAsTable("iot_devices")

// COMMAND ----------

// MAGIC %md List the tables and databases

// COMMAND ----------

display(spark.catalog.listDatabases)

// COMMAND ----------

display(spark.catalog.listTables)

// COMMAND ----------

// MAGIC %md Let's cache the table for efficiency

// COMMAND ----------

// MAGIC %sql CACHE TABLE iot_devices

// COMMAND ----------

// MAGIC %md #### Count all devices for a partiular country and display them using Spark SQL
// MAGIC Note: Limit to 100 rows

// COMMAND ----------

//The result of any SQL query is always an immutable Dataset
val sqlDS = spark.sql("select cca3, count(distinct device_id) as device_id from iot_devices group by cca3 order by device_id desc limit 100")
display(sqlDS)

// COMMAND ----------

spark.sql("select cca3, count(distinct device_id) as device_id from iot_devices group by cca3 order by device_id desc limit 100").explain(true)

// COMMAND ----------

// MAGIC %md #### The same results if we use SQL

// COMMAND ----------

// MAGIC %sql select cca3, count(distinct device_id) as device_id from iot_devices group by cca3 order by device_id desc limit 100

// COMMAND ----------

// MAGIC %md #### we issue the same query as Q1 and Q2 using Dataset API?

// COMMAND ----------

// MAGIC %md Import SQL functions that allow you to compute max, aggregate, sum etc.

// COMMAND ----------

import org.apache.spark.sql.functions._

// COMMAND ----------

val dslDS = ds.select("cca3", "device_id")
            .groupBy($"cca3")
            .count()                 // creates a column name "count"
            .distinct
            .orderBy($"count".desc) //we want in a descending order
            .limit(100)

// COMMAND ----------

display(dslDS)

// COMMAND ----------

// MAGIC %md Let's visualize the results as a pie chart and distribution for devices in the country where C02 are high.

// COMMAND ----------

// MAGIC %sql select cca3, c02_level from iot_devices where c02_level > 1400 order by c02_level desc

// COMMAND ----------

// MAGIC %md Select all countries' devices with high-levels of C02 and group by cca3 and order by device_ids 

// COMMAND ----------

// MAGIC %sql select cca3, count(distinct device_id) as device_id from iot_devices where lcd == 'red' group by cca3 order by device_id desc limit 100

// COMMAND ----------

// MAGIC %md Find out all devices in countries whose batteries need replacements 

// COMMAND ----------

// MAGIC %sql select cca3, count(distinct device_id) as device_id from iot_devices where battery_level == 0 group by cca3 order by device_id desc limit 100

// COMMAND ----------

// MAGIC %md ##PART 4: Writing and Reading from Parquet files

// COMMAND ----------

// MAGIC %md Parquet is an efficient columnar file format to save transformed data for columnar analysis. For example, after your initial IoT ETL, you want to save
// MAGIC for other applications down the pipeline to do further analysis, in the data format you have cleaned, Parquet is the recommended file format.

// COMMAND ----------

// MAGIC %md #### Drop columns you don't need, and save it as a parquet file?

// COMMAND ----------

val parquetDS = ds.drop("latitude", "longitude")
display(parquetDS)

// COMMAND ----------

parquetDS.write
   .mode("overwrite")
   .format("parquet").save("/tmp/iotDevicesParquet/")

// COMMAND ----------

// MAGIC %md Spark writes each RDD partition into a separate Parquet file. Our Dataset is divided into 8 RDD low-level partitions.

// COMMAND ----------

parquetDS.rdd.partitions.size

// COMMAND ----------

// MAGIC %fs ls /tmp/iotDevicesParquet

// COMMAND ----------

// MAGIC %md ####Read back from the Parquet file. 

// COMMAND ----------

//reading back is simple. Use the SparkSession object.
val parquetIotDevicesDS = spark.read.parquet("/tmp/iotDevicesParquet")

// COMMAND ----------

display(parquetIotDevicesDS)

// COMMAND ----------

// MAGIC %md ### PART 5: Writing your own High-level functions for notification

// COMMAND ----------

// MAGIC %md Create a class that sends alerts. For now we can send it to driver log. But in real application, you might want to send to the NOC, using your
// MAGIC own notification mechanisms.

// COMMAND ----------

object DeviceAlerts {

  def sendTwilio(message: String): Unit = {
    //TODO: fill as necessary
    println("Twilio:" + message)
  }

  def sendSNMP(message: String): Unit = {
    //TODO: fill as necessary
    println("SNMP:" + message)
  }

  def sendPOST(message: String): Unit = {
    //TODO: fill as necessary
    println("HTTP POST:" + message)
  }

  def publishOnConfluent(message:String): Unit = {
    //TODO: fill as necessary
    println("Kafka Topic 'DeviceAlerts':" + message)
  }

  def publishOnPubNub(message: String): Unit = {
    //TODO: fill as necessary
    println("PubNub Channel 'DeviceAlerts':" + message)
  }
}

def logAlerts(log: java.io.PrintStream = Console.out, device_name: String, device_id: Long, cca3:String, alert: String, notify: String ="kafka"): Unit = {
  
  val message = "[***ALERT***: %s : device_name: %s; device_id: %s ; cca3: %s]" format(alert, device_name, device_id, cca3)
  //default log to Stderr
  log.println(message)
  // use an appropriate notification method
  val notifyFunc = notify match {
      case "twilio" => DeviceAlerts.sendTwilio _
      case "snmp" => DeviceAlerts.sendSNMP _
      case "post" => DeviceAlerts.sendPOST _
      case "kafka" => DeviceAlerts.publishOnConfluent _
      case "pubnub" => DeviceAlerts.publishOnPubNub _
  }
  //send the appropriate alert
  notifyFunc(message)
}

// COMMAND ----------

// MAGIC %md Find out all devices whose batteries are dying and notify the NOC

// COMMAND ----------

val dsBatteryZero = ds.filter(d => {d.battery_level <= 1})
//iterate over the filtered dataset and send an alert
dsBatteryZero.foreach(d=> {logAlerts(Console.err, d.device_name, d.device_id, d.cca3, "REPLACE DEVICE BATTERY", "twilio")} )
println("**** Total Devices with Bad Batteries **** = " + dsBatteryZero.count())

// COMMAND ----------

// MAGIC %md Converting a Dataset to RDDs.
// MAGIC 
// MAGIC Let's find all sensor devices whose C02 levels are high

// COMMAND ----------

val deviceEventsDS = ds.select($"device_name",$"cca3", $"c02_level").where($"c02_level" > 1300).limit(10)
display(deviceEventsDS)

// COMMAND ----------

// convert to RDDs
val eventsRDD = deviceEventsDS.rdd.take(10).foreach(println(_))