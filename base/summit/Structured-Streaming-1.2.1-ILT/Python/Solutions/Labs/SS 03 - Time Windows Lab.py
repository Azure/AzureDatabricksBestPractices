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
# MAGIC # Working with Time Windows Lab
# MAGIC 
# MAGIC ## Instructions
# MAGIC * Insert solutions wherever it says `FILL_IN`
# MAGIC * Feel free to copy/paste code from the previous notebook, where applicable
# MAGIC * Run test cells to verify that your solution is correct

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Getting Started</h2>
# MAGIC 
# MAGIC Run the following cell to configure our "classroom."

# COMMAND ----------

# MAGIC %run "../Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Exercise 1: Read data into a stream</h2>
# MAGIC 
# MAGIC The dataset used in this exercise consists of flight information about flights from/to various airports in 2007.
# MAGIC 
# MAGIC You have already seen this dataset in Exercise 1 of Notebook 02.
# MAGIC 
# MAGIC To refresh your memory, take a look at the first few lines of the dataset.

# COMMAND ----------

display(
  spark.read.parquet("dbfs:/mnt/training/asa/flights/2007-01-stream.parquet/part-00000-tid-9167815511861375854-22d81a30-d5b4-43d0-9216-0c20d14c3f54-178-c000.snappy.parquet")
)

# COMMAND ----------

# MAGIC %md
# MAGIC For this exercise you will need to complete the following tasks:
# MAGIC 0. Start a stream that reads parquet files dumped to the directory `dataPath`
# MAGIC 0. Control the size of each partition by forcing Spark to processes only 1 file per trigger.
# MAGIC 
# MAGIC Other notes:
# MAGIC 0. The source data has already been defined as `dataPath`
# MAGIC 0. The schema has already be defined as `parquetSchema`

# COMMAND ----------

# ANSWER
dataPath = "/mnt/training/asa/flights/2007-01-stream.parquet/"

parquetSchema = "DepartureAt timestamp, FlightDate string, DepTime string, CRSDepTime string, ArrTime string, CRSArrTime string, UniqueCarrier string, FlightNum integer, TailNum string, ActualElapsedTime string, CRSElapsedTime string, AirTime string, ArrDelay string, DepDelay string, Origin string, Dest string, Distance string, TaxiIn string, TaxiOut string, Cancelled integer, CancellationCode string, Diverted integer, CarrierDelay string, WeatherDelay string, NASDelay string, SecurityDelay string, LateAircraftDelay string"
  
# Configure the shuffle partitions to match the number of cores  
spark.conf.set("spark.sql.shuffle.partitions", sc.defaultParallelism)

streamDF = (spark                   # Start with the SparkSesion
  .readStream                       # Get the DataStreamReader
  .format("parquet")                # Configure the stream's source for the appropriate file type
  .schema(parquetSchema)            # Specify the parquet files' schema
  .option("maxFilesPerTrigger", 1)  # Restrict Spark to processing only 1 file per trigger
  .load(dataPath)                   # Load the DataFrame specifying its location as dataPath
)

# COMMAND ----------

# TEST - Run this cell to test your solution.
schemaStr = str(streamDF.schema)

dbTest("SS-03-shuffles",  sc.defaultParallelism, spark.conf.get("spark.sql.shuffle.partitions"))

dbTest("SS-03-schema-1",  True, "(DepartureAt,TimestampType,true)" in schemaStr)
dbTest("SS-03-schema-2",  True, "(FlightDate,StringType,true)" in schemaStr)
dbTest("SS-03-schema-3",  True, "(DepTime,StringType,true)" in schemaStr)
dbTest("SS-03-schema-4",  True, "(CRSDepTime,StringType,true)" in schemaStr)
dbTest("SS-03-schema-5",  True, "(ArrTime,StringType,true)" in schemaStr)
dbTest("SS-03-schema-6",  True, "(CRSArrTime,StringType,true)" in schemaStr)
dbTest("SS-03-schema-7",  True, "(UniqueCarrier,StringType,true)" in schemaStr)
dbTest("SS-03-schema-8",  True, "(FlightNum,IntegerType,true)" in schemaStr)
dbTest("SS-03-schema-9",  True, "(TailNum,StringType,true)" in schemaStr)
dbTest("SS-03-schema-10",  True, "(ActualElapsedTime,StringType,true)" in schemaStr)
dbTest("SS-03-schema-11",  True, "(CRSElapsedTime,StringType,true)" in schemaStr)
dbTest("SS-03-schema-12",  True, "(AirTime,StringType,true)" in schemaStr)
dbTest("SS-03-schema-13",  True, "(ArrDelay,StringType,true)" in schemaStr)
dbTest("SS-03-schema-14",  True, "(DepDelay,StringType,true)" in schemaStr)
dbTest("SS-03-schema-15",  True, "(Origin,StringType,true)" in schemaStr)
dbTest("SS-03-schema-16",  True, "(Dest,StringType,true)" in schemaStr)
dbTest("SS-03-schema-17",  True, "(Distance,StringType,true)" in schemaStr)
dbTest("SS-03-schema-18",  True, "(TaxiIn,StringType,true)" in schemaStr)
dbTest("SS-03-schema-19",  True, "(TaxiOut,StringType,true)" in schemaStr)
dbTest("SS-03-schema-20",  True, "(Cancelled,IntegerType,true)" in schemaStr)
dbTest("SS-03-schema-21",  True, "(CancellationCode,StringType,true)" in schemaStr)
dbTest("SS-03-schema-22",  True, "(Diverted,IntegerType,true)" in schemaStr)
dbTest("SS-03-schema-23",  True, "(CarrierDelay,StringType,true)" in schemaStr)
dbTest("SS-03-schema-24",  True, "(WeatherDelay,StringType,true)" in schemaStr)
dbTest("SS-03-schema-25",  True, "(NASDelay,StringType,true)" in schemaStr)
dbTest("SS-03-schema-26",  True, "(SecurityDelay,StringType,true)" in schemaStr)
dbTest("SS-03-schema-27",  True, "(LateAircraftDelay,StringType,true)" in schemaStr)

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Exercise 2: Plot grouped events</h2>
# MAGIC 
# MAGIC Plot the count of all flights aggregated by a 30 minute window and `UniqueCarrier`. 
# MAGIC 
# MAGIC Ignore any events delayed by 300 minutes or more.
# MAGIC 
# MAGIC You will need to:
# MAGIC 0. Use a watermark to discard events not received within 300 minutes
# MAGIC 0. Configure the stream for a 30 minute sliding window
# MAGIC 0. Aggregate by the 30 minute window and the column `UniqueCarrier`
# MAGIC 0. Add the column `start` by extracting it from `window.start`
# MAGIC 0. Sort the stream by `start`
# MAGIC 
# MAGIC In order to create a LIVE bar chart of the data, you'll need to specify the following <b>Plot Options</b>:
# MAGIC * **Keys** is set to `start`
# MAGIC * **Series groupings** is set to `UniqueCarrier`
# MAGIC * **Values** is set to `count`

# COMMAND ----------

# ANSWER
from pyspark.sql.functions import hour, window, col

countsDF = (streamDF                                             # Start with the DataFrame
  .withWatermark("DepartureAt", "300 minutes")                   # Specify the watermark
  .groupBy(window("DepartureAt", "30 minute"), "UniqueCarrier" ) # Aggregate the data
  .count()                                                       # Produce a count for each aggreate
  .withColumn("start", col("window.start"))                      # Add the column "start", extracting it from "window.start"
  .orderBy("start")                                              # Sort the stream by "start" 
)

display(countsDF)

# COMMAND ----------

# TEST - Run this cell to test your solution.
schemaStr = str(countsDF.schema)

dbTest("SS-03-schema-1",  True, "(UniqueCarrier,StringType,true)" in schemaStr)
dbTest("SS-03-schema-2",  True, "(count,LongType,false)" in schemaStr)
dbTest("SS-03-schema-5",  True, "(start,TimestampType,true)" in schemaStr)

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC Wait until stream is done initializing...

# COMMAND ----------

untilStreamIsReady("delta_1p")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Exercise 3: Stop streaming jobs</h2>
# MAGIC 
# MAGIC Before we can conclude, we need to shut down all active streams.

# COMMAND ----------

# ANSWER
for s in spark.streams.active: # Iterate over all the active streams
  s.stop()                     # Stop the stream

# COMMAND ----------

# TEST - Run this cell to test your solution.
dbTest("SS-03-numActiveStreams", 0, len(spark.streams.active))

print("Tests passed!")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Next Steps</h2>
# MAGIC 
# MAGIC Start the next lesson, [Using Kafka]($../SS 04 - Using Kafka) **OR** [Using Event Hubs]($../SS 05 - Using Event Hubs).<br/>
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Both notebooks are functionally identical with the exception of using different sources.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>