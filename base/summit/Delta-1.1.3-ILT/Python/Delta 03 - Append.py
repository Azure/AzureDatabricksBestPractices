# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Databricks Delta Batch Operations - Append
# MAGIC 
# MAGIC Databricks&reg; Delta allows you to read, write and query data in data lakes in an efficient manner.
# MAGIC 
# MAGIC ## In this lesson you:
# MAGIC * Append new records to a Databricks Delta table
# MAGIC 
# MAGIC ## Audience
# MAGIC * Primary Audience: Data Engineers 
# MAGIC * Secondary Audience: Data Analysts and Data Scientists
# MAGIC 
# MAGIC ## Prerequisites
# MAGIC * Web browser: current versions of Google Chrome, Firefox, Safari, Microsoft Edge and 
# MAGIC Internet Explorer 11 on Windows 7, 8, or 10 (see <a href="https://docs.databricks.com/user-guide/supported-browsers.html#supported-browsers#" target="_blank">Supported Web Browsers</a>)
# MAGIC * Databricks Runtime 4.2 or greater
# MAGIC * Completed courses Spark-SQL, DataFrames or ETL-Part 1 from <a href="https://academy.databricks.com/" target="_blank">Databricks Academy</a>, or have similar knowledge
# MAGIC 
# MAGIC ## Datasets Used
# MAGIC We will use online retail datasets from
# MAGIC * `/mnt/training/online_retail` in the demo part and
# MAGIC * `/mnt/training/structured-streaming/events/` in the exercises

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

miniDataInputPath = "/mnt/training/online_retail/outdoor-products/outdoor-products-mini.csv"

basePath         = userhome + "/delta/python"
genericDataPath  = basePath + "/generic-data/"
deltaDataPath    = basePath + "/customer-data/"
deltaIotPath     = basePath + "/iot-pipeline/"

# Configure our shuffle partitions for these exercises
spark.conf.set("spark.sql.shuffle.partitions", 8)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Here, we add new data to the consumer product data.
# MAGIC 
# MAGIC Before we load data into non-Databricks Delta and Databricks Delta tables, do a simple pre-processing step:
# MAGIC 
# MAGIC * The column `StockCode` should be of type `String`.

# COMMAND ----------

newDataDF = (spark.read
  .option("header", "true")
  .schema("InvoiceNo INT, StockCode STRING, Description STRING, Quantity INT, InvoiceDate STRING, UnitPrice DOUBLE, CustomerID INT, Country STRING")
  .csv(miniDataInputPath)
)

# COMMAND ----------

# MAGIC %md
# MAGIC Do a simple count of number of new items to be added to production data.

# COMMAND ----------

newDataDF.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## APPEND Using Non-Databricks Delta pipeline
# MAGIC Append to the production table.
# MAGIC 
# MAGIC In the next cell, load the new data in `parquet` format and save to `../generic/customer-data/`.

# COMMAND ----------

(newDataDF
  .write
  .format("parquet")
  .partitionBy("Country")
  .mode("append")
  .save(genericDataPath)
)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC We expect to see `65499 + 36 = 65535` rows, but we do not.
# MAGIC 
# MAGIC We may even see an error message.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM customer_data

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC Strange: we got a count we were not expecting!
# MAGIC 
# MAGIC This is the <b>schema on read</b> problem. It means that as soon as you put data into a data lake, 
# MAGIC the schema is unknown <i>until</i> you perform a read operation.
# MAGIC 
# MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> Repair the table again and count the number of records.

# COMMAND ----------

# MAGIC %sql
# MAGIC MSCK REPAIR TABLE customer_data;
# MAGIC 
# MAGIC SELECT count(*) FROM customer_data

# COMMAND ----------

# MAGIC %md
# MAGIC ## APPEND Using Databricks Delta Pipeline
# MAGIC 
# MAGIC Next, repeat the process by writing to Databricks Delta format. 
# MAGIC 
# MAGIC In the next cell, load the new data in Databricks Delta format and save to `../delta/customer-data/`.

# COMMAND ----------

(newDataDF
  .write
  .format("delta")
  .partitionBy("Country")
  .mode("append")
  .save(deltaDataPath)
)

# COMMAND ----------

# MAGIC %md
# MAGIC Perform a simple `count` query to verify the number of records and notice it is correct and does not first require a table repair.
# MAGIC 
# MAGIC Should be `65535`.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM customer_data_delta

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 1
# MAGIC 
# MAGIC 0. Read the JSON data under `streamingEventPath` into a DataFrame
# MAGIC 0. Add a `date` column using `from_unixtime(col("time").cast('String'),'MM/dd/yyyy').cast("date"))`
# MAGIC 0. Add a `deviceId` column consisting of random numbers from 0 to 99 using this expression `expr("cast(rand(5) * 100 as int)")`
# MAGIC 0. Use the `repartition` method to split the data into 200 partitions
# MAGIC 
# MAGIC Refer to  <a href="http://spark.apache.org/docs/2.1.0/api/python/pyspark.sql.html#" target="_blank">Pyspark function documentation</a>.

# COMMAND ----------

# TODO
from pyspark.sql.functions import expr, col, from_unixtime, to_date
streamingEventPath = "/mnt/training/structured-streaming/events/"

rawDataDF = (spark
 .read 
  FILL_IN
 .repartition(200)

# COMMAND ----------

# TEST - Run this cell to test your solution.

schema = str(rawDataDF.schema)
dbTest("assert-1", True, "action,StringType" in schema)
dbTest("assert-2", True, "time,LongType" in schema)
dbTest("assert-3", True, "date,DateType" in schema)
dbTest("assert-4", True, "deviceId,IntegerType" in schema)

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 2
# MAGIC 
# MAGIC Write out the raw data in Databricks Delta format to `/delta/iot-pipeline/` and create a Databricks Delta table called `demo_iot_data_delta`.
# MAGIC 
# MAGIC Remember to
# MAGIC * partition by `date`
# MAGIC * save to `deltaIotPath`

# COMMAND ----------

# TODO
(rawDataDF
 .write
 .mode("overwrite")
  FILL_IN

spark.sql("""
   DROP TABLE IF EXISTS demo_iot_data_delta
 """)

spark.sql("""
   CREATE TABLE demo_iot_data_delta
   FILL_IN

# COMMAND ----------

# TEST - Run this cell to test your solution.
  
tables = map(lambda t: t.name, spark.catalog.listTables())
dbTest("Delta-03-tableExists", True, "demo_iot_data_delta" in tables)  

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 3
# MAGIC 
# MAGIC Create a new DataFrame with columns `action`, `time`, `date` and `deviceId`. The columns contain the following data:
# MAGIC 
# MAGIC * `action` contains the value `Open`
# MAGIC * `time` contains the Unix time cast into a long integer `cast(1529091520 as bigint)`
# MAGIC * `date` contains `cast('2018-06-01' as date)`
# MAGIC * `deviceId` contains a random number from 0 to 499 given by `expr("cast(rand(5) * 500 as int)")`

# COMMAND ----------

# TODO
from pyspark.sql.functions import expr

newDataDF = (spark.range(10000) 
  .repartition(200)
  .selectExpr("'Open' as action", FILL_IN) 
  .FILL_IN

# COMMAND ----------

# TEST - Run this cell to test your solution.
total = newDataDF.count()

dbTest("Delta-03-newDataDF-count", 10000, total)

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Exercise 4
# MAGIC 
# MAGIC Append new data to `demo_iot_data_delta`.
# MAGIC 
# MAGIC * Use `append` mode
# MAGIC * Save to `deltaIotPath`

# COMMAND ----------

# TODO
(newDataDF
 .write
FILL_IN

# COMMAND ----------

# TEST - Run this cell to test your solution.
numFiles = spark.sql("SELECT count(*) as total FROM demo_iot_data_delta").collect()[0][0]

dbTest("Delta-03-numFiles", 110000 , numFiles)

print("Tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC With Databricks Delta, you can easily append new data without schema-on-read issues.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Topics & Resources
# MAGIC 
# MAGIC * <a href="https://docs.databricks.com/delta/delta-batch.html#" target="_blank">Table Batch Read and Writes</a>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>