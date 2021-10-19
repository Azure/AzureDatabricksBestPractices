// Databricks notebook source
// MAGIC 
// MAGIC %md-sandbox
// MAGIC 
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC # Reading Data - CSV Files
// MAGIC 
// MAGIC **Technical Accomplishments:**
// MAGIC - Start working with the API documentation
// MAGIC - Introduce the class `SparkSession` and other entry points
// MAGIC - Introduce the class `DataFrameReader`
// MAGIC - Read data from:
// MAGIC   * CSV without a Schema.
// MAGIC   * CSV with a Schema.

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Getting Started
// MAGIC 
// MAGIC Run the following cell to configure our "classroom."

// COMMAND ----------

// MAGIC %run "../Includes/Classroom Setup"

// COMMAND ----------

// MAGIC %run "../Includes/Utility-Methods"

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Entry Points
// MAGIC 
// MAGIC Our entry point for Spark 2.0 applications is the class `SparkSession`.
// MAGIC 
// MAGIC An instance of this object is already instantiated for us which can be easily demonstrated by running the next cell:

// COMMAND ----------

println(spark)

// COMMAND ----------

// MAGIC %md
// MAGIC It's worth noting that in Spark 2.0 `SparkSession` is a replacement for the other entry points:
// MAGIC * `SparkContext`, available in our notebook as **sc**.
// MAGIC * `SQLContext`, or more specifically it's subclass `HiveContext`, available in our notebook as **sqlContext**.

// COMMAND ----------

println(sc)
println(sqlContext)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Before we can dig into the functionality of the `SparkSession` class, we need to know how to access the API documentation for Apache Spark.

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Spark API

// COMMAND ----------

// MAGIC %md
// MAGIC ### **Spark API Home Page**
// MAGIC 0. Open a new browser tab.
// MAGIC 0. Google for **Spark API Latest** or **Spark API _x.x.x_** for a specific version.
// MAGIC 0. Select **Spark API Documentation - Spark _x.x.x_ Documentation - Apache Spark**. 
// MAGIC 0. Which set of documentation you will use depends on which language you will use.
// MAGIC 
// MAGIC Other Documentation:
// MAGIC * Programming Guides for DataFrames, SQL, Graphs, Machine Learning, Streaming...
// MAGIC * Deployment Guides for Spark Standalone, Mesos, Yarn...
// MAGIC * Configuration, Monitoring, Tuning, Security...
// MAGIC 
// MAGIC Here are some shortcuts
// MAGIC   * <a href="https://spark.apache.org/docs/latest/api.html" target="_blank">Spark API Documentation - Latest</a>
// MAGIC   * <a href="https://spark.apache.org/docs/2.2.0/api.html" target="_blank">Spark API Documentation - 2.2.0</a>
// MAGIC   * <a href="https://spark.apache.org/docs/2.1.1/api.html" target="_blank">Spark API Documentation - 2.1.1</a>
// MAGIC   * <a href="https://spark.apache.org/docs/2.0.2/api.html" target="_blank">Spark API Documentation - 2.0.2</a>
// MAGIC   * <a href="https://spark.apache.org/docs/1.6.3/api.html" target="_blank">Spark API Documentation - 1.6.3</a>

// COMMAND ----------

// MAGIC %md
// MAGIC ### Spark API (Scala)
// MAGIC 
// MAGIC 0. Select **Spark Scala API (Scaladoc)**.
// MAGIC 0. Look up the documentation for `org.apache.spark.sql.SparkSession`.
// MAGIC   0. In the upper-left-hand-corner type **SparkSession** into the search field.
// MAGIC   0. The search will execute automatically.
// MAGIC   0. In the class/package list, click on **SparkSession**.
// MAGIC   0. The documentation should open in the right-hand pane.

// COMMAND ----------

// MAGIC %md
// MAGIC ### Spark API (Python)
// MAGIC 
// MAGIC 0. Select **Spark Python API (Sphinx)**.
// MAGIC 0. Look up the documentation for `pyspark.sql.SparkSession`.
// MAGIC   0. In the lower-left-hand-corner type **SparkSession** into the search field.
// MAGIC   0. Hit **[Enter]**.
// MAGIC   0. The search results should appear in the right-hand pane.
// MAGIC   0. Click on **pyspark.sql.SparkSession (Python class, in pyspark.sql module)**
// MAGIC   0. The documentation should open in the right-hand pane.

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) SparkSession
// MAGIC 
// MAGIC Quick function review:
// MAGIC * `createDataSet(..)`
// MAGIC * `createDataFrame(..)`
// MAGIC * `emptyDataSet(..)`
// MAGIC * `emptyDataFrame(..)`
// MAGIC * `range(..)`
// MAGIC * `read(..)`
// MAGIC * `readStream(..)`
// MAGIC * `sparkContext(..)`
// MAGIC * `sqlContext(..)`
// MAGIC * `sql(..)`
// MAGIC * `streams(..)`
// MAGIC * `table(..)`
// MAGIC * `udf(..)`
// MAGIC 
// MAGIC The function we are most interested in is `SparkSession.read()` which returns a `DataFrameReader`.

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) DataFrameReader
// MAGIC 
// MAGIC Look up the documentation for `DataFrameReader`.
// MAGIC 
// MAGIC Quick function review:
// MAGIC * `csv(path)`
// MAGIC * `jdbc(url, table, ..., connectionProperties)`
// MAGIC * `json(path)`
// MAGIC * `format(source)`
// MAGIC * `load(path)`
// MAGIC * `orc(path)`
// MAGIC * `parquet(path)`
// MAGIC * `table(tableName)`
// MAGIC * `text(path)`
// MAGIC * `textFile(path)`
// MAGIC 
// MAGIC Configuration methods:
// MAGIC * `option(key, value)`
// MAGIC * `options(map)`
// MAGIC * `schema(schema)`

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Reading from CSV w/InferSchema
// MAGIC 
// MAGIC We are going to start by reading in a very simple text file.

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### The Data Source
// MAGIC * For this exercise, we will be using a tab-separated file called **pageviews_by_second.tsv** (255 MB file from Wikipedia)
// MAGIC * We can use **&percnt;fs ls ...** to view the file on the DBFS.

// COMMAND ----------

// MAGIC %fs ls "dbfs:/mnt/training/wikipedia/pageviews"

// COMMAND ----------

// MAGIC %md
// MAGIC We can use **&percnt;fs head ...** to peek at the first couple thousand characters of the file.

// COMMAND ----------

// MAGIC %fs head "dbfs:/mnt/training/wikipedia/pageviews/pageviews_by_second.tsv"

// COMMAND ----------

// MAGIC %md
// MAGIC There are a couple of things to note here:
// MAGIC * The file has a header.
// MAGIC * The file is tab separated (we can infer that from the file extension and the lack of other characters between each "column").
// MAGIC * The first two columns are strings and the third is a number.
// MAGIC 
// MAGIC Knowing those details, we can read in the "CSV" file.

// COMMAND ----------

// MAGIC %md
// MAGIC ### Step #1 - Read The CSV File
// MAGIC Let's start with the bare minimum by specifying the tab character as the delimiter and the location of the file:

// COMMAND ----------

// A reference to our tab-seperated-file
val csvFile = "/mnt/training/wikipedia/pageviews/pageviews_by_second.tsv"

var tempDF = spark.read       // The DataFrameReader
  .option("sep", "\t")        // Use tab delimiter (default is comma-separator)
  .csv(csvFile)               // Creates a DataFrame from CSV after reading in the file

// COMMAND ----------

// MAGIC %md
// MAGIC This is guaranteed to <u>trigger one job</u>.
// MAGIC 
// MAGIC A *Job* is triggered anytime we are "physically" __required to touch the data__.
// MAGIC 
// MAGIC In some cases, __one action may create multiple jobs__ (multiple reasons to touch the data).
// MAGIC 
// MAGIC In this case, the reader has to __"peek" at the first line__ of the file to determine how many columns of data we have.

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC We can see the structure of the `DataFrame` by executing the command `printSchema()`
// MAGIC 
// MAGIC It prints to the console the name of each column, its data type and if it's null or not.
// MAGIC 
// MAGIC ** *Note:* ** *We will be covering the other `DataFrame` functions in other notebooks.*

// COMMAND ----------

tempDF.printSchema()

// COMMAND ----------

// MAGIC %md
// MAGIC We can see from the schema that...
// MAGIC * there are three columns
// MAGIC * the column names **_c0**, **_c1**, and **_c2** (automatically generated names)
// MAGIC * all three columns are **strings**
// MAGIC * all three columns are **nullable**
// MAGIC 
// MAGIC And if we take a quick peek at the data, we can see that line #1 contains the headers and not data:

// COMMAND ----------

display(tempDF)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Step #2 - Use the File's Header
// MAGIC Next, we can add an option that tells the reader that the data contains a header and to use that header to determine our column names.
// MAGIC 
// MAGIC ** *NOTE:* ** *We know we have a header based on what we can see in "head" of the file from earlier.*

// COMMAND ----------

spark.read                    // The DataFrameReader
  .option("sep", "\t")        // Use tab delimiter (default is comma-separator)
  .option("header", "true")   // Use first line of all files as header
  .csv(csvFile)               // Creates a DataFrame from CSV after reading in the file
  .printSchema()

// COMMAND ----------

// MAGIC %md
// MAGIC A couple of notes about this iteration:
// MAGIC * again, only one job
// MAGIC * there are three columns
// MAGIC * all three columns are **strings**
// MAGIC * all three columns are **nullable**
// MAGIC * the column names are specified: **timestamp**, **site**, and **requests** (the change we were looking for)
// MAGIC 
// MAGIC A "peek" at the first line of the file is all that the reader needs to determine the number of columns and the name of each column.
// MAGIC 
// MAGIC Before going on, make a note of the duration of the previous call - it should be just under 3 seconds.

// COMMAND ----------

// MAGIC %md
// MAGIC ### Step #3 - Infer the Schema
// MAGIC 
// MAGIC Lastly, we can add an option that tells the reader to infer each column's data type (aka the schema)

// COMMAND ----------

spark.read                        // The DataFrameReader
  .option("header", "true")       // Use first line of all files as header
  .option("sep", "\t")            // Use tab delimiter (default is comma-separator)
  .option("inferSchema", "true")  // Automatically infer data types
  .csv(csvFile)                   // Creates a DataFrame from CSV after reading in the file
  .printSchema()

// COMMAND ----------

// MAGIC %md
// MAGIC ### Review: Reading CSV w/InferSchema
// MAGIC * we still have three columns
// MAGIC * all three columns are still **nullable**
// MAGIC * all three columns have their proper names
// MAGIC * two jobs were executed (not one as in the previous example)
// MAGIC * our three columns now have distinct data types:
// MAGIC   * **timestamp** == **timestamp**
// MAGIC   * **site** == **string**
// MAGIC   * **requests** == **integer**
// MAGIC 
// MAGIC **Question:** Why were there two jobs?
// MAGIC 
// MAGIC **Question:** How long did the last job take?
// MAGIC 
// MAGIC **Question:** Why did it take so much longer?
// MAGIC 
// MAGIC Discuss...

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Reading from CSV w/User-Defined Schema
// MAGIC 
// MAGIC This time we are going to read the same file.
// MAGIC 
// MAGIC The difference here is that we are going to define the schema beforehand and hopefully avoid the execution of any extra jobs.

// COMMAND ----------

// MAGIC %md
// MAGIC ### Step #1
// MAGIC Declare the schema.
// MAGIC 
// MAGIC This is just a list of field names and data types.

// COMMAND ----------

// Required for StructField, StringType, IntegerType, etc.
import org.apache.spark.sql.types._

val csvSchema = StructType(
  List(
    StructField("timestamp", StringType, false),
    StructField("site", StringType, false),
    StructField("requests", IntegerType, false)
  )
)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Step #2
// MAGIC Read in our data (and print the schema).
// MAGIC 
// MAGIC We can specify the schema, or rather the `StructType`, with the `schema(..)` command:

// COMMAND ----------

spark.read                    // The DataFrameReader
  .option("header", "true")   // Ignore line #1 - it's a header
  .option("sep", "\t")        // Use tab delimiter (default is comma-separator)
  .schema(csvSchema)          // Use the specified schema
  .csv(csvFile)               // Creates a DataFrame from CSV after reading in the file
  .printSchema()

// COMMAND ----------

// MAGIC %md
// MAGIC ### Review: Reading CSV w/ User-Defined Schema
// MAGIC * We still have three columns
// MAGIC * All three columns are **NOT** nullable because we declared them as such.
// MAGIC * All three columns have their proper names
// MAGIC * Zero jobs were executed
// MAGIC * Our three columns now have distinct data types:
// MAGIC   * **timestamp** == **string**
// MAGIC   * **site** == **string**
// MAGIC   * **requests** == **integer**
// MAGIC 
// MAGIC **Question:** Why were there no jobs?
// MAGIC 
// MAGIC **Question:** What is different about the data types of these columns compared to the previous exercise & why?
// MAGIC 
// MAGIC **Question:** Do I need to indicate that the file has a header?
// MAGIC 
// MAGIC **Question:** Do the declared column names need to match the columns in the header of the TSV file?
// MAGIC 
// MAGIC Discuss...

// COMMAND ----------

// MAGIC %md
// MAGIC For a list of all the options related to reading CSV files, please see the documentation for `DataFrameReader.csv(..)`

// COMMAND ----------

// MAGIC %md
// MAGIC Let's take a look at some of the other details of the `DataFrame` we just created for comparison sake.

// COMMAND ----------

val csvDF = spark.read
  .option("header", "true")
  .option("sep", "\t")
  .schema(csvSchema)
  .csv(csvFile)

println("Partitions: " + csvDF.rdd.getNumPartitions)
printRecordsPerPartition(csvDF)
println("-"*80)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Next Steps
// MAGIC 
// MAGIC * Reading Data #1 - CSV
// MAGIC * [Reading Data #2 - Parquet]($./Reading Data 2 - Parquet)
// MAGIC * [Reading Data #3 - Tables]($./Reading Data 3 - Tables)
// MAGIC * [Reading Data #4 - JSON]($./Reading Data 4 - JSON)
// MAGIC * [Reading Data #5 - Text]($./Reading Data 5 - Text)
// MAGIC * [Reading Data #6 - JDBC]($./Reading Data 6 - JDBC)
// MAGIC * [Reading Data #7 - Summary]($./Reading Data 7 - Summary)

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>