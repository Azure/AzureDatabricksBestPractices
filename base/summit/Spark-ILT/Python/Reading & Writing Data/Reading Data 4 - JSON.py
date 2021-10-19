# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Reading Data - JSON Files
# MAGIC 
# MAGIC **Technical Accomplishments:**
# MAGIC - Read data from:
# MAGIC   * JSON without a Schema
# MAGIC   * JSON with a Schema

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Getting Started
# MAGIC 
# MAGIC Run the following cell to configure our "classroom."

# COMMAND ----------

# MAGIC %run "../Includes/Classroom Setup"

# COMMAND ----------

# MAGIC %run "../Includes/Utility-Methods"

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Reading from JSON w/ InferSchema
# MAGIC 
# MAGIC Reading in JSON isn't that much different than reading in CSV files.
# MAGIC 
# MAGIC Let's start with taking a look at all the different options that go along with reading in JSON files.

# COMMAND ----------

# MAGIC %md
# MAGIC ### JSON Lines
# MAGIC 
# MAGIC Much like the CSV reader, the JSON reader also assumes...
# MAGIC * That there is one JSON object per line and...
# MAGIC * That it's delineated by a new-line.
# MAGIC 
# MAGIC This format is referred to as **JSON Lines** or **newline-delimited JSON** 
# MAGIC 
# MAGIC More information about this format can be found at <a href="http://jsonlines.org/" target="_blank">http://jsonlines.org</a>.
# MAGIC 
# MAGIC ** *Note:* ** *Spark 2.2 was released on July 11th 2016. With that comes File IO improvements for CSV & JSON, but more importantly, **Support for parsing multi-line JSON and CSV files**. You can read more about that (and other features in Spark 2.2) in the <a href="https://databricks.com/blog/2017/07/11/introducing-apache-spark-2-2.html" target="_blank">Databricks Blog</a>.*

# COMMAND ----------

# MAGIC %md
# MAGIC ### The Data Source
# MAGIC * For this exercise, we will be using the file called **snapshot-2016-05-26.json** (<a href="https://wikitech.wikimedia.org/wiki/Stream.wikimedia.org/rc" target="_blank">4 MB</a> file from Wikipedia).
# MAGIC * The data represents a set of edits to Wikipedia articles captured in May of 2016.
# MAGIC * It's located on the DBFS at **dbfs:/mnt/training/wikipedia/edits/snapshot-2016-05-26.json**
# MAGIC * Like we did with the CSV file, we can use **&percnt;fs ls ...** to view the file on the DBFS.

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/training/wikipedia/edits/snapshot-2016-05-26.json

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Like we did with the CSV file, we can use **&percnt;fs head ...** to peek at the first couple lines of the JSON file.

# COMMAND ----------

# MAGIC %fs head dbfs:/mnt/training/wikipedia/edits/snapshot-2016-05-26.json

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read The JSON File
# MAGIC 
# MAGIC The command to read in JSON looks very similar to that of CSV.
# MAGIC 
# MAGIC In addition to reading the JSON file, we will also print the resulting schema.

# COMMAND ----------

jsonFile = "dbfs:/mnt/training/wikipedia/edits/snapshot-2016-05-26.json"

wikiEditsDF = (spark.read           # The DataFrameReader
    .option("inferSchema", "true")  # Automatically infer data types & column names
    .json(jsonFile)                 # Creates a DataFrame from JSON after reading in the file
 )
wikiEditsDF.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC With our DataFrame created, we can now take a peak at the data.
# MAGIC 
# MAGIC But to demonstrate a unique aspect of JSON data (or any data with embedded fields), we will first create a temporary view and then view the data via SQL:

# COMMAND ----------

# create a view called wiki_edits
wikiEditsDF.createOrReplaceTempView("wiki_edits")

# COMMAND ----------

# MAGIC %md
# MAGIC And now we can take a peak at the data with simple SQL SELECT statement:

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM wiki_edits 

# COMMAND ----------

# MAGIC %md
# MAGIC Notice the **geocoding** column has embedded data.
# MAGIC 
# MAGIC You can expand the fields by clicking the right triangle in each row.
# MAGIC 
# MAGIC But we can also reference the sub-fields directly as we see in the following SQL statement:

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT channel, page, geocoding.city, geocoding.latitude, geocoding.longitude 
# MAGIC FROM wiki_edits 
# MAGIC WHERE geocoding.city IS NOT NULL

# COMMAND ----------

# MAGIC %md
# MAGIC ### Review: Reading from JSON w/ InferSchema
# MAGIC 
# MAGIC While there are similarities between reading in CSV & JSON there are some key differences:
# MAGIC * We only need one job even when inferring the schema.
# MAGIC * There is no header which is why there isn't a second job in this case - the column names are extracted from the JSON object's attributes.
# MAGIC * Unlike CSV which reads in 100% of the data, the JSON reader only samples the data.  
# MAGIC **Note:** In Spark 2.2 the behavior was changed to read in the entire JSON file.

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Reading from JSON w/ User-Defined Schema
# MAGIC 
# MAGIC To avoid the extra job, we can (just like we did with CSV) specify the schema for the `DataFrame`.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step #1 - Create the Schema
# MAGIC 
# MAGIC Compared to our CSV example, the structure of this data is a little more complex.
# MAGIC 
# MAGIC Note that we can support complex data types as seen in the field `geocoding`.

# COMMAND ----------

# Required for StructField, StringType, IntegerType, etc.
from pyspark.sql.types import *

jsonSchema = StructType([
  StructField("channel", StringType(), True),
  StructField("comment", StringType(), True),
  StructField("delta", IntegerType(), True),
  StructField("flag", StringType(), True),
  StructField("geocoding", StructType([
    StructField("city", StringType(), True),
    StructField("country", StringType(), True),
    StructField("countryCode2", StringType(), True),
    StructField("countryCode3", StringType(), True),
    StructField("stateProvince", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True)
  ]), True),
  StructField("isAnonymous", BooleanType(), True),
  StructField("isNewPage", BooleanType(), True),
  StructField("isRobot", BooleanType(), True),
  StructField("isUnpatrolled", BooleanType(), True),
  StructField("namespace", StringType(), True),
  StructField("page", StringType(), True),
  StructField("pageURL", StringType(), True),
  StructField("timestamp", StringType(), True),
  StructField("url", StringType(), True),
  StructField("user", StringType(), True),
  StructField("userURL", StringType(), True),
  StructField("wikipediaURL", StringType(), True),
  StructField("wikipedia", StringType(), True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC That was a lot of typing to get our schema!
# MAGIC 
# MAGIC For a small file, manually creating the the schema may not be worth the effort.
# MAGIC 
# MAGIC However, for a large file, the time to manually create the schema may be worth the trade off of a really long infer-schema process.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step #2 - Read in the JSON
# MAGIC 
# MAGIC Next, we will read in the JSON file and once again print its schema.

# COMMAND ----------

(spark.read            # The DataFrameReader
  .schema(jsonSchema)  # Use the specified schema
  .json(jsonFile)      # Creates a DataFrame from JSON after reading in the file
  .printSchema()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Review: Reading from JSON w/ User-Defined Schema
# MAGIC * Just like CSV, providing the schema avoids the extra jobs.
# MAGIC * The schema allows us to rename columns and specify alternate data types.
# MAGIC * Can get arbitrarily complex in its structure.

# COMMAND ----------

# MAGIC %md
# MAGIC Let's take a look at some of the other details of the `DataFrame` we just created for comparison sake.

# COMMAND ----------

jsonDF = (spark.read
  .schema(jsonSchema)
  .json(jsonFile)    
)
print("Partitions: " + str(jsonDF.rdd.getNumPartitions()))
printRecordsPerPartition(jsonDF)
print("-"*80)

# COMMAND ----------

# MAGIC %md
# MAGIC And of course we can view that data here:

# COMMAND ----------

display(jsonDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC 
# MAGIC * [Reading Data #1 - CSV]($./Reading Data 1 - CSV)
# MAGIC * [Reading Data #2 - Parquet]($./Reading Data 2 - Parquet)
# MAGIC * [Reading Data #3 - Tables]($./Reading Data 3 - Tables)
# MAGIC * Reading Data #4 - JSON
# MAGIC * [Reading Data #5 - Text]($./Reading Data 5 - Text)
# MAGIC * [Reading Data #6 - JDBC]($./Reading Data 6 - JDBC)
# MAGIC * [Reading Data #7 - Summary]($./Reading Data 7 - Summary)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>