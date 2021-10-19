# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://cdn2.hubspot.net/hubfs/438089/docs/training/dblearning-banner.png" alt="Databricks Learning" width="555" height="64">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Reading Data & Writing Files - Parquet and CSV
# MAGIC 
# MAGIC **Technical Accomplishments:**
# MAGIC - Read data from CSV 
# MAGIC - write out data in Parquet format with Schema

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/wiki-book/general/logo_spark_tiny.png) Databricks File System - DBFS
# MAGIC * Is a layer over Azure's blob store.
# MAGIC * Files in DBFS persist to the blob store, so you won’t lose data even after you terminate the clusters.
# MAGIC  * Our notebooks.
# MAGIC  * Revisions to our notebooks.
# MAGIC  * Your personal temp files.
# MAGIC 
# MAGIC #### Databricks Utilities - dbutils
# MAGIC * You can access the DBFS through the Databricks Utilities class (and other file IO routines).
# MAGIC * An instance of DBUtils is already declared for us as `dbutils`.
# MAGIC * For in-notebook documentation on DBUtils you can execute the command `dbutils.help()`.
# MAGIC * See also <a href="https://docs.databricks.com/user-guide/dbutils.html" target="_blank">Databricks Utilities - dbutils</a>

# COMMAND ----------

dbutils.help()

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Reading from CSV

# COMMAND ----------

# MAGIC %md
# MAGIC ### The Data Source
# MAGIC * For this exercise, we will be using a file called **products.csv**.
# MAGIC * The data represents new products we are planning to add to our online store.
# MAGIC * We can use **&percnt;head ...** to view the first few lines of the file.

# COMMAND ----------

# MAGIC %fs head /mnt/training-msft/initech/Product.csv

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step #1 - Read The CSV File
# MAGIC Let's start with the bare minimum by specifying that the file we want to read is delimited and the location of the file:
# MAGIC The default delimiter for `spark.read.csv( )` is comma but we can change by specifying the option delimiter parameter.

# COMMAND ----------

# A reference to our csv file
csvFile = "dbfs:/mnt/training-msft/initech/Product.csv"
tempDF = (spark.read           # The DataFrameReader
    #.option("delimiter", "\t") This is how we could pass in a Tab or other delimiter.
   .csv(csvFile)               # Creates a DataFrame from CSV after reading in the file
)

# COMMAND ----------

display(tempDF)

# COMMAND ----------

# MAGIC %md
# MAGIC This is guaranteed to <u>trigger one job</u>.
# MAGIC 
# MAGIC A *Job* is triggered anytime we are "physically" __required to touch the data__.
# MAGIC 
# MAGIC In some cases, __one action may create multiple jobs__ (multiple reasons to touch the data).
# MAGIC 
# MAGIC In this case, the reader has to __"peek" at the first line__ of the file to determine how many columns of data we have.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC We can see the structure of the `DataFrame` by executing the command `printSchema()`
# MAGIC 
# MAGIC It prints the name of each column to the console, its data type, and if it's null or not.
# MAGIC 
# MAGIC ** *Note:* ** *We will be covering the other `DataFrame` functions in other notebooks.*

# COMMAND ----------

tempDF.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC We can see from the schema that...
# MAGIC * the columns in our dataframe.
# MAGIC * the column names **_c0**, **_c1**, and **_c2**... (automatically generated names)
# MAGIC * all columns are **strings**
# MAGIC * all columns are **nullable**
# MAGIC 
# MAGIC And if we take a quick peek at the data, we can see that line #1 contains the headers and not data:

# COMMAND ----------

display(tempDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step #2 - Use the File's Header
# MAGIC Next, we can add an option that tells the reader that the data contains a header and to use that header to determine our column names.
# MAGIC 
# MAGIC ** *NOTE:* ** *We know we have a header based on what we can see in "head" of the file from earlier.*

# COMMAND ----------

(spark.read                    # The DataFrameReader
   .option("header", "true")   # Use first line of all files as header
   .csv(csvFile)               # Creates a DataFrame from CSV after reading in the file
   .printSchema()
)

# COMMAND ----------

# MAGIC %md
# MAGIC This is progress but notice that all the column datatypes are still string.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step #3 - Infer the Schema
# MAGIC 
# MAGIC Lastly, we can add an option that tells the reader to infer each column's data type (aka the schema)

# COMMAND ----------

(spark.read                        # The DataFrameReader
   .option("header", "true")       # Use first line of all files as header
   .option("inferSchema", "true")  # Automatically infer data types
   .csv(csvFile)                   # Creates a DataFrame from CSV after reading in the file
   .printSchema()
)

# COMMAND ----------

# MAGIC %md
# MAGIC This can be costly when reading in a large file as spark is forced to read through all the data in the files in order to determine data types.  To read in a file and avoid this costly extra job we can provide the schema to the DataFrameReader.

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Reading from CSV w/User-Defined Schema
# MAGIC 
# MAGIC This time we are going to read the same file.
# MAGIC 
# MAGIC The difference here is that we are going to define the schema beforehand to avoid the execution of any extra jobs.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step #1
# MAGIC Declare the schema.
# MAGIC 
# MAGIC This is just a list of field names and data types.

# COMMAND ----------

# Required for StructField, StringType, IntegerType, etc.
from pyspark.sql.types import *

csvSchema = StructType([
  StructField("product_id", LongType(), True),
  StructField("category", StringType(), True),
  StructField("brand", StringType(), True),
  StructField("model", StringType(), True),
  StructField("price", DoubleType(), True),
  StructField("processor", StringType(), True),
  StructField("size", StringType(), True),
  StructField("display", StringType(), True)
 ])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step #2
# MAGIC Read in our data (and print the schema).
# MAGIC 
# MAGIC We can specify the schema, or rather the `StructType`, with the `schema(..)` command:

# COMMAND ----------

productDF = (spark.read                   # The DataFrameReader
  .option('header', 'true')   # Ignore line #1 - it's a header
  .schema(csvSchema)          # Use the specified schema
  .csv(csvFile)               # Creates a DataFrame from CSV after reading in the file
)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC With our DataFrame created, we can now create a temporary view and then view the data via SQL:

# COMMAND ----------

# create a view called products
productDF.createOrReplaceTempView("products")

# COMMAND ----------

# MAGIC %md
# MAGIC And now we can take a peak at the data with simple SQL SELECT statement:

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM products

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Writing to Parquet
# MAGIC 
# MAGIC Parquet is a columnar format that is supported by many other data processing systems. Spark SQL provides support for both reading and writing Parquet files that automatically preserves the schema of the original data. When writing Parquet files, all columns are automatically converted to be nullable for compatibility reasons.
# MAGIC 
# MAGIC More discussion on <a href="http://parquet.apache.org/documentation/latest/" target="_blank">Parquet</a>
# MAGIC 
# MAGIC Documentation on <a href="https://spark.apache.org/docs/latest/api/python/pyspark.sql.html?highlight=dataframe%20reader#pyspark.sql.DataFrameReader" target="_blank">DataFrameReader</a>

# COMMAND ----------

productDF.write.mode("overwrite").parquet("dbfs:/mnt/training-msft/initech/Products.parquet")    

# COMMAND ----------

# MAGIC %md we can see the parquet file in the file system

# COMMAND ----------

# MAGIC %fs ls /mnt/training-msft/initech/Products.parquet/

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2018 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>