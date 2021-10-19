# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://cdn2.hubspot.net/hubfs/438089/docs/training/dblearning-banner.png" alt="Databricks Learning" width="555" height="64">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2018 Databricks, Inc. All rights reserved.<br/>

# COMMAND ----------

# MAGIC %md
# MAGIC # Reading Data Lab
# MAGIC * The goal of this lab is to put into practice some of what you have learned about reading data with Azure Databricks and Apache Spark.
# MAGIC * The instructions are provided below along with empty cells for you to do your work.
# MAGIC * At the bottom of this notebook are additional cells that will help verify that your work is accurate.

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Challenges
# MAGIC * Data is all over the place - reports, KPIs and DS is hard with bigger data from disparate sources on exiting tools
# MAGIC 
# MAGIC ### Why Initech Needs a Data Lake
# MAGIC * Store both big and data in one location for all personas - Data Engineering, Data Science, Analysts 
# MAGIC * They need to access this data in diffrent languages and tools - SQL, Python, Scala, Java, R with Notebooks, IDE, Power BI, Tableau, JDBC/ODBC
# MAGIC 
# MAGIC ### Azure Databricks Solutions
# MAGIC * Azure Storage or Azure Data Lake - Is a place to store all data, big and small
# MAGIC * Access both big (TB to PB) and small data easily with Databricks' scaleable clusters
# MAGIC * Use Python, Scala, R, SQL, Java

# COMMAND ----------

# MAGIC %md 
# MAGIC ####Azure Databricks for Batch ETL & Data Engineers 
# MAGIC 
# MAGIC ![arch](https://kpistoropen.blob.core.windows.net/collateral/roadshow/azure_roadshow_de.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://kpistoropen.blob.core.windows.net/collateral/roadshow/logo_spark_tiny.png) Instructions
# MAGIC 0. Start with the file **dbfs:/mnt/training-sources/initech/productsCsv/product.csv**, a file containing product details.
# MAGIC 0. Read in the data and assign it to a `DataFrame` named **productDF**.
# MAGIC 0. Run the last cell to verify that the data was loaded correctly and to print its schema.
# MAGIC 
# MAGIC Bonus: Create the `DataFrame` and print its schema **without** executing a single job.

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://kpistoropen.blob.core.windows.net/collateral/roadshow/logo_spark_tiny.png) The Data Source
# MAGIC * For this exercise, we will be using a file called **products.csv**.
# MAGIC * The data represents new products we are planning to add to our online store.
# MAGIC * We can use **&percnt;fs head ...** to view the first few lines of the file.

# COMMAND ----------

# MAGIC %fs head /mnt/training-sources/initech/productsCsv/product.csv

# COMMAND ----------

# MAGIC %md 
# MAGIC ##![Spark Logo Tiny](https://kpistoropen.blob.core.windows.net/collateral/roadshow/logo_spark_tiny.png) *Part-1:* Read The CSV File
# MAGIC 
# MAGIC Let's start with the bare minimum by specifying that the file we want to read is delimited and the location of the file:
# MAGIC The default delimiter for `spark.read.csv( )` is comma but we can change by specifying the option delimiter parameter.

# COMMAND ----------

# A reference to our csv file
csv_file = "/mnt/training-sources/initech/productsCsv/"
temp_df = (spark.read           # The DataFrameReader
    #.option("delimiter", "\t") This is how we could pass in a Tab or other delimiter.
   .csv(csv_file)               # Creates a DataFrame from CSV after reading in the file
)

# COMMAND ----------

display(temp_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://kpistoropen.blob.core.windows.net/collateral/roadshow/logo_spark_tiny.png) *Part-2:* Read CSV using Spark's built in CSV reader to Infer the Schema
# MAGIC * Spark Documentation on CSV Reader: http://spark.apache.org/docs/latest/sql-programming-guide.html#manually-specifying-options

# COMMAND ----------

# DBTITLE 1,TO DO:
# TO-DO
csv_file = "dbfs:/mnt/training-sources/initech/productsCsv/product.csv"
product_df = (spark.read                        # The DataFrameReader
   .option("header", "true")       # Use first line of all files as header
   .option("inferSchema", "true")  # Automatically infer data types
   .csv(csv_file)                   # Creates a DataFrame from CSV after reading in the file
)

# COMMAND ----------

display(product_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://kpistoropen.blob.core.windows.net/collateral/roadshow/logo_spark_tiny.png) Verify Your Work
# MAGIC Run the following cell to verify that your `DataFrame` was created properly.

# COMMAND ----------

# MAGIC %python
# MAGIC product_df.printSchema()
# MAGIC 
# MAGIC columns = product_df.dtypes
# MAGIC assert len(columns) == 8, "Expected 8 columns but found " + str(len(columns))
# MAGIC 
# MAGIC assert columns[0][0] == "product_id", "Expected column 0 to be \"product_id\" but found \"" + columns[0][0] + "\"."
# MAGIC assert columns[0][1] == "int",        "Expected column 0 to be of type \"int\" but found \"" + columns[0][1] + "\"."
# MAGIC 
# MAGIC assert columns[1][0] == "category",   "Expected column 1 to be \"category\" but found \"" + columns[1][0] + "\"."
# MAGIC assert columns[1][1] == "string",     "Expected column 1 to be of type \"string\" but found \"" + columns[1][1] + "\"."
# MAGIC 
# MAGIC assert columns[2][0] == "brand",      "Expected column 2 to be \"brand\" but found \"" + columns[2][0] + "\"."
# MAGIC assert columns[2][1] == "string",     "Expected column 2 to be of type \"string\" but found \"" + columns[2][1] + "\"."
# MAGIC 
# MAGIC assert columns[3][0] == "model",      "Expected column 3 to be \"model\" but found \"" + columns[3][0] + "\"."
# MAGIC assert columns[3][1] == "string",     "Expected column 3 to be of type \"string\" but found \"" + columns[3][1] + "\"."
# MAGIC 
# MAGIC assert columns[4][0] == "price",      "Expected column 4 to be \"price\" but found \"" + columns[4][0] + "\"."
# MAGIC assert columns[4][1] == "double",     "Expected column 4 to be of type \"double\" but found \"" + columns[4][1] + "\"."
# MAGIC 
# MAGIC assert columns[5][0] == "processor",  "Expected column 5 to be \"processor\" but found \"" + columns[5][0] + "\"."
# MAGIC assert columns[5][1] == "string",     "Expected column 5 to be of type \"string\" but found \"" + columns[5][1] + "\"."
# MAGIC 
# MAGIC assert columns[6][0] == "size",       "Expected column 6 to be \"size\" but found \"" + columns[6][0] + "\"."
# MAGIC assert columns[6][1] == "string",     "Expected column 6 to be of type \"string\" but found \"" + columns[6][1] + "\"."
# MAGIC 
# MAGIC assert columns[7][0] == "display",    "Expected column 7 to be \"disolay\" but found \"" + columns[7][0] + "\"."
# MAGIC assert columns[7][1] == "string",     "Expected column 7 to be of type \"string\" but found \"" + columns[7][1] + "\"."
# MAGIC 
# MAGIC print("Congratulations, all tests passed!\n")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <h2 style="color:red">End of Lab 1 </h2>

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://kpistoropen.blob.core.windows.net/collateral/roadshow/logo_spark_tiny.png) *Bonus Part 3:* - If you have time you can work on this
# MAGIC * Create the `DataFrame` and print its schema **without** executing a single job. Hint: [Specify the schema manually](https://spark.apache.org/docs/latest/sql-programming-guide.html#programmatically-specifying-the-schema)
# MAGIC * Write the data to Azure Storage in parquet (Note: Use the path `dbfs:/tmp/{YOUR-NAME}/output/`) - https://spark.apache.org/docs/latest/sql-programming-guide.html#parquet-files

# COMMAND ----------

# Required for StructField, StringType, IntegerType, etc.
from pyspark.sql.types import *
csv_schema = StructType([
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

product_df = (spark.read                   # The DataFrameReader
  .option('header', 'true')   # Ignore line #1 - it's a header
  .schema(csv_schema)          # Use the specified schema
  .csv(csv_file)               # Creates a DataFrame from CSV after reading in the file
)

# COMMAND ----------

product_df.write.parquet("dbfs:/tmp/az/output/reading_data.lab.parquet")

# COMMAND ----------

# MAGIC %fs ls /tmp/az/output/reading_data.lab.parquet

# COMMAND ----------

# MAGIC %fs head /tmp/az/output/reading_data.lab.parquet/part-00000-tid-3287088091765916918-7d3f177e-2ae6-4eb7-a1dd-7303f8280aeb-8-c000.snappy.parquet

# COMMAND ----------

display(product_df)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2018 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>