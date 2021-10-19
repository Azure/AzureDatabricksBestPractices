# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Reading Data - Text Files
# MAGIC 
# MAGIC **Technical Accomplishments:**
# MAGIC - Reading data from a simple text file

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
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Reading from Fixed-Width Text File
# MAGIC 
# MAGIC We can read in just about any file when each record is delineated only by a new line just as we saw with CSV and JSON (or rather JSON-Lines), formats.
# MAGIC 
# MAGIC To accomplish this, we can use `DataFrameReader.text(..)` which gives a `DataFrame` with just one column named **value** of type **string**.
# MAGIC 
# MAGIC The difference is that we now need to take responsibility for parsing out the data in each "column" ourselves.
# MAGIC 
# MAGIC One of the more common use cases is fixed-width files or even Apache's HTTP Access Logs. In the first case, it would require a sequence of substrings. In the second, a sequence of regular expressions would be a better solution to extract the value of each column. In either case, additional transformations are required - which we will go into later.
# MAGIC 
# MAGIC For this example, we are going to create a `DataFrame` from the full text of the book *The Adventures of Tom Sawyer* by Mark Twain.

# COMMAND ----------

# MAGIC %fs ls /mnt/training/tom-sawyer/tom.txt

# COMMAND ----------

# MAGIC %fs head /mnt/training/tom-sawyer/tom.txt

# COMMAND ----------

textFile = "/mnt/training/tom-sawyer/tom.txt"

textDF = (spark.read        # The DataFrameReader
          .text(textFile)   # Creates a DataFrame from raw text after reading in the file
)

textDF.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC And with the `DataFrame` created, we can view the data, one record for each line in the text file.

# COMMAND ----------

display(textDF)

# COMMAND ----------

# MAGIC %md
# MAGIC As simple as this example is, it's also the premise for loading more complex text files like fixed-width text files.
# MAGIC 
# MAGIC We will see later exactly how to do this, but for each line that is read in, it's simply a matter of a couple of more transformations (like substring-ing values) to convert each line into something more meaningful.
# MAGIC 
# MAGIC Let's take a look at some of the other details of the `DataFrame` we just created for comparison sake.

# COMMAND ----------

print("Partitions: " + str(textDF.rdd.getNumPartitions()))
printRecordsPerPartition(textDF)
print("-"*80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC 
# MAGIC * [Reading Data #1 - CSV]($./Reading Data 1 - CSV)
# MAGIC * [Reading Data #2 - Parquet]($./Reading Data 2 - Parquet)
# MAGIC * [Reading Data #3 - Tables]($./Reading Data 3 - Tables)
# MAGIC * [Reading Data #4 - JSON]($./Reading Data 4 - JSON)
# MAGIC * Reading Data #5 - Text
# MAGIC * [Reading Data #6 - JDBC]($./Reading Data 6 - JDBC)
# MAGIC * [Reading Data #7 - Summary]($./Reading Data 7 - Summary)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>