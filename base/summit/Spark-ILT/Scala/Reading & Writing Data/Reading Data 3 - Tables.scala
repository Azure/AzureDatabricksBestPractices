// Databricks notebook source
// MAGIC 
// MAGIC %md-sandbox
// MAGIC 
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC # Reading Data - Tables
// MAGIC 
// MAGIC **Technical Accomplishments:**
// MAGIC * Demonstrate how to pre-register data sources in Databricks.
// MAGIC * Introduce temporary views over files.
// MAGIC * Read data from tables/views.
// MAGIC * Regarding `printRecordsPerPartition(..)`, it 
// MAGIC   * converts the specified `DataFrame` to an RDD
// MAGIC   * counts the number of records in each partition
// MAGIC   * prints the results to the console.

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
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Registering Tables in Databricks
// MAGIC 
// MAGIC So far we've seen purely programmatic methods for reading in data.
// MAGIC 
// MAGIC Databricks allows us to "register" the equivalent of "tables" so that they can be easily accessed by all users. 
// MAGIC 
// MAGIC It also allows us to specify configuration settings such as secret keys, tokens, username & passwords, etc without exposing that information to all users.

// COMMAND ----------

// MAGIC %md
// MAGIC ## Register a Table/View
// MAGIC * Databrick's UI has built in support for working with a number of different data sources
// MAGIC * New ones are being added regularly
// MAGIC * In our case we are going to upload the file <a href="http://files.training.databricks.com/static/data/pageviews_by_second_example.tsv">pageviews_by_second_example.tsv</a>
// MAGIC * .. and then use the UI to create a table.

// COMMAND ----------

// MAGIC %md
// MAGIC There are several benefits to this strategy:
// MAGIC * Once setup, it never has to be done again
// MAGIC * It is available for any user on the platform (permissions permitting)
// MAGIC * Minimizes exposure of credentials
// MAGIC * No real overhead to reading the schema (no infer-schema)
// MAGIC * Easier to advertise available datasets to other users

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Reading from a Table/View
// MAGIC 
// MAGIC We can now read in the "table" **pageviews_by_seconds_example** as a `DataFrame` with one simple command (and then print the schema):

// COMMAND ----------

val pageviewsBySecondsExampleDF = spark.read.table("pageviews_by_second_example_tsv")

pageviewsBySecondsExampleDF.printSchema()

// COMMAND ----------

// MAGIC %md
// MAGIC And of course we can now view that data as well:

// COMMAND ----------

display(pageviewsBySecondsExampleDF)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Review: Reading from Tables
// MAGIC * No job is executed - the schema is stored in the table definition on Databricks.
// MAGIC * The data types shown here are those we defined when we registered the table.
// MAGIC * In our case, the file was uploaded to Databricks and is stored on the DBFS.
// MAGIC   * If we used JDBC, it would open the connection to the database and read it in.
// MAGIC   * If we used an object store (like what is backing the DBFS), it would read the data from source.
// MAGIC * The "registration" of the table simply makes future access, or access by multiple users easier.
// MAGIC * The users of the notebook cannot see username and passwords, secret keys, tokens, etc.

// COMMAND ----------

// MAGIC %md
// MAGIC Let's take a look at some of the other details of the `DataFrame` we just created for comparison sake.

// COMMAND ----------

println("Partitions: " + pageviewsBySecondsExampleDF.rdd.getNumPartitions)
printRecordsPerPartition(pageviewsBySecondsExampleDF)
println("-"*80)

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Temporary Views
// MAGIC 
// MAGIC Tables that are loadable by the call `spark.read.table(..)` are also accessible through the SQL APIs.
// MAGIC 
// MAGIC For example, we already used Databricks to expose **pageviews_by_second_example_tsv** as a table/view.

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from pageviews_by_second_example_tsv limit(5)

// COMMAND ----------

// MAGIC %md
// MAGIC You can also take an existing `DataFrame` and register it as a view exposing it as a table to the SQL API.
// MAGIC 
// MAGIC If you recall from earlier, we have an instance called `parquetDF`.
// MAGIC 
// MAGIC We can create a [temporary] view with this call...

// COMMAND ----------

// create a DataFrame from a parquet file
val parquetFile = "/mnt/training/wikipedia/pagecounts/staging_parquet_en_only_clean/"
val parquetDF = spark.read.parquet(parquetFile)

// create a temporary view from the resulting DataFrame
parquetDF.createOrReplaceTempView("parquet_table")

// COMMAND ----------

// MAGIC %md
// MAGIC And now we can use the SQL API to reference that same `DataFrame` as the table **parquet_table**.

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from parquet_table order by requests desc limit(5)

// COMMAND ----------

// MAGIC %md
// MAGIC ** *Note #1:* ** *The method createOrReplaceTempView(..) is bound to the SparkSession meaning it will be discarded once the session ends.*
// MAGIC 
// MAGIC ** *Note #2:* ** On the other hand, the method createOrReplaceGlobalTempView(..) is bound to the spark application.*
// MAGIC 
// MAGIC *Or to put that another way, I can use createOrReplaceTempView(..) in this notebook only. However, I can call createOrReplaceGlobalTempView(..) in this notebook and then access it from another.*

// COMMAND ----------

// MAGIC %md
// MAGIC ## Next Steps
// MAGIC 
// MAGIC * [Reading Data #1 - CSV]($./Reading Data 1 - CSV)
// MAGIC * [Reading Data #2 - Parquet]($./Reading Data 2 - Parquet)
// MAGIC * Reading Data #3 - Tables
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