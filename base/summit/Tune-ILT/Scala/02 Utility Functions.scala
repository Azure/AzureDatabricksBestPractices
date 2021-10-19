// Databricks notebook source
// MAGIC 
// MAGIC %md-sandbox
// MAGIC 
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC #Utility Functions
// MAGIC 
// MAGIC **Technical Accomplishments:**
// MAGIC * Discover techniques for counting files
// MAGIC * Develop tools for future diagnostics efforts
// MAGIC * Introduce the lab format

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Getting Started
// MAGIC 
// MAGIC Run the following cell to configure our "classroom."

// COMMAND ----------

// MAGIC %run "./Includes/Classroom-Setup"

// COMMAND ----------

// MAGIC %run "./Includes/Initialize-Labs"

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC <div style="display:table; height:600px; width:100%">
// MAGIC   <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"/> How would you do it?</h2>
// MAGIC 
// MAGIC   <p>A significant number of problems stem from problems "on disk".</p>
// MAGIC 
// MAGIC   <p>When we are looking at the files "on disk" every system requires different techniques.</p>
// MAGIC 
// MAGIC   <p><b>Typically we need to answer two questions:</b></p>
// MAGIC   <li>How big are the files?</li>
// MAGIC   <li>How many files are there?</li>
// MAGIC 
// MAGIC   <p><b>Different Systems:</b></p>
// MAGIC   <li>Object Stores</li>
// MAGIC   <li>NAS</li>
// MAGIC   <li>HDFS</li>
// MAGIC   <li>Locally mounted file systems</li>
// MAGIC 
// MAGIC   <p><b>Different Techniques:</b></p>
// MAGIC   <li>Cloud Tools</li>
// MAGIC   <li>OS Tools</li>
// MAGIC   <li>Language Tools: Scala, Python, etc</li>
// MAGIC     
// MAGIC </div>  

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) The Introduction
// MAGIC 
// MAGIC When we do this on Databricks, we have our own set of tricks.
// MAGIC * How you do this will vary from system to system
// MAGIC * But the "who", "what", "where", <span style="text-decoration:line-through">how</span>, and "why" applies to everyone
// MAGIC * Today we are on Databricks, so we need to at least cover the Databricks techniques for the sake of this class
// MAGIC 
// MAGIC Let's start by exploring your cluster's file system.
// MAGIC 
// MAGIC A quick and easy way to do this is to shell out to the Driver and issue simple commands like **ls -la *target***

// COMMAND ----------

// MAGIC %sh ls -la /

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Visible on the Driver's root is the Databricks File System (DBFS).
// MAGIC 
// MAGIC The DBFS is a virtual file system hosted in an Azure Blob Store.
// MAGIC 
// MAGIC You can see it in the output above as **dbfs**, about the 7th item from the top.

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC You can read more about the DBFS here:
// MAGIC * <a href="https://docs.azuredatabricks.net/user-guide/dbfs-databricks-file-system.html#dbfs" target="_blank">https&#58;//docs.azuredatabricks.net/user-guide/dbfs-databricks-file-system.html#dbfs</a>

// COMMAND ----------

// MAGIC %md
// MAGIC Of particular interest is the other object stores mounted to DBFS.
// MAGIC 
// MAGIC We can dig further into the contents of the DBFS, and specifically the **training** mount, with the following command:

// COMMAND ----------

// MAGIC %sh ls -la /dbfs/mnt/training

// COMMAND ----------

// MAGIC %md
// MAGIC The Databricks notebooks provide several other techniques for working with the file system.
// MAGIC 
// MAGIC The next is **%fs ls *target***

// COMMAND ----------

// MAGIC %fs ls dbfs:/mnt/training

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> Note the difference in the paths for <b>%sh</b> and <b>%fs</b>:
// MAGIC * <b>%sh ls -la <b style="color:blue">/dbfs</b>/mnt/training</b>
// MAGIC * <b>%fs ls <b style="color:blue">dbfs:</b>/mnt/training</b>
// MAGIC 
// MAGIC With **%sh** the **dbfs** is a folder at the root of your driver's file system.
// MAGIC 
// MAGIC With **%fs** the **dbfs** is a protocol and represents the root of the virtualized **Databricks File System**.

// COMMAND ----------

// MAGIC %md
// MAGIC **%fs ls dbfs:/mnt/training** is actually shorthand for the following piece of code:

// COMMAND ----------

val path = "dbfs:/mnt/training"
val files = dbutils.fs.ls(path)

display(files)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC The **dbutils.fs.ls(..)** command returns a sequence in Scala (or a list in Python) containing **FileInfo** objects.
// MAGIC 
// MAGIC Of special interest to us are its attributes:
// MAGIC 
// MAGIC | Attribute   | Type      | Description |
// MAGIC |-------------|-----------|-------------|
// MAGIC | **path**    | string    | The path of the file or directory. |
// MAGIC | **name**    | string    | The name of the file or directory. |
// MAGIC | **isDir()** | boolean   | True if the path is a directory. |
// MAGIC | **size**    | long/int64| The length of the file in bytes or zero if the path is a directory. |

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC For more information on **FileInfo** objects, you can see the Databricks documentation here:  
// MAGIC * <a href="https://docs.azuredatabricks.net/api/latest/dbfs.html#dbfsfileinfo" target="_blank">https&#58;//docs.azuredatabricks.net/api/latest/dbfs.html#dbfsfileinfo</a>

// COMMAND ----------

printf("Collection Type: %s\n", files.getClass.getName())

val first = files(0)
printf("Contained Type:  %s\n", first.getClass().getName())

println("-"*80)

// COMMAND ----------

// MAGIC %md
// MAGIC We can use this information to programmatically explore the file system.

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Challenge #1
// MAGIC 
// MAGIC You have 10 minutes to determine how many individual files make up our dataset and its total size:
// MAGIC * **dbfs:/mnt/training/asa/flights/all-by-year/** (easy)
// MAGIC * **dbfs:/mnt/training/asa/flights/1990-1999-by-year.parquet/** (hard)
// MAGIC 
// MAGIC ### Requirements:
// MAGIC * Report the two pieces of information as 
// MAGIC   * **totalCount**: The total number of files (but not directories) 
// MAGIC   * **totalSize**: The sum of the size of all files 
// MAGIC * The operation/technique should recurse into all subdirectories
// MAGIC 
// MAGIC ###Strategies:
// MAGIC * **Option #1**: Use **%fs ls dbfs:/mnt/training/asa/flights/all-by-year/**, manually count every file, repeat for every subdirectory, and then add them all up.
// MAGIC * **Option #2**: Use **dbutls.fs.ls(..)** to programmatically sum and/or count the number of files in each directory.
// MAGIC   * **2.A:** Use Scala's (or Python's) native functions to sum the count of all files.
// MAGIC   * **2.B:** Convert the raw data to a DataFrame and use the DataFrame's API to sum the count of all files.
// MAGIC * **Option #3** Use **%sh** to run linux commands like **du** (which sadly doesn't work)
// MAGIC * **Option #4**: Surprise me!
// MAGIC 
// MAGIC **Bonus #1**<br/>
// MAGIC Wrap it all up into a re-usable function.
// MAGIC 
// MAGIC **Bonus #2**<br/>
// MAGIC Recursively processes subdirectories.<br/>
// MAGIC For this, use the directory **dbfs:/mnt/training/asa/flights/1990-1999-by-year.parquet/**

// COMMAND ----------

// TODO

val path = "dbfs:/mnt/training/asa/flights/all-by-year/"

var totalCount = 0L
var totalBytes = 0L

printf("%,d files\n", totalCount)
printf("%,d bytes\n", totalBytes)
println("-"*80)

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Validate Your Answer to #1
// MAGIC Run the cell below to validate your answer:

// COMMAND ----------

clearYourResults()

validateYourAnswer("01.A) Total Count", 1059805429, totalCount)
validateYourAnswer("01.B) Total Bytes", 1517958800, totalBytes)

// validateYourAnswer("01.C) Bonus Count", 1680788708, bonusCount)
// validateYourAnswer("01.D) Bonus Bytes", 1928383137, bonusBytes)

summarizeYourResults()

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Our Utility Methods
// MAGIC 
// MAGIC Before moving on, let's take a look at some of the utility methods we created just for this class:

// COMMAND ----------

// MAGIC %run "./Includes/Utility-Methods"

// COMMAND ----------

// MAGIC %md
// MAGIC ### Compute File Stats
// MAGIC 
// MAGIC Given a path, returns the number of files and the total size on disk.
// MAGIC 
// MAGIC Useful for diagnosing a number of on-disk problems

// COMMAND ----------

val path = "dbfs:/mnt/training/asa/flights/all-by-year"

val (count, bytes) = computeFileStats(path)

println("%,14d files".format(count))
println("%,14d bytes".format(bytes))
println("%,14d KB/file".format(bytes/count/1024))

println("-"*80)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Print Records Per Partition
// MAGIC Given a DataFrame, this method will count the number of records in each Spark-Partition.
// MAGIC 
// MAGIC Usefull for identifying things like in-memory skew

// COMMAND ----------

val path = "dbfs:/mnt/training/asa/flights/small.csv"
val df = spark.read.csv(path)

printRecordsPerPartition(df)

println()
println("-"*80)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Which partition has the most "skew"?
// MAGIC 
// MAGIC ### Is the amount of skew something we need to worry about?

// COMMAND ----------

// MAGIC %md
// MAGIC ### Benchmark Count
// MAGIC 
// MAGIC A quick & dirty benchmarking tool
// MAGIC 
// MAGIC Used to evaluate the performance difference between different iterations of code.

// COMMAND ----------

val path = "dbfs:/mnt/training/asa/flights/1990-1999-by-year.parquet"
val dfA = spark.read.parquet(path)

val (dfB, total, duration) = benchmarkCount(() => dfA)

println("Query Duration: %,d ms".format(duration))
println("-"*80)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Tracker & Cache As
// MAGIC We will introduce these in a later module along with a tool called **StatFiles**.

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>