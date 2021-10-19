# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Types & Data Structures?
# MAGIC 
# MAGIC **Dataset:**
# MAGIC * This is synthetic data generated specifically for these exercises
# MAGIC * Each year's data is roughly the same with some variation for market growth
# MAGIC * We are looking at retail purchases from the top N retailers
# MAGIC 
# MAGIC **Technical Accomplishments:**
# MAGIC * Look at the effects of 
# MAGIC   * Caching (or lack there of)
# MAGIC   * Usage of different data structures

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> This notebook is only available in Scala due to the use of APIs not available in Python.<br/>
# MAGIC However, the fundamental principles taught here remain the same for both languages.

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Getting Started
# MAGIC 
# MAGIC Run the following cell to configure our "classroom."

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %run "./Includes/Initialize-Labs"

# COMMAND ----------

# MAGIC %run "./Includes/Utility-Methods"

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Ready, Set, Go!
# MAGIC 
# MAGIC * We have 9 experiments to run
# MAGIC * We can expedite things by running **ALL** the cells first
# MAGIC 
# MAGIC <img style="float:left; margin-right:1em; box-shadow: 5px 5px 5px 0px rgba(0,0,0,0.25); border: 1px solid rgba(0,0,0,0.25);" src="https://files.training.databricks.com/images/notebook-cell-run-cmd.png"/>
# MAGIC <p><br/><br/><br/><img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** From the top of the notebook, select **Run All**<br/>
# MAGIC    or from a subsequent cell, select **Run All Below**.</p>

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC import org.apache.spark.sql.DataFrame
# MAGIC import org.apache.spark.storage.StorageLevel
# MAGIC import org.apache.spark.scheduler._
# MAGIC import org.apache.spark.sql.functions._
# MAGIC 
# MAGIC val fixedPath17 = "/mnt/training/global-sales/solutions/2017-fixed.parquet"
# MAGIC 
# MAGIC val pathA = "%s/cache-test-a.parquet".format(userhome)
# MAGIC val pathB = "%s/cache-test-b.parquet".format(userhome)
# MAGIC val pathC = "%s/cache-test-c.parquet".format(userhome)

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Comparing Apples to Apples
# MAGIC 
# MAGIC We need a single dataset in three different formats.
# MAGIC 
# MAGIC We can start by loading and caching the "fixed" version of our 2017 data.

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC spark.catalog.clearCache()
# MAGIC 
# MAGIC val df = spark.read
# MAGIC   .parquet(fixedPath17)
# MAGIC   .repartition(sc.defaultParallelism)
# MAGIC 
# MAGIC cacheAs(df, "temp_2017", StorageLevel.MEMORY_ONLY)
# MAGIC val totalRecords = df.count() // materialize the cache

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Using the "fixed" version of the 2017 data:
# MAGIC * Create a parquet file where the datatypes are all Strings
# MAGIC * Create a copy of the dataset as a CSV file
# MAGIC * Create a "standard" copy to use as a baseline

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC // Just in case you need to 
# MAGIC // start over from scratch...
# MAGIC dbutils.fs.rm(pathA, true)
# MAGIC dbutils.fs.rm(pathB, true)
# MAGIC dbutils.fs.rm(pathC, true)
# MAGIC 
# MAGIC // Cast each column as a string
# MAGIC var tempDF = df
# MAGIC for (column <- df.columns) {
# MAGIC   tempDF = tempDF.withColumn(column, col(column).cast("string"))
# MAGIC }
# MAGIC 
# MAGIC tempDF.write.parquet(pathA) // Save the "string" version
# MAGIC df.write.csv(pathB)         // Save the "CSV" version 
# MAGIC df.write.parquet(pathC)     // Save the "standard" version

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Size on Disk
# MAGIC 
# MAGIC Use the utility method **computeFileStats(..)** to determine the size of each dataset on disk.

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC val (_, sizeOnDiskA) = computeFileStats(pathA)
# MAGIC val (_, sizeOnDiskB) = computeFileStats(pathB)
# MAGIC val (_, sizeOnDiskC) = computeFileStats(pathC)
# MAGIC 
# MAGIC printf("A: %,d bytes - Strings\n", sizeOnDiskA)
# MAGIC printf("B: %,d bytes - CSV\n", sizeOnDiskB)
# MAGIC printf("C: %,d bytes - Typed\n", sizeOnDiskC)
# MAGIC println("-"*80)

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Uncached Counts
# MAGIC 
# MAGIC Run the following tests and compare the duration of each datatype and its effect on performance.

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC spark.catalog.clearCache()
# MAGIC 
# MAGIC val countA = tracker.track(() => spark.read.parquet(pathA).count() )
# MAGIC val countB = tracker.track(() => spark.read.csv(pathB).count() )
# MAGIC val countC = tracker.track(() => spark.read.parquet(pathC).count() )
# MAGIC 
# MAGIC countA.printTime()
# MAGIC println("-"*80)
# MAGIC 
# MAGIC countB.printTime()
# MAGIC println("-"*80)
# MAGIC 
# MAGIC countC.printTime()
# MAGIC println("-"*80)

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Benchmark MEMORY_ONLY

# COMMAND ----------

# MAGIC %md
# MAGIC ### #4) Cache Dataset-A: MEMORY_ONLY

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC spark.catalog.clearCache()
# MAGIC 
# MAGIC val moA = tracker.track(() => {
# MAGIC   val df = cacheAs( spark.read.parquet(pathA), "memory_only_a", StorageLevel.MEMORY_ONLY)
# MAGIC   df.count() // materialize
# MAGIC   df         // return value
# MAGIC })
# MAGIC 
# MAGIC moA.print()
# MAGIC println("-"*80)

# COMMAND ----------

# MAGIC %md
# MAGIC Count Dataset-A from MEMORY_ONLY

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC val moCountA = tracker.track(() => {
# MAGIC   moA.result.count()
# MAGIC })
# MAGIC 
# MAGIC moCountA.print()
# MAGIC println("-"*80)

# COMMAND ----------

# MAGIC %md
# MAGIC ### #5) Cache Dataset-B: MEMORY_ONLY

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC spark.catalog.clearCache()
# MAGIC 
# MAGIC val moB = tracker.track(() => {
# MAGIC   val df = cacheAs( spark.read.csv(pathB), "memory_only_b", StorageLevel.MEMORY_ONLY)
# MAGIC   df.count() // materialize
# MAGIC   df         // return value
# MAGIC })
# MAGIC 
# MAGIC moB.print()
# MAGIC println("-"*80)

# COMMAND ----------

# MAGIC %md
# MAGIC Count Dataset-B from MEMORY_ONLY

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC val moCountB = tracker.track(() => {
# MAGIC   moB.result.count()
# MAGIC })
# MAGIC 
# MAGIC moCountB.print()
# MAGIC println("-"*80)

# COMMAND ----------

# MAGIC %md
# MAGIC ### #6) Cache Dataset-C: MEMORY_ONLY

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC spark.catalog.clearCache()
# MAGIC 
# MAGIC val moC = tracker.track(() => {
# MAGIC   val df = cacheAs( spark.read.parquet(pathC), "memory_only_c", StorageLevel.MEMORY_ONLY)
# MAGIC   df.count() // materialize
# MAGIC   df         // return value
# MAGIC })
# MAGIC 
# MAGIC moC.print()
# MAGIC println("-"*80)

# COMMAND ----------

# MAGIC %md
# MAGIC Count Dataset-C from MEMORY_ONLY

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC val moCountC = tracker.track(() => {
# MAGIC   moC.result.count()
# MAGIC })
# MAGIC 
# MAGIC moCountC.print()
# MAGIC println("-"*80)

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Benchmark DISK_ONLY

# COMMAND ----------

# MAGIC %md
# MAGIC ### #7) Cache Dataset-A: DISK_ONLY

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC spark.catalog.clearCache()
# MAGIC 
# MAGIC var doA = tracker.track(() => {
# MAGIC   val df = cacheAs( spark.read.parquet(pathA), "disk_only_a", StorageLevel.DISK_ONLY)
# MAGIC   df.count() // materialize
# MAGIC   df         // return value
# MAGIC })
# MAGIC 
# MAGIC doA = doA.copy(cacheSize= sc.getRDDStorageInfo.filter(_.name == "In-memory table disk_only_a").map(_.diskSize).head )
# MAGIC 
# MAGIC doA.print()
# MAGIC println("-"*80)

# COMMAND ----------

# MAGIC %md
# MAGIC Count Dataset-A from DISK_ONLY

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC val doCountA = tracker.track(() => {
# MAGIC   doA.result.count()
# MAGIC })
# MAGIC 
# MAGIC doCountA.print()
# MAGIC println("-"*80)

# COMMAND ----------

# MAGIC %md
# MAGIC ### #8) Cache Dataset-B: DISK_ONLY

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC spark.catalog.clearCache()
# MAGIC 
# MAGIC var doB = tracker.track(() => {
# MAGIC   val df = cacheAs( spark.read.csv(pathB), "disk_only_b", StorageLevel.DISK_ONLY)
# MAGIC   df.count() // materialize
# MAGIC   df         // return value
# MAGIC })
# MAGIC 
# MAGIC doB = doB.copy(cacheSize= sc.getRDDStorageInfo.filter(_.name == "In-memory table disk_only_b").map(_.diskSize).head )
# MAGIC 
# MAGIC doB.print()
# MAGIC println("-"*80)

# COMMAND ----------

# MAGIC %md
# MAGIC Count Dataset-B from DISK_ONLY

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC val doCountB = tracker.track(() => {
# MAGIC   doB.result.count()
# MAGIC })
# MAGIC 
# MAGIC doCountB.print()
# MAGIC println("-"*80)

# COMMAND ----------

# MAGIC %md
# MAGIC ### #9) Cache Dataset-C: DISK_ONLY

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC spark.catalog.clearCache()
# MAGIC 
# MAGIC var doC = tracker.track(() => {
# MAGIC   val df = cacheAs( spark.read.parquet(pathC), "disk_only_c", StorageLevel.DISK_ONLY)
# MAGIC   df.count() // materialize
# MAGIC   df         // return value
# MAGIC })
# MAGIC 
# MAGIC doC = doC.copy(cacheSize= sc.getRDDStorageInfo.filter(_.name == "In-memory table disk_only_c").map(_.diskSize).head )
# MAGIC 
# MAGIC doC.print()
# MAGIC println("-"*80)

# COMMAND ----------

# MAGIC %md
# MAGIC Count Dataset-C from DISK_ONLY

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC val doCountC = tracker.track(() => {
# MAGIC   doC.result.count()
# MAGIC })
# MAGIC 
# MAGIC doCountC.print()
# MAGIC println("-"*80)

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Review The Results

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC displayHTML(f"""
# MAGIC <html>
# MAGIC   <body>
# MAGIC     <style>
# MAGIC       input { width: 10em; text-align:right }
# MAGIC     </style>
# MAGIC 
# MAGIC     <table style="margin:0">
# MAGIC       <tr style="background-color:#F0F0F0"><th colspan="2">&nbsp;</th><th>Sample A<br/>(Parquet/String)</th><th>Sample B<br/>(CSV/String)</th><th>Sample C<br/>(Parquet/Mixed)</th></tr>
# MAGIC       <tr>
# MAGIC         <td colspan="2">Number of Records:</td>
# MAGIC         <td><input type="text" value="${totalRecords}%,d"></td> 
# MAGIC         <td><input type="text" value="${totalRecords}%,d"></td> 
# MAGIC         <td><input type="text" value="${totalRecords}%,d"></td> 
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC         <td colspan="2">Query Duration:</td>
# MAGIC         <td><input type="text" value="${countA.duration}%,d ms"></td> 
# MAGIC         <td><input type="text" value="${countB.duration}%,d ms"></td> 
# MAGIC         <td><input type="text" value="${countC.duration}%,d ms"></td> 
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC         <td colspan="2">Records per MS:</td>
# MAGIC         <td><input type="text" value="${totalRecords/countA.duration}%,d"></td> 
# MAGIC         <td><input type="text" value="${totalRecords/countB.duration}%,d"></td> 
# MAGIC         <td><input type="text" value="${totalRecords/countC.duration}%,d"></td> 
# MAGIC       </tr>
# MAGIC 
# MAGIC 
# MAGIC       <tr style="background-color:#F0F0F0"><th colspan="5">&nbsp;</th></tr>
# MAGIC 
# MAGIC 
# MAGIC       <tr>
# MAGIC         <td colspan="2">Size on Disk:</td>
# MAGIC         <td><input type="text" value="${sizeOnDiskA/1024.0/1024.0/1024.0}%,.2f GB"></td> 
# MAGIC         <td><input type="text" value="${sizeOnDiskB/1024.0/1024.0/1024.0}%,.2f GB"></td> 
# MAGIC         <td><input type="text" value="${sizeOnDiskC/1024.0/1024.0/1024.0}%,.2f GB"></td> 
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC         <td colspan="2">Bytes per Record:</td>
# MAGIC         <td><input type="text" value="${sizeOnDiskA/totalRecords}"></td> 
# MAGIC         <td><input type="text" value="${sizeOnDiskB/totalRecords}"></td> 
# MAGIC         <td><input type="text" value="${sizeOnDiskC/totalRecords}"></td> 
# MAGIC       </tr>
# MAGIC 
# MAGIC 
# MAGIC       <tr style="background-color:#F0F0F0"><th colspan="5">&nbsp;</th></tr>
# MAGIC 
# MAGIC 
# MAGIC       <tr>
# MAGIC         <td colspan="2">Sized Cached in RAM:</td>
# MAGIC         <td><input type="text" value="${moA.cacheSize/1024.0/1024.0/1024.0}%,.2f GB"></td> 
# MAGIC         <td><input type="text" value="${moB.cacheSize/1024.0/1024.0/1024.0}%,.2f GB"></td> 
# MAGIC         <td><input type="text" value="${moC.cacheSize/1024.0/1024.0/1024.0}%,.2f GB"></td> 
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC         <td colspan="2">Bytes per Record:</td>
# MAGIC         <td><input type="text" value="${moA.cacheSize/totalRecords}"></td> 
# MAGIC         <td><input type="text" value="${moB.cacheSize/totalRecords}"></td> 
# MAGIC         <td><input type="text" value="${moC.cacheSize/totalRecords}"></td> 
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC         <td colspan="2">Cache Duration:</td>
# MAGIC         <td><input type="text" value="${moA.duration/1000.0}%,.2f sec"></td> 
# MAGIC         <td><input type="text" value="${moB.duration/1000.0}%,.2f sec"></td> 
# MAGIC         <td><input type="text" value="${moC.duration/1000.0}%,.2f sec"></td> 
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC         <td colspan="2">Query Duration:</td>
# MAGIC         <td><input type="text" value="${moCountA.duration} ms"></td> 
# MAGIC         <td><input type="text" value="${moCountB.duration} ms"></td> 
# MAGIC         <td><input type="text" value="${moCountC.duration} ms"></td> 
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC         <td colspan="2">Records per MS:</td>
# MAGIC         <td><input type="text" value="${totalRecords/moCountA.duration}%,d"></td> 
# MAGIC         <td><input type="text" value="${totalRecords/moCountB.duration}%,d"></td> 
# MAGIC         <td><input type="text" value="${totalRecords/moCountC.duration}%,d"></td> 
# MAGIC       </tr>
# MAGIC 
# MAGIC 
# MAGIC       <tr style="background-color:#F0F0F0"><th colspan="5">&nbsp;</th></tr>
# MAGIC 
# MAGIC 
# MAGIC       <tr>
# MAGIC         <td colspan="2">Sized Cached to Disk:</td>
# MAGIC         <td><input type="text" value="${doA.cacheSize/1024.0/1024.0/1024.0}%,.2f GB"></td> 
# MAGIC         <td><input type="text" value="${doB.cacheSize/1024.0/1024.0/1024.0}%,.2f GB"></td> 
# MAGIC         <td><input type="text" value="${doC.cacheSize/1024.0/1024.0/1024.0}%,.2f GB"></td> 
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC         <td colspan="2">Bytes per Record:</td>
# MAGIC         <td><input type="text" value="${doA.cacheSize/totalRecords}"></td> 
# MAGIC         <td><input type="text" value="${doB.cacheSize/totalRecords}"></td> 
# MAGIC         <td><input type="text" value="${doC.cacheSize/totalRecords}"></td> 
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC         <td colspan="2">Cache Duration:</td>
# MAGIC         <td><input type="text" value="${doA.duration/1000.0}%,.2f sec"></td> 
# MAGIC         <td><input type="text" value="${doB.duration/1000.0}%,.2f sec"></td> 
# MAGIC         <td><input type="text" value="${doC.duration/1000.0}%,.2f sec"></td> 
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC         <td colspan="2">Query Duration:</td>
# MAGIC         <td><input type="text" value="${doCountA.duration} ms"></td> 
# MAGIC         <td><input type="text" value="${doCountB.duration} ms"></td> 
# MAGIC         <td><input type="text" value="${doCountC.duration} ms"></td> 
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC         <td colspan="2">Records per MS:</td>
# MAGIC         <td><input type="text" value="${totalRecords/doCountA.duration}%,d"></td> 
# MAGIC         <td><input type="text" value="${totalRecords/doCountB.duration}%,d"></td> 
# MAGIC         <td><input type="text" value="${totalRecords/doCountC.duration}%,d"></td> 
# MAGIC       </tr>
# MAGIC 
# MAGIC 
# MAGIC     </table>
# MAGIC   </body>
# MAGIC </html>
# MAGIC """)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Why is the uncached query of CSV slower than the other two?
# MAGIC 
# MAGIC ### Why is Parquet/String on disk smaller than CSV if they are effectively the same data structure?
# MAGIC 
# MAGIC ### Why is **Parquet/Mixed** on disk smaller than **Parquet/String**?
# MAGIC 
# MAGIC ### Why are **Parquet/String** and **CSV** in RAM effectively identical?
# MAGIC 
# MAGIC ### Why is **Parquet/Mixed** in RAM smaller than the other two?
# MAGIC 
# MAGIC ### Which data structure caches the fastest?
# MAGIC 
# MAGIC ### Which data structure caches the slowest?
# MAGIC 
# MAGIC ### How important is it to use the correct file format?
# MAGIC 
# MAGIC ### How important is it to use the correct data structures?

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>