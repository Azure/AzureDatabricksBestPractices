// Databricks notebook source
// MAGIC 
// MAGIC %md-sandbox
// MAGIC 
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC #From 9x Faster to 300x Faster
// MAGIC 
// MAGIC **Dataset:**
// MAGIC * This is synthetic data generated specifically for these exercises
// MAGIC * Each year's data is roughly the same with some variation for market growth
// MAGIC * We are looking at retail purchases from the top N retailers

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

// MAGIC %run "./Includes/Utility-Methods"

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Introducing "tracker"
// MAGIC 
// MAGIC We are going to look at one more performance issue before moving on.
// MAGIC 
// MAGIC Namely how our "slow" data is actually slow for some other reasons.
// MAGIC 
// MAGIC To do that we are also going to look at one of the more advanced API components of Apache Spark
// MAGIC * Note the reference to **tracker** declared above
// MAGIC * This is a utility class for benchmarking code blocks, Spark jobs and even the cache
// MAGIC * Open the notebook **Utility-Methods** in a new tab
// MAGIC 
// MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> This is one of the many cases where the Python API lags behind the Scala version.

// COMMAND ----------

// MAGIC %md
// MAGIC An instance of this tool has already been declared for us.
// MAGIC 
// MAGIC We can see how it works here.

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

val resultsSample = tracker.track(() => {
  spark.read.parquet("/mnt/training/global-sales/cities/all.parquet").count()
})

resultsSample.printTime()
println("-"*80)

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) 2017 On Disk

// COMMAND ----------

var path2017 = "/mnt/training/global-sales/transactions/2017.parquet"

val (slowCountFiles, slowTotalSize) = computeFileStats(path2017)
val slowAvgSize = slowTotalSize/slowCountFiles 

println("Count:      %,10d files".format(slowCountFiles))
println("Total Size: %,10.2f MB".format(slowTotalSize/1000.0/1000.0))
println("Avg Size:   %,10.2f KB".format(slowAvgSize/1000.0))
println("-"*80)

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC <div style="display:table; height:600px; width:100%">
// MAGIC   <h2><img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png"/> Review 2017 On Disk</h2>
// MAGIC   
// MAGIC   <h3>&nbsp;</h3>
// MAGIC   <h3>What one detail stands out above all others?</h3>
// MAGIC   
// MAGIC   <h3>&nbsp;</h3>
// MAGIC   <h3>Based on everything we've seen so far, how would we go about fixing this?</h3>
// MAGIC   
// MAGIC   <h3>&nbsp;</h3>
// MAGIC   <h3>Assuming a specific solution, what parameters would you use?</h3>
// MAGIC   
// MAGIC   <h3>&nbsp;</h3>
// MAGIC   <h3>Before implementing our solution, let's grab numbers for both 2017-Fast &amp; 2017-Slow</h3>
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Loading the 2017-Fast &amp; 2017-Slow Benchmarks

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> If you were unable to complete some of the previous exercises, the following<br/>
// MAGIC entries in our **results** map will not exist and consequently produce an error.<br/>
// MAGIC In this case, use the provided sample-results instead.

// COMMAND ----------

val logPath = "%s/test-results".format(userhome)

// Optionally comment out your results and use the sample results instead
val results = loadYourTestMap(logPath)
// val results = loadYourTestMap("dbfs:/mnt/training/global-sales/solutions/sample-results")

val duration17Slow = results("Duration 2017-Slow").toLong
val duration17Fast = results("Duration 2017-Fast").toLong
val times14vs17 = (duration17Slow - duration17Fast) / duration17Fast.toDouble

println("2017-Slow Duration: %,8d ms".format(duration17Slow))
println("2017-Fast Duration: %,8d ms".format(duration17Fast))
println("2017-Fast vs 2017-Slow: %6.2fx".format(times14vs17))
println("-"*80)

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC <div style="display:table; height:600px; width:100%">
// MAGIC   <h2><img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png"/> Review 2017-Slow vs 2017-Fast</h2>
// MAGIC   
// MAGIC   <h3>&nbsp;</h3>
// MAGIC   <h3>These results are nothing new</h3>
// MAGIC   
// MAGIC   <h3>&nbsp;</h3>
// MAGIC   <h3>The key thing to focus on is the 20x to 30x performance increase</h3>
// MAGIC   
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Challenge #1
// MAGIC 
// MAGIC Our **slow** dataset actually contains tiny files.
// MAGIC 
// MAGIC A quick fix is to repartition the data.
// MAGIC 
// MAGIC But repartition to what?
// MAGIC 
// MAGIC Remember our rules of thumb for data-on-disk?
// MAGIC * 10s of GB per partition (~10-50 GB)
// MAGIC * 100s of MBs (usually 1GB is ideal)
// MAGIC 
// MAGIC But there are two "problems"
// MAGIC * We are not partitioning on disk
// MAGIC * The entire dataset is < 2 GB
// MAGIC 
// MAGIC In this case we might considering repartitioning down to one or two partitions.
// MAGIC 
// MAGIC **For the sake of this challenge, we want to produce ~175 MB part files.**
// MAGIC * Why 175 MB? Because our boss said so!

// COMMAND ----------

// MAGIC %md
// MAGIC ### Step #1
// MAGIC * Create a dataframe from our **slow** 2017 data.
// MAGIC * Assign it to the variable **trx2017DF**.

// COMMAND ----------

// TODO

var path2017 = "/mnt/training/global-sales/transactions/2017.parquet"

// In case you need to start over
// dbutils.fs.rm(fixedPath17, true)

val trx2017DF = FILL_IN

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ### Step #2
// MAGIC * Declare **fixedPath17** pointing to our "fixed" parquet directory
// MAGIC * Declare **tempPath** pointing to our temporary parquet directory
// MAGIC 
// MAGIC In both cases, make sure these folders exist within your personal "home" directory.
// MAGIC * <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** Use **userhome** in constructing the paths.

// COMMAND ----------

// TODO

val fixedPath17 = FILL_IN
val tempPath = FILL_IN

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ### Step #3
// MAGIC * Starting with **trx2017DF**
// MAGIC   0. Reduce the number of partitions down two **N**.
// MAGIC   0. Write the data out to **tempPath** as a parquet file.
// MAGIC   
// MAGIC **Question:** Should you reduce with **repartition(n)** or **coalesce(n)**?<br/>
// MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** **repartition(n)** is a wide transformation and **coalesce(n)** is a narrow transformation.
// MAGIC 
// MAGIC **Question:** What should the value of **N** be?<br/>
// MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** The entire dataset is just over 1 GB, the ideal size for on-disk partitions. This means we can handle any partition size, including a partition of 1.<br/>
// MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** The cluster has 8 cores to work with.<br/>
// MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** We are starting with 203 partitions (as evident by **trx2017DF.rdd.getNumPartitions**)

// COMMAND ----------

// TODO

trx2017DF
  .FILL_IN   // Reduce to N partitions
  .FILL_IN   // Write the parquet file to disk

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ### Step #4
// MAGIC * Compute the size of all files in our "temp" directory.
// MAGIC * <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** Use **computeFileStats(path)**

// COMMAND ----------

// TODO

val (tempFileCount, tempBytes) = FILL_IN

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ### Step #5
// MAGIC * Count the number of records in our dataset.
// MAGIC * <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** It will be faster to use the data in our temporary parquet file.

// COMMAND ----------

// TODO

val total2017 = FILL_IN

println("%,d records".format(total2017))
println("-"*80)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Step #6
// MAGIC * Calculate **bytesPerRecord**: divide **tempBytes** by **total2017**
// MAGIC * Calculate **recordsPerPartition**: divide **targetFileSize** by **bytesPerRecord**
// MAGIC * Calculate **numPartitions**: divide **total2017** by **recordsPerPartition**
// MAGIC * Round the final result to a whole number

// COMMAND ----------

// TODO

val targetFileSize = 175 * 1024.0 * 1024.0

val bytesPerRecord = FILL_IN
val recordsPerPartition = FILL_IN
val numPartitions = FILL_IN

println("%,10d bytes per record".format(bytesPerRecord))
println("%,10d records per partition".format(recordsPerPartition.toLong))
println("%,10d partitions".format(numPartitions))
println("-"*80)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Step #7
// MAGIC * Read in the temp data
// MAGIC * Repartition it according to **numPartitions**
// MAGIC * Write the data back out as a parquet file to **fixedPath17**

// COMMAND ----------

// TODO

spark.read
  .FILL_IN // Read the temp file
  .FILL_IN // Repartition the data
  .FILL_IN // Write the file out as parquet

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Validate Your Answer to #1
// MAGIC 
// MAGIC Run the cell below.
// MAGIC 
// MAGIC It's not possible to hit 175 MB with whole partitions.
// MAGIC 
// MAGIC However, 182 MB per partition would be considered close enough.

// COMMAND ----------

val files = dbutils.fs.ls(fixedPath17).map(f => (f.path, f.size))
val df = spark.createDataFrame(files)
  .toDF("path", "size")
  .filter($"path".endsWith(".parquet"))
  .withColumn("size", ($"size"/1024.0/1024.0).cast("decimal(10,1)"))

display(df)

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Benchmarking 2017-Fixed

// COMMAND ----------

val fixed17 = spark.read.parquet(fixedPath17)

val resultsFixed = tracker.track(() => fixed17.count)

val duration17Fixed = resultsFixed.duration
logYourTest(logPath, "Duration 2017-Fixed", duration17Fixed)

resultsFixed.printTime()
println("-"*80)

// COMMAND ----------

val (fixedCountFiles, fixedTotalSize) = computeFileStats(fixedPath17)
val fixedAvgSize = fixedTotalSize/fixedCountFiles 

println("Fast  vs Slow:  %,10dx faster".format((duration17Slow - duration17Fast) / duration17Fast))
println("Fixed vs Slow:  %,10dx faster".format((duration17Slow - duration17Fixed) / duration17Fixed))
println("-"*80)

println("Slow File Count:   %,8d files".format(slowCountFiles))
println("Fixed File Count:  %,8d files".format(fixedCountFiles))
println("-"*80)

println("Slow Total Size:   %,8.2f MB".format(slowTotalSize/1000.0/1000.0))
println("Fixed Total Size:  %,8.2f MB".format(fixedTotalSize/1000.0/1000.0))
println("-"*80)

println("Slow Avg Size:   %,10.2f MB".format(slowAvgSize/1000.0/1000.0))
println("Fixed Avg Size:  %,10.2f MB".format(fixedAvgSize/1000.0/1000.0))
println("-"*80)


// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>