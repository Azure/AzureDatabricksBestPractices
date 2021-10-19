// Databricks notebook source
// MAGIC 
// MAGIC %md-sandbox
// MAGIC 
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC #Comparing 2014 vs 2018
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

// We know these numbers from the previous lab
val expectedFast14 = "413199362.16"
val expectedFast17 = "492940869.09"

val expectedSlow14 = "4132674423.72"
val expectedSlow17 = "4923864114.44"

def validateSchema(df:DataFrame):Unit = {
  assert(df.columns.size == 3, "Expected three and only three columns")

  val schema = df.schema.mkString
  assert(schema.contains("year,IntegerType"), "Missing the year column")
  assert(schema.contains("day,IntegerType"), "Missing the day column")
  assert(schema.contains("amount,DecimalType"), "Missing the amount column")
  
  val expected = 61
  val total = df.count()

  assert(total == expected, "Expected %s records, found %s".format(expected, total))
}

def validateSum(df:DataFrame, expected:String):Unit = {
  import org.apache.spark.sql.functions.sum
  val total = df.select( sum($"amount").cast("decimal(20,2)").cast("string").as("total") ).as[String].first
  assert(total == expected, "Expected the final sum to be %s but found %s".format(expected, total))
}

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) The Introduction
// MAGIC 
// MAGIC If you recall from our previous lab, we induced a performance bug.
// MAGIC 
// MAGIC In this lab we are going to alter our function to address said bug.

// COMMAND ----------

// MAGIC %md
// MAGIC ### Load Data for 2014 & 2017

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

spark.catalog.clearCache()

var path2014 = "/mnt/training/global-sales/transactions/2014.parquet"
val trx2014DF = spark.read.parquet(path2014)

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ### Load our "faster" dataset for us to test with
// MAGIC 
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Because we will not be working with the 2017<br/>
// MAGIC data in this iteration, we will only prepare the 2014 data.

// COMMAND ----------

val fastPath14 = "/mnt/training/global-sales/solutions/2014-fast.parquet"
val fast2014DF = spark.read.parquet(fastPath14)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Carrying our ETL method forward
// MAGIC 
// MAGIC Now would be a good time to move this method to an imported notebook like **Utility-Methods**
// MAGIC 
// MAGIC Or perhaps something new like **Shared ETL Methods**, a new Jar or a new egg.
// MAGIC 
// MAGIC But for clarity and simplicity, we are simply going to re-implement it here:

// COMMAND ----------

def sumByDayOfYear(sourceDF:DataFrame):DataFrame = {
  var df = sourceDF
  
  if (sourceDF.columns.contains("year") == false) {
    df = df.withColumn("year", year($"transacted_at"))
  }
  
  if (sourceDF.columns.contains("month") == false) {
    df = df.withColumn("month", month($"transacted_at"))
  }
  
  df.filter($"month" >= 11)
    .withColumn("day", dayofyear($"transacted_at"))
    .groupBy("year", "day").sum("amount")
    .withColumnRenamed("sum(amount)", "amount")
    .orderBy("day")
    .persist()
}

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) November and December of 2018
// MAGIC 
// MAGIC With our various testing and utility methods, we should be able to add one more year to our analysis fairly easily
// MAGIC 
// MAGIC Let's take a look at 2018, namely the files on disk:

// COMMAND ----------

// MAGIC %fs ls /mnt/training/global-sales/transactions/2018.parquet

// COMMAND ----------

// MAGIC %md
// MAGIC We can see from above that 2018 is partitioned by year.
// MAGIC 
// MAGIC Let's take a look at the contents of **year=2018**:

// COMMAND ----------

// MAGIC %fs ls dbfs:/mnt/training/global-sales/transactions/2018.parquet/year=2018

// COMMAND ----------

// MAGIC %md
// MAGIC In addition to year, we are also partitioned by month.
// MAGIC 
// MAGIC Let's take a further look at the contents of **month=1**:

// COMMAND ----------

// MAGIC %fs ls dbfs:/mnt/training/global-sales/transactions/2018.parquet/year=2018/month=1/

// COMMAND ----------

// MAGIC %md
// MAGIC We can say fairly conclusively that this data is partitioned just like our 2014 data.
// MAGIC 
// MAGIC That means we should have no problems analyzing this data with our function **sumByDayOfYear(..)**

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Challenge #1
// MAGIC 
// MAGIC **Load and analyze the 2018 data**
// MAGIC * Declare the variable **path2018** and initialize it to the location of the 2018 parquet file.
// MAGIC * Read in the parquet file and assign it to **trx2018DF**
// MAGIC * Create a "fast" version of our data for testing purposes:
// MAGIC   * Starting with **trx2018DF**, take a 10% sample without replacement, using the seed **42**
// MAGIC   * Write the result to the file **2018-fast.parquet** in your **userhome** directory represented by **fast18Path**
// MAGIC * Read in the "fast" parquet file and assign it to **fast2018DF**, reading it back in from **fast18Path**

// COMMAND ----------

// ANSWER 

var path2018 = "/mnt/training/global-sales/transactions/2018.parquet"
val trx2018DF = spark.read.parquet(path2018)

val fast18Path = "%s/2018-fast.parquet".format(userhome)

// In case you need to start over
// dbutils.fs.rm(fast18Path, true)

trx2018DF.sample(false, fraction=0.10, seed=42)
         .write
         .mode("overwrite")
         .parquet(fast18Path)

val fast2018DF = spark.read.parquet(fast18Path)

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Validate Your Answer to #1
// MAGIC 
// MAGIC For the sake of a fast iteration, let's test the "fast" version of the 2018 data.

// COMMAND ----------

val expectedFast18 = "447558919.47"

val holidayFast18DF = sumByDayOfYear(fast2018DF)
validateSchema(holidayFast18DF)
validateSum(holidayFast18DF, expectedFast18)

// COMMAND ----------

// MAGIC %md
// MAGIC Now that we know the basics are working, let's test with the full set of 2018 data.

// COMMAND ----------

val expectedSlow18 = "4471732275.67"

val holidaySlow18DF = sumByDayOfYear(trx2018DF)
validateSchema(holidaySlow18DF)
validateSum(holidaySlow18DF, expectedSlow18)

// COMMAND ----------

// MAGIC %md
// MAGIC And now that all the data is loaded, we can union the three datasets together and graph them.

// COMMAND ----------

// Load & prepare the fast version of 2017 we created previously
val fast2017DF = spark.read.parquet("%s/2017-fast.parquet".format(userhome))
val holidayFast17DF = sumByDayOfYear(fast2017DF)

// Prepare the fast version of 2017 we created previously
val holidayFast14DF = sumByDayOfYear(fast2014DF)

val allFastDF = holidayFast18DF.unionByName(holidayFast17DF).unionByName(holidayFast14DF)
display(allFastDF.select("day", "amount", "year"))

// COMMAND ----------

// MAGIC %md
// MAGIC And if you are feeling really brave, we union the three "slow" datasets together and graph them as well.

// COMMAND ----------

// Load & prepare the slow version of 2017 we created previously
val path2017 = "/mnt/training/global-sales/transactions/2017.parquet"
val trx2017DF = spark.read.parquet(path2014)
val holidaySlow17DF = sumByDayOfYear(trx2017DF)

// Prepare the fast version of 2014 we created previously
val holidaySlow14DF = sumByDayOfYear(trx2014DF)

val allSlowDF = holidaySlow18DF.unionByName(holidaySlow17DF).unionByName(holidaySlow14DF)
display(allFastDF.select("day", "amount", "year"))

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC <div style="display:table; height:600px; width:100%">
// MAGIC   <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"/> Review Challenge #1</h2>
// MAGIC   <h3>What new revelation have we stumbled upon? </h3>
// MAGIC   <h3>&nbsp;</h3>
// MAGIC   <h3>How long should it have taken to spot the problem? </h3>
// MAGIC </div>

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC <img alt="Best Practice" title="Best Practice" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-blue-ribbon.svg"/> You really need to know your data...

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Challenge #2
// MAGIC 
// MAGIC If the 2018 data has the same on-disk partitioning benefits as 2014, why is there such a big performance difference?
// MAGIC 
// MAGIC **Identify the difference between 2018 & 2014**
// MAGIC * We can ignore 2017 - it is not partitioned on disk - significantly different
// MAGIC * Clear the cache with the call **spark.catalog.clearCache()**
// MAGIC * Re-run the count for 2014 and 2018 on the full datasets (**trx2014DF** and **trx2018DF**)
// MAGIC * As it's running, start to answer the following questions:

// COMMAND ----------

// Clear any caches so that we get a true test
spark.catalog.clearCache()

// COMMAND ----------

printf("2014 Count: %,d records\n", trx2014DF.count())

// COMMAND ----------

printf("2018 Count: %,d records\n", trx2018DF.count())

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC <html>
// MAGIC   <head>
// MAGIC     <script src="https://files.training.databricks.com/static/assets/spark-ilt/labs.js"></script>
// MAGIC     <link rel="stylesheet" type="text/css" href="https://s3-us-west-2.amazonaws.com/files.training.databricks.com/static/assets/spark-ilt/labs.css">
// MAGIC   </head>
// MAGIC   <body>
// MAGIC     <div id="drink-me"><button style="width:15em" onclick="block('next-step', 'drink-me')">Start Step #1</button></div>
// MAGIC 
// MAGIC     <div id="next-step" style="display:none">
// MAGIC       <div style="float:right; margin-right:1em"><button onclick="block('drink-me', 'next-step')">Close</button></div>
// MAGIC       <h2><img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png"/>What is the problem?</h2>
// MAGIC 
// MAGIC       <p><button style="width:20em" onclick="block('ca', 'wa-1', 'wa-2')">Degraded Performance</button></p>
// MAGIC       <p><button style="width:20em" onclick="block('wa-1', 'ca', 'wa-2')">Application Crashed</button></p>
// MAGIC       <p><button style="width:20em" onclick="block('wa-2', 'ca', 'wa-1')">Wrong Result</button></p>
// MAGIC 
// MAGIC       <div id="ca" style="display:none">
// MAGIC         <p>Good - If we let this run long enough, we will get the correct result and it will complete succesfully.</p>
// MAGIC         <p class="next-step">Go to the next step</p>
// MAGIC       </div>
// MAGIC 
// MAGIC       <p id="wa-1" style="display:none">Are you sure? Have you allowed it to run to completion?</p>
// MAGIC 
// MAGIC       <p id="wa-2" style="display:none">Are you sure? If we wait long enough, we should get the correct result.</p>
// MAGIC       
// MAGIC     </div>
// MAGIC   </body>
// MAGIC </html>

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC <html>
// MAGIC   <head>
// MAGIC     <script src="https://files.training.databricks.com/static/assets/spark-ilt/labs.js"></script>
// MAGIC     <link rel="stylesheet" type="text/css" href="https://s3-us-west-2.amazonaws.com/files.training.databricks.com/static/assets/spark-ilt/labs.css">
// MAGIC   </head>
// MAGIC   <body>
// MAGIC     <div id="drink-me"><button style="width:15em" onclick="block('next-step', 'drink-me')">Start Step #2</button></div>
// MAGIC 
// MAGIC     <div id="next-step" style="display:none">
// MAGIC       <div style="float:right; margin-right:1em"><button onclick="block('drink-me', 'next-step')">Close</button></div>
// MAGIC       <h2><img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png"/>How is the application degrading?</h2>
// MAGIC 
// MAGIC       <p><button style="width:20em" onclick="block('wa-1', 'ca', 'wa-2')">Degradation is Exponential</button></p>
// MAGIC       <p><button style="width:20em" onclick="block('ca', 'wa-1', 'wa-2')">Degradation between Dataset</button></p>
// MAGIC       <p><button style="width:20em" onclick="block('wa-2', 'ca', 'wa-1')">Degradation is Linear</button></p>
// MAGIC 
// MAGIC       <div id="ca" style="display:none">
// MAGIC         <p>Good - The code worked with the 2014 data and does not work with the 2018 data.<br/>
// MAGIC            If the code is identical, stands to reason that there is something different about the data.</p>
// MAGIC         
// MAGIC         <p>It is still possible that the number of records in the 2014 dataset vs the 2018 dataset might be the issue.</p>
// MAGIC       
// MAGIC         <p>By what percentage did the number of records increase?</p>
// MAGIC         <table>
// MAGIC           <tr><td>Operation</td><td>Count</td></tr>
// MAGIC           <tr><td style="font-weight:bold">sumByDayOfYear(trx2014DF)</td><td style="text-align:right">?</td></tr>
// MAGIC           <tr><td style="font-weight:bold">sumByDayOfYear(trx2018DF)</td><td style="text-align:right">?</td></tr>
// MAGIC           <tr><td style="font-weight:bold">countA / countB</td><td style="text-align:right">?</td></tr>
// MAGIC         </table>
// MAGIC 
// MAGIC         <p>By what percentage did the execution time increase?</p>
// MAGIC         <table>
// MAGIC           <tr><td>Operation</td><td>Duration</td></tr>
// MAGIC           <tr><td style="font-weight:bold">trx2014DF.count()</td><td style="text-align:right">?</td></tr>
// MAGIC           <tr><td style="font-weight:bold">trx2018DF.count()</td><td style="text-align:right">?</td></tr>
// MAGIC           <tr><td style="font-weight:bold">durA / durB</td><td style="text-align:right">?</td></tr>
// MAGIC         </table>
// MAGIC 
// MAGIC         <p>It should become clear that the increase in execution time is disproportionate to the increase in the number of records.</p>
// MAGIC         
// MAGIC         <p class="next-step">Go to the next step</p>
// MAGIC         
// MAGIC       </div>
// MAGIC 
// MAGIC       <p id="wa-1" style="display:none">
// MAGIC         If the degradation was exponential, we should see an exponential growth in execution time.<br/>
// MAGIC         This often occurs as we incrementally add transformations to our pipline - which we have not.
// MAGIC       </p>
// MAGIC 
// MAGIC       <p id="wa-2" style="display:none">
// MAGIC         If the degradation was linear, we see a steady growth in execution time.<br/>
// MAGIC         This often occurs as we add more data - the amount of data between 2018 and 2014 is not enough to explain a jump from ~1 minute to 10+ minutes.
// MAGIC       </p>
// MAGIC       
// MAGIC     </div>
// MAGIC   </body>
// MAGIC </html>

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC <html>
// MAGIC   <head>
// MAGIC     <script src="https://files.training.databricks.com/static/assets/spark-ilt/labs.js"></script>
// MAGIC     <link rel="stylesheet" type="text/css" href="https://s3-us-west-2.amazonaws.com/files.training.databricks.com/static/assets/spark-ilt/labs.css">
// MAGIC   </head>
// MAGIC   <body>
// MAGIC     <div id="drink-me"><button style="width:15em" onclick="block('next-step', 'drink-me')">Start Step #3</button></div>
// MAGIC 
// MAGIC     <div id="next-step" style="display:none">
// MAGIC       <div style="float:right; margin-right:1em"><button onclick="block('drink-me', 'next-step')">Close</button></div>
// MAGIC       <h2><img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png"/>How is the data different?</h2>
// MAGIC 
// MAGIC       <p>The trick here is to rule out those issues that are the easiest to investigate:</p>
// MAGIC       <ol>
// MAGIC         <li>The easiest are immediately available from the notebook such as the number of jobs, tasks and stages</li>
// MAGIC         <li>The Spark UI puts a wealth of information at your fingertips</li>
// MAGIC         <li>Investigating the makeup and structure of the data on disk takes a bit more work (but we have some shortcuts for you)</li>
// MAGIC         <li>And log files, while easily accessible, are never easily digested</li>
// MAGIC       </ol>
// MAGIC       
// MAGIC       <p>Over time you will develop an intuition as to the cause of problems and where to first look for explinations.</p>
// MAGIC       
// MAGIC       <p>Let's start by answering the following questions:</p>
// MAGIC       
// MAGIC     </div>
// MAGIC   </body>
// MAGIC </html>

// COMMAND ----------

// MAGIC %run "./Worksheets/2014 vs 2018"

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC <html>
// MAGIC   <head>
// MAGIC     <script src="https://files.training.databricks.com/static/assets/spark-ilt/labs.js"></script>
// MAGIC     <link rel="stylesheet" type="text/css" href="https://s3-us-west-2.amazonaws.com/files.training.databricks.com/static/assets/spark-ilt/labs.css">
// MAGIC   </head>
// MAGIC   <body>
// MAGIC     <div id="drink-me"><button style="width:15em" onclick="block('next-step', 'drink-me')">Start Step #4</button></div>
// MAGIC 
// MAGIC     <div id="next-step" style="display:none">
// MAGIC       <div style="float:right; margin-right:1em"><button onclick="block('drink-me', 'next-step')">Close</button></div>
// MAGIC       <h2><img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png"/>Is there an easier way?</h2>
// MAGIC 
// MAGIC       <p>Some of our RSA came up with this little trick:</p>
// MAGIC       <ol>
// MAGIC         <li>List the files on disk</li>
// MAGIC         <li>Collect the count and sum of each file</li>
// MAGIC         <li>Aggregrate the results by partition</li>
// MAGIC         <li>Graph it - because a picture is worth a 1K words</li>
// MAGIC         <li>And do it all with DataFrames and a Databricks notebook</li>
// MAGIC       </ol>
// MAGIC     
// MAGIC       <p>We'll explore this option in our next section, **StatsFile**
// MAGIC       
// MAGIC     </div>
// MAGIC     
// MAGIC   </body>
// MAGIC </html>

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC <style>td {padding:0} table { border-spacing: 0; border-collapse: collapse; }</style>
// MAGIC <div style="display:table; height:600px; width:100%">
// MAGIC   <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"/> Review Challenge #2A, Data Storage Rules of Thumb</h2>
// MAGIC 
// MAGIC   <h3>&nbsp;</h3>
// MAGIC   <h3 style="margin:0">Partitioning</h3>
// MAGIC   <ul style="font-size:x-large">
// MAGIC     <li>Evenly distributed data across all partitions</li>
// MAGIC     <li style="color:green; font-weight:bold">Tens of GB per partition (~10-50GB)</li>
// MAGIC     <li>Small data sets should not be partitioned</li>
// MAGIC     <li>Beware of over-partitioning</li>
// MAGIC   </ul>
// MAGIC 
// MAGIC   <h3>&nbsp;</h3>
// MAGIC   <h3>Files</h3>
// MAGIC   <ul style="font-size:x-large">
// MAGIC     <li>Parquet is best, ORC is okay, CSV &amp; JSON are BAD!</li>
// MAGIC     <li>Small files are bad (slow listing from storage)</li>
// MAGIC     <li style="color:green; font-weight:bold">Hundreds of MBs (usually 1GB is ideal)</li>
// MAGIC   </ul>
// MAGIC 
// MAGIC </div>

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC <style>td {padding:0} table { border-spacing: 0; border-collapse: collapse; }</style>
// MAGIC <div style="display:table; height:600px; width:100%">
// MAGIC   <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"/> Review Challenge #2B, The 4 Ses</h2>
// MAGIC   <h3>Look for these four symptoms</h3>
// MAGIC   
// MAGIC   <ul style="font-size:x-large">
// MAGIC     <li>1) Spill</li>
// MAGIC     <li>2) Shuffle</li>
// MAGIC     <li>3) Skew/Stragglers</li>
// MAGIC     <li>4) Small Files</li>
// MAGIC   </ul>
// MAGIC </div>

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>