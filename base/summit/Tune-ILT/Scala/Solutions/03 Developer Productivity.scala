// Databricks notebook source
// MAGIC 
// MAGIC %md-sandbox
// MAGIC 
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC #Developer Productivity
// MAGIC 
// MAGIC **Dataset:**
// MAGIC * This is synthetic data generated specifically for these exercises
// MAGIC * Each year's data is roughly the same with some variation for market growth
// MAGIC * We are looking at retail purchases from the top N retailers
// MAGIC 
// MAGIC **Technical Accomplishments:**
// MAGIC * Shorten development time
// MAGIC * Explore ways to increase query performance

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Getting Started
// MAGIC 
// MAGIC Run the following cells to configure our "classroom", initialize our labs and pull in some utility methods:

// COMMAND ----------

// MAGIC %run "./Includes/Classroom-Setup"

// COMMAND ----------

// MAGIC %run "./Includes/Initialize-Labs"

// COMMAND ----------

// MAGIC %run "./Includes/Utility-Methods"

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) The Introduction
// MAGIC 
// MAGIC We are going to start by looking at data for 2014.
// MAGIC 
// MAGIC We are then going to take a look at data for 2017.
// MAGIC 
// MAGIC Then we are then going to attempt to compare the total volume of sales for these two years.
// MAGIC 
// MAGIC Let's start with the basic setup:

// COMMAND ----------

// MAGIC %md
// MAGIC ### Load Data for 2014

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

var path2014 = "/mnt/training/global-sales/transactions/2014.parquet"
val trx2014DF = spark.read.parquet(path2014)

display(trx2014DF)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Load Data for 2017

// COMMAND ----------

var path2017 = "/mnt/training/global-sales/transactions/2017.parquet"
val trx2017DF = spark.read.parquet(path2017)

display(trx2017DF)

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> The **display(..)** command is Databricks' version of the **show(n)** command
// MAGIC * It's prettier and easier to work with
// MAGIC * Limits the query result to the first 1,000 records
// MAGIC * It doesn't accept any other arguments
// MAGIC * From it we can download CSV files, graph results, etc.

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Comparing Counts
// MAGIC 
// MAGIC We want to know programatically how long the query took so we are<br/>
// MAGIC going to employ a crude benchmark from our **Utility-Methods** notebook.
// MAGIC 
// MAGIC We will introduce a better solution in a later module.
// MAGIC 
// MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** Run the next couple of cells and proceed to **While we are waiting... #1**

// COMMAND ----------

val logPath = "%s/test-results".format(userhome)

println(logPath)
println("-"*80)

// COMMAND ----------

val (df, total2014Slow, duration14Slow) = benchmarkCount(
  () => trx2014DF
)

// Save our numbers for later
logYourTest(logPath, "Total 2014-Slow", total2014Slow)
logYourTest(logPath, "Duration 2014-Slow", duration14Slow)

// Print the results
println("2014 Totals:   %,d transactions".format(total2014Slow))
println("2014 Duration: %,.3f sec".format(duration14Slow/1000.0))
println("-"*80)

// COMMAND ----------

val (df, total2017Slow, duration17Slow) = benchmarkCount(
  () => trx2017DF
)

// Save our numbers for later
logYourTest(logPath, "Total 2017-Slow", total2017Slow)
logYourTest(logPath, "Duration 2017-Slow", duration17Slow)

println("2017 Totals:   %,d transactions".format(total2017Slow))
println("2017 Duration: %,.3f sec".format(duration17Slow/1000.0))
println("-"*80)

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC And if you are curious about the results we just logged,<br/>
// MAGIC you can view them by running the following command.
// MAGIC 
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> We will use these numbers later as we evaluate the results of different experiments.

// COMMAND ----------

val df = loadYourTestResults(logPath)
display(df)

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> We need the results of the previous queries later so make sure they complete, eventually.

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC <div style="display:table; height:600px; width:100%">
// MAGIC   <h2><img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png"/> While we are waiting... #1</h2>
// MAGIC 
// MAGIC 
// MAGIC   <p>The count operation is going to take a "long" time. </p>
// MAGIC     
// MAGIC   <p>10-15 minutes just for the count.</p>
// MAGIC 
// MAGIC   <p>And it's only going to get longer as we start doing more with this data.</p>
// MAGIC 
// MAGIC   <p>How hard would it be to increase developer productivity?</p>
// MAGIC   
// MAGIC   <p>We can always run against the full dataset when we are done.</p>
// MAGIC 
// MAGIC   <p>Again, our immediate goal is to speed up our development.</p>
// MAGIC 
// MAGIC   <h3 style="margin-top:1em">What options do we have for making this run faster [without caching]?</h3>
// MAGIC   
// MAGIC   <ul>
// MAGIC     <li>Option #1...</li>
// MAGIC     <li>Option #2...</li>
// MAGIC     <li>Option #3...</li>
// MAGIC     <li>Option #4...</li>
// MAGIC     <li>Option #5...</li>
// MAGIC     <li>Option #6...</li>
// MAGIC     <li>Option #7...</li>
// MAGIC   </ul>
// MAGIC 
// MAGIC   <p><img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** Take a look at the <b>Typed transformations</b> and the <b>Untyped transformations</b> section of the <a href="https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.Dataset" target="_blank">Scala docs for the class <b>Dataset</b></a> (aka DataFrame).</p>
// MAGIC   
// MAGIC   <p><img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** Then take a look at the <a href="https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.SparkSession" target="_blank">Scala docs for the class <b>SparkSession</b></a>.</p>
// MAGIC 
// MAGIC   <p><img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Even if you are using Python, the Scala docs are often easier to digest.<br/>
// MAGIC      Just the same, both sets of docs provide information not found in the other.</p>
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Challenge #1
// MAGIC 
// MAGIC Compared to the **count()** executed above, our next set of queries could take "forever."
// MAGIC 
// MAGIC 5-30 minutes for every query means we can be here all day.
// MAGIC 
// MAGIC And we don't get paid by the hour
// MAGIC 
// MAGIC **Implement our solution for speeding up our queries:**
// MAGIC * Remember our goal: faster development by getting more iterations on our ETL
// MAGIC * Start with the DataFrames **trx2014DF** and **trx2017DF** respectively
// MAGIC * Take a **10% sample** of the data **without replacement**
// MAGIC   * For the sample seed, use **42** <a href="https://en.wikipedia.org/wiki/Phrases_from_The_Hitchhiker%27s_Guide_to_the_Galaxy#Answer_to_the_Ultimate_Question_of_Life,_the_Universe,_and_Everything_(42)" target="_blank">because...</a>
// MAGIC * Write the data to the temp files **fastPath14** and **fastPath17** respectively
// MAGIC 
// MAGIC Once we are done, we'll read the "faster" data back in and assign the `DataFrame` to **fast2014DF** and **fast2017DF** respectively

// COMMAND ----------

val fastPath14 = s"%s/2014-fast.parquet".format(userhome)
val fastPath17 = s"%s/2017-fast.parquet".format(userhome)

// In case you need to start over
// dbutils.fs.rm(fastPath14, true)
// dbutils.fs.rm(fastPath17, true)

// COMMAND ----------

// ANSWER

trx2014DF.sample(false, fraction=0.10, seed=42)
         .write.mode("overwrite")
         .parquet(fastPath14)

trx2017DF.sample(false, fraction=0.10, seed=42)
         .write.mode("overwrite")
         .parquet(fastPath17)

// COMMAND ----------

// Use these only if you were unable to complete the previous exercise
// val fastPath14 = "/mnt/training/global-sales/solutions/2014-fast.parquet"
// val fastPath17 = "/mnt/training/global-sales/solutions/2017-fast.parquet"

val fast2014DF = spark.read.parquet(fastPath14)
val fast2017DF = spark.read.parquet(fastPath17)

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Validate Your Answer to #1
// MAGIC 
// MAGIC We can call your solution good if we can get it down to 1/10 of the original time or 9x faster.
// MAGIC 
// MAGIC **Note**: A 300x improvement is possible even with the full dataset
// MAGIC 
// MAGIC Run the cell below to validate your answer:

// COMMAND ----------

val (_, total2014Fast, duration14Fast) = benchmarkCount(() => fast2014DF)
val times14 = (duration14Slow - duration14Fast) / duration14Fast

val (_, total2017Fast, duration17Fast) = benchmarkCount(() => fast2017DF)
val times17 = (duration17Slow-duration17Fast)/duration17Fast


// Record our results for later
logYourTest(logPath, "Total 2014-Fast", total2014Fast)
logYourTest(logPath, "Duration 2014-Fast", duration14Fast)

logYourTest(logPath, "Total 2017-Fast", total2017Fast)
logYourTest(logPath, "Duration 2017-Fast", duration17Fast)

// COMMAND ----------

clearYourResults()

validateYourAnswer("01.A) Count 2014 Speed", 646192812, times14 > 9)
validateYourAnswer("01.B) Count 2017 Speed", 646192812, times17 > 9)

summarizeYourResults()

// COMMAND ----------

val results = loadYourTestMap(logPath)
// val results = loadYourTestMap("dbfs:/mnt/training/global-sales/solutions/sample-results")

displayHTML("""
<html><style>body {font-size:larger} td {padding-right:1em}td.value {text-align:right; font-weight:bold}</style><body><table>
  <tr><td>2014 Duration A:</td><td class="value">%,.3f sec</td></tr>
  <tr><td>2014 Duration B:</td><td class="value">%,.3f sec</td></tr>
  <tr><td>Improvement:</td>    <td class="value">%,dx faster</td></tr>
  <tr><td>&nbsp;</td></tr>
  <tr><td>2017 Duration A:</td><td class="value">%,.3f sec</td></tr>
  <tr><td>2017 Duration B:</td><td class="value">%,.3f sec</td></tr>
  <tr><td>Improvement:</td>    <td class="value">%,dx faster</td></tr>
</table></html></body>""".format(results("Duration 2014-Slow")/1000.0,
                                 results("Duration 2014-Fast")/1000.0,
                                 times14,
                                 results("Duration 2017-Slow")/1000.0,
                                 results("Duration 2017-Fast")/1000.0,
                                 times17))

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC <div style="display:table; height:600px; width:100%">
// MAGIC   <h2><img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png"/> While we are waiting... #2</h2>
// MAGIC 
// MAGIC   <h3>&nbsp;</h3>
// MAGIC   <h3>What are some of the pitfalls in benchmarking?</h3>
// MAGIC 
// MAGIC   <h3>&nbsp;</h3>
// MAGIC   <h3>What is wrong with our benchmarking strategy?</h3>
// MAGIC   
// MAGIC   <h3>&nbsp;</h3>
// MAGIC   <h3>What specific danger is there in regards to benchmarking the <b>count()</b> operation?</h3>
// MAGIC   
// MAGIC </div>

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC <div style="display:table; height:600px; width:100%">
// MAGIC   <h2><img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png"/> While we are waiting... #3</h2>
// MAGIC   <h3>In regards to a "test" dataset, what are the ramifications and/or merits of our other options?</h3>
// MAGIC   <h3>...Run with more cores</h3>
// MAGIC   <h3>...Caching</h3>
// MAGIC   <h3>...limit(n)</h3>
// MAGIC   <h3>...filter(x)</h3>
// MAGIC   <h3>...sample(..)</h3>
// MAGIC   <h3>...Create an empty DataFrame</h3>
// MAGIC   <h3>...Create "test" data</h3>
// MAGIC   
// MAGIC   <h3>&nbsp;</h3>
// MAGIC   <h3>What techniques do we have for creating "test" data?</h3>
// MAGIC 
// MAGIC </div>

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Challenge #2
// MAGIC 
// MAGIC Using **sample(..)** has a lot of benefits but it does require the up-front cost to produce it.
// MAGIC 
// MAGIC Creating a CSV file is straightforward but distributing it and putting it under version control can present new challenges.
// MAGIC 
// MAGIC **Programmatically create some test data**
// MAGIC * Create a collection of test values and assign it to the value **candies**
// MAGIC   * In Python this will be a list
// MAGIC   * In Scala this will be an Array
// MAGIC * The collection should have the following candies:
// MAGIC <table>
// MAGIC   <tr style="background-color:#f0f0f0"><td>color:String</td><td>chocolate:Boolean</td><td>grams:Long</td></tr>
// MAGIC   <tr><td>red</td><td>false</td><td style="text-align:right">3</td></tr>
// MAGIC   <tr><td>green</td><td>false</td><td style="text-align:right">4</td></tr>
// MAGIC   <tr><td>brown</td><td>true</td><td style="text-align:right">10</td></tr>
// MAGIC   <tr><td>white</td><td>true</td><td style="text-align:right">9</td></tr>
// MAGIC   <tr><td>red</td><td>false</td><td style="text-align:right">3</td></tr>
// MAGIC   <tr><td>green</td><td>false</td><td style="text-align:right">4</td></tr>
// MAGIC </table>
// MAGIC <br/>
// MAGIC * Once the collection has been created, use an instance of **SparkSession** to create the DataFrame
// MAGIC * Convert to a DataFrame and in doing so, name each column "color", "chocolate" and "grams"
// MAGIC * Aggregate the results by **chocolate** and then count them
// MAGIC * Extract the total number of chocolate candies and assign it to the variable **chocolateCount**
// MAGIC * Extract the total number of non-chocolate candies and assign it to the variable **otherCount**

// COMMAND ----------

// ANSWER

val candies = Array(
  ("red", false, 3),
  ("green", false, 4),
  ("brown", true, 10),
  ("white", true, 9),
  ("red", false, 3),
  ("green", false, 4)
)

val df = spark
  .createDataFrame(candies)
  .toDF("color", "chocolate", "grams")
  .groupBy("chocolate")
  .count()

val chocolateCount = df.filter($"chocolate" === true).head.getAs[Long]("count")
val otherCount = df.filter($"chocolate" === false).head.getAs[Long]("count")

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Validate Your Answer to #2
// MAGIC 
// MAGIC Run the cell below to validate your answer:

// COMMAND ----------

clearYourResults()
validateYourAnswer("02.A) Chocolate Count", 870267989, chocolateCount)
validateYourAnswer("02.B) Other Count", 2142269034, otherCount)
summarizeYourResults()


// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>