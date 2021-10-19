// Databricks notebook source
// MAGIC 
// MAGIC %md-sandbox
// MAGIC 
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC #Comparing 2014 to 2017
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

import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel

// Why 8 partitions?
spark.conf.set("spark.sql.shuffle.partitions", 8)

val logPath = "%s/test-results".format(userhome)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### Why would we settle on 8 partitions for the spark.sql.shuffle.partitions property ?

// COMMAND ----------

// MAGIC %md
// MAGIC ### Load Data for 2014

// COMMAND ----------

// Why specify the schema?
val schema2014 = "transacted_at timestamp, trx_id integer, retailer_id integer, description string, amount decimal(38,18), city_id integer, year integer, month integer"

// The "slow" version of the 2014 dataset
var path2014 = "/mnt/training/global-sales/transactions/2014.parquet"
val trx2014DF = spark.read.schema(schema2014).parquet(path2014)

// The "fast" version of the 2014 dataset
val fastPath14 = "/mnt/training/global-sales/solutions/2014-fast.parquet"
val fast2014DF = spark.read.schema(schema2014).parquet(fastPath14)

display(fast2014DF)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Load Data for 2017

// COMMAND ----------

val schema2017 = "transacted_at timestamp, trx_id integer, retailer_id integer, description string, amount decimal(38,18), city_id integer"

// The "slow" version of the 2017 dataset
var path2017 = "/mnt/training/global-sales/transactions/2017.parquet"
val trx2017DF = spark.read.schema(schema2017).parquet(path2017)

// The "fast" version of the 2017 dataset
val fastPath17 = "/mnt/training/global-sales/solutions/2017-fast.parquet"
val fast2017DF = spark.read.schema(schema2017).parquet(fastPath17)

display(fast2017DF)

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> It's very common to see slight variations between two "semantically identical" sets of data.
// MAGIC * In the case of 2014, someone saw fit to add an extra **year** and **month** column to the base dataset.
// MAGIC * This is convenient, but can add extra storage overhead to our datasets.
// MAGIC * In the case of 2017, the **year** and **month** columns are missing but extracting these from the **transacted_at** column is cheap.

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Challenge #1
// MAGIC 
// MAGIC **Compare the amount of sales for each month:**
// MAGIC * For 2017 only
// MAGIC   * Add the column **year** by extracting it from **transacted_at**
// MAGIC   * Add the column **month** by extracting it from **transacted_at**
// MAGIC   * In 2014 someone had the presence of mind to do this for us
// MAGIC * Aggregate by **year** and then by **month**
// MAGIC * For each aggregate, sum the **amount**
// MAGIC * Sort the data by **year** and then by **month**
// MAGIC * Rename the aggregate value to **amount**
// MAGIC * The final schema must include:
// MAGIC   * `year`: `integer`
// MAGIC   * `month`: `integer`
// MAGIC   * `amount`: `decimal`
// MAGIC * For the 2014 dataset, assign the final `DataFrame` to **byMonth2014DF**
// MAGIC * For the 2017 dataset, assign the final `DataFrame` to **byMonth2017DF**
// MAGIC * Lastly, cache the result so that evaluating it doesn't re-trigger a full execution

// COMMAND ----------

// ANSWER

spark.catalog.clearCache()

val byMonth2014DF = fast2014DF
  // no need to extract the year from transacted_at
  // no need to extract the month from transacted_at
  .groupBy("year", "month")
  .sum("amount")
  .withColumnRenamed("sum(amount)", "amount")
  .orderBy("year", "month")
  .persist(StorageLevel.DISK_ONLY)

display(byMonth2014DF.select("month", "amount"))

// COMMAND ----------

// ANSWER

val byMonth2017DF = fast2017DF
  .withColumn("year", year($"transacted_at"))
  .withColumn("month", month($"transacted_at"))
  .groupBy("year", "month")
  .sum("amount")
  .withColumnRenamed("sum(amount)", "amount")
  .orderBy("year", "month")
  .persist(StorageLevel.DISK_ONLY)

display(byMonth2017DF.select("month", "amount"))

// COMMAND ----------

// MAGIC %md
// MAGIC When you are done, render the results above as a line graph.

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Validate Your Answer to #1
// MAGIC Run the cells below to validate your answer:

// COMMAND ----------

clearYourResults()

validateYourSchema("01.A) byMonth2014DF", byMonth2014DF, "year", "integer")
validateYourSchema("01.B) byMonth2014DF", byMonth2014DF, "month", "integer")
validateYourSchema("01.C) byMonth2014DF", byMonth2014DF, "amount", "decimal")

val count2014 = byMonth2014DF.count()
validateYourAnswer("01.D) 2014 Expected 12 Records", 155192030, count2014)

summarizeYourResults()

// COMMAND ----------

clearYourResults()

validateYourSchema("01.E) byMonth2017DF", byMonth2017DF, "year", "integer")
validateYourSchema("01.F) byMonth2017DF", byMonth2017DF, "month", "integer")
validateYourSchema("01.G) byMonth2017DF", byMonth2017DF, "amount", "decimal")

val count2017 = byMonth2017DF.count()
validateYourAnswer("01.H) 2017 expected 12 Records", 155192030, count2017)

summarizeYourResults()

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC 
// MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> The following assertions will pass only if using the "faster" datasets and the exact same solution:  
// MAGIC Take a 10% sample of the data without replacement using the seed "42"

// COMMAND ----------

clearYourResults()

val sum2014 = byMonth2014DF.select( sum($"amount").cast("decimal(20,2)").cast("string").as("total") ).as[String].first
validateYourAnswer("01.I) 2014 Sum", 1392416866, sum2014)

summarizeYourResults()

// COMMAND ----------

clearYourResults()

val sum2017 = byMonth2017DF.select( sum($"amount").cast("decimal(20,2)").cast("string").as("total") ).as[String].first
validateYourAnswer("01.J) 2017 Sum", 1111158808, sum2017)

summarizeYourResults()

// COMMAND ----------

// MAGIC %md
// MAGIC ### Next, put our datasets to work
// MAGIC 
// MAGIC Let's merge the two results, present them as a line graph, and then compare.
// MAGIC 
// MAGIC You will need to adjust the plot options to group by year (Series groupings).

// COMMAND ----------

val bothYearsDF = byMonth2014DF.unionByName(byMonth2017DF)

display(bothYearsDF.select("month", "amount", "year"))

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> There is a big difference between **union** and **unionByName**

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC <div style="display:table; height:600px; width:100%">
// MAGIC   <h2><img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png"/> Review Challenges #1</h2>
// MAGIC   
// MAGIC   <h3>&nbsp;</h3>
// MAGIC   <h3>What can we conclude about the data?</h3>
// MAGIC     
// MAGIC   <h3>&nbsp;</h3>
// MAGIC   <h3>Go back to byMonth2014DF and byMonth2017DF...</h3>
// MAGIC   <h3>...when we created the "fast" version of this dataset, how many partitions did we write out?</h3>
// MAGIC   <h3>...how many partitions are we getting on the initial read?</h3>
// MAGIC   <h3>...how do these numbers relate to total number of cores?</h3>
// MAGIC   <h3>...can you control the number of partitions initially read in?</h3>
// MAGIC   
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Challenge #1b
// MAGIC 
// MAGIC **Go back and update the query to read 8 partitions without adding additional stages.**

// COMMAND ----------

// ANSWER

// Blow away the old results
spark.catalog.clearCache()

val byMonth2014DF = fast2014DF
  .coalesce(8)
  // no need to extract the year from transacted_at
  // no need to extract the month from transacted_at
  .groupBy("year", "month")
  .sum("amount")
  .withColumnRenamed("sum(amount)", "amount")
  .orderBy("year", "month")
  .persist(StorageLevel.DISK_ONLY)

display(byMonth2014DF.select("month", "amount"))

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Review Challenge #1b
// MAGIC 
// MAGIC Start by taking a look at the following output:

// COMMAND ----------

printRecordsPerPartition(fast2014DF)
println()
printRecordsPerPartition(fast2014DF.coalesce(8))

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC <div style="display:table; height:600px; width:100%">
// MAGIC   <h3>How many tasks were generated by the first job?</h3>
// MAGIC 
// MAGIC   <h3>&nbsp;</h3>
// MAGIC   <h3>How many tasks were generated by the second job?</h3>
// MAGIC 
// MAGIC   <h3>&nbsp;</h3>
// MAGIC   <h3>Were any new stages added to the queries?</h3>
// MAGIC 
// MAGIC   <h3>&nbsp;</h3>
// MAGIC   <h3>Compare the number of records between the first and last partitions...</h3>
// MAGIC 
// MAGIC   <h3>&nbsp;</h3>
// MAGIC   <h3>What is this problem referred to?</h3>
// MAGIC 
// MAGIC   <h3>&nbsp;</h3>
// MAGIC   <h3>What effect will this have with the spark scheduler?</h3>
// MAGIC   
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) November and December
// MAGIC 
// MAGIC We can make some rather bland conclusions about January to October
// MAGIC 
// MAGIC Something changes in November
// MAGIC 
// MAGIC And things really pick up in December.
// MAGIC 
// MAGIC We are going to start over and zero in on the Christmas shopping season.
// MAGIC 
// MAGIC Instead of looking at a month at a time, let's break it down by day.

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Challenge #2
// MAGIC 
// MAGIC **Compare the amount of sales in November & December of 2014 to 2017:**
// MAGIC * Start with our "faster" DataFrames **fast2017DF** and **fast2014DF** respectively
// MAGIC   * This will save time as you code and test the various transformations
// MAGIC   * **When you are done**, replace with **trx2014DF** and **trx2017DF** respectively
// MAGIC * For 2017 only
// MAGIC   * Add the column **month** by extracting it from **transacted_at**
// MAGIC   * Add the column **year** by extracting it from **transacted_at**
// MAGIC   * In 2014 someone had the presence of mind to do this for us
// MAGIC * Limit the datasets to November and December only
// MAGIC * For 2017 & 2014, add the column **day** by extracting it from **transacted_at**
// MAGIC   * Because the intent is to compare day-to-day over several months, use **dayofyear(..)**
// MAGIC * Aggregate by **year** and then by **day**
// MAGIC * For each aggregate, sum the **amount**
// MAGIC * Rename the aggregate value to **amount**
// MAGIC * Sort the data by **day**
// MAGIC * The final schema must include:
// MAGIC   * `year`: `integer`
// MAGIC   * `day`: `integer`
// MAGIC   * `amount`: `decimal`
// MAGIC * Do this for the 2017 and 2014 datasets
// MAGIC * Assign the final `DataFrame` to **holiday14DF** and **holiday17DF** respectively
// MAGIC 
// MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> Unlike the challenges in the previous notebook, here we<br/>
// MAGIC  do **NOT** want to cache the data. It will adversely affect our benchmarks.

// COMMAND ----------

// ANSWER

val holiday14DF = trx2014DF // fast2014DF or trx2014DF
  // no need to extract the month from transacted_at
  // no need to extract the year from transacted_at
  .filter($"year" === 2014 && $"month" >= 11)
  .withColumn("day", dayofyear($"transacted_at"))
  .groupBy("year", "day").sum("amount")
  .withColumnRenamed("sum(amount)", "amount")
  .orderBy("day")

// Don't use on the full dataset
// display(holiday14DF)

// COMMAND ----------

// ANSWER

val holiday17DF = trx2017DF // fast2017DF or trx2017DF
  .withColumn("month", month($"transacted_at"))
  .withColumn("year", year($"transacted_at"))
  .filter($"year" === 2017 && $"month" >= 11)
  .withColumn("day", dayofyear($"transacted_at"))
  .groupBy("year", "day").sum("amount")
  .withColumnRenamed("sum(amount)", "amount")
  .orderBy("day")

// Don't use on the full dataset
// display(holiday17DF)

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Validate Your Answer to #2
// MAGIC Run the cells below to validate your answer.
// MAGIC 
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> This first set of validations will work with either the **fast** or **slow** datasets

// COMMAND ----------

clearYourResults()

validateYourSchema("02.A) holiday14DF", holiday14DF, "year", "integer")
validateYourSchema("02.B) holiday14DF", holiday14DF, "day", "integer")
validateYourSchema("02.C) holiday14DF", holiday14DF, "amount", "decimal")

validateYourSchema("02.D) holiday17DF", holiday17DF, "year", "integer")
validateYourSchema("02.E) holiday17DF", holiday17DF, "day", "integer")
validateYourSchema("02.F) holiday17DF", holiday17DF, "amount", "decimal")

summarizeYourResults()

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> This second set of validations will only work with the **slow** datasets.

// COMMAND ----------

// No cheating! :-)
spark.catalog.clearCache() 

// COMMAND ----------

clearYourResults()

////////////////////////////////////////////////////////
// Validate the 2014 count
val (validated14, holidayCount14, holidayDuration14) = benchmarkCount(() => {
  holiday14DF.withColumn("temp", lit(2014)).persist() 
})
validateYourAnswer("03.G 2014 expected 61 records", 1551101942, holidayCount14)

////////////////////////////////////////////////////////
// Validate the 2014 sum to ensure we have the right records
val sum2014 = validated14.select( sum($"amount").cast("decimal(20,2)").cast("string").as("total") ).as[String].first
validateYourAnswer("03.H 2014 sum", 1358795957, sum2014)

////////////////////////////////////////////////////////

logYourTest(logPath, "Duration 2014-Holiday-Slow", holidayDuration14)

summarizeYourResults()

// COMMAND ----------

clearYourResults()

////////////////////////////////////////////////////////
// Validate the 2017 count
val (validated17, holidayCount17, holidayDuration17) = benchmarkCount(() => {
  holiday17DF.withColumn("temp", lit(2017)).persist() 
})
validateYourAnswer("03.I 2017 expected 61 records", 1551101942, holidayCount17)

////////////////////////////////////////////////////////
// Validate the 2017 sum to ensure we have the right records
val sum2017 = validated17.select( sum($"amount").cast("decimal(20,2)").cast("string").as("total") ).as[String].first
validateYourAnswer("03.J 2017 sum", 760268806, sum2017)

////////////////////////////////////////////////////////

logYourTest(logPath, "Duration 2017-Holiday-Slow", holidayDuration17)

summarizeYourResults()

// COMMAND ----------

// MAGIC %md
// MAGIC ### Now we can merge the two and graph them side by side

// COMMAND ----------

val bothHolidaysDF = validated14.unionByName(validated17)
display(bothHolidaysDF.select("day", "amount", "year"))

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC <div style="display:table; height:600px; width:100%">
// MAGIC   <h2><img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png"/> Review Challenge #2, Part 1/3</h2>
// MAGIC 
// MAGIC   <h3>&nbsp;</h3>
// MAGIC   <h3>What can we conclude about the data?</h3>
// MAGIC 
// MAGIC   <h3>&nbsp;</h3>
// MAGIC   <h3>What are the spikes?</h3>
// MAGIC 
// MAGIC   <h3>&nbsp;</h3>
// MAGIC   <h3>Why did the level change between day 305 & 365?</h3>
// MAGIC 
// MAGIC   <h3>&nbsp;</h3>
// MAGIC   <h3>Why are the spikes not aligned?</h3>
// MAGIC </div>

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC 
// MAGIC <div style="display:table; height:600px; width:100%">
// MAGIC   <h2><img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png"/> Review Challenge #2, Part 2/3</h2>
// MAGIC   
// MAGIC   <h3>&nbsp;</h3>
// MAGIC   <h3>About how much time did we save by using the "faster" dataset?</h3>
// MAGIC </div>

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC <div style="display:table; width:100%">
// MAGIC   <h2><img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png"> Review Challenge #2, Part 3/3</h2>
// MAGIC   <p>Start by reviewing the numbers from the benchmark of our initial counts:</p>
// MAGIC </div>
// MAGIC     

// COMMAND ----------

val results = loadYourTestMap(logPath)
// val results = loadYourTestMap("dbfs:/mnt/training/global-sales/solutions/sample-results")

println("2014 Totals: %,d transactions".format(results("Total 2014-Slow").toInt))
println("2017 Totals: %,d transactions".format(results("Total 2017-Slow").toInt))
println()
println("2014 Count Duration:   %,.3f sec".format(results("Duration 2014-Slow")/1000.0))
println("2017 Count Duration:   %,.3f sec".format(results("Duration 2017-Slow")/1000.0))
println("Slow 2017 vs 2014:     %,.2fx faster".format((results("Duration 2014-Slow") - results("Duration 2017-Slow")) / results("Duration 2017-Slow").toDouble))
println()
println("2014 Holiday Duration: %,.3f sec".format(results("Duration 2014-Holiday-Slow")/1000.0))
println("2017 Holiday Duration: %,.3f sec".format(results("Duration 2017-Holiday-Slow")/1000.0))
println("Holiday 2014 vs 2017:  %,.2fx faster".format((results("Duration 2017-Holiday-Slow") - results("Duration 2014-Holiday-Slow")) / results("Duration 2014-Holiday-Slow")))
println("-"*80)

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC <div style="display:table; height:200px; width:100%">
// MAGIC   <h3>Which has more records, 2014 or 2017?</h3>
// MAGIC 
// MAGIC   <h3>&nbsp;</h3>
// MAGIC   <h3>How does the full count of 2014 compare to 2017?</h3>
// MAGIC 
// MAGIC   <h3>&nbsp;</h3>
// MAGIC   <h3>How does the holiday count of 2014 compare to 2017?</h3>
// MAGIC 
// MAGIC </div>

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Challenge #3
// MAGIC 
// MAGIC Why is **2017 Count Duration** [minusculely] faster than **2014 Count Duration** when there are more records in 2017?
// MAGIC 
// MAGIC Why is **2014 Holiday Duration** some 3x faster than **2017 Holiday Duration**
// MAGIC 
// MAGIC **Hint:** Explore the different facets of our app to see what accounts for the difference?
// MAGIC * Explore the data - is there a difference in the schema, number of records, datatypes, etc?
// MAGIC * Explore the code - is there something different between 2014 and 2017's code?
// MAGIC * Explore the data-on-disk - is there something about the files on disk?
// MAGIC * Did you rule out flukes in the benchmark by rerunning the queries 2-3 times to establish a baseline?
// MAGIC * Did you rule out a misconfiguration of the cluster (too much or too little RAM)?
// MAGIC * Always check the easy things first - save the time consuming investigation for later.
// MAGIC 
// MAGIC <p style="color:red">PLEASE don't give away the answer to your fellow students.<br/>
// MAGIC The purpose of this exercise it to learn how to diagnose the problem.</p>
// MAGIC 
// MAGIC As one last reminder, you have the following utility methods available to you:

// COMMAND ----------

// MAGIC %fs ls /mnt/training/global-sales

// COMMAND ----------

// MAGIC %run "./Includes/Utility-Methods"

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>