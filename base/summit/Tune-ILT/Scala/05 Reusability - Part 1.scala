// Databricks notebook source
// MAGIC 
// MAGIC %md-sandbox
// MAGIC 
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC #Reusability Part 1
// MAGIC 
// MAGIC **Dataset:**
// MAGIC * This is synthetic data generated specifically for these exercises
// MAGIC * Each year's data is roughly the same with some variation for market growth
// MAGIC * We are looking at retail purchases from the top N retailers
// MAGIC 
// MAGIC **Technical Accomplishments:**
// MAGIC * Look at common techniques for code reusability
// MAGIC * Explore some of the common pitfalls

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
// MAGIC <div style="display:table; height:600px; width:100%">
// MAGIC   <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"/> The Introduction</h2>
// MAGIC 
// MAGIC   <p>We spent a fair amount of time comparing 2014 and 2017</p>
// MAGIC 
// MAGIC   <p>Each time, we had to write nearly the exact same code.</p>
// MAGIC 
// MAGIC   <p>We are going to start by refactoring our code into something more reusable</p>
// MAGIC 
// MAGIC   <h3>What options do we have for code reusability?</h3>
// MAGIC 
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC ### Load Data for 2014 & 2017

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

var path2014 = "/mnt/training/global-sales/transactions/2014.parquet"
val trx2014DF = spark.read.parquet(path2014)

var path2017 = "/mnt/training/global-sales/transactions/2017.parquet"
val trx2017DF = spark.read.parquet(path2017)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Load our "faster" dataset for us to test with

// COMMAND ----------

val fastPath14 = "/mnt/training/global-sales/solutions/2014-fast.parquet"
val fast2014DF = spark.read.parquet(fastPath14)

val fastPath17 = "/mnt/training/global-sales/solutions/2017-fast.parquet"
val fast2017DF = spark.read.parquet(fastPath17)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Our common set of transformations
// MAGIC 
// MAGIC Let's review the end result of the previous notebook:

// COMMAND ----------

val prevDF = fast2017DF.withColumn("month", month($"transacted_at"))
                       .withColumn("year", year($"transacted_at"))
                       .filter($"year" === 2017 && $"month" >= 11)
                       .withColumn("day", dayofyear($"transacted_at"))
                       .groupBy("year", "day").sum("amount")
                       .withColumnRenamed("sum(amount)", "amount")
                       .orderBy("day")
                       .persist()

display(prevDF)

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Challenge #1
// MAGIC 
// MAGIC **Create a function to encompass our transformations:**
// MAGIC * Name the function **sumByDayOfYear**
// MAGIC * The function should return a **DataFrame**
// MAGIC * The function should have the single parameter **sourceDF** of type **DataFrame**
// MAGIC * The body of the function should be the set of transformations above

// COMMAND ----------

// TODO

def FILL_IN(FILL_IN):FILL_IN = {
  sourceDF.FILL_IN
}

val holiday17DF = sumByDayOfYear(fast2017DF)
display(holiday17DF.select("day", "amount", "year"))

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC <img alt="Best Practice" title="Best Practice" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-blue-ribbon.svg"/> At least on Databricks, if the first two columns in a select statement are<br/>
// MAGIC the X and Y axis respectively, then the graphs are able to use reasonable default settings.

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Validate Your Answer to #1
// MAGIC 
// MAGIC We can test your work by executing your method with the "fast" version of the 2017 data.
// MAGIC 
// MAGIC Before we do, let's create a utility method to help us test our results as we refactor our code.

// COMMAND ----------

// MAGIC %md
// MAGIC ### Test methods for later refactoring

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
// MAGIC Run the cell below to validate your answer:

// COMMAND ----------

// Execute your method with the 2017 "fast" data
val holidayFast17DF = sumByDayOfYear(fast2017DF)

validateSchema(holidayFast17DF)
validateSum(holidayFast17DF, expectedFast17)

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Challenge #2
// MAGIC 
// MAGIC **Adapt our function to work with the 2014 data:**
// MAGIC * Do not add any new parameters
// MAGIC * We can assume for now that the DataFrame will only ever contain a single year's worth of data.

// COMMAND ----------

// TODO
// Copied from the solution of the previous challenge

def sumByDayOfYear(sourceDF:DataFrame):DataFrame = {
  return sourceDF.withColumn("month", month($"transacted_at"))
                 .withColumn("year", year($"transacted_at"))
                 .filter($"year" === 2017 && $"month" >= 11)
                 .withColumn("day", dayofyear($"transacted_at"))
                 .groupBy("year", "day").sum("amount")
                 .withColumnRenamed("sum(amount)", "amount")
                 .orderBy("day")
                 .persist()
}

val holiday14DF = sumByDayOfYear(fast2014DF)
display(holiday14DF.select("day", "amount", "year"))

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Validate Your Answer to #2
// MAGIC 
// MAGIC We can test your work by executing your method with the "fast" version of the 2014 data
// MAGIC 
// MAGIC Run the cell below to validate your answer:

// COMMAND ----------

// Execute your method with the 2014 "fast" data
val holidayFast14DF = sumByDayOfYear(fast2014DF)

validateSchema(holidayFast14DF)
validateSum(holidayFast14DF, expectedFast14)

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Full Validation of 2014 & 2017
// MAGIC 
// MAGIC We know now that our function is working on our "fast" dataset.
// MAGIC 
// MAGIC Rerun on the full datasets:

// COMMAND ----------

val holidaySlow14DF = sumByDayOfYear(trx2014DF)

validateSchema(holidaySlow14DF)
validateSum(holidaySlow14DF, expectedSlow14)

// COMMAND ----------

val holidaySlow17DF = sumByDayOfYear(trx2017DF)

validateSchema(holidaySlow17DF)
validateSum(holidaySlow17DF, expectedSlow17)

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC <div style="display:table; height:600px; width:100%">
// MAGIC   <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"/> Review Challenges #1 &amp; #2</h2>
// MAGIC   <h3>What are the key differences between the "slow" run on 2014 vs 2017?</h3>
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Challenge #3
// MAGIC 
// MAGIC **Figure out what went wrong:**
// MAGIC * The goal is not to fix the problem
// MAGIC * The goal is simply to identify the problem
// MAGIC * Granted, you might inadvertently fix the problem as you explore
// MAGIC * We'll look at solutions in the next lab
// MAGIC 
// MAGIC To help with the processes, here is the completed version of our function:

// COMMAND ----------

spark.catalog.clearCache()

def sumByDayOfYear(sourceDF:DataFrame):DataFrame = {
  return sourceDF.withColumn("month", month($"transacted_at"))
                 .withColumn("year", year($"transacted_at"))
                 .filter($"month" >= 11) // dropped the year
                 .withColumn("day", dayofyear($"transacted_at"))
                 .groupBy("year", "day").sum("amount")
                 .withColumnRenamed("sum(amount)", "amount")
                 .orderBy("day")
                 .persist()
}

// A simple "count" is good enough to 
// exercise the function as you explore
sumByDayOfYear(trx2014DF).count()

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC <div style="display:table; height:600px; width:100%">
// MAGIC   <h2><img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png"/> Review Challenge #3</h2>
// MAGIC 
// MAGIC   <h3>What is wrong?</h3>
// MAGIC 
// MAGIC   <h3>What happened?</h3>
// MAGIC 
// MAGIC   <h3>How could we go about fixing it?</h3>
// MAGIC 
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Before moving on...
// MAGIC 
// MAGIC We just created a bunch of test methods
// MAGIC 
// MAGIC Along with a function for specific ETL, namely **sumByDayOfYear(..)**
// MAGIC 
// MAGIC We got a fair amount of reuse out of them
// MAGIC 
// MAGIC They are great for refactoring, and we know we are not done yet
// MAGIC 
// MAGIC We will need to copy-and-paste them into the next notebook if we want to resuse them.
// MAGIC 
// MAGIC But there is an easier solution:
// MAGIC * If you are building a library, publish it and then attach the library to your cluster
// MAGIC * In Databricks, we can actually get reusability via the notebooks

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>