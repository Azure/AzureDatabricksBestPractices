// Databricks notebook source
// MAGIC 
// MAGIC %md-sandbox
// MAGIC 
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC #Introduction to DataFrames, Part #3
// MAGIC 
// MAGIC ** Data Source **
// MAGIC * English Wikipedia pageviews by second
// MAGIC * Size on Disk: ~255 MB
// MAGIC * Type: Parquet files
// MAGIC * More Info: <a href="https://datahub.io/en/dataset/english-wikipedia-pageviews-by-second" target="_blank">https&#58;//datahub.io/en/dataset/english-wikipedia-pageviews-by-second</a>
// MAGIC 
// MAGIC **Technical Accomplishments:**
// MAGIC * Introduce the various aggregate functions.
// MAGIC * Explore more of the `...sql.functions` operations
// MAGIC   * more aggregate functions
// MAGIC   * date & time functions

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Getting Started
// MAGIC 
// MAGIC Run the following cell to configure our "classroom."

// COMMAND ----------

// MAGIC %run "../Includes/Classroom Setup"

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) The Data Source
// MAGIC 
// MAGIC This data uses the **Pageviews By Seconds** data set.
// MAGIC 
// MAGIC The parquet files are located on the DBFS at **/mnt/training/wikipedia/pageviews/pageviews_by_second.parquet**.

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// I've already gone through the exercise to determine
// how many partitions I want and in this case it is...
val partitions = 8

// Make sure wide operations don't repartition to 200
spark.conf.set("spark.sql.shuffle.partitions", partitions.toString)

// The directory containing our parquet files.
val parquetFile = "/mnt/training/wikipedia/pageviews/pageviews_by_second.parquet/"

// Create our initial DataFrame. We can let it infer the 
// schema because the cost for parquet files is really low.
val initialDF = spark.read
  .option("inferSchema", "true") // The default, but not costly w/Parquet
  .parquet(parquetFile)          // Read the data in
  .repartition(partitions)       // From 7 >>> 8 partitions
  .cache()                       // Cache the expensive operation

// materialize the cache
initialDF.count()

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Preparing Our Data
// MAGIC 
// MAGIC If we will be working on any given dataset for a while, there are a handful of "necessary" steps to get us ready...
// MAGIC 
// MAGIC Most of which we've just knocked out above.
// MAGIC 
// MAGIC **Basic Steps**
// MAGIC 0. <div style="text-decoration:line-through">Read the data in</div>
// MAGIC 0. <div style="text-decoration:line-through">Balance the number of partitions to the number of slots</div>
// MAGIC 0. <div style="text-decoration:line-through">Cache the data</div>
// MAGIC 0. <div style="text-decoration:line-through">Adjust the `spark.sql.shuffle.partitions`</div>
// MAGIC 0. Perform some basic ETL (i.e., convert strings to timestamp)
// MAGIC 0. Possibly re-cache the data if the ETL was costly
// MAGIC 
// MAGIC What we haven't done is some of the basic ETL necessary to explore our data.
// MAGIC 
// MAGIC Namely, the problem is that the field "timestamp" is a string.
// MAGIC 
// MAGIC In order to performed date/time - based computation I need to convert this to an alternate datetime format.

// COMMAND ----------

initialDF.printSchema()

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) withColumnRenamed(..), withColumn(..), select(..)
// MAGIC 
// MAGIC My first hangup is that we have a **column named timestamp** and the **datatype will also be timestamp**
// MAGIC 
// MAGIC The nice thing about Apache Spark is that I'm allowed the have an issue with this because it's very easy to fix...
// MAGIC 
// MAGIC Just rename the column...

// COMMAND ----------

initialDF
  .select($"timestamp".as("capturedAt"), $"site", $"requests")
  .printSchema()

// COMMAND ----------

// MAGIC %md
// MAGIC There are a number of different ways to rename a column...

// COMMAND ----------

initialDF
  .withColumnRenamed("timestamp", "capturedAt")
  .printSchema()

// COMMAND ----------

initialDF
  .toDF("capturedAt", "site", "requests")
  .printSchema()

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) unix_timestamp(..) & cast(..)

// COMMAND ----------

// MAGIC %md
// MAGIC Now that **we** are over **my** hangup, we can focus on converting the **string** to a **timestamp**.
// MAGIC 
// MAGIC For this we will be looking at more of the functions in the `functions` package
// MAGIC * `pyspark.sql.functions` in the case of Python
// MAGIC * `org.apache.spark.sql.functions` in the case of Scala & Java
// MAGIC 
// MAGIC And so that we can watch the transformation, will will take one step at a time...

// COMMAND ----------

// MAGIC %md
// MAGIC The first function is `unix_timestamp(..)`
// MAGIC 
// MAGIC If you look at the API docs, `unix_timestamp(..)` is described like this:
// MAGIC > Convert time string with given pattern (see <a href="http://docs.oracle.com/javase/tutorial/i18n/format/simpleDateFormat.html" target="_blank">SimpleDateFormat</a>) to Unix time stamp (in seconds), return null if fail.
// MAGIC 
// MAGIC `SimpleDataFormat` is part of the Java API and provides support for parsing and formatting date and time values.
// MAGIC 
// MAGIC In order to know what format the data is in, let's take a look at the first row...

// COMMAND ----------

initialDF
  .withColumnRenamed("timestamp", "capturedAt")
  .show(1, false)

// COMMAND ----------

// MAGIC %md
// MAGIC Comparing that value with the patterns express in the docs for the `SimpleDateFormat` class, we can come up with a format:
// MAGIC 
// MAGIC **yyyy-MM-dd HH:mm:ss**

// COMMAND ----------

val tempA = initialDF
  .withColumnRenamed("timestamp", "capturedAt")
  .select( $"*", unix_timestamp($"capturedAt", "yyyy-MM-dd HH:mm:ss") )

tempA.printSchema()

// COMMAND ----------

display(tempA)

// COMMAND ----------

// MAGIC %md
// MAGIC ** *Note:* ** *If you haven't caught it yet, there is a bug in the previous code....*

// COMMAND ----------

// MAGIC %md
// MAGIC A couple of things happened...
// MAGIC 0. We ended up with a new column - that's OK for now
// MAGIC 0. The new column has a really funky name - based upon the name of the function we called and its parameters.
// MAGIC 0. The data type is now a long.
// MAGIC   * This value is the Java Epoch
// MAGIC   * The number of seconds since 1970-01-01T00:00:00Z
// MAGIC   
// MAGIC We can now take that epoch value and use the `Column.cast(..)` method to convert it to a **timestamp**.

// COMMAND ----------

val tempB = initialDF
  .withColumnRenamed("timestamp", "capturedAt")
  .select( $"*", unix_timestamp($"capturedAt", "yyyy-MM-dd'T'HH:mm:ss").cast("timestamp") )

tempB.printSchema()

// COMMAND ----------

display(tempB)

// COMMAND ----------

// MAGIC %md
// MAGIC Now that our column `createdAt` has been converted from a **string** to a **timestamp**, we just need to deal with this REALLY funky column name.
// MAGIC 
// MAGIC Again.. there are several ways to do this.
// MAGIC 
// MAGIC I'll let you decide which you like better...

// COMMAND ----------

// MAGIC %md
// MAGIC ### Option #1
// MAGIC The `as()` or `alias()` method can be appended to the chain of calls.
// MAGIC 
// MAGIC This version will actually produce an odd little bug.<br/>
// MAGIC That is how do you get rid of only one of the two `capturedAt` columns?

// COMMAND ----------

val tempC = initialDF
  .withColumnRenamed("timestamp", "capturedAt")
  .select( $"*", unix_timestamp($"capturedAt", "yyyy-MM-dd'T'HH:mm:ss").cast("timestamp").as("capturedAt") )

tempC.printSchema()

// COMMAND ----------

display(tempC)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Option #2
// MAGIC The `withColumn(..)` renames the column (first param) and accepts as a<br/>
// MAGIC second parameter the expression(s) we need for our transformation

// COMMAND ----------

val tempD = initialDF
  .withColumnRenamed("timestamp", "capturedAt")
  .withColumn("capturedAt", unix_timestamp($"capturedAt", "yyyy-MM-dd'T'HH:mm:ss").cast("timestamp") )

tempD.printSchema()

// COMMAND ----------

display(tempD)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Option #3
// MAGIC 
// MAGIC We can take the big ugly name explicitly rename it.
// MAGIC 
// MAGIC This version will actually produce an odd little bug.<br/>
// MAGIC That is how do you get rid of only one of the two "capturedAt" columns?

// COMMAND ----------

val tempE = initialDF
  .withColumnRenamed("timestamp", "capturedAt")
  .select( $"*", unix_timestamp($"capturedAt", "yyyy-MM-dd'T'HH:mm:ss").cast("timestamp") )
  .withColumnRenamed("CAST(unix_timestamp(capturedAt, yyyy-MM-dd'T'HH:mm:ss) AS TIMESTAMP)", "capturedAt")
  // .drop("timestamp")

tempE.printSchema()

// COMMAND ----------

display(tempE)

// COMMAND ----------

display(tempE)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Option #4
// MAGIC 
// MAGIC The last version is a twist on the others in which we start with the <br/>
// MAGIC name `timestamp` and rename it and the expression all in one call<br/>
// MAGIC 
// MAGIC But this version leaves us with the old column in the DF

// COMMAND ----------

val tempF = initialDF
  .withColumn("capturedAt", unix_timestamp($"timestamp", "yyyy-MM-dd'T'HH:mm:ss").cast("timestamp") )

tempF.printSchema()

// COMMAND ----------

display(tempF)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Let's pick the "cleanest" version...
// MAGIC 
// MAGIC And with our base `DataFrame` in place we can start exploring the data a little...

// COMMAND ----------

val pageviewsDF = initialDF
  .withColumnRenamed("timestamp", "capturedAt")
  .withColumn("capturedAt", unix_timestamp($"capturedAt", "yyyy-MM-dd'T'HH:mm:ss").cast("timestamp") )

pageviewsDF.printSchema()

// COMMAND ----------

display(pageviewsDF)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC And just so that we don't have to keep performing these transformations.... 
// MAGIC 
// MAGIC Mark the `DataFrame` as cached and then materialize the result.

// COMMAND ----------

pageviewsDF.cache().count()

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) year(..), month(..), dayofyear(..)
// MAGIC 
// MAGIC Let's take a look at some of the other date & time functions...
// MAGIC 
// MAGIC With that we can answer a simple question: When was this data captured.
// MAGIC 
// MAGIC We can start specifically with the year...

// COMMAND ----------

display(
  pageviewsDF
    .select( year($"capturedAt") ) // Every record converted to a single column - the year captured
    .distinct()                    // Reduce all years to the list of distinct years
)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Now let's take a look at in which months was this data captured...

// COMMAND ----------

display(
  pageviewsDF
    .select( month($"capturedAt") ) // Every record converted to a single column - the month captured
    .distinct()                     // Reduce all months to the list of distinct years
)

// COMMAND ----------

// MAGIC %md
// MAGIC And of course this both can be combined as a single call...

// COMMAND ----------

pageviewsDF
  .select( month($"capturedAt") as "month", year($"capturedAt") as "year" )
  .distinct()
  .show()                     

// COMMAND ----------

// MAGIC %md
// MAGIC It's pretty easy to see that the data was captured during March & April of 2015.
// MAGIC 
// MAGIC We will have more opportunities to play with the various date and time functions in the next lab.
// MAGIC 
// MAGIC For now, let's just make sure to review them in the Spark API

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) groupBy()
// MAGIC 
// MAGIC Aggregating data is one of the more common tasks when working with big data.
// MAGIC * How many customers are over 65?
// MAGIC * What is the ratio of men to women?
// MAGIC * Group all emails by their sender.
// MAGIC 
// MAGIC The function `groupBy()` is one tool that we can use for this purpose.
// MAGIC 
// MAGIC If you look at the API docs, `groupBy(..)` is described like this:
// MAGIC > Groups the Dataset using the specified columns, so that we can run aggregation on them.
// MAGIC 
// MAGIC This function is a **wide** transformation - it will produce a shuffle and conclude a stage boundary.
// MAGIC 
// MAGIC Unlike all of the other transformations we've seen so far, this transformation does not return a `DataFrame`.
// MAGIC * In Scala it returns `RelationalGroupedDataset`
// MAGIC * In Python it returns `GroupedData`
// MAGIC 
// MAGIC This is because the call `groupBy(..)` is only 1/2 of the transformation.
// MAGIC 
// MAGIC To see the other half, we need to take a look at it's return type, `RelationalGroupedDataset`.

// COMMAND ----------

// MAGIC %md
// MAGIC ### RelationalGroupedDataset
// MAGIC 
// MAGIC If we take a look at the API docs for `RelationalGroupedDataset`, we can see that it supports the following aggregations:
// MAGIC 
// MAGIC | Method | Description |
// MAGIC |--------|-------------|
// MAGIC | `avg(..)` | Compute the mean value for each numeric columns for each group. |
// MAGIC | `count(..)` | Count the number of rows for each group. |
// MAGIC | `sum(..)` | Compute the sum for each numeric columns for each group. |
// MAGIC | `min(..)` | Compute the min value for each numeric column for each group. |
// MAGIC | `max(..)` | Compute the max value for each numeric columns for each group. |
// MAGIC | `mean(..)` | Compute the average value for each numeric columns for each group. |
// MAGIC | `agg(..)` | Compute aggregates by specifying a series of aggregate columns. |
// MAGIC | `pivot(..)` | Pivots a column of the current DataFrame and perform the specified aggregation. |
// MAGIC 
// MAGIC With the exception of `pivot(..)`, each of these functions return our new `DataFrame`.

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Together, `groupBy(..)` and `RelationalGroupedDataset` (or `GroupedData` in Python) give us what we need to answer some basic questions.
// MAGIC 
// MAGIC For Example, how many more requests did the desktop site receive than the mobile site receive?
// MAGIC 
// MAGIC For this all we need to do is group all records by **site** and then sum all the requests.

// COMMAND ----------

display(
  pageviewsDF
    .groupBy($"site")
    .sum()
)

// COMMAND ----------

// MAGIC %md
// MAGIC Notice above that we didn't actually specify which column we were summing....
// MAGIC 
// MAGIC In this case you will actually receive a total for all numerical values.
// MAGIC 
// MAGIC There is a performance catch to that - if I have 2, 5, 10? columns, then they will all be summed and I may only need one.
// MAGIC 
// MAGIC I can first reduce my columns to those that I wanted or I can simply specify which column(s) to sum up.

// COMMAND ----------

display(
  pageviewsDF
    .groupBy($"site")
    .sum("requests")
)

// COMMAND ----------

// MAGIC %md
// MAGIC And because I don't like the resulting column name, **sum(requests)** I can easily rename it...

// COMMAND ----------

display(
  pageviewsDF
    .groupBy($"site")
    .sum("requests")
    .withColumnRenamed("sum(requests)", "totalRequests")
)

// COMMAND ----------

// MAGIC %md
// MAGIC How about the total number of requests per site? mobile vs desktop?

// COMMAND ----------

display(
  pageviewsDF
    .groupBy($"site")
    .count()
)

// COMMAND ----------

// MAGIC %md
// MAGIC This result shouldn't surprise us... there were after all one record, per second, per site....

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) sum(), count(), avg(), min(), max()

// COMMAND ----------

// MAGIC %md
// MAGIC The `groupBy(..)` operation is not our only option for aggregating.
// MAGIC 
// MAGIC The `...sql.functions` package actually defines a large number of aggregate functions
// MAGIC * `org.apache.spark.sql.functions` in the case of Scala & Java
// MAGIC * `pyspark.sql.functions` in the case of Python
// MAGIC 
// MAGIC 
// MAGIC Let's take a look at this in the Scala API docs (only because the documentation is a little easier to read).

// COMMAND ----------

// MAGIC %md
// MAGIC Let's take a look at our last two examples... 
// MAGIC 
// MAGIC We saw the count of records and the sum of records.
// MAGIC 
// MAGIC Let's take do this a slightly different way...
// MAGIC 
// MAGIC This time with the `...sql.functions` operations.
// MAGIC 
// MAGIC And just for fun, let's throw in the average, minimum and maximum

// COMMAND ----------

pageviewsDF
  .filter("site = 'mobile'")
  .select( sum($"requests"), count($"requests"), avg($"requests"), min($"requests"), max($"requests") )
  .show()

pageviewsDF
  .filter("site = 'desktop'")
  .select( sum($"requests"), count($"requests"), avg($"requests"), min($"requests"), max($"requests") )
  .show()

// COMMAND ----------

// MAGIC %md
// MAGIC And let's just address one more pet-peeve...
// MAGIC 
// MAGIC Was that 3.6M records or 360K records?

// COMMAND ----------

pageviewsDF
  .filter("site = 'mobile'")
  .select( 
    format_number(sum($"requests"), 0).as("sum"), 
    format_number(count($"requests"), 0).as("count"), 
    format_number(avg($"requests"), 2).as("avg"), 
    format_number(min($"requests"), 0).as("min"), 
    format_number(max($"requests"), 0).as("max") 
  )
  .show()

pageviewsDF
  .filter("site = 'desktop'")
  .select( 
    format_number(sum($"requests"), 0), 
    format_number(count($"requests"), 0), 
    format_number(avg($"requests"), 2), 
    format_number(min($"requests"), 0), 
    format_number(max($"requests"), 0) 
  )
  .show()

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/labs.png) Data Frames Lab #3
// MAGIC It's time to put what we learned to practice.
// MAGIC 
// MAGIC Go ahead and open the notebook [Introduction to DataFrames, Lab #3]($./Intro To DF Part 3 Lab) and complete the exercises.

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>