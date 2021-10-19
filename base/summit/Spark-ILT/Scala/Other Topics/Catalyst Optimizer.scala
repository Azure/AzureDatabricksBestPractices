// Databricks notebook source
// MAGIC 
// MAGIC %md-sandbox
// MAGIC 
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC # Catalyst Optimizer
// MAGIC 
// MAGIC **Technical Accomplishments:**
// MAGIC * Understand what the Catalyst Optimizer is
// MAGIC * Understand the different stages of the Catalyst Optimizer
// MAGIC * Example of Physical Plan Optimization (x2)
// MAGIC * Example of Predicate Pushdown

// COMMAND ----------

// Because we will need it later...
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Getting Started
// MAGIC 
// MAGIC Run the following cell to configure our "classroom."

// COMMAND ----------

// MAGIC %run "../Includes/Classroom Setup"

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Catalyst Optimizer
// MAGIC 
// MAGIC * Fundamental to the `SQL` and `DataFrames` API is the Catalyst Optimizer.
// MAGIC * It's an **extensible query optimizer**.
// MAGIC * Contains a **general library for representing trees and applying rules** to manipulate them.
// MAGIC * Several public extension points, including external data sources and user-defined types.
// MAGIC 
// MAGIC See also: <a href="https://databricks.com/blog/2015/04/13/deep-dive-into-spark-sqls-catalyst-optimizer.html" target="_blank">Deep Dive into Spark SQL’s Catalyst Optimizer</a> (April 13, 2015)
// MAGIC 
// MAGIC Processing is broken down into several stages as we can see here:

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ![Catalyst](https://files.training.databricks.com/images/105/catalyst-diagram.png)

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Optimized Logical Plan
// MAGIC 
// MAGIC One of the many optimizations performed by the Catalyst Optimizer involves **rewriting our code**.
// MAGIC   
// MAGIC In this case, we will see **two examples** involving the rewriting of our filters.
// MAGIC 
// MAGIC The first is an **innocent mistake** almost most every new Spark developer makes.
// MAGIC 
// MAGIC The second "mistake" is... well... **really bad** - but Spark can fix it.

// COMMAND ----------

// MAGIC %md
// MAGIC ### Example #1: Innocent Mistake
// MAGIC 
// MAGIC I don't want any project that starts with **en.zero**.
// MAGIC 
// MAGIC There are **better ways of doing this**, as in it can be done with a single condition.
// MAGIC 
// MAGIC But we will make **8 passes** on the data **with 8 different filters**.
// MAGIC 
// MAGIC After every individual pass, we will **go back over the remaining dataset** to filter out the next set of records.

// COMMAND ----------

val allDF = spark.read.parquet("/mnt/training/wikipedia/pagecounts/staging_parquet_en_only_clean/")

val pass1 = allDF.filter($"project" =!= "en.zero")
val pass2 = pass1.filter($"project" =!= "en.zero.n")
val pass3 = pass2.filter($"project" =!= "en.zero.s")
val pass4 = pass3.filter($"project" =!= "en.zero.d")
val pass5 = pass4.filter($"project" =!= "en.zero.voy")
val pass6 = pass5.filter($"project" =!= "en.zero.b")
val pass7 = pass6.filter($"project" =!= "en.zero.v")
val pass8 = pass7.filter($"project" =!= "en.zero.q")

printf("Pass 1: %,d%n", pass1.count)
printf("Pass 2: %,d%n", pass2.count)
printf("Pass 3: %,d%n", pass3.count)
printf("Pass 4: %,d%n", pass4.count)
printf("Pass 5: %,d%n", pass5.count)
printf("Pass 6: %,d%n", pass6.count)
printf("Pass 7: %,d%n", pass7.count)
printf("Pass 8: %,d%n", pass8.count)
println("-"*80)

// COMMAND ----------

// MAGIC %md
// MAGIC **Logically**, the code above is the same as the code below.
// MAGIC 
// MAGIC The only real difference is that we are **not asking for a count** after every filter.

// COMMAND ----------

val innocentDF = spark.read.parquet("/mnt/training/wikipedia/pagecounts/staging_parquet_en_only_clean/")
  .filter($"project" =!= "en.zero")
  .filter($"project" =!= "en.zero.n")
  .filter($"project" =!= "en.zero.s")
  .filter($"project" =!= "en.zero.d")
  .filter($"project" =!= "en.zero.voy")
  .filter($"project" =!= "en.zero.b")
  .filter($"project" =!= "en.zero.v")
  .filter($"project" =!= "en.zero.q")

printf("Final Count: %,d%n", innocentDF.count)
println("-"*80)

// COMMAND ----------

// MAGIC %md
// MAGIC We don't even have to execute the code to see what is **logically** or **physically** taking place under the hood.
// MAGIC 
// MAGIC Here we can use the `explain(..)` command.

// COMMAND ----------

innocentDF.explain(true)

// COMMAND ----------

// MAGIC %md
// MAGIC Of course, if we were to write this the correct way, the first time, ignoring the fact that there are better methods, it would look something like this...

// COMMAND ----------

val betterDF = spark.read.parquet("/mnt/training/wikipedia/pagecounts/staging_parquet_en_only_clean/")
  .filter($"project".isNotNull &&
      $"project" =!= "en.zero" && 
      $"project" =!= "en.zero.n" && 
      $"project" =!= "en.zero.s" && 
      $"project" =!= "en.zero.d" && 
      $"project" =!= "en.zero.voy" && 
      $"project" =!= "en.zero.b" && 
      $"project" =!= "en.zero.v" && 
      $"project" =!= "en.zero.q")

printf("Final: %,d%n%n", betterDF.count)

betterDF.explain(true)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Example #2: Bad Programmer
// MAGIC 
// MAGIC This time we are going to do something **REALLY** bad...
// MAGIC 
// MAGIC Even if the compiler combines these filters into a single filter, **we still have five different tests** for any column that doesn't have the value "whatever".

// COMMAND ----------

val stupidDF = spark.read.parquet("/mnt/training/wikipedia/pagecounts/staging_parquet_en_only_clean/")
  .filter($"project" =!= "whatever")
  .filter($"project" =!= "whatever")
  .filter($"project" =!= "whatever")
  .filter($"project" =!= "whatever")
  .filter($"project" =!= "whatever")

stupidDF.explain(true)

// COMMAND ----------

// MAGIC %md
// MAGIC ** *Note:* ** *`explain(..)` is not the only way to get access to this level of detail...<br/>
// MAGIC We can also see it in the **Spark UI**. *

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Columnar Predicate Pushdown
// MAGIC 
// MAGIC The Columnar Predicate Pushdown takes place when a filter can be pushed down to the original data source, such as a database server.
// MAGIC 
// MAGIC In this example, we are going to compare `DataFrames` from two different sources:
// MAGIC * JDBC - where a predicate pushdown **WILL** take place.
// MAGIC * CSV - where a predicate pushdown will **NOT** take place.
// MAGIC 
// MAGIC In each case, we can see evidence of the pushdown (or lack of it) in the **Physical Plan**.

// COMMAND ----------

// MAGIC %md
// MAGIC ### Example #3: JDBC
// MAGIC 
// MAGIC Start by initializing the JDBC driver.
// MAGIC 
// MAGIC This needs to be done regardless of language.

// COMMAND ----------

// MAGIC %scala
// MAGIC 
// MAGIC // Ensure that the driver class is loaded. 
// MAGIC // Seems to be necessary sometimes.
// MAGIC Class.forName("org.postgresql.Driver") 

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Next, we can create a `DataFrame` via JDBC and then filter by **gender**.

// COMMAND ----------

val jdbcURL = "jdbc:postgresql://54.213.33.240/training"

// Username and Password w/read-only rights
val connProperties = new java.util.Properties()
connProperties.put("user", "training")
connProperties.put("password", "training")

val ppExampleThreeDF = spark.read.jdbc(
    jdbcURL,                      // the JDBC URL
    "training.people_1m",         // the name of the table
    "id",                         // the name of a column of an integral type that will be used for partitioning
    1,                            // the minimum value of columnName used to decide partition stride
    1000000,                      // the maximum value of columnName used to decide partition stride
    8,                            // the number of partitions/connections
    connProperties                // the connection properties
  )
  .filter($"gender" === "M")      // Filter the data by gender

// COMMAND ----------

// MAGIC %md
// MAGIC With the `DataFrame` created, we can ask Spark to `explain(..)` the **Physical Plan**.
// MAGIC 
// MAGIC What we are looking for...
// MAGIC * is the lack of a **Filter** and
// MAGIC * the presence of a **PushedFilters** in the **Scan**

// COMMAND ----------

ppExampleThreeDF.explain()

// COMMAND ----------

// MAGIC %md
// MAGIC This will make a little more sense if we **compare it to some examples** that don't push down the filter.

// COMMAND ----------

// MAGIC %md
// MAGIC ### Example #4: Cached JDBC
// MAGIC 
// MAGIC In this example, we are going to cache our data before filtering and thus eliminating the possibility for the predicate push down:

// COMMAND ----------

val ppExampleFourCachedDF = spark.read
  .jdbc(
    jdbcURL,                      // the JDBC URL
    "training.people_1m",         // the name of the table
    "id",                         // the name of a column of an integral type that will be used for partitioning
    1,                            // the minimum value of columnName used to decide partition stride
    1000000,                      // the maximum value of columnName used to decide partition stride
    8,                            // the number of partitions/connections
    connProperties                // the connection properties
  )
  .cache()                        // cache the data

ppExampleFourCachedDF.count()     // materialize the cache

val ppExampleFourFilteredDF = ppExampleFourCachedDF
  .filter($"gender" === "M")      // Filter the data by gender

// COMMAND ----------

// MAGIC %md
// MAGIC Now that we have cached the data and THEN filtered it, we have eliminated the possibility to bennifet from the predicate push down.
// MAGIC 
// MAGIC And so that it's easier to compare the two examples, we can re-print the physical plan for the previous example too.

// COMMAND ----------

println("****Example Three****\n")
ppExampleThreeDF.explain()

println("\n****Example Four****\n")
ppExampleFourFilteredDF.explain()

// COMMAND ----------

// MAGIC %md
// MAGIC It should be clearer now...
// MAGIC 
// MAGIC In the first example we see only the **Scan** which is the JDBC read.
// MAGIC 
// MAGIC In the second example, you can see the **Scan** but you also see the **InMemoryTableScan** followed by a **Filter** which means Spark had to filter ALL the data from RAM instead of in the Database.

// COMMAND ----------

// MAGIC %md
// MAGIC ### Example #5: CSV File
// MAGIC 
// MAGIC This example is identical to the previous one except...
// MAGIC * this is a CSV file instead of JDBC source
// MAGIC * we are filtering on **site**

// COMMAND ----------

val schema = StructType(
  List(
    StructField("timestamp", StringType, false),
    StructField("site", StringType, false),
    StructField("requests", IntegerType, false)
  )
)

val ppExampleThreeDF = spark.read
   .option("header", "true")
   .option("sep", "\t")
   .schema(schema)
   .csv("/mnt/training/wikipedia/pageviews/pageviews_by_second.tsv")
   .filter($"site" === "desktop")

// COMMAND ----------

// MAGIC %md
// MAGIC With the `DataFrame` created, we can ask Spark to `explain(..)` the **Physical Plan**.
// MAGIC 
// MAGIC What we are looking for...
// MAGIC * is the presence of a **Filter** and
// MAGIC * the presence of a **PushedFilters** in the **FileScan csv**
// MAGIC 
// MAGIC And again, we see **PushedFilters** because Spark is *trying* to push down to the CSV file.
// MAGIC 
// MAGIC But that doesn't work here and so we see that just like in the last example, we have a **Filter** after the **FileScan**, actually an **InMemoryFileIndex**.

// COMMAND ----------

ppExampleThreeDF.explain()


// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>