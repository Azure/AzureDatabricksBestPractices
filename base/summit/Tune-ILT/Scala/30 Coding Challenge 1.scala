// Databricks notebook source
// MAGIC 
// MAGIC %md-sandbox
// MAGIC 
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC # Coding Challenge #1
// MAGIC 
// MAGIC **Dataset:**
// MAGIC * This is synthetic data generated specifically for these exercises
// MAGIC * Each year's data is roughly the same with some variation for market growth
// MAGIC * We are looking at retail purchases from the top N retailers
// MAGIC 
// MAGIC **Technical Accomplishments:**
// MAGIC * Diagnose and fix performance problems

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

import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel

spark.catalog.clearCache()

val trxPath = "dbfs:/mnt/training/global-sales/transactions/2017.parquet/"
val citiesPath = "dbfs:/mnt/training/global-sales/cities/all.parquet/"

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Optimize Me
// MAGIC 
// MAGIC **Optimize the query below for the fastest execution time:**
// MAGIC * The final **DataFrame** should be assigned to **finalDF**
// MAGIC * The benchmark will be based on
// MAGIC   * A **count()** operation iterating over all records.
// MAGIC   * The sum of the duration of all jobs triggered
// MAGIC * Caching is not permitted for this exercise
// MAGIC * Use of shuffle files is not permitted for the sake of improving benchmarking
// MAGIC 
// MAGIC **Note:** The best time is ~1.5 seconds

// COMMAND ----------

// TODO

import org.apache.spark.sql.functions._
spark.conf.set("spark.sql.shuffle.partitions", 8)

val finalDF = spark.read.parquet(trxPath).join(spark.read.parquet(citiesPath), "city_id")
 .groupBy("city", "state_abv", "country").sum("amount")
 .withColumn("cityState", concat_ws(", ", $"city", $"state_abv"))
 .filter($"cityState".contains(","))
 .select($"sum(amount)".as("amount"), $"cityState", $"country")
 .orderBy($"cityState")

val (df, total, duration) = benchmarkCount(() => finalDF)
println(duration)
println("-"*80)

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Benchmark Your Solution
// MAGIC 
// MAGIC Run the cell below to benchmark your solution:

// COMMAND ----------

spark.catalog.clearCache()

val (df, total, duration) = benchmarkCount(() => finalDF)

println("%s ms".format(duration))
println("-"*80)

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Validate Your Solution
// MAGIC 
// MAGIC Run the cell below to validate your solution:

// COMMAND ----------

assert(3 == finalDF.columns.size, s"Expected only three columns, found ${finalDF.columns.size}")

val schema = finalDF.schema.mkString
assert(schema.contains("amount,DecimalType(38,18)"), "Expected the schema to include amount of type Decimal(38,18)")
assert(schema.contains("cityState,StringType"), "Expected the schema to include city-state of type String")
assert(schema.contains("country,StringType"), "Expected the schema to include country of type String")

val cities = finalDF.select($"cityState").as[String].collect
assert(cities(0) == "Albany, NY", """The first city should be "Albany, NY".""")
assert(cities(cities.size-1) == "Vaiaku village, Funafuti province", """The last city should be "Vaiaku village, Funafuti province".""")

displayHTML("""<div style="font-weight:bold; color:green">Congratulations, all tests passed!</div>""")


// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>