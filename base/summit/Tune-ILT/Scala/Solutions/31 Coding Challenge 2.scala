// Databricks notebook source
// MAGIC 
// MAGIC %md-sandbox
// MAGIC 
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC # Coding Challenge #2
// MAGIC 
// MAGIC **Dataset:**
// MAGIC * This is synthetic data generated specifically for these exercises
// MAGIC * Each year's data is roughly the same with some variation for market growth
// MAGIC * We are looking at retail purchases from the top N retailers
// MAGIC 
// MAGIC **Technical Accomplishments:**
// MAGIC * Diagnose and fix common coding problems

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
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Optimize Me
// MAGIC 
// MAGIC **Optimize the query below:**
// MAGIC * The final **DataFrame** should be assigned to **finalDF**
// MAGIC * The final dataset, on disk, should be partitioned by **zip_code** at an optimal size and written to **finalPath**
// MAGIC * The use of temporary files / datasets is prohibited for this exercise
// MAGIC * Caching can be used during development, but not in the final solution
// MAGIC * The final solution should have only one job and two or fewer stages
// MAGIC * Total execution time should be under 3 minutes.<br/>
// MAGIC   <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> No one knows for sure how long it takes<br/>
// MAGIC   It has never been allowed to run to completion<br/>
// MAGIC   We do know that it will not complete within an hour<br/>
// MAGIC 
// MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** There are at least nine different problems with the Scala version and at least ten with the Python version.

// COMMAND ----------

// ANSWER

/////////////////////////////////////////////////////////////////////////////////////
// This is the same "TODO" code as above and documents what is wrong with this code.
/////////////////////////////////////////////////////////////////////////////////////

import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

spark.catalog.clearCache()

val trxPath = "dbfs:/mnt/training/global-sales/solutions/2018-fixed.parquet/"
val citiesPath = "dbfs:/mnt/training/global-sales/cities/all.parquet/"
val finalPath = "%s/coding-challenge-2.parquet".format(userhome)

// PROBLEM #1: 24 partitions here means that after we do the join, we are going to end
// up with 3x times as many partitions are we need. Our cluster can easily handle 8.
// But even then, it turns out we only need 1 partition post-join
spark.conf.set("spark.sql.shuffle.partitions", 24)

// Problem #2: It is completely unnecissary overhead.
class RestClient extends Serializable {
  def lookupCity (city:String, state:String): String = {
    try {
      val url = "http://api.zippopotam.us/us/%s/%s".format(state, city.replace(" ", "%20"))
      val json = scala.io.Source.fromURL(url).mkString
      val posA = json.indexOf("\"post code\": \"")+14
      val posB = json.indexOf("\"", posA)
      json.substring(posA, posB)
      
    } catch {
      case e:java.io.FileNotFoundException => null
    }
  }
}

def fetch(city:String, state:String):String = {
  // PROBLEM #3: we are doing object creation. This means we have objects created
  // and the resulting garbage collection for EVERY row we process.
  val client = new RestClient()
  return client.lookupCity(city, state)
}

val fetchUDF = spark.udf.register("fetch", fetch _)

val citiesDF = spark.read.parquet(citiesPath)
val trxDF = spark.read.parquet(trxPath)

// PROBLEM #4: We are joining before updating the cities. This means we will actually make
// this rest call on thousands of records instead of the 50 distinct cities.

// PROBLEM #5: We can broadcast the citiesDF - after the filter, it should be small enough
val finalDF = trxDF.join(citiesDF, "city_id")
  // PROBLEM #6: Repartitioning here is comletely unnecissary - this will actually be 
  // governed by the propery "spark.sql.shuffle.partitions"
  .repartition(sc.defaultParallelism)
  // PROBLEM #7: This should be called directly against citiesDF BEFORE we join the data
  .filter($"state_abv".isNotNull)
  // PROBLEM #8: This should be callled directly against citiesDF, after we filter it down BEFORE we join the data.
  .withColumn("zip_code", fetchUDF($"city", $"state_abv"))
  // PROBLEM #9: We should partition the data by zip_code as well so 
  // that we have the best on-disk scenario for the size of our data
  .write.mode("overwrite")
  .partitionBy("zip_code")
  .parquet(finalPath)

// COMMAND ----------

// ANSWER

import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

spark.catalog.clearCache()

val trxPath = "dbfs:/mnt/training/global-sales/solutions/2018-fixed.parquet/"
val citiesPath = "dbfs:/mnt/training/global-sales/cities/all.parquet/"
val finalPath = "%s/coding-challenge-2.parquet".format(userhome)

// In case you need to start over
// dbutils.fs.rm(finalPath, true)

// We know post-join we only want one spark-partition so that when
// we write to disk, we get one parquet part-file per zip code.
spark.conf.set("spark.sql.shuffle.partitions", 1)

// Dump the extra object entirely.
def fetch(city:String, state:String):String = {
  try {
    val url = "http://api.zippopotam.us/us/%s/%s".format(state, city.replace(" ", "%20"))
    val json = scala.io.Source.fromURL(url).mkString
    val posA = json.indexOf("\"post code\": \"")+14
    val posB = json.indexOf("\"", posA)
    json.substring(posA, posB)

  } catch {
    case e:java.io.FileNotFoundException => null
  }
}
val fetchUDF = spark.udf.register("fetch", fetch _)
  
// Specify the schema to avoid the extra job reading the parquet schema
val citiesSchema = "city_id integer, city string, state string, state_abv string, country string"

// Cities is a small table - filter it first and then fetch the zip codes
val citiesDF = spark.read.schema(citiesSchema).parquet(citiesPath)
  .filter($"state_abv".isNotNull)
  .withColumn("zip_code", fetchUDF($"city", $"state_abv"))

// Specify the schema to avoid the extra job reading the parquet schema
val trxSchema = "city_id integer, transacted_at timestamp, trx_id integer, retailer_id integer, description string, amount decimal(10,2), year integer, month integer"
val trxDF = spark.read.schema(trxSchema).parquet(trxPath)

val finalDF = trxDF
  .join(broadcast(citiesDF), "city_id") // Broadcast the cities table which we know will be small enough
  .repartition($"zip_code")             // Repartition the data by zip-code into spark.sql.shuffle.partitions
  .write.mode("overwrite")              // Replace the existing file
  .partitionBy("zip_code")              // Partition the data on disk by zip_code
  .parquet(finalPath)                   // Write the file out as parquet

// COMMAND ----------

val finalSchema = "city_id integer, transacted_at timestamp, trx_id integer, retailer_id integer, description string, amount decimal(10,2), year integer, month integer, city string, state string, state_abv string, country string, zip_code string"

val testDF = spark.read.schema(finalSchema).parquet(finalPath)

display(testDF)

// COMMAND ----------

display( dbutils.fs.ls(finalPath) )

// COMMAND ----------

display( dbutils.fs.ls(finalPath+"/zip_code=02101") )


// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>