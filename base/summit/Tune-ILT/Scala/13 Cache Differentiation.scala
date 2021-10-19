// Databricks notebook source
// MAGIC 
// MAGIC %md-sandbox
// MAGIC 
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC # Cache Differentiation
// MAGIC 
// MAGIC **Dataset:**
// MAGIC * This is synthetic data generated specifically for these exercises
// MAGIC * Each year's data is roughly the same with some variation for market growth
// MAGIC * We are looking at retail purchases from the top N retailers
// MAGIC 
// MAGIC **Technical Accomplishments:**
// MAGIC * Explore techniques for working with caches

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

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Let's cache some data
// MAGIC 
// MAGIC Regardless of "Should I cache", caching is a viable component of developing Spark applications. 
// MAGIC 
// MAGIC In many cases we share a cluster with other users making it hard to figure out which cache is mine.
// MAGIC 
// MAGIC Even when one is the sole user of a cluster, differentiating between multiple caches can be difficult.
// MAGIC 
// MAGIC Let's take a look at an example:

// COMMAND ----------

import org.apache.spark.sql.DataFrame
import org.apache.spark.scheduler._
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

// Clear the cache from any leftover experiements
spark.catalog.clearCache()

// Specify the schema just to save a few seconds on the creation of the DataFrame
val schema = "transacted_at timestamp, trx_id integer, retailer_id integer, description string, amount decimal(38,2), city_id integer"

// Cache the first DataFrame
val fastPath14 = "/mnt/training/global-sales/solutions/2014-fast.parquet"
spark.read.schema(schema).parquet(fastPath14).cache().count()

// Cache the second DataFrame
val fastPath17 = "/mnt/training/global-sales/solutions/2017-fast.parquet"
spark.read.schema(schema).parquet(fastPath17).cache().count()

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Which is Which?
// MAGIC 
// MAGIC Open the Spark UI and try to determine which cache belongs to which DataFrame.

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Challenge #1
// MAGIC 
// MAGIC The ability to name a cache is a feature of Spark 1.x's RDD API.
// MAGIC 
// MAGIC The feature is planned for, but not yet part of the Spark 2.x DataFrame API.
// MAGIC 
// MAGIC But there is a way to accomplish a "close enough" solution:
// MAGIC 
// MAGIC **Create a utility method to cache a DataFrame with a given name:**
// MAGIC * Name the function **cacheAs**
// MAGIC * The function should return **DataFrame**
// MAGIC * The function should have three parameters:
// MAGIC   * **df**:**DataFrame**
// MAGIC   * **name**:**String**
// MAGIC   * **level**:**StorageLevel**
// MAGIC * The function should:
// MAGIC   * Create a temp view for the specified **DataFrame** with the specified **name**
// MAGIC   * Use **spark.catalog.cacheTable(..)** to cache the table with the specified **level**
// MAGIC   * Return the original **DataFrame**
// MAGIC   
// MAGIC **Bonus:** Update the method to remove any existing cache for the specified name:
// MAGIC   * Use **spark.cataog.uncacheTable(..)** to uncache the table before caching the table
// MAGIC   * Surround in a try-catch block that ignores any exception - e.g. the table doesn't exist

// COMMAND ----------

// TODO

def FILL_IN(FILL_IN, FILL_IN, FILL_IN):FILL_IN = {

  FILL_IN // uncache the table by name - ignore exception
  
  FILL_IN // Create the temp view
  FILL_IN // cache the table via the spark catalog

  return df
}

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Give it a try!

// COMMAND ----------

spark.catalog.clearCache()

// Cache the first DataFrame
cacheAs(spark.read.schema(schema).parquet(fastPath14), "fast_2014", StorageLevel.DISK_ONLY).count()

// Cache the second DataFrame
cacheAs(spark.read.schema(schema).parquet(fastPath17), "fast_2017", StorageLevel.DISK_ONLY).count()

// COMMAND ----------

// MAGIC %md
// MAGIC Now take a look at the Spark UI

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>