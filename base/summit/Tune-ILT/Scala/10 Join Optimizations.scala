// Databricks notebook source
// MAGIC 
// MAGIC %md-sandbox
// MAGIC 
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC #Join Optimizations
// MAGIC 
// MAGIC **Dataset:**
// MAGIC * This is synthetic data generated specifically for these exercises
// MAGIC * Each year's data is roughly the same with some variation for market growth
// MAGIC * We are looking at retail purchases from the top N retailers

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> This notebook is only available in Scala due to the use of APIs not available in Python.<br/>
// MAGIC However, the fundamental principles taught here remain the same for both languages.

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) A Little Back Story
// MAGIC 
// MAGIC * Spark was designed for 1-shot data
// MAGIC * No real focus on optimized queries
// MAGIC * Optimizers:
// MAGIC   * Catalyst Optimizer
// MAGIC   * Incrementalization Optimizer
// MAGIC   * Cost Based Optimizer
// MAGIC     * Introduced in 2016 as a Demo at Spark Summit
// MAGIC     * Introduced into Spark 2.2 about a year later

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Getting Started
// MAGIC 
// MAGIC Run the following cell to configure our "classroom."

// COMMAND ----------

// MAGIC %run "./Includes/Classroom-Setup"

// COMMAND ----------

// MAGIC %run "../Includes/Initialize-Labs"

// COMMAND ----------

// MAGIC %run "./Includes/Utility-Methods"

// COMMAND ----------

// MAGIC %scala
// MAGIC 
// MAGIC import org.apache.spark.sql.functions._
// MAGIC import org.apache.spark.sql.DataFrame
// MAGIC import org.apache.spark.sql.types.StructType
// MAGIC import org.apache.spark.storage.StorageLevel

// COMMAND ----------

// MAGIC %md
// MAGIC Because CBO on Databricks is enabled by default, we need to disable it for demonstration purposes:

// COMMAND ----------

// MAGIC %scala
// MAGIC 
// MAGIC val enabled = spark.conf.get("spark.sql.cbo.enabled")

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC We can do so by setting the following two properties false:
// MAGIC 
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> These are also the same two properties to enable in other distributions of Spark should you want to use the CBO there.

// COMMAND ----------

// MAGIC %scala
// MAGIC 
// MAGIC spark.conf.set("spark.sql.cbo.enabled", false)
// MAGIC spark.conf.set("spark.sql.cbo.joinReorder.enabled", false)

// COMMAND ----------

// MAGIC %md
// MAGIC So as to avoid collisions, we should create our own database.
// MAGIC 
// MAGIC We will use our **username** as the basis for the DB's name.

// COMMAND ----------

// MAGIC %scala
// MAGIC 
// MAGIC val dbName = username.substring(0, username.indexOf("@")).replace(".","_").replace("-","_")
// MAGIC val query = "CREATE DATABASE %s".format(dbName)
// MAGIC 
// MAGIC try {
// MAGIC   spark.sql(query) 
// MAGIC   println("Created the database %s".format(dbName))
// MAGIC } catch { case e:org.apache.spark.sql.AnalysisException => 
// MAGIC   println("The database %s already exists".format(dbName))
// MAGIC }
// MAGIC 
// MAGIC println("-"*80)

// COMMAND ----------

// MAGIC %md
// MAGIC Define three table names in our private DB:

// COMMAND ----------

// MAGIC %scala
// MAGIC 
// MAGIC val sales = "%s.sales".format(dbName)
// MAGIC val cities = "%s.cities".format(dbName)
// MAGIC val stores = "%s.stores".format(dbName)

// COMMAND ----------

// MAGIC %md
// MAGIC Drop the tables just in case they already exist...

// COMMAND ----------

// MAGIC %scala
// MAGIC 
// MAGIC val queryA = "DROP TABLE %s".format(sales)
// MAGIC try{ spark.sql(queryA) } 
// MAGIC catch { case e:org.apache.spark.sql.AnalysisException => println("The sales table doesn't exit.") }
// MAGIC 
// MAGIC val queryB = "DROP TABLE %s".format(cities)
// MAGIC try{ spark.sql(queryB) } 
// MAGIC catch { case e:org.apache.spark.sql.AnalysisException => println("The cities table doesn't exist.") }
// MAGIC 
// MAGIC val queryC = "DROP TABLE %s".format(stores)
// MAGIC try{ spark.sql(queryC) } 
// MAGIC catch { case e:org.apache.spark.sql.AnalysisException => println("The stores table doesn't exit.") }
// MAGIC 
// MAGIC println("-"*80)

// COMMAND ----------

// MAGIC %md
// MAGIC Next, initialize three DataFrames and then use those to create tables:

// COMMAND ----------

// MAGIC %scala
// MAGIC 
// MAGIC // A reasonable default for this type of cluster and our datasets
// MAGIC spark.conf.set("spark.sql.shuffle.partitions", 8)
// MAGIC 
// MAGIC // Create a table for the "fast" version of our 2017 data
// MAGIC val salesPath = "/mnt/training/global-sales/solutions/2017-fast.parquet"
// MAGIC val salesDF = spark.read.parquet(salesPath)
// MAGIC salesDF.write.saveAsTable(sales)
// MAGIC 
// MAGIC // Create a table for the "cities" data
// MAGIC val citiesPath = "/mnt/training/global-sales/cities/all.parquet"
// MAGIC val citiesDF = spark.read.parquet(citiesPath)
// MAGIC citiesDF.write.saveAsTable(cities)
// MAGIC 
// MAGIC // Create a table for the "stores" data
// MAGIC val storesPath = "/mnt/training/global-sales/retailers/all.parquet"
// MAGIC val storesDF = spark.read.parquet(storesPath)
// MAGIC storesDF.write.saveAsTable(stores)

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Our First Query
// MAGIC 
// MAGIC With everything configured, we can take a look at our first query:

// COMMAND ----------

// MAGIC %scala
// MAGIC 
// MAGIC spark.table(sales)
// MAGIC      .join( spark.table(cities), "city_id")
// MAGIC      .join( spark.table(stores), "retailer_id")
// MAGIC      .foreach( x => () )

// COMMAND ----------

// MAGIC %md
// MAGIC ### Review the query
// MAGIC 
// MAGIC * Open the Spark UI
// MAGIC * Select the **SQL** tab
// MAGIC * Find the corresponding query (compare job number above to the job number in the last column)
// MAGIC * Open the SQL query by clicking on the link in the description column

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Enabling CBO
// MAGIC 
// MAGIC * CBO should be disabled by default
// MAGIC * We can turn it and the **joinReorder** feature on via the following two properties:

// COMMAND ----------

// MAGIC %scala
// MAGIC 
// MAGIC spark.conf.set("spark.sql.cbo.enabled", true)                // Enable the CBO
// MAGIC spark.conf.set("spark.sql.cbo.joinReorder.enabled", true)    // Enable Join re-ordering

// COMMAND ----------

// MAGIC %md
// MAGIC Next, analyze the three tables.

// COMMAND ----------

// MAGIC %scala
// MAGIC 
// MAGIC val queryA = "ANALYZE TABLE %s COMPUTE STATISTICS".format(sales)
// MAGIC spark.sql(queryA)
// MAGIC 
// MAGIC val queryB = "ANALYZE TABLE %s COMPUTE STATISTICS".format(cities)
// MAGIC spark.sql(queryB)
// MAGIC 
// MAGIC val queryC = "ANALYZE TABLE %s COMPUTE STATISTICS".format(stores)
// MAGIC spark.sql(queryC)

// COMMAND ----------

// MAGIC %md
// MAGIC Run our query again.

// COMMAND ----------

// MAGIC %scala
// MAGIC 
// MAGIC spark.table(sales)
// MAGIC      .join( spark.table(cities), "city_id")
// MAGIC      .join( spark.table(stores), "retailer_id")
// MAGIC      .foreach( x => () )

// COMMAND ----------

// MAGIC %md
// MAGIC And then back to the Spark UI to review this query

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### According to the Spark UI, how far off was Spark on its estimation of the final *SortMergeJoin*?
// MAGIC 
// MAGIC ### How much data from the *stores* table was broadcast?
// MAGIC 
// MAGIC ### Why was the **cities** table not broadcast? How large was it?

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) How many cities?
// MAGIC 
// MAGIC Before moving forward, let's take a look at the cities table... More specifically, it's size.
// MAGIC 
// MAGIC If it's small enough, we should be able to broadcast it.
// MAGIC 
// MAGIC Start by caching the data - we are going to use a temp view to control the **RDD Name** in the Spark UI.

// COMMAND ----------

// MAGIC %scala
// MAGIC 
// MAGIC spark.catalog.clearCache()
// MAGIC 
// MAGIC val cacheName = "temp_cities"
// MAGIC 
// MAGIC // Cache and materialize with the name "temp_cities"
// MAGIC cacheAs(spark.table(cities), cacheName, StorageLevel.MEMORY_ONLY).count() 

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC I can go to the Spark UI, select the **Storage** tab and then read the **Size in Memory**
// MAGIC 
// MAGIC Or... I can do it programatically via the **Spark Context**:
// MAGIC 
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> For more information, see <a href="https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.SparkContext" target="_blank">SparkContext</a> and <a href="https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.storage.RDDInfo" target="_blank">RDDInfo</a>

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ### What is the default threshold for broadcasting?
// MAGIC 
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Notice the use of **getRDDStorageInfo** below.<br/>
// MAGIC This gives us programtatic access to the storage info (cache).<br/>
// MAGIC These tools are currently not available via the Python API.

// COMMAND ----------

// MAGIC %scala
// MAGIC 
// MAGIC val rddName = "In-memory table %s".format(cacheName)
// MAGIC val fullSize = sc.getRDDStorageInfo.filter(_.name == rddName).map(_.memSize).head
// MAGIC 
// MAGIC println("Cached Size: %,.1f MB".format(fullSize/1024.0/1024.0))
// MAGIC println("-"*80)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Is that suitable for auto broadcasting?

// COMMAND ----------

// MAGIC %scala
// MAGIC 
// MAGIC val threshold = spark.conf.get("spark.sql.autoBroadcastJoinThreshold").toLong
// MAGIC val broadcastable = fullSize < threshold
// MAGIC 
// MAGIC println("Broadcastable: %s".format(broadcastable))
// MAGIC println("Cached Size:   %,.1f MB".format(fullSize/1024.0/1024.0))
// MAGIC println("Threshold:     %,.1f MB".format(threshold/1024.0/1024.0))
// MAGIC println("-"*80)

// COMMAND ----------

// MAGIC %md
// MAGIC ### What is wrong with using the size-in-RAM to judge the ability to broadcast?

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Only US Retailers?
// MAGIC 
// MAGIC What if we were to limit the cities to the US only?
// MAGIC 
// MAGIC Let's see how much data that would represent:

// COMMAND ----------

// MAGIC %scala
// MAGIC 
// MAGIC spark.catalog.clearCache()
// MAGIC 
// MAGIC val cacheName = "temp_us_cities"
// MAGIC val rddName = "In-memory table %s".format(cacheName)
// MAGIC 
// MAGIC val usOnlyDF = spark.table(cities).filter($"state".isNotNull)
// MAGIC cacheAs(usOnlyDF, cacheName, StorageLevel.MEMORY_ONLY).count() 
// MAGIC 
// MAGIC val usSize = sc.getRDDStorageInfo.filter(_.name == rddName).map(_.memSize).head
// MAGIC 
// MAGIC val usBroadcastable = usSize < threshold
// MAGIC 
// MAGIC println("Broadcastable: %s".format(usBroadcastable))
// MAGIC println("Cached Size:   %,.1f KB".format(usSize/1024.0))
// MAGIC println("Threshold:     %,.1f MB".format(threshold/1024.0/1024.0))
// MAGIC println("-"*80)
// MAGIC 
// MAGIC // Clean up after ourselves
// MAGIC spark.catalog.clearCache()

// COMMAND ----------

// MAGIC %md
// MAGIC If we limit to the US cities only, then we should be under the auto-broadcast threshold!

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) CBO with US Cities Only
// MAGIC 
// MAGIC Same query as before, this time we are limiting the cities to the US only.

// COMMAND ----------

// MAGIC %scala
// MAGIC 
// MAGIC spark.table(sales)
// MAGIC      .join( spark.table(cities).filter($"state".isNotNull), "city_id")
// MAGIC      .join( spark.table(stores), "retailer_id")
// MAGIC      .foreach( x => () )

// COMMAND ----------

// MAGIC %md
// MAGIC Once again, let's take a look at the query plan in the Spark UI

// COMMAND ----------

// MAGIC %md
// MAGIC ### Why did that not broadcast?
// MAGIC 
// MAGIC ### What was the size of the data that we expected to be broadcast?
// MAGIC 
// MAGIC ### How does the size in the exchange compare to the cached size?
// MAGIC 
// MAGIC ### Why are they so drastically different?

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Can we add a Broadcast Hint?
// MAGIC 
// MAGIC We can add a broadcast hint to the filtered table to let Spark know that it can be broadcast.
// MAGIC 
// MAGIC The downside of this is that developer needs to know to ask for this behavior.
// MAGIC 
// MAGIC Asking for it indiscriminately can lead to various bugs and prerformance problems such as OOM Errors.

// COMMAND ----------

// MAGIC %scala
// MAGIC 
// MAGIC spark.table(sales)
// MAGIC      .join( broadcast(spark.table(cities).filter($"state".isNotNull)), "city_id")
// MAGIC      .join( spark.table(stores), "retailer_id")
// MAGIC      .foreach( x => () )

// COMMAND ----------

// MAGIC %md
// MAGIC And just to confirm, that it still doesn't work without the hint:

// COMMAND ----------

// MAGIC %scala
// MAGIC 
// MAGIC spark.table(sales)
// MAGIC      .join( spark.table(cities).filter($"state".isNotNull), "city_id")
// MAGIC      .join( spark.table(stores), "retailer_id")
// MAGIC      .foreach( x => () )

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Column Analytics
// MAGIC 
// MAGIC We analyzed only the table, not specific columns.
// MAGIC 
// MAGIC If we give the CBO just a little more information it can do a better job including auto-broadcasting the filtered dataset.
// MAGIC 
// MAGIC All we should need to do is re-analyze the cities table, and specifically the **city_id** and **state** columns.

// COMMAND ----------

// MAGIC %scala
// MAGIC 
// MAGIC val query = "ANALYZE TABLE %s COMPUTE STATISTICS FOR COLUMNS city_id, state".format(cities)
// MAGIC spark.sql(query)

// COMMAND ----------

// MAGIC %md
// MAGIC And now that we have column specific stats, let's try our query one more time.

// COMMAND ----------

// MAGIC %scala
// MAGIC 
// MAGIC spark.table(sales)
// MAGIC      .join( spark.table(cities).filter($"state".isNotNull), "city_id")
// MAGIC      .join( spark.table(stores), "retailer_id")
// MAGIC      .foreach( x => () )

// COMMAND ----------

// MAGIC %md
// MAGIC And back to the Spark UI just one more time.

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Conclusions?
// MAGIC 
// MAGIC ### What type of user is the CBO best suited for?
// MAGIC 
// MAGIC ### What type of user is the broadcast() hint suited for?

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>