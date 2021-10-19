// Databricks notebook source
// MAGIC 
// MAGIC %md-sandbox
// MAGIC 
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC # The Overhead of Caching
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
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Ready, Set, Go!
// MAGIC 
// MAGIC * We have 7 experiments to run
// MAGIC * Each experiment can take roughly 5-10 minutes to execute
// MAGIC * We can expedite things by running **ALL** the cells first
// MAGIC 
// MAGIC <img style="float:left; margin-right:1em; box-shadow: 5px 5px 5px 0px rgba(0,0,0,0.25); border: 1px solid rgba(0,0,0,0.25);" src="https://files.training.databricks.com/images/notebook-cell-run-cmd.png"/>
// MAGIC <p><br/><br/><br/><img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** From the top of the notebook, select **Run All**<br/>
// MAGIC    or from a subsequent cell, select **Run All Below**.</p>

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) While We Wait
// MAGIC 
// MAGIC Let's take a look at a **[A Caching Story]($../Extras/A Caching Story)**    

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) A Couple of Experiments
// MAGIC 
// MAGIC We'll use the following method to load the 2017 dataset over and over again.
// MAGIC 
// MAGIC For each "experiment", we have to modify the DAG so that Spark doesn't optimize our code unexpectedly.
// MAGIC 
// MAGIC 
// MAGIC 
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> The cache should fill up after ~36 partitions.</br>The remaining 74 partitions will result in some form of expulsion.

// COMMAND ----------

// MAGIC %scala
// MAGIC 
// MAGIC import org.apache.spark.sql.functions._
// MAGIC import org.apache.spark.sql.DataFrame
// MAGIC import org.apache.spark.sql.types.StructType
// MAGIC import org.apache.spark.storage.StorageLevel
// MAGIC 
// MAGIC def load(iteration:Int):DataFrame = {
// MAGIC   // Specify the schema just to save a few seconds on the creation of the DataFrame
// MAGIC   val schema = "transacted_at timestamp, trx_id integer, retailer_id integer, description string, amount decimal(38,2), city_id integer"
// MAGIC 
// MAGIC   // Load the "fast" version of our data
// MAGIC   val fixedPath17 = "/mnt/training/global-sales/solutions/2017-fixed.parquet"
// MAGIC   val df = spark.read.schema(schema).parquet(fixedPath17)
// MAGIC   
// MAGIC   // Each experiement needs a custom/unique transformation so that
// MAGIC   // Spark doesn't use the cache from previous experiments
// MAGIC   df.withColumn("iteration", lit(iteration))
// MAGIC }
// MAGIC 
// MAGIC // If you have some spare time, increase the MULTIPLIER 
// MAGIC // from 10 up to 20 or 40 to see a more dramatic contrast
// MAGIC val MULTIPLIER = 10

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ### Track your experiments
// MAGIC 
// MAGIC As you work through each experiment, you can keep track of your results in the following worksheet:
// MAGIC 
// MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** Don't close this notebook while you are filling out the worksheet - you will lose any results.
// MAGIC 
// MAGIC **Storage Memory Instructions:**
// MAGIC 0. Open the **Spark UI**
// MAGIC 0. Select the **Executors** tab
// MAGIC 0. Find the **Summary** table, **Active(n)** row, **Storage Memory** column, second value
// MAGIC 0. Subtract the Driver's **Storage Memory**
// MAGIC   * Find the **Executors** table, **driver** row, **Storage Memory** column, second value
// MAGIC   
// MAGIC **Duration and GC Time Instructions:**
// MAGIC 0. Open the **Spark UI**
// MAGIC 0. Select the **Jobs** tab
// MAGIC 0. Find the corresponding job and open the details for that job
// MAGIC 0. Open the details for the first stage
// MAGIC 0. Find the **Summary Metrics** table, **Duration** (or **GC Time**) row, **Min**, **Median** and **Max** columns

// COMMAND ----------

// MAGIC %run "./Worksheets/Caching Worksheet"

// COMMAND ----------

// MAGIC %md
// MAGIC ### Experiment #1
// MAGIC 
// MAGIC **Establishing a baseline - no caching**
// MAGIC * Run the following code block
// MAGIC * All 10 datasets are unioned together into an uber-dataset.
// MAGIC * **The data will NOT be cached**
// MAGIC 
// MAGIC ** Once the experiment has completed:**
// MAGIC 0. Open the **Spark UI**
// MAGIC 0. Navigate to the details for the first stage
// MAGIC 0. Examine the query's statistics and note
// MAGIC   * The **Min**, **Median** and **Max** of the **Duration** of each task
// MAGIC   * The **Min**, **Median** and **Max** of the **GC Time** of each task
// MAGIC 0. Record the results in the worksheet above

// COMMAND ----------

// MAGIC %scala
// MAGIC 
// MAGIC spark.catalog.clearCache()
// MAGIC 
// MAGIC var df = load(0)
// MAGIC 
// MAGIC for (i <- 1 until MULTIPLIER) 
// MAGIC   df = df.unionByName(load(i))
// MAGIC 
// MAGIC df.count() // no cache

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ### Experiment #2
// MAGIC 
// MAGIC **Testing the default, MEMORY_AND_DISK**
// MAGIC * Run the following code block
// MAGIC * All 10 datasets are unioned together into an uber-dataset
// MAGIC * **The data will be written directly to memory and then spilled to disk once the RAM cache fills up**
// MAGIC 
// MAGIC ** While the experiment is running:**
// MAGIC 0. Open the **Spark UI**
// MAGIC 0. Examine the behavior of the cache via the **Storage** tab in the **Spark UI**
// MAGIC 0. <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** You can easily refresh the page by clicking on the **Storage** tab
// MAGIC   
// MAGIC ** Once the experiment has completed:**
// MAGIC 0. Open the **Spark UI**
// MAGIC 0. Navigate to the details for the first stage
// MAGIC 0. Examine the query's statistics and note
// MAGIC   * The **Min**, **Median** and **Max** of the **Duration** of each task
// MAGIC   * The **Min**, **Median** and **Max** of the **GC Time** of each task
// MAGIC 0. Record the results in the worksheet above

// COMMAND ----------

// MAGIC %scala
// MAGIC 
// MAGIC spark.catalog.clearCache()
// MAGIC 
// MAGIC var df = load(0)
// MAGIC 
// MAGIC for (i <- 1 until MULTIPLIER) 
// MAGIC   df = df.unionByName(load(i))
// MAGIC 
// MAGIC cacheAs(df, "experiment_2", StorageLevel.MEMORY_AND_DISK).count()

// COMMAND ----------

// MAGIC %md
// MAGIC ### Experiment #3
// MAGIC 
// MAGIC **Testing DISK_ONLY**
// MAGIC * Run the following code block
// MAGIC * All 10 datasets are unioned together into an uber-dataset
// MAGIC * **The data will be written directly to disk**
// MAGIC 
// MAGIC Follow the same basic instructions for Experiment #2

// COMMAND ----------

// MAGIC %scala
// MAGIC 
// MAGIC spark.catalog.clearCache()
// MAGIC 
// MAGIC var df = load(0)
// MAGIC 
// MAGIC for (i <- 1 until MULTIPLIER) 
// MAGIC   df = df.unionByName(load(i))
// MAGIC 
// MAGIC cacheAs(df, "experiment_3", StorageLevel.DISK_ONLY).count()

// COMMAND ----------

// MAGIC %md
// MAGIC ### Experiment #4
// MAGIC 
// MAGIC **Testing MEMORY_ONLY**
// MAGIC * Run the following code block
// MAGIC * All 10 datasets are unioned together into an uber-dataset
// MAGIC * **The data will be written directly to memory and then expunged once it runs out of space**
// MAGIC 
// MAGIC Follow the same basic instructions for Experiment #2

// COMMAND ----------

// MAGIC %scala
// MAGIC 
// MAGIC spark.catalog.clearCache()
// MAGIC 
// MAGIC var df = load(0)
// MAGIC 
// MAGIC for (i <- 1 until MULTIPLIER) 
// MAGIC   df = df.unionByName(load(i))
// MAGIC 
// MAGIC cacheAs(df, "experiment_4", StorageLevel.MEMORY_ONLY).count()

// COMMAND ----------

// MAGIC %md
// MAGIC ### Experiment #5
// MAGIC 
// MAGIC **Testing MEMORY_ONLY_SER**
// MAGIC * Run the following code block
// MAGIC * All 10 datasets are unioned together into an uber-dataset
// MAGIC * **The data will be serialized, written directly to memory and then expunged once it runs out of space**
// MAGIC 
// MAGIC Follow the same basic instructions for Experiment #2

// COMMAND ----------

// MAGIC %scala
// MAGIC 
// MAGIC spark.catalog.clearCache()
// MAGIC 
// MAGIC var df = load(0)
// MAGIC 
// MAGIC for (i <- 1 until MULTIPLIER) 
// MAGIC   df = df.unionByName(load(i))
// MAGIC 
// MAGIC cacheAs(df, "experiment_5", StorageLevel.MEMORY_ONLY_SER).count()

// COMMAND ----------

// MAGIC %md
// MAGIC ### Experiment #6
// MAGIC 
// MAGIC **Testing MEMORY_AND_DISK_SER**
// MAGIC * Run the following code block
// MAGIC * All 10 datasets are unioned together into an uber-dataset
// MAGIC * **The data will be serialized, written directly to memory and then spilled to disk once the RAM cache fills up**
// MAGIC 
// MAGIC Follow the same basic instructions for Experiment #2

// COMMAND ----------

// MAGIC %scala
// MAGIC 
// MAGIC spark.catalog.clearCache()
// MAGIC 
// MAGIC var df = load(0)
// MAGIC 
// MAGIC for (i <- 1 until MULTIPLIER) 
// MAGIC   df = df.unionByName(load(i))
// MAGIC 
// MAGIC cacheAs(df, "experiment_6", StorageLevel.MEMORY_AND_DISK_SER).count()

// COMMAND ----------

// MAGIC %run "./Worksheets/Caching Worksheet - Sample Results"

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Conclusions?
// MAGIC 
// MAGIC ### Why was not caching so much faster than any of the caching scenarios?
// MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** Compare the **Input** size on the Job's detail page of the non-caching job vs the others.
// MAGIC 
// MAGIC ### What effect does serialization have on performance?
// MAGIC 
// MAGIC ### What is the overhead of MEMORY_ONLY or DISK_ONLY compared to not caching?
// MAGIC 
// MAGIC ### What is the overhead of MEMORY_AND_DISK compared to MEMORY_ONLY and DISK_ONLY?
// MAGIC 
// MAGIC ### Is caching worth the performance hit?
// MAGIC 
// MAGIC ### If I'm having OOM and GC issues, which of the options (besides not caching) is the best option?

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>