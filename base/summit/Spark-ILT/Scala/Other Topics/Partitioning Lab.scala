// Databricks notebook source
// MAGIC 
// MAGIC %md-sandbox
// MAGIC 
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC # Partitioning Lab
// MAGIC ## Exploring Partitions

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/wiki-book/general/logo_spark_tiny.png) Play Time
// MAGIC 
// MAGIC Let's play around a little bit
// MAGIC 0. Start with 8 partitions.
// MAGIC 0. Run the code.
// MAGIC 0. Open the **Spark UI**
// MAGIC 0. Dig into the second job (remember the first is for the range partitioner)
// MAGIC 0. Dig into the second stage (the one with 8 partitions)
// MAGIC 0. Expand the **Event Timeline**
// MAGIC 0. Notice how with 8 partitions all tests run roughly in parallel.
// MAGIC 0. Notice how long it took to execute that one stage.
// MAGIC   * Make note of the duration.
// MAGIC   * You may want to execute it several times to get a decent average.
// MAGIC 
// MAGIC Repeat the exercise with
// MAGIC * 1 partition
// MAGIC * 7 partitions
// MAGIC * 9 partitions
// MAGIC * 16 partitions
// MAGIC * 24 partitions
// MAGIC * 96 partitions
// MAGIC * 200 partitions
// MAGIC * 4000 partitions
// MAGIC 
// MAGIC Go back and replace `repartition(n)` with `coalesce(n)` using:
// MAGIC * 6 partitions
// MAGIC * 5 partitions
// MAGIC * 4 partitions
// MAGIC * 3 partitions
// MAGIC * 2 partitions
// MAGIC * 1 partitions
// MAGIC 
// MAGIC ** *Note:* ** *Our data isn't large enough to see big differences with small partitions.*<br/>*However, we can see the flip side of this, the effect of small data with many partitions.*

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Getting Started
// MAGIC 
// MAGIC Run the following cell to configure our "classroom."

// COMMAND ----------

// MAGIC %run "../Includes/Classroom Setup"

// COMMAND ----------

// MAGIC %run "../Includes/Utility-Methods"

// COMMAND ----------

val parquetDir = "/mnt/training/wikipedia/pageviews/pageviews_by_second.parquet"

val df = spark.read
  .parquet(parquetDir)
  .repartition(8)
  // .coalesce(6)

df.count()
printRecordsPerPartition(df)


// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>