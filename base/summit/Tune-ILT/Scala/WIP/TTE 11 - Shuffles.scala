// Databricks notebook source
// MAGIC 
// MAGIC %md-sandbox
// MAGIC 
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC # Tuning Shuffling
// MAGIC 
// MAGIC ## When does shuffle come into play?
// MAGIC - When we transfer data from one stage to the next
// MAGIC - May cause repartitioning
// MAGIC - Possible network traffic (very expensive)
// MAGIC 
// MAGIC ## Wide vs Narrow transformations
// MAGIC 
// MAGIC ### Narrow transformations
// MAGIC 
// MAGIC <img src="https://files.training.databricks.com/images/105/transformations-narrow.png" alt="Narrow Transformations" style="height: 300px"/>
// MAGIC 
// MAGIC Narrow transformations can be pipelined together into one stage
// MAGIC 
// MAGIC ### Wide transformations
// MAGIC 
// MAGIC <img src="https://files.training.databricks.com/images/105/transformations-wide.png" alt="Wide Transformations" style="height: 300px"/>
// MAGIC 
// MAGIC - Wide transformations cause shuffling as they introduce stage splits.
// MAGIC - Some wide transformations we can perform on a DataFrame: `distinct`, `cube`, `join`, `orderBy`, `groupBy`.

// COMMAND ----------

var df = (spark
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("sep", ":")
      .csv("/mnt/training/dataframes/people-with-header-10m.txt"))

// COMMAND ----------

import org.apache.spark.sql.functions._
display(df
        .withColumn("birthYear", year($"birthDate"))
        .groupBy("birthYear")
        .count()
        .orderBy(desc("count"))
       )

// COMMAND ----------

// MAGIC %md
// MAGIC Let's go ahead and change the `spark.sql.shuffle.partitions`.

// COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", "8")

display(df
        .withColumn("birthYear", year($"birthDate"))
        .groupBy("birthYear")
        .count()
        .orderBy(desc("count"))
       )

// COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", "200") // reset

// COMMAND ----------

// MAGIC %md
// MAGIC OK, that works well when we have a simple query but what about large queries with multiple stages of shuffles??
// MAGIC 
// MAGIC That gets more complicated... Using `spark.sql.shuffle.partitions` it's 1 value for the WHOLE query. What if we're heavily reducing data or exploding data??
// MAGIC 
// MAGIC Look at `Shuffle Write` and `Shuffle Read` metrics in Stages UI as well as `Memory Usage` and `GC Time` of tasks. You may need to increase shuffle size for stability of most expensive stage at the cost of some speed for later stages.
// MAGIC 
// MAGIC Another interesting config is Adaptive Execution...
// MAGIC 
// MAGIC Dynamically sets shuffle partitions based on previous stage map output size!
// MAGIC 
// MAGIC *WARNING* Still in development!

// COMMAND ----------

spark.conf.set("spark.sql.adaptive.enabled", "true")

display(df
        .withColumn("birthYear", year($"birthDate"))
        .groupBy("birthYear")
        .count()
        .orderBy(desc("count"))
       )

// COMMAND ----------

spark.conf.set("spark.sql.adaptive.enabled", "false")

// COMMAND ----------

// MAGIC %md
// MAGIC Remember to set shuffle partitions for streaming! Remember, Structured Streaming is using same framework as batch queries, so shuffle partitions still apply.
// MAGIC 
// MAGIC 200 shuffle partitions will most likely be too much for streaming aggregations!

// COMMAND ----------

var logsDF = (spark.readStream
    .format("socket")
    .option("host", "server1.databricks.training")
    .option("port", 9001)
    .load()
)

// COMMAND ----------

var cleanDF = (logsDF
  .withColumn("ts_string", col("value").substr(2, 23))
  .withColumn("epoc", unix_timestamp($"ts_string", "yyyy/MM/dd HH:mm:ss.SSS"))
  .withColumn("capturedAt", col("epoc").cast("timestamp"))
  .withColumn("logData", regexp_extract($"value", """^.*\]\s+(.*)$""", 1))
)

// COMMAND ----------

var messagesPerSecond = (cleanDF
  .select("capturedAt")
  .groupBy( window(col("capturedAt"), "1 second"))
  .count()
  .orderBy("window.start")
)

// COMMAND ----------

display(
  messagesPerSecond.select(col("window.start").alias("start"), 
                           col("window.end").alias("end"), 
                           col("count") )
)

// COMMAND ----------

// play around with shuffle partitions to see what works best!

spark.conf.set("spark.sql.shuffle.partitions", 4)

display(
  messagesPerSecond.select(col("window.start").alias("start"), 
                           col("window.end").alias("end"), 
                           col("count") )
)

// COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", 200) // reset to default

// COMMAND ----------

// MAGIC %md
// MAGIC Another issue to consider with shuffles is skew.
// MAGIC 
// MAGIC If you notice a single task taking longer than the rest, most likely it's due to skew.
// MAGIC 
// MAGIC * Consider fields like "account id" (e.g. for financial services, most active accounts)
// MAGIC 
// MAGIC * Watch out for NULL values (when doing aggregations or joins)

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>