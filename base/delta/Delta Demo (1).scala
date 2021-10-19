// Databricks notebook source
// MAGIC %md ![stream](https://2s7gjr373w3x22jf92z99mgm5w-wpengine.netdna-ssl.com/wp-content/uploads/2017/10/databricks_delta_slide.png)

// COMMAND ----------

// MAGIC %md ![stream](https://storage.googleapis.com/cdn.thenewstack.io/media/2016/02/Spark-Structured-Streaming.png)

// COMMAND ----------

// MAGIC %md
// MAGIC ####What is Databricks Delta and what does it do? 
// MAGIC - A transactional layer on top of parquet files stored in blob storage that’s unified with Spark
// MAGIC - Enables massive scale from blob storage
// MAGIC - Data correctness is a first class citizen
// MAGIC - Transactions and the transaction log allow for easy handling of changing data
// MAGIC - Streaming and batch to and from same table
// MAGIC - UPSERTs
// MAGIC - File compaction jobs and optimization
// MAGIC - Read optimizations [biggest gap]
// MAGIC 
// MAGIC ####What is it useful for?
// MAGIC 
// MAGIC - Scale of a Datalake (S3, Azure Blob Store)
// MAGIC - Performance Reliability of Datawarehouse 
// MAGIC - Low latency Streaming Updates 

// COMMAND ----------

import org.apache.spark.sql.functions.lit
val df = spark.range(500).repartition(2).withColumn("some_string", lit("Delta Rocks!"))

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC # Create Delta location

// COMMAND ----------

df.write.format("delta").mode("overwrite").save("/mnt/ved-demo/delta/")

// COMMAND ----------

// MAGIC %fs ls /mnt/ved-demo/delta/

// COMMAND ----------

val read_in_delta = spark.read.format("delta").load("/mnt/ved-demo/delta/")

// COMMAND ----------

display(read_in_delta)

// COMMAND ----------

df.createOrReplaceTempView("new_table")

// COMMAND ----------

// MAGIC %sql select count(*) from new_table

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC # Now Upsert New Data

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC MERGE INTO tahoe.`/mnt/ved-demo/delta/` AS target
// MAGIC USING new_table
// MAGIC ON new_table.id == target.id AND new_table.id % 2 == 0
// MAGIC when matched then
// MAGIC   UPDATE SET target.id = new_table.id, some_string = "Delta is even better than I thought"
// MAGIC WHEN NOT MATCHED 
// MAGIC   THEN INSERT (target.id, some_string) VALUES (70000, "not gonna happen")

// COMMAND ----------

// MAGIC %sql select * from new_table

// COMMAND ----------

display(read_in_delta)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Now query times are **SLOW**.
// MAGIC Let's optimize this.

// COMMAND ----------

spark.sql("""
OPTIMIZE "/mnt/ved-demo/delta/"
""")

// COMMAND ----------

// MAGIC %md #### Full table scan earlier took >8 secs. Now let's try a full table scan on a Delta table

// COMMAND ----------

display(read_in_delta)

// COMMAND ----------

// MAGIC %md #### Delta table scan was much quicker! <1 sec

// COMMAND ----------

// MAGIC %fs ls "/mnt/ved-demo/delta/"

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC In Databricks Runtime >= v3.5, we can vacuum the old files up. Here we'll do this immediately. This will reveal _only_ the valid files in our table.

// COMMAND ----------

spark.sql("""
VACUUM "/mnt/ved-demo/delta/" 
""")

// COMMAND ----------

// MAGIC %sql DESCRIBE HISTORY delta.`/mnt/ved-demo/delta/`

// COMMAND ----------

// MAGIC %fs ls "/mnt/ved-demo/delta/_delta_log"

// COMMAND ----------

// MAGIC %fs head /mnt/ved-demo/delta/_delta_log/00000000000000000000.json

// COMMAND ----------

