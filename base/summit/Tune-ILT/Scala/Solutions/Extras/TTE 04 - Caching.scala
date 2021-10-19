// Databricks notebook source
// MAGIC 
// MAGIC %md-sandbox
// MAGIC 
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC ### Caching
// MAGIC 
// MAGIC Spark provides built-in memory caching, useful when you plan to iterate over a dataset multiple times (e.g. ML training).
// MAGIC 
// MAGIC *NOTE* This is different than DBIO Caching (covered later)
// MAGIC 
// MAGIC Let's take a look at the API docs for
// MAGIC * `Dataset.persist(..)` if using Scala
// MAGIC * `DataFrame.persist(..)` if using Python
// MAGIC 
// MAGIC Other Storage Levels:
// MAGIC * DISK_ONLY
// MAGIC * DISK_ONLY_2
// MAGIC * MEMORY_AND_DISK
// MAGIC * MEMORY_AND_DISK_2
// MAGIC * MEMORY_AND_DISK_SER
// MAGIC * MEMORY_AND_DISK_SER_2
// MAGIC * MEMORY_ONLY
// MAGIC * MEMORY_ONLY_2
// MAGIC * MEMORY_ONLY_SER
// MAGIC * MEMORY_ONLY_SER_2
// MAGIC * OFF_HEAP
// MAGIC 
// MAGIC ** *Note:* ** *The default storage level for...*
// MAGIC * *RDDs are **MEMORY_ONLY**.*
// MAGIC * *DataFrames are **MEMORY_AND_DISK**.* 
// MAGIC * *Streaming is **MEMORY_AND_DISK_2**.*

// COMMAND ----------

// MAGIC %run "./Includes/Classroom Setup"

// COMMAND ----------

val df = spark
         .read
         .option("header", "true")
         .option("sep", ":")
         .csv("mnt/training/dataframes/people-with-header-10m.txt")

// COMMAND ----------

df.cache() // cache is a lazy operation

// COMMAND ----------

display(df) // action

// COMMAND ----------

// MAGIC %md
// MAGIC `display` causes an action so cache is materialized but only for 1 partition!

// COMMAND ----------

df.count // cache for whole dataset

// COMMAND ----------

// MAGIC %md
// MAGIC Don't ignore data types. How big is the file compared to in-memory?

// COMMAND ----------

// MAGIC %fs ls /mnt/training/dataframes/people-with-header-10m.txt

// COMMAND ----------

df.unpersist()

// COMMAND ----------

// MAGIC %md
// MAGIC It's bigger in memory than on disk! Why? Due to Java string object storage.
// MAGIC 
// MAGIC <img src="https://files.training.databricks.com/images/tuning/java-string.png" alt="Java String Memory allocation"/><br/>
// MAGIC 
// MAGIC 
// MAGIC - A regular 4 byte string would end up taking 48 bytes. 
// MAGIC - The diagram shows how the 40 bytes are allocated and we also need to round up byte usage to be divisible of 8 due to JVM padding. 
// MAGIC - This is a very bloated representation knowing that of these 48 bytes, we're actually after only 4. 
// MAGIC 
// MAGIC For an excellent presentation on Project Tungsten, see this [presentation](https://www.youtube.com/watch?v=5ajs8EIPWGI&t=1s) from Databricks' engineer, Josh Rosen.
// MAGIC 
// MAGIC Let's try with `inferSchema` instead...

// COMMAND ----------

val df2 = spark
         .read
         .option("header", "true")
         .option("sep", ":")
         .option("inferSchema", "true")
         .csv("/mnt/training/dataframes/people-with-header-10m.txt")

df2.cache().count

// COMMAND ----------

// MAGIC %md
// MAGIC Only takes up ~450MB vs ~660MB...

// COMMAND ----------

df2.unpersist() // we have to unpersist here otherwise the next cell won't re-cache the same dataset

// COMMAND ----------

// MAGIC %md
// MAGIC Let's use MEMORY_AND_DISK_SER.

// COMMAND ----------

import org.apache.spark.storage.StorageLevel

val df3 = spark
         .read
         .option("header", "true")
         .option("sep", ":")
         .option("inferSchema", "true")
         .csv("/mnt/training/dataframes/people-with-header-10m.txt")

df3.persist(StorageLevel.MEMORY_AND_DISK_SER).count

// COMMAND ----------

// MAGIC %md
// MAGIC Now only ~336MB, almost half of what we started with on storage! Let's compare that to an RDD.

// COMMAND ----------

val myRDD = df3.rdd
myRDD.setName("myRDD").cache().count

// COMMAND ----------

// MAGIC %md
// MAGIC Wow! The RDD took up significantly less space. Let's unpersist both of them and see how we can cache DataFrames with cleaner names.

// COMMAND ----------

df3.unpersist
myRDD.unpersist

df.createOrReplaceTempView("df")

// COMMAND ----------

// MAGIC %sql
// MAGIC CACHE TABLE df

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>