# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Intro to Spark
# MAGIC 
# MAGIC Before we get started with Machine Learning and Deep Learning, let's make sure we all understand how to use Databricks and Spark.
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lesson you:<br>
# MAGIC  - Create a Spark DataFrame
# MAGIC  - Analyze the Spark UI
# MAGIC  - Cache data
# MAGIC  - Change Spark default configurations to speed up queries

# COMMAND ----------

# MAGIC %md
# MAGIC Let's start off with running some code on our driver, such as `x = 1`.

# COMMAND ----------

# ANSWER
x = 1

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spark DataFrame
# MAGIC 
# MAGIC Great! Now let's start with a distributed Spark DataFrame.

# COMMAND ----------

from pyspark.sql.functions import col, rand

df = (spark.range(1, 1000000)
      .withColumn('id', (col('id') / 1000).cast('integer'))
      .withColumn('v', rand(seed=1)))

# COMMAND ----------

# MAGIC %md
# MAGIC Why were no Spark jobs kicked off above? Well, we didn't have to actually "touch" our data, so Spark didn't need to execute anything across the cluster.

# COMMAND ----------

display(df.sample(.001))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Count
# MAGIC 
# MAGIC Let's see how many records we have.

# COMMAND ----------

df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spark UI
# MAGIC 
# MAGIC Open up the Spark UI - what are the shuffle read and shuffle write fields? The command below should give you a clue.

# COMMAND ----------

df.rdd.getNumPartitions()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cache
# MAGIC 
# MAGIC For repeated access, it will be much faster if we cache our data.

# COMMAND ----------

df.cache().count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Re-run Count
# MAGIC 
# MAGIC Wow! Look at how much faster it is now!

# COMMAND ----------

df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Group By
# MAGIC 
# MAGIC Let's get the count of the # records per id.

# COMMAND ----------

df.groupBy("id").count().collect()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Shuffle Partitions
# MAGIC 
# MAGIC Where did that 200/200 come from? Well, that is due to [Spark SQL Shuffle Partitions](https://spark.apache.org/docs/latest/sql-performance-tuning.html). It configures the number of partitions to use when shuffling data for joins or aggregations (such as our group by).
# MAGIC 
# MAGIC **Narrow Transformations**
# MAGIC 
# MAGIC <img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/transformations-narrow.png" alt="Narrow Transformations" style="height:300px"/>
# MAGIC 
# MAGIC **Wide Transformations**
# MAGIC 
# MAGIC <img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/transformations-wide.png" alt="Wide Transformations" style="height:300px"/>
# MAGIC 
# MAGIC For our small dataset, 200 is too large. Let's reduce it to 8.

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", "8")

df.groupBy("id").count().collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pandas
# MAGIC 
# MAGIC Let's convert our Spark DataFrame to a Pandas DataFrame.

# COMMAND ----------

df.toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apache Arrow
# MAGIC 
# MAGIC That was too slow - let's enable [Apache Arrow](https://spark.apache.org/docs/latest/sql-pyspark-pandas-with-arrow.html) for faster conversion from a Spark DataFrame to a Pandas DataFrame.

# COMMAND ----------

spark.conf.set("spark.sql.execution.arrow.enabled", "true")

df.toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Wrap-up
# MAGIC 
# MAGIC Alright! Now that you know the basics of Spark and some tuning tricks, let's get started!

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>