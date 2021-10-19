# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Partitioning & File Layout
# MAGIC 
# MAGIC In this notebook, we will cover how to partition your data for fast querying.
# MAGIC 
# MAGIC Spark Summit Europe 2017: [Lessons From the Field: Applying Best Practices to Your Apache Spark Applications](https://databricks.com/session/lessons-from-the-field-applying-best-practices-to-your-apache-spark-applications)

# COMMAND ----------

# MAGIC %run "./Includes/Classroom Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC Let's start with some CSV data in a single folder...

# COMMAND ----------

# MAGIC %fs ls /mnt/training/dataframes/people-10m.csv

# COMMAND ----------

df = spark.read.csv("/mnt/training/dataframes/people-10m.csv", header="true", inferSchema="true")

# COMMAND ----------

df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC What if we filter by birth year?

# COMMAND ----------

df.where("year(birthDate) between 1970 and 1980").count()

# COMMAND ----------

# MAGIC %md
# MAGIC Takes as much time or ***even more*** to count filtered vs whole dataset, why? Look at the query plan to understand.
# MAGIC 
# MAGIC So let's try with a partitioned version instead...

# COMMAND ----------

# MAGIC %fs ls /mnt/training/dataframes/people-10m-partitioned.csv

# COMMAND ----------

df_by_year = spark.read.csv("/mnt/training/dataframes/people-10m-partitioned.csv", header="true", inferSchema="true")

# COMMAND ----------

df_by_year.where("birthYear between 1970 and 1980").count()

# COMMAND ----------

# MAGIC %md
# MAGIC That's good, but let's examine the query plan...
# MAGIC 
# MAGIC Why such small reads with 8 tasks?

# COMMAND ----------

df_by_year.where("birthYear between 1970 and 1980").explain()

# COMMAND ----------

# MAGIC %fs ls /mnt/training/dataframes/people-10m-partitioned.csv/birthYear=1978

# COMMAND ----------

# MAGIC %md
# MAGIC We have 8 small files per partition folder, very inefficient especially on cloud storage!
# MAGIC 
# MAGIC **Question**: Why do we need `repartition` AND `partitionBy`?

# COMMAND ----------

import re
tableName = re.sub('\W', '', username) + "people_by_year_optimized"

(df_by_year.repartition("birthYear")
  .write.partitionBy("birthYear")
  .format("parquet")
  .mode("overwrite")
  .option("path", userhome + "/tuning/people_by_year.parquet")
  .saveAsTable(tableName))

# COMMAND ----------

display(dbutils.fs.ls(userhome + "/tuning/people_by_year.parquet/birthYear=1979"))

# COMMAND ----------

spark.read.table(tableName).where("birthYear between 1970 AND 1980").count()

# COMMAND ----------

# MAGIC %md
# MAGIC Now, we're reading in a single larger file per partition!

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>