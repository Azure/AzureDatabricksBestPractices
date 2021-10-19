# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Intro To DataFrames, Lab #4
# MAGIC ## What-The-Monday?

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Instructions
# MAGIC 
# MAGIC The datasource for this lab is located on the DBFS at **/mnt/training/wikipedia/pageviews/pageviews_by_second.parquet**.
# MAGIC 
# MAGIC As we saw in the previous notebook...
# MAGIC * There are a lot more requests for sites on Monday than on any other day of the week.
# MAGIC * The variance is **NOT** unique to the mobile or desktop site.
# MAGIC 
# MAGIC Your mission, should you choose to accept it, is to demonstrate conclusively why there are more requests on Monday than on any other day of the week.
# MAGIC 
# MAGIC Feel free to copy & paste from the previous notebook.

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Getting Started
# MAGIC 
# MAGIC Run the following cell to configure our "classroom."

# COMMAND ----------

# MAGIC %run "../Includes/Classroom Setup"

# COMMAND ----------

# ANSWER

from pyspark.sql.functions import *
from pyspark.sql.types import *

# I've already gone through the exercise to determine
# how many partitions I want and in this case it is...
partitions = 8

# Make sure wide operations don't repartition to 200
spark.conf.set("spark.sql.shuffle.partitions", str(partitions))

# The directory containing our parquet files.
parquetFile = "/mnt/training/wikipedia/pageviews/pageviews_by_second.parquet/"

# Create our initial DataFrame. We can let it infer the 
# schema because the cost for parquet files is really low.
pageviewsDF = (spark.read
  .option("inferSchema", "true")                # The default, but not costly w/Parquet
  .parquet(parquetFile)                         # Read the data in
  .repartition(partitions)                      # From 7 >>> 8 partitions
  .withColumnRenamed("timestamp", "capturedAt") # rename and convert to timestamp datatype
  .withColumn("capturedAt", unix_timestamp( col("capturedAt"), "yyyy-MM-dd'T'HH:mm:ss").cast("timestamp") )
  .orderBy( col("capturedAt"), col("site") )    # sort our records
  .cache()                                      # Cache the expensive operation
)
# materialize the cache
pageviewsDF.count()

# COMMAND ----------

# ANSWER

recordCount = pageviewsDF.count()
distinctCount = pageviewsDF.distinct().count()

print("The DataFrame contains {} records. Number of distinct records: {}\n{} records are duplicated.".format(
       recordCount,
       distinctCount,
       recordCount - distinctCount
      ))

# COMMAND ----------

# ANSWER
from pyspark.sql.functions import col

# Let's see the duplicates
display(pageviewsDF.groupBy(col("capturedAt"), col("site")).count().orderBy(col("count").desc(), col("capturedAt")))

# COMMAND ----------

# ANSWER

answerDF = (pageviewsDF
  .withColumn("day_of_year", dayofyear(col("capturedAt")))
  .filter("day_of_year = 110")
  .orderBy("capturedAt", "site")
)
display(answerDF)


# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>