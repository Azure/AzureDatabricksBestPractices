# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC #Reusability #2
# MAGIC 
# MAGIC **Dataset:**
# MAGIC * This is synthetic data generated specifically for these exercises
# MAGIC * Each year's data is roughly the same with some variation for market growth
# MAGIC * We are looking at retail purchases from the top N retailers
# MAGIC 
# MAGIC **Technical Accomplishments:**
# MAGIC * Encapsulate a common set of transformation robust enough to handle subtle variations in structure
# MAGIC * Learn how to diagnose a new performance problem

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Getting Started
# MAGIC 
# MAGIC Run the following cell to configure our "classroom."

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %run "./Includes/Initialize-Labs"

# COMMAND ----------

# MAGIC %run "./Includes/Utility-Methods"

# COMMAND ----------

# We know these numbers from the previous lab
expectedFast14 = "413199362.16"
expectedFast17 = "492940869.09"

expectedSlow14 = "4132674423.72"
expectedSlow17 = "4923864114.44"

def validateSchema(df):
  assert __builtin__.len(df.columns) == 3, "Expected three and only three columns"

  schema = str(df.schema)
  assert "year,IntegerType" in schema, "Missing the year column"
  assert "day,IntegerType" in schema, "Missing the day column"
  assert "amount,DecimalType" in schema, "Missing the amount column"
  
  expected = 61
  total = df.count()

  assert total == expected, "Expected {} records, found {}".format(expected, total)

def validateSum(df, expected):
  from pyspark.sql.functions import sum
  total = df.select( sum(col("amount")).cast("decimal(20,2)").cast("string").alias("total") ).first()[0]
  assert total == expected, "Expected the final sum to be {} but found {}".format(expected, total)

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) The Introduction
# MAGIC 
# MAGIC If you recall from our previous lab, we induced a performance bug.
# MAGIC 
# MAGIC In this lab we are going to alter our function to address said bug.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load Data for 2014 & 2017

# COMMAND ----------

from pyspark.sql.functions import *

spark.catalog.clearCache()

path2014 = "/mnt/training/global-sales/transactions/2014.parquet"
trx2014DF = spark.read.parquet(path2014)

path2017 = "/mnt/training/global-sales/transactions/2017.parquet"
trx2017DF = spark.read.parquet(path2017)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load our "faster" dataset for us to test with

# COMMAND ----------

fastPath14 = "/mnt/training/global-sales/solutions/2014-fast.parquet"
fast2014DF = spark.read.parquet(fastPath14)

fastPath17 = "/mnt/training/global-sales/solutions/2017-fast.parquet"
fast2017DF = spark.read.parquet(fastPath17)

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Challenge #1
# MAGIC 
# MAGIC **Refactor the function to optionally add the year and month columns:**
# MAGIC * The previous version of our function simply chained a bunch of transformations together
# MAGIC * Because we now have conditional logic, we will need multiple **DataFrames** references
# MAGIC * Start by assigning **sourceDF** to the variable **df**
# MAGIC * Create a conditional expression that checks to see if **df**'s **columns** contain the value "year"
# MAGIC   * If the column DOESN'T exist, add the transformation to extract the **year** from **transacted_at**
# MAGIC   * Assign the result of that transformation back to **df**
# MAGIC * Create a conditional expression that checks to see if **df**'s **columns** contains the value "month"
# MAGIC   * If the column DOESN'T exist, add the transformation to extract the **month** from **transacted_at**
# MAGIC   * Assign the result of that transformation back to **df**
# MAGIC * Update the function to apply the remaining transformations
# MAGIC * Return **df**
# MAGIC 
# MAGIC For reference, our original function is included below:

# COMMAND ----------

#################################
## Included only for reference ##
#################################

def sumByDayOfYear(sourceDF):
  return (sourceDF.withColumn("month", month(col("transacted_at")))
                  .withColumn("year", year(col("transacted_at")))
                  .filter(col("month") >= 11)
                  .withColumn("day", dayofyear(col("transacted_at")))
                  .groupBy("year", "day").sum("amount")
                  .withColumnRenamed("sum(amount)", "amount")
                  .orderBy("day")
                  .persist()
  )

# COMMAND ----------

# MAGIC %md
# MAGIC Create the new version of this function in the subsequent cell:

# COMMAND ----------

# ANSWER

def sumByDayOfYear(sourceDF):
  
  df = sourceDF
  
  if ("year" not in sourceDF.columns):
    df = df.withColumn("year", year(col("transacted_at")))
  
  if ("month" not in sourceDF.columns):
    df = df.withColumn("month", month(col("transacted_at")))
  
  outDF = (df.filter(col("month") >= 11)
    .withColumn("day", dayofyear(col("transacted_at")))
    .groupBy("year", "day").sum("amount")
    .withColumnRenamed("sum(amount)", "amount")
    .orderBy("day")
    .persist()
   )
  
  return outDF

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Validate Your Answer to #1
# MAGIC 
# MAGIC We can test your work by executing your method with the "fast" version of the 2014 & 2017 data
# MAGIC 
# MAGIC Run the cell below to validate your answer:

# COMMAND ----------

# MAGIC %md
# MAGIC ### Do a quick test of our function

# COMMAND ----------

holidayFast14DF = sumByDayOfYear(fast2014DF)
validateSchema(holidayFast14DF)
validateSum(holidayFast14DF, expectedFast14)

# COMMAND ----------

holidayFast17DF = sumByDayOfYear(fast2017DF)
validateSchema(holidayFast17DF)
validateSum(holidayFast17DF, expectedFast17)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Once we know the quick test passes, run the full tests
# MAGIC 
# MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** Besides waiting for the query to complete there is <br/>
# MAGIC  another way to confirm that the query is behaving properly.

# COMMAND ----------

holidaySlow14DF = sumByDayOfYear(trx2014DF)
validateSchema(holidaySlow14DF)
validateSum(holidaySlow14DF, expectedSlow14)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC The real test is in the performance - if we fixed the bug, we should be looking at the faster execution time.
# MAGIC 
# MAGIC And of course the 2017 data should be just as slow as before:

# COMMAND ----------

holidaySlow17DF = sumByDayOfYear(trx2017DF)
validateSchema(holidaySlow17DF)
validateSum(holidaySlow17DF, expectedSlow17)


# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>