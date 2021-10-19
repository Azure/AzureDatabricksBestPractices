# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Working with Numpy
# MAGIC 
# MAGIC 0. Take the square root of the values in a Spark DataFrame using numpy (regular UDF).
# MAGIC 0. Take the square root of the values in a Spark DataFrame using numpy (vectorized UDF).
# MAGIC 0. Take the square root of the values in a Spark DataFrame using the built-in `sqrt` method.
# MAGIC 
# MAGIC Compare the time performance of these three approaches.
# MAGIC 
# MAGIC **HINT**: Be careful with the return data type from numpy. You might have to do some type casting...

# COMMAND ----------

df = spark.range(1, 100000)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Regular UDF

# COMMAND ----------

# ANSWER
import numpy as np
from pyspark.sql.functions import *

@udf("double")
def sqrt_func(x):
  return float(np.sqrt(x))

%timeit -r1 -n1 df.withColumn('v', sqrt_func(df.id)).agg(count(col('v'))).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Vectorized UDF

# COMMAND ----------

# ANSWER
from pyspark.sql.functions import pandas_udf
@pandas_udf('double')
def sqrt_func(x):
  return np.sqrt(x)

%timeit -r1 -n1 df.withColumn('v', sqrt_func(df.id)).agg(count(col('v'))).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Built-in Method

# COMMAND ----------

# ANSWER

%timeit -r1 -n1 df.withColumn('v', sqrt(df.id)).agg(count(col('v'))).show()


# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>