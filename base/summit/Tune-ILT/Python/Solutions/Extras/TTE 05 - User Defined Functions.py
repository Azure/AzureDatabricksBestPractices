# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## User Defined Functions
# MAGIC 
# MAGIC In this notebook we compare the performance of UDFs, Vectorized UDFs, and built-in methods.
# MAGIC 
# MAGIC Let's start by generating some dummy data.

# COMMAND ----------

# MAGIC %python
# MAGIC from pyspark.sql.types import *
# MAGIC from pyspark.sql.functions import col, count, rand, collect_list, explode, struct, count, pandas_udf
# MAGIC 
# MAGIC df = (spark
# MAGIC       .range(0, 10 * 1000 * 1000)
# MAGIC       .withColumn('id', (col('id') / 1000).cast('integer'))
# MAGIC       .withColumn('v', rand()))
# MAGIC 
# MAGIC df.cache()
# MAGIC df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC # Incrementing a column by one
# MAGIC 
# MAGIC Let's start off with a simple example of adding one to each value in our DataFrame.

# COMMAND ----------

# MAGIC %md
# MAGIC ## pySpark UDF

# COMMAND ----------

# MAGIC %python
# MAGIC @udf("double")
# MAGIC def plus_one(v):
# MAGIC     return v + 1
# MAGIC 
# MAGIC %timeit -n1 -r1 df.withColumn('v', plus_one(df.v)).agg(count(col('v'))).show()

# COMMAND ----------

# MAGIC %md
# MAGIC Alternate Syntax (can also use in the SQL namespace now)

# COMMAND ----------

# MAGIC %python
# MAGIC from pyspark.sql.types import DoubleType
# MAGIC 
# MAGIC def plus_one(v):
# MAGIC     return v + 1
# MAGIC   
# MAGIC spark.udf.register("plus_one_udf", plus_one, DoubleType())
# MAGIC 
# MAGIC %timeit -n1 -r1 df.selectExpr("id", "plus_one_udf(v) as v").agg(count(col('v'))).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scala UDF
# MAGIC 
# MAGIC Yikes! That took awhile to add 1 to each value. Let's see how long this takes with a Scala UDF.

# COMMAND ----------

# MAGIC %python
# MAGIC df.createOrReplaceTempView("df")

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql.functions._
# MAGIC 
# MAGIC val df = spark.table("df")
# MAGIC 
# MAGIC def plusOne: (Double => Double) = { v => v+1 }
# MAGIC val plus_one = udf(plusOne)

# COMMAND ----------

# MAGIC %scala
# MAGIC df.withColumn("v", plus_one($"v"))
# MAGIC   .agg(count(col("v")))
# MAGIC   .show()

# COMMAND ----------

# MAGIC %md
# MAGIC Wow! That Scala UDF was a lot faster. However, as of Spark 2.3, there are Vectorized UDFs available in Python to help speed up the computation.
# MAGIC 
# MAGIC * [Blog post](https://databricks.com/blog/2017/10/30/introducing-vectorized-udfs-for-pyspark.html)
# MAGIC * [Documentation](https://spark.apache.org/docs/latest/sql-programming-guide.html#pyspark-usage-guide-for-pandas-with-apache-arrow)
# MAGIC 
# MAGIC ![Benchmark](https://databricks.com/wp-content/uploads/2017/10/image1-4.png)
# MAGIC 
# MAGIC Vectorized UDFs utilize Apache Arrow to speed up computation. Let's see how that helps improve our processing time.

# COMMAND ----------

# MAGIC %md
# MAGIC [Apache Arrow](https://arrow.apache.org/), is an in-memory columnar data format that is used in Spark to efficiently transfer data between JVM and Python processes. See more [here](https://spark.apache.org/docs/latest/sql-pyspark-pandas-with-arrow.html).
# MAGIC 
# MAGIC Let's take a look at how long it takes to convert a Spark DataFrame to Pandas with and without Apache Arrow enabled.

# COMMAND ----------

# MAGIC %python
# MAGIC spark.conf.set("spark.sql.execution.arrow.enabled", "true")
# MAGIC 
# MAGIC %timeit -n1 -r1 df.toPandas()

# COMMAND ----------

# MAGIC %python
# MAGIC spark.conf.set("spark.sql.execution.arrow.enabled", "false")
# MAGIC 
# MAGIC %timeit -n1 -r1 df.toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Vectorized UDF

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC @pandas_udf('double')
# MAGIC def vectorized_plus_one(v):
# MAGIC     return v + 1
# MAGIC 
# MAGIC %timeit -n1 -r1 df.withColumn('v', vectorized_plus_one(df.v)).agg(count(col('v'))).show()

# COMMAND ----------

# MAGIC %md
# MAGIC Alright! Still not as great as the Scala UDF, but at least it's better than the regular Python UDF!
# MAGIC 
# MAGIC Here's some altnernate syntax for the Pandas UDF.

# COMMAND ----------

# MAGIC %python
# MAGIC from pyspark.sql.functions import pandas_udf
# MAGIC 
# MAGIC def vectorized_plus_one(v):
# MAGIC     return v + 1
# MAGIC 
# MAGIC vectorized_plus_one_udf = pandas_udf(vectorized_plus_one, "double")
# MAGIC 
# MAGIC %timeit -n1 -r1 df.withColumn('v', vectorized_plus_one_udf(df.v)).agg(count(col('v'))).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Built-in Method
# MAGIC 
# MAGIC Let's compare the performance of the UDFs with using the built-in method.

# COMMAND ----------

# MAGIC %python
# MAGIC from pyspark.sql.functions import lit
# MAGIC 
# MAGIC %timeit -n1 -r1 df.withColumn('v', df.v + lit(1)).agg(count(col('v'))).show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Computing substract mean
# MAGIC 
# MAGIC Up above, we were working with Scalar return types. Now we can use grouped UDFs.

# COMMAND ----------

# MAGIC %md
# MAGIC ## pySpark UDF

# COMMAND ----------

# MAGIC %python
# MAGIC from pyspark.sql import Row
# MAGIC 
# MAGIC @udf(ArrayType(df.schema))
# MAGIC def substract_mean(rows):
# MAGIC   vs = pd.Series([r.v for r in rows])
# MAGIC   vs = vs - vs.mean()
# MAGIC   return [Row(id=rows[i]['id'], v=float(vs[i])) for i in range(len(rows))]
# MAGIC   
# MAGIC %timeit -n1 -r1 (df.groupby('id').agg(collect_list(struct(df['id'], df['v'])).alias('rows')).withColumn('new_rows', substract_mean(col('rows'))).withColumn('new_row', explode(col('new_rows'))).withColumn('id', col('new_row.id')).withColumn('v', col('new_row.v')).agg(count(col('v'))).show())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Vectorized UDF

# COMMAND ----------

# MAGIC %python
# MAGIC from pyspark.sql.functions import PandasUDFType
# MAGIC 
# MAGIC @pandas_udf(df.schema, PandasUDFType.GROUPED_MAP)
# MAGIC def vectorized_subtract_mean(pdf):
# MAGIC 	return pdf.assign(v=pdf.v - pdf.v.mean())
# MAGIC 
# MAGIC %timeit -n1 -r1 df.groupby('id').apply(vectorized_subtract_mean).agg(count(col('v'))).show()

# COMMAND ----------

# MAGIC %md
# MAGIC Wow! The Vectorized UDF is significantly faster!!

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>