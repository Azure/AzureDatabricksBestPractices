# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://cdn2.hubspot.net/hubfs/438089/docs/training/dblearning-banner.png" alt="Databricks Learning" width="555" height="64">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Instructions
# MAGIC 
# MAGIC In this exercise, we're doing ETL on a file containing details about 40 new products  we are adding to our online marketplace. The file contains data including:
# MAGIC 
# MAGIC * product_id
# MAGIC * category
# MAGIC * brand
# MAGIC * model
# MAGIC * price
# MAGIC * processor
# MAGIC * size
# MAGIC * display
# MAGIC 
# MAGIC But, as is unfortunately common in raw data, this particular dataset contains a lot of escape characters such as `O&#39;Keeffe&#39;s`
# MAGIC 
# MAGIC The file's path is: `dbfs:/mnt/training-msft/initech/Products.parquet`
# MAGIC 
# MAGIC Your job is to remove the escape characters. The specific requirements of your job are:
# MAGIC 
# MAGIC * Remove escape characters 
# MAGIC * Convert to lower case
# MAGIC * Write the result as a Parquet file to `dbfs:/mnt/training-msft/class-files/retail.parquet/`

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Hints
# MAGIC 
# MAGIC * Use the <a href="http://spark.apache.org/docs/latest/api/python/index.html" target="_blank">API docs</a>. Specifically, you might find 
# MAGIC   <a href="http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrame" target="_blank">DataFrame</a> and
# MAGIC   <a href="http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#module-pyspark.sql.functions" target="_blank">functions</a> to be helpful.
# MAGIC * Let's use `dbutils.fs.ls()` or simply `%fs ls` to verify the path and take a look inside the Parquet directory.

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/training-msft/initech/Products.parquet

# COMMAND ----------

# MAGIC %md
# MAGIC We will start by using the `DataFrameReader` to load the parquet data into a DataFrame.

# COMMAND ----------

retailDF = spark.read.parquet("dbfs:/mnt/training-msft/initech/Products.parquet")

# COMMAND ----------

# MAGIC %md
# MAGIC The `display()` function gives us a clean, formatted look at the data in our DataFrame.

# COMMAND ----------

display(retailDF)

# COMMAND ----------

# MAGIC %md We can see imediately that our data is not formatted unformly and in a way that is ideal for ETL. For example, 
# MAGIC * some brand names contain special characters.
# MAGIC * the sizes for some of the columns contain extra `"`
# MAGIC * the prices for Laptops aren't rounded properly
# MAGIC 
# MAGIC Pretty ugly!
# MAGIC 
# MAGIC We have to do some cleanup before the data will be ready to load into our transactional Azure SQL Database and eventually our Azure Data Warehouse.

# COMMAND ----------

escapeCharsDF = retailDF.select("Brand", "size").filter(retailDF["Brand"].contains('Cube') | retailDF["size"].contains('\"'))
display(escapeCharsDF)

# COMMAND ----------

# MAGIC %md The first step is to check out the Spark built-in function library for functions that can do the task of un-escaping those sequences. Sadly, we see that there are no specialized functions that we can use to un-escape those ugly strings!
# MAGIC 
# MAGIC The solution is to make our own specialized functions. More formally, these are called <b>User Defined Functions (UDFs)</b>.

# COMMAND ----------

# MAGIC %md ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) User Defined Functions (UDF)
# MAGIC 
# MAGIC UDFs add more versatility to DataFrames.
# MAGIC 
# MAGIC However, they come with a cost...
# MAGIC * **UDFs cannot be optimized** by the Catalyst Optimizer - someday, maybe, but for now it has not insight to your code.
# MAGIC * The function **has to be serialized** and sent out to the executors - this is a one-time cost, but a cost just the same.
# MAGIC * If you are not careful, you could **serialize the whole world**.
# MAGIC * In the case of Python, there is even more over head - we have to **spin up a Python interpreter** on every Executor to run your Lambda.
# MAGIC 
# MAGIC Let's start with our function...

# COMMAND ----------

from HTMLParser import HTMLParser

def unEscape(text):
    h = HTMLParser()
    return h.unescape(h.unescape(text))

# COMMAND ----------

# MAGIC %md Notice how we apply the Python `unescape` function twice: this is because we have to apply the function to remove two levels of unescape i.e.
# MAGIC `HP&amp;reg;`
# MAGIC 
# MAGIC In order to get the function into a form that can be passed to executors for parallel processing, we wrap it in a `udf()`.
# MAGIC 
# MAGIC We give it a unique name, say `unescapeUDF`, so it can be used in a SQL statement.
# MAGIC 
# MAGIC Also, we use `alias` to rename the column to `Brand`

# COMMAND ----------

unescapeUDF = udf(unEscape)

# COMMAND ----------

from pyspark.sql.functions import col
unescapedCharsDF = retailDF.select(unescapeUDF(col("Brand").alias("brand"))) 

display(unescapedCharsDF)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Exercise 1
# MAGIC 
# MAGIC Round the floating point values from the `price` column i.e. `1949.989990234375` to `1949.99`. 
# MAGIC 
# MAGIC Hint: use the built-in function `round` from the `pyspark.sql.functions` library.

# COMMAND ----------

# solution
from pyspark.sql.functions import round
newDF = retailDF.select(round("Price", 2).alias("Price"))
                 
newDF.show(40)               

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Exercise 2
# MAGIC 
# MAGIC Finally, in the `size` column contains a number of strings that look like this `"12.0"" "`. The goal, instead, is to have them look more like `12.0"`.
# MAGIC 
# MAGIC Write & apply a UDFs to strip those extra `"` and also, strip any trailing whitespace.
# MAGIC 
# MAGIC Display your cleaned-up dataset.

# COMMAND ----------

# solution
from pyspark.sql.functions import udf

def charReplace(string):
    if string:
        return string.replace('"','').strip()
    return None
  
charReplaceUDF = udf(charReplace)

# COMMAND ----------

from pyspark.sql.functions import col

sizeDF = retailDF.select(charReplaceUDF(col("size")).alias("size"))

display(sizeDF)

# COMMAND ----------

retailDF.printSchema()

# COMMAND ----------

productDF = retailDF \
  .select("product_id", "category", unescapeUDF(col("brand")).alias("brand"), "model", round("Price",2).alias("Price"), "processor", charReplaceUDF(col("size")).alias("size"), "display")

# COMMAND ----------

display(productDF)

# COMMAND ----------

productDF.write.parquet("dbfs:/mnt/training-adl/retail.parquet/", mode="overwrite")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2018 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>