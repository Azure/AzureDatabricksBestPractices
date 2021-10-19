# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://cdn2.hubspot.net/hubfs/438089/docs/training/dblearning-banner.png" alt="Databricks Learning" width="555" height="64">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC #Introduction to Transformations and Actions
# MAGIC 
# MAGIC **Technical Accomplishments:**
# MAGIC * Develop familiarity with the `DataFrame` APIs
# MAGIC * Introduce transformations and actions
# MAGIC * Sharing/exporting notebooks
# MAGIC * Scheduling Jobs

# COMMAND ----------

# MAGIC %md
# MAGIC The data is located at `dbfs:/mnt/training-msft/initech/Products.csv`.

# COMMAND ----------

# MAGIC %fs ls /mnt/training-msft/initech/

# COMMAND ----------

csvDir = "dbfs:/mnt/training-msft/initech/Product.csv"

retailDF = (spark                    # Our SparkSession & Entry Point
           .read                     # Our DataFrameReader
           .option("header", "true")
           .option("inferSchema", "true")
           .csv(csvDir))           # Returns an instance of DataFrame

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Our Data
# MAGIC 
# MAGIC Let's continue by taking a look at the type of data we have. 
# MAGIC 
# MAGIC We can do this with the `printSchema()` command:

# COMMAND ----------

retailDF.printSchema()

# COMMAND ----------

retailDF.show(3)

# COMMAND ----------

# MAGIC %md
# MAGIC We should now be able to see that we have eight columns of data:
# MAGIC * **productCode** (*string*) Unique product identifier.
# MAGIC * **category** (*string*): The product is either a tablet or a laptop.
# MAGIC * **brand** (*string*): The name of the product's brand.
# MAGIC * **model** (*string*): The name of the product's model.
# MAGIC * **price** (*double*): Price of the product.
# MAGIC * **processor** (*string*): The type of processor the product uses.
# MAGIC * **size** (*string*): The size of the product in inches.
# MAGIC * **display** (*string*): The aspect-ratio of the display.

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) show(..)
# MAGIC 
# MAGIC The `show(..)` method has two optional parameters:
# MAGIC * **n**: The number of records to print to the console, the default being 20.
# MAGIC * **truncate**: If true, columns wider than 20 characters will be truncated, where the default is true.
# MAGIC 
# MAGIC Let's take a look at the data in our `DataFrame` with the `show()` command.
# MAGIC 
# MAGIC [Python Docs](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html?highlight=show#pyspark.sql.DataFrame.show)
# MAGIC 
# MAGIC [Scala Docs](https://spark.apache.org/docs/latest/api/scala/#org.apache.spark.sql.Dataset)

# COMMAND ----------

retailDF.show()

# COMMAND ----------

# MAGIC %md
# MAGIC In the cell above, change the parameters of the show command to:
# MAGIC * print only the first 5 records
# MAGIC * disable truncation
# MAGIC * print only the first 10 records and disable truncation
# MAGIC 
# MAGIC **Note:** The function `show(..)` is an **action** which triggers a job.

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) display(..)
# MAGIC 
# MAGIC The `show(..)` command is part of the core Spark API and simply prints the results to the console.
# MAGIC 
# MAGIC Our notebooks have a slightly more elegant alternative.
# MAGIC 
# MAGIC Instead of calling `show(..)` on an existing `DataFrame` we can instead pass our `DataFrame` to the `display(..)` command:

# COMMAND ----------

retailDF.select

# COMMAND ----------

display(retailDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) limit(..)
# MAGIC 
# MAGIC Both `show(..)` and `display(..)` are **actions** that trigger jobs (though in slightly different ways).
# MAGIC 
# MAGIC If you recall, `show(..)` has a parameter to control how many records are printed but `display(..)` does not.
# MAGIC 
# MAGIC We can address that difference with our first transformation, `limit(..)`.
# MAGIC 
# MAGIC If you look at the API docs, `limit(..)` is described like this:
# MAGIC > Returns a new Dataset by taking the first n rows...
# MAGIC 
# MAGIC `show(..)`, like many actions, does not return anything. 
# MAGIC 
# MAGIC On the other hand, transformations like `limit(..)` return a **new** `DataFrame`:

# COMMAND ----------

limitedDF = retailDF.limit(5) # "limit" the number of records to the first 5

# COMMAND ----------

# MAGIC %md
# MAGIC ### Nothing Happened
# MAGIC * Notice how "nothing" happened - that is no job was triggered.
# MAGIC * This is because we are simply defining the second step in our transformations.
# MAGIC   0. Read in the parquet file (represented by **retailDF**).
# MAGIC   0. Limit those records to just the first 5 (represented by **limitedDF**).
# MAGIC * It's not until we induce an action that a job is triggered and the data is processed
# MAGIC 
# MAGIC We can induce a job by calling either the `show(..)` or the `display(..)` actions:

# COMMAND ----------

limitedDF.show(100, False) #show up to 100 records and don't truncate the columns

# COMMAND ----------

display(limitedDF) # defaults to the first 1000 records

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) count()
# MAGIC 
# MAGIC How many rows are in our dataset? Let's use the `count()` action to find out!
# MAGIC 
# MAGIC Take a look at the [documentation
# MAGIC ](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html)to see how to use `count()`.

# COMMAND ----------

total = retailDF.count()

print("Record Count: {:,}".format( total ))

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) select(..)
# MAGIC 
# MAGIC In our case, the `img` column is of no value to us.
# MAGIC 
# MAGIC We can disregard it by selecting only the 5 columns that we want:

# COMMAND ----------

minusOneDF = retailDF.select("product_id", "category", "brand", "model", "price")
  
minusOneDF.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Again, notice how the call to `select(..)` does not trigger a job. That's because `select(..)` is a transformation.
# MAGIC 
# MAGIC Let's go ahead and invoke the action `show(..)` and take a look at the result.

# COMMAND ----------

minusOneDF.show(5) #Since we did not specify an argument for Truncate, it defaults to True

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) drop(..)
# MAGIC 
# MAGIC As a quick side note, you will quickly discover there are a lot of ways to accomplish the same task.
# MAGIC 
# MAGIC Instead of selecting everything we wanted, `drop(..)` allows us to specify the columns we don't want.
# MAGIC 
# MAGIC And we can see that we can produce the exact same result as the last exercise this way:

# COMMAND ----------

droppedDF = retailDF.drop("processor")

droppedDF.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) withColumnRenamed(..)
# MAGIC 
# MAGIC There are many ways to rename columns of a DataFrame in Spark. 
# MAGIC 
# MAGIC `withColumnRenamed(oldName, newName)` allows to rename columns one at a time.

# COMMAND ----------

droppedDF.withColumnRenamed("product_id", "prodID").printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) withColumn(..)
# MAGIC 
# MAGIC `withColumn` allows us to add new columns, or overwrite the values of existing columns.
# MAGIC 
# MAGIC Let's make a new column which equals double the `price` field. There are a few ways we can do this.
# MAGIC 
# MAGIC One is by importing the `col` (column) function and applying it to the `price` column (recommended).

# COMMAND ----------

from pyspark.sql.functions import col

doublePriceDF = droppedDF.withColumn("doublePrice", col("price") * 2)

doublePriceDF.show(3)

# COMMAND ----------

# MAGIC %md Another way is to use Python Pandas syntax i.e. `droppedDF["price"]` as shown below.

# COMMAND ----------

droppedDF.withColumn("doublePrice", droppedDF["price"] * 2).show(3)

# COMMAND ----------

# MAGIC %md
# MAGIC Above, we used the `col("price")` or `droppedDF["price"] * 2` syntax.
# MAGIC 
# MAGIC Using this syntax will not work:
# MAGIC `droppedDF.withColumn("doublePrice", "price")`
# MAGIC 
# MAGIC * The problem is that `.withColumn(..)` expects a column type, thus the notation
# MAGIC `col("price") * 2` or `droppedDF["price"] * 2`
# MAGIC 
# MAGIC Refer to: 
# MAGIC https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrame.withColumn

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) The Column Class
# MAGIC 
# MAGIC The `Column` class is an object that encompasses more than just the name of the column, but also column-level-transformations, such as sorting in a descending order.
# MAGIC 
# MAGIC The first question to ask is how do I create a `Column` object?
# MAGIC 
# MAGIC In Python we have these options:

# COMMAND ----------

# Scala & Python both support accessing a column from a known DataFrame
columnA = retailDF["price"]
print columnA

columnB = retailDF.price
print columnB

# The $"column-name" version that works for Scala does not work in Python

# If we import ...sql.functions, we get a couple of more options:
from pyspark.sql.functions import *

# This uses the col(..) function
columnC = col("price")
print columnC

# This uses the expr(..) function which parses an SQL Expression
columnD = expr("a + 1")
print columnD

# This uses the lit(..) to create a literal (constant) value.
columnE = lit("abc")
print columnE

# COMMAND ----------

# MAGIC %md
# MAGIC In the case of Python, the cleanest version is the **col("column-name")** variant.
# MAGIC 
# MAGIC ** *Note:* ** *We are introducing `...sql.functions` specifically for creating `Column` objects.*<br/>
# MAGIC *We will be reviewing the multitude of other commands available from this part of the API in future notebooks.*

# COMMAND ----------

display(droppedDF.withColumn("doubleReq", col("price") * 2))

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) selectExpr(..)
# MAGIC 
# MAGIC `selectExpr` is very slick - it allows you to select columns, rename columns, and create new columns all in one.

# COMMAND ----------

display(retailDF.selectExpr("product_id as prodID", "category", "brand","price", "price*2 as 2xPrice"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) filter
# MAGIC 
# MAGIC Let's filter out all of the records where the number of requests is less than 100.

# COMMAND ----------

display(retailDF.filter("price >= 300"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) groupBy
# MAGIC 
# MAGIC Let's group by the `project` field in our dataset.
# MAGIC 
# MAGIC Look at the [docs](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.GroupedData) to see all of the different methods we can call on groupedData.

# COMMAND ----------

display(retailDF.groupBy("brand").count().filter("brand = 'Microsoft'"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) take(n)
# MAGIC 
# MAGIC Use `take` if you want to retrieve just a few records of your data.

# COMMAND ----------

retailDF.take(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) collect()
# MAGIC 
# MAGIC Collect returns all of the data to the driver - calling collect on a large dataset is the easiest way to crash a Spark cluster. You want to be very careful when you use `collect`.

# COMMAND ----------

retailDF.groupBy("brand").count().limit(40).collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Sharing/Exporting Notebooks
# MAGIC 
# MAGIC We will walk through the different ways you can share this notebook with your colleagues, as well as how to create scheduled jobs.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2018 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>