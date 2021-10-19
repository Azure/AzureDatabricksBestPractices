# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Intro To DataFrames, Lab #5
# MAGIC 
# MAGIC **Technical Accomplishments:**
# MAGIC * Introduce the concept of Broadcast Joins

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Getting Started
# MAGIC 
# MAGIC Run the following cell to configure our "classroom."

# COMMAND ----------

# MAGIC %run "../Includes/Classroom Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) The Data Source
# MAGIC 
# MAGIC This data uses the **Pageviews By Seconds** data set.
# MAGIC 
# MAGIC The parquet files are located on the DBFS at **/mnt/training/wikipedia/pageviews/pageviews_by_second.parquet**.

# COMMAND ----------

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

# MAGIC %md
# MAGIC 
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Broadcast Joins
# MAGIC 
# MAGIC If you saw the section on UDFs, you know that we can **aggregate by the Day-Of-Week**.
# MAGIC 
# MAGIC We **first used a UDF** only to discover that there was a built in function to do the exact same thing.
# MAGIC 
# MAGIC We saw that **Monday had more data** than any other day of the week.
# MAGIC 
# MAGIC We then forked the `DataFrame` so as to compare **Mobile Requests to Desktop Requests**.
# MAGIC 
# MAGIC Next, we **joined those to `DataFrames`** into one so that we could easily compare the two sets of data.
# MAGIC 
# MAGIC We know that the problem with the data **has nothing to do with Mobile vs Desktop**.
# MAGIC 
# MAGIC So we don't need that type of join (two ~large `DataFrames`)
# MAGIC 
# MAGIC However, what if we wanted to **reproduce our first exercise** (counts per day-of-week)...
# MAGIC * without a UDF...
# MAGIC * with a lookup table for the day-of-week...
# MAGIC * with a join between the pageviews and the lookup table.
# MAGIC 
# MAGIC What's different about this example is that we are **joining a big `DataFrame` to a small `DataFrame`**.
# MAGIC 
# MAGIC In this scenario, Spark can optimize the join and **avoid the expensive shuffle** with a **Broadcast Join**.
# MAGIC 
# MAGIC Let's start with two `DataFrames`
# MAGIC * The first we will derive from our original `DataFrame`. In this case, we will use a simple number for the day-of-week.
# MAGIC * The second `DataFrame` will map that number (1-7) to the labels **Monday**, **Tue**, **W**, or whatever...
# MAGIC 
# MAGIC Let's take a look at our first `DataFrame`.

# COMMAND ----------

columnTrans = date_format(col("capturedAt"), "u").alias("dow")

pageviewsWithDowDF = (pageviewsDF
    .withColumn("dow", columnTrans)  # Add the column dow
)
(pageviewsWithDowDF
  .cache()                           # mark the data as cached
  .count()                           # materialize the cache
)
display(pageviewsWithDowDF)

# COMMAND ----------

# MAGIC %md
# MAGIC All we did is add one column **dow** that has the value **1** for **Monday**, **2** for **Tuesday**, etc.
# MAGIC 
# MAGIC Next, we are going to load a mapping of 1, 2, 3, etc. to Mon, Tue, Wed, etc from a **REALLY** small `DataFrame`.

# COMMAND ----------

labelsDF = spark.read.parquet("/mnt/training/day-of-week")

display(labelsDF) # view our labels

# COMMAND ----------

# MAGIC %md
# MAGIC Now that we have two `DataFrames`...
# MAGIC 
# MAGIC We can execute a join between the two `DataFrames`

# COMMAND ----------

joinedDowDF = (pageviewsWithDowDF
  .join(labelsDF, pageviewsWithDowDF["dow"] == labelsDF["dow"])
  .drop( pageviewsWithDowDF["dow"] )
)
display(joinedDowDF)

# COMMAND ----------

# MAGIC %md
# MAGIC Now that the data is joined, we can aggregate by any (or all) of the various labels representing the day-of-week.
# MAGIC 
# MAGIC Notice that we are not losing the numerical **dow** column which we can use to sort.
# MAGIC 
# MAGIC And when we graph this, you can graph by any one of the labels...

# COMMAND ----------

aggregatedDowDF = (joinedDowDF
  .groupBy(col("dow"), col("longName"), col("abbreviated"), col("shortName"))  
  .sum("requests")                                             
  .withColumnRenamed("sum(requests)", "Requests")
  .orderBy(col("dow"))
)
# Display and then graph...
display(aggregatedDowDF)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Already Broadcasted
# MAGIC 
# MAGIC Beleive it or not, that was a broadcast join.
# MAGIC 
# MAGIC The proof can be seen by looking at the physical plan.
# MAGIC 
# MAGIC Run the `explain(..)` below and then look for **BroadcastHashJoin** and/or **BroadcastExchange**.

# COMMAND ----------

aggregatedDowDF.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC From the code perspective, it looks just like other joins.
# MAGIC 
# MAGIC So what's the difference between a regular and a broadcast-join?

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Standard Join
# MAGIC 
# MAGIC * In a standard join, **ALL** the data is shuffled
# MAGIC * This can be really expensive
# MAGIC <br/><br/>
# MAGIC <div style="text-align:center"><img src="https://files.training.databricks.com/images/join-standard.png" style="max-height:400px"/></div>
# MAGIC <p>Here we see how all the records keyed by "green" are moved to the same partition.<br/>The process would be repeated for "red" and "blue" records.</p>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Broadcast Join
# MAGIC * In a Broadcast Join, only the "small" data is moved.
# MAGIC * It duplicates the "small" data across all executors.
# MAGIC * But the "big" data is left untouched.
# MAGIC * If the "small" data is small enough, this can be **VERY** efficient.
# MAGIC <br/><br/>
# MAGIC <div style="text-align:center"><img src="https://files.training.databricks.com/images/join-broadcasted.png" style="max-height:400px"/></div>
# MAGIC 
# MAGIC <p>Here we see the records keyed by "red" being replicated into the first partition.<br/>
# MAGIC    The process would be repeated for each executor.<br/>
# MAGIC    The entire process would be repeated again for "green" and "blue" records.</p>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Broadcasted, How?
# MAGIC 
# MAGIC Behind the scenes, Spark is analyzing our two `DataFrames`.
# MAGIC 
# MAGIC It attempts to estimate if either or both are < 10MB.
# MAGIC 
# MAGIC We can see/change this threshold value with the config **spark.sql.autoBroadcastJoinThreshold**. 
# MAGIC 
# MAGIC The documentation reads as follows:
# MAGIC > Configures the maximum size in bytes for a table that will be broadcast to all worker nodes when performing a join. By setting this value to -1 broadcasting can be disabled...

# COMMAND ----------

threshold = spark.conf.get("spark.sql.autoBroadcastJoinThreshold")
print("Threshold: {0:,}".format( int(threshold) ))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC In such a case it will take the small `DataFrame`, the `labelsDF` in our case
# MAGIC * Send the entire `DataFrame` to every **Executor**
# MAGIC * Then do a join on the local copy of `labelsDF`
# MAGIC * Compared to taking our big `DataFrame` `pageviewsWithDowDF` and shuffling it across all executors.

# COMMAND ----------

# MAGIC %md
# MAGIC We can see proof of this by dropping the threshold:

# COMMAND ----------

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 0)

# COMMAND ----------

# MAGIC %md
# MAGIC Run the `explain(..)` below and then look for the **ABSENCE OF** the **BroadcastHashJoin** and/or **BroadcastExchange**.

# COMMAND ----------

(joinedDowDF
  .groupBy(col("dow"), col("longName"), col("abbreviated"), col("shortName"))  
  .sum("requests")                                             
  .withColumnRenamed("sum(requests)", "Requests")
  .orderBy(col("dow"))
  .explain()
)

# COMMAND ----------

# MAGIC %md
# MAGIC And now that we are done, let's restore the original threshold:

# COMMAND ----------

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", threshold)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) broadcast(..)

# COMMAND ----------

# MAGIC %md
# MAGIC What if I wanted to broadcast the data and it was over the 10MB [default] threshold?
# MAGIC 
# MAGIC We can specify that a `DataFrame` is to be broadcasted by using the `broadcast(..)` operation from the `...sql.functions` package.
# MAGIC 
# MAGIC However, **it is only a hint**. Spark is allowed to ignore it.

# COMMAND ----------

pageviewsWithDowDF.join(   broadcast(labelsDF)   , pageviewsWithDowDF["dow"] == labelsDF["dow"])


# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>