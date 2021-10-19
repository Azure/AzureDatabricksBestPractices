# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Intro To DataFrames, Part #4
# MAGIC 
# MAGIC **Technical Accomplishments:**
# MAGIC * Create a User Defined Function (UDF)
# MAGIC * Execute a join operation between two `DataFrames`

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

display(pageviewsDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) date_format()
# MAGIC 
# MAGIC Today, we want to aggregate all the data **by the day of week** (Monday, Tuesday, Wednesday)...
# MAGIC 
# MAGIC ...and then **sum all the requests**.
# MAGIC 
# MAGIC Our goal is to see **which day of the week** has the most traffic.
# MAGIC 
# MAGIC On of the functions that can help us with this the operation `date_format(..)` from the `functions` package.
# MAGIC 
# MAGIC If you recall from our review of `unix_timestamp(..)` Spark uses the <a href="https://docs.oracle.com/javase/8/docs/api/java/text/SimpleDateFormat.html" target="_blank">SimpleDateFormat</a> from the Java API for parsing (and formatting).

# COMMAND ----------

# Create a new DataFrame
byDayOfWeekDF = (pageviewsDF
  .groupBy( date_format( col("capturedAt"), "E") )         # format as Mon, Tue and then aggregate
  .sum()                                                   # produce the sum of all records
  .select( col("date_format(capturedAt, E)").alias("dow"), # rename to "dow"
           col("sum(requests)").alias("total"))            # rename to "total"
  .orderBy( col("dow") )                                   # sort by "dow" MTWTFSS
)

# COMMAND ----------

# MAGIC %md
# MAGIC With that's done, let's look at the result.
# MAGIC 
# MAGIC But not just as a list of tables...
# MAGIC 
# MAGIC Sometimes you miss important details when you are looking at just numbers.
# MAGIC 
# MAGIC Let's try a bar graph - all you have to do is click on the graph icon below your results.

# COMMAND ----------

display(byDayOfWeekDF)

# COMMAND ----------

# MAGIC %md
# MAGIC What's wrong with this graph?
# MAGIC 
# MAGIC What if we could change the labels from "Mon" to "1-Mon" and "Tue" to "2-Tue"?
# MAGIC 
# MAGIC Would that fix the problem? Sure...
# MAGIC 
# MAGIC What API call(s) would solve that problem...
# MAGIC 
# MAGIC Well there isn't one. We'll just have to create our own...

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) User Defined Functions (UDF)
# MAGIC 
# MAGIC As you've seen, `DataFrames` can do anything!
# MAGIC 
# MAGIC Actually they can't.
# MAGIC 
# MAGIC There will always be the case for a transformation that cannot be accomplished with the provided functions.
# MAGIC 
# MAGIC To address those cases, Apache Spark provides for **User Defined Functions** or **UDF** for short.
# MAGIC 
# MAGIC However, they come with a cost...
# MAGIC * **UDFs cannot be optimized** by the Catalyst Optimizer - someday, maybe, but for now it has no insight to your code.
# MAGIC * The function **has to be serialized** and sent out to the executors - this is a one-time cost, but a cost just the same.
# MAGIC * If you are not careful, you could **serialize the whole world**.
# MAGIC * In the case of Python, there is even more over head - we have to **spin up a Python interpreter** on every Executor to run your Lambda.
# MAGIC 
# MAGIC Let's start with our function...

# COMMAND ----------

def mapDayOfWeek(day):
  _dow = {"Mon": "1", "Tue": "2", "Wed": "3", "Thu": "4", "Fri": "5", "Sat": "6", "Sun": "7"}
  
  n = _dow.get(day)
  if n:
    return n + "-" + day
  else:
    return "UNKNOWN"

# COMMAND ----------

# MAGIC %md
# MAGIC And now we can test our function...

# COMMAND ----------

assert "1-Mon" == mapDayOfWeek("Mon")
assert "2-Tue" == mapDayOfWeek("Tue")
assert "3-Wed" == mapDayOfWeek("Wed")
assert "4-Thu" == mapDayOfWeek("Thu")
assert "5-Fri" == mapDayOfWeek("Fri")
assert "6-Sat" == mapDayOfWeek("Sat")
assert "7-Sun" == mapDayOfWeek("Sun")
assert "UNKNOWN" == mapDayOfWeek("Xxx")

# COMMAND ----------

# MAGIC %md
# MAGIC Great, it works! Now define a UDF that wraps this function:

# COMMAND ----------

prependNumberUDF = udf(mapDayOfWeek)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC At this point, we have told Spark that we want that **function to be serialized**...
# MAGIC 
# MAGIC and sent out **to each executor**.
# MAGIC 
# MAGIC **We can test it** as a UDF with a simple query...

# COMMAND ----------

(byDayOfWeekDF
   .select( prependNumberUDF( col("dow")) )
   .show()
)

# COMMAND ----------

# MAGIC %md
# MAGIC Our UDF looks like **it's working**. 
# MAGIC 
# MAGIC Next, let's **apply the UDF** and also **order the x axis** from Mon -> Sun
# MAGIC 
# MAGIC Once it's done executing, we can **render our bar graph**.

# COMMAND ----------

display(
  byDayOfWeekDF
    .select( prependNumberUDF(col("dow")).alias("Day Of Week"), col("total") )
    .orderBy("Day Of Week")
)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) I Lied
# MAGIC 
# MAGIC > What API call(s) would solve that problem...<br/>
# MAGIC > Well there isn't one. We'll just have to create our own...
# MAGIC 
# MAGIC The truth is that **there is already a function** to do exactly what we want.
# MAGIC 
# MAGIC Before you go and create your own UDF, check, double check, triple check to make sure it doesn't exist.
# MAGIC 
# MAGIC Remember...
# MAGIC * UDFs induce a performance hit.
# MAGIC * Big ones in the case of Python.
# MAGIC * The Catalyst Optimizer cannot optimize it.
# MAGIC * And you are re-inventing the wheel.
# MAGIC 
# MAGIC In this case, the solution to our problem is the operation `date_format(..)` from the `...sql.functions`.

# COMMAND ----------

display(
  pageviewsDF
    .groupBy( date_format(col("capturedAt"), "u-E").alias("Day Of Week") )
    .sum()
    .orderBy(col("Day Of Week"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Conclusions?
# MAGIC   
# MAGIC So what can we infer from this data?
# MAGIC   
# MAGIC Remember... we draw our conclusions first and **then** back 'em up with the data.
# MAGIC 0. Why does Monday have more records than any day of the week?
# MAGIC 0. Why does the weekend have less records than the rest of the week?
# MAGIC 0. What can we conclude about usage from Friday to Saturday to Sunday?
# MAGIC 0. Is there a correlation between that and Sunday to Monday?

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC See also <a href="https://docs.azuredatabricks.net/spark/latest/spark-sql/udaf-scala.html" target="_blank">User Defined Aggregate Functions</a>

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) What About Mondays?
# MAGIC 
# MAGIC Something is up with Mondays.
# MAGIC 
# MAGIC But is the problem unique to Mobile or Desktop?
# MAGIC 
# MAGIC To answer this, we can fork the `DataFrame`.
# MAGIC 
# MAGIC One for Mobile, another for Desktop.

# COMMAND ----------

mobileDF = (pageviewsDF
    .filter(col("site") == "mobile")
    .groupBy( date_format(col("capturedAt"), "u-E").alias("Day Of Week") )
    .sum()
    .withColumnRenamed("sum(requests)", "Mobile Total")
    .orderBy(col("Day Of Week"))
)
desktopDF = (pageviewsDF
    .filter(col("site") == "desktop")
    .groupBy( date_format(col("capturedAt"), "u-E").alias("Day Of Week") )
    .sum()
    .withColumnRenamed("sum(requests)", "Desktop Total")
    .orderBy(col("Day Of Week"))
)
# Cache and materialize
mobileDF.cache().count()
desktopDF.cache().count()

# COMMAND ----------

# MAGIC %md
# MAGIC Now we can create two graphs, one for Mobile and another for Desktop.
# MAGIC 
# MAGIC But more realistically, I can better see the scale between the two if I could view them in one graph.
# MAGIC 
# MAGIC But even more importatnly, one graph is really just a semi-contrived reason to demonstrate a `join(..)` operation.

# COMMAND ----------

display(mobileDF)

# COMMAND ----------

display(desktopDF)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) join(..)
# MAGIC 
# MAGIC If you a familiar with SQL joins then `DataFrame.join(..)` should be pretty strait forward.
# MAGIC 
# MAGIC We start with the left side - in this example `desktopDF`.
# MAGIC 
# MAGIC Then join the right side - in this example `mobileDF`.
# MAGIC 
# MAGIC The tricky part is the join condition.
# MAGIC 
# MAGIC We want all records to align by the **Day Of Week** column.
# MAGIC 
# MAGIC The problem is that if we used `$"Day Of Week"` or `col("Day Of Week")` the `DataFrame` cannot tell which source we are referring to...
# MAGIC * `desktopDF`'s version of **Day Of Week** or
# MAGIC * `mobileDF`'s version of **Day Of Week**
# MAGIC 
# MAGIC If you recall from our discussions on the `Column` class, one option was to use the `DataFrame` to return an instance of a `Column`.
# MAGIC 
# MAGIC For example.

# COMMAND ----------

columnA = desktopDF["Day Of Week"]
columnB = mobileDF["Day Of Week"]

print(columnA) # Python hack to print the data type
print(columnB)

# COMMAND ----------

# MAGIC %md
# MAGIC And now we can put it all together...

# COMMAND ----------

tempDF = desktopDF.join(mobileDF, desktopDF["Day Of Week"] == mobileDF["Day Of Week"])

display(tempDF)

# COMMAND ----------

# MAGIC %md
# MAGIC As we can see above, we now have the two `DataFrames` "joined" into a single one.
# MAGIC 
# MAGIC However, if you notice, we have two **Day Of Week** columns.
# MAGIC 
# MAGIC This will just create headaches later.
# MAGIC 
# MAGIC Let's drop `desktopDF`'s copy of **Day Of Week** using the same technique we used in the join.
# MAGIC 
# MAGIC And while we are at it, use a `select(..)` transformation to rearrange the columns.

# COMMAND ----------

joinedDF = (tempDF
  .drop(desktopDF["Day Of Week"])
  .select(col("Day Of Week"), col("Desktop Total"), col("Mobile Total"))
)
display(joinedDF)

# COMMAND ----------

# MAGIC %md
# MAGIC Of course, there is always more than one way to solve the same problem.
# MAGIC 
# MAGIC In thise case we can use an equi-join:

# COMMAND ----------

altDF = desktopDF.join(mobileDF, "Day Of Week")

display(altDF)

# COMMAND ----------

# MAGIC %md
# MAGIC And just to wrap it up, plot the graph.
# MAGIC 
# MAGIC It should only show one of the two numbers.
# MAGIC 
# MAGIC Open the **Plot Options..** and...
# MAGIC * Set the **Keys** to **Day Of Week**.
# MAGIC * Set the **Values** to both **Desktop Total** AND **Mobile Total**.
# MAGIC * Hint: You can drag and drop the values.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) More Conclusions?
# MAGIC   
# MAGIC Now, what can we infer from this data?
# MAGIC   
# MAGIC 0. What is the difference between the weekend numbers (F/S/S) for Mobile vs Desktop?
# MAGIC 0. What would explain the difference in weekend numbers for the two sites?
# MAGIC 0. How does the weekend numbers compare to Mondays? For Desktop? For Mobile?
# MAGIC 0. What conclusion can we draw about Mondays?

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/labs.png) Data Frames Lab #4
# MAGIC 
# MAGIC Unlike the previous labs, this next lab does not have an emphasis on UDFs or Joins.
# MAGIC 
# MAGIC However, you will have to draw on everything you have learned so far in order to answer the question...
# MAGIC 
# MAGIC What's up with Monday?
# MAGIC 
# MAGIC Go ahead and open the notebook [Introduction to DataFrames, Lab #4]($./Intro To DF Part 4 Lab) and complete the exercises.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>