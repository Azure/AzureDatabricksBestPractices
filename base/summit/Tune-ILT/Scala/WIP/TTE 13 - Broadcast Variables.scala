// Databricks notebook source
// MAGIC 
// MAGIC %md-sandbox
// MAGIC 
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ### Broadcast Variables
// MAGIC 
// MAGIC Broadcast variables allow us to keep a read-only variable cached on each machine rather than shipping a copy of it with tasks. This can be useful when tasks of a job require access to the same variable. Typically tasks **larger than approximately 20 KB** should be optimized to use broadcast variables.
// MAGIC 
// MAGIC Popular use cases:
// MAGIC - Sharing a variable between multiple tasks
// MAGIC - Joining a small table to a very large table 
// MAGIC 
// MAGIC #### Sharing a variable
// MAGIC <img src="https://files.training.databricks.com/images/tuning/broadcast_isin.png" style="height:240px;" alt="Spill to disk"/><br/><br/>    

// COMMAND ----------

import org.apache.spark.sql.functions.{col}
val parquetDF = spark.read.parquet("mnt/training/dataframes/people-10m.parquet/")
val gender= "F"
val bgender = spark.sparkContext.broadcast(gender)
var b = parquetDF.select($"*", col("gender").isin(bgender.value).alias("valid"))
b.filter(col("valid") === false).show()

// COMMAND ----------

// MAGIC %md
// MAGIC It's generally a good idea to destroy the broadcast variable when you're done with it.

// COMMAND ----------

bgender.destroy()

// COMMAND ----------

// MAGIC %md
// MAGIC In practice, Spark *automatically* broadcasts the common data needed by tasks within each stage; thus, broadcast variables are useful when data is required across  multiple stages.

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC #### Broadcast Join
// MAGIC <img src="https://files.training.databricks.com/images/tuning/broadcast-join.png" style="height:300px;"  alt="Spill to disk"/><br/><br/>    
// MAGIC 
// MAGIC The high level idea is that sharing an entire small table is more efficient that splitting it up and shuffling both the large and small tables. This means that the large table doesn't need to be shuffled, as Spark has a full copy of the smaller table and can carry out the join on the mapper side. 

// COMMAND ----------

// approx 18.6 MB in memory
var names = spark.read.parquet("/mnt/training/ssn/names.parquet")

// approx 500KB in memory
var people = (
  spark
    .read
    .option("delimiter", ":")
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("/mnt/training/dataframes/people-with-header.txt")
)
var test = names.join(people, "firstName")
test.show()

// COMMAND ----------

// MAGIC %md
// MAGIC #### Automatic and Manual broadcasting
// MAGIC 
// MAGIC - Depending on size of the data that is being loaded into Spark, Spark uses internal heuristics to decide how to join that data to other data.
// MAGIC - Automatic broadcast depends on `spark.sql.autoBroadcastJoinThreshold`
// MAGIC     - The setting configures the **maximum size in bytes** for a table that will be broadcast to all worker nodes when performing a join 
// MAGIC     - Default is 10MB
// MAGIC 
// MAGIC - A `broadcast` function can be used in Spark to instruct Catalyst that it should probably broadcast one of the tables that is being joined. 
// MAGIC - The function is important, as sometimes our table might fall just outside of the limit of what Spark will broadcast automatically.
// MAGIC 
// MAGIC If the `broadcast` hint isn't used, but one side of the join is small enough (i.e., its size is below the threshold), that data source will be read into
// MAGIC the Driver and broadcast to all Executors.
// MAGIC 
// MAGIC If both sides of the join are small enough to be broadcast, the [current Spark source code](https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/execution/SparkStrategies.scala#L153)
// MAGIC will choose the right side of the join to broadcast.
// MAGIC 
// MAGIC Below we join two DataFrames where both DataFrames exceed the default 10MB limit of `autoBroadcastJoinThreshold` by a significant amount.
// MAGIC 
// MAGIC Note that we're supplying the schema explicitly, to speed things up.

// COMMAND ----------

// MAGIC %md
// MAGIC To get a rough sense of the sizes of the DataFrames in memory, you can cache each one, run an action that traverses the whole data set (e.g., `count`), and then check the UI. 

// COMMAND ----------

 import org.apache.spark.sql._
 import org.apache.spark.sql.types._

val people_schema = StructType(List(
  StructField("id", IntegerType, true),
  StructField("firstName", StringType, true),
  StructField("middleName", StringType, true),
  StructField("lastName", StringType, true),
  StructField("gender", StringType, true),
  StructField("birthDate", TimestampType, true),
  StructField("ssn", StringType, true),
  StructField("salary", IntegerType, true)
))
// 229.5 MB in tungsten format.
var people1 = (
  spark
    .read
    .option("header", "true")
    .option("delimiter", ":")
    .schema(people_schema)
    .csv("/mnt/training/dataframes/people-with-header-5m.txt")
)
// 46 MB in tungsten format.
var people2 = (
  spark
    .read
    .option("header", "true")
    .option("delimiter", ":")
    .schema(people_schema)
    .csv("/mnt/training/dataframes/people-with-header-1m.txt")
)
// If we were to join the two tables on say, the first name, spark wouldn't carry out a broadcast.
var peopleNames = people2.join(people1, people1("firstName") === people2("firstName"))
peopleNames.explain()

// COMMAND ----------

// MAGIC %md
// MAGIC Both tables are above the default 10 MB limit of `spark.sql.autoBroadcastJoinThreshold` but we can hint that we want a broadcasting to happen on one of the tables. Using the explain function to render the final physical execution plan a `BroadcastHashJoin` can be seen.

// COMMAND ----------

import org.apache.spark.sql.functions._
var bcName = broadcast(names)

// COMMAND ----------

var peopleNamesBcast = people2.join(bcName, names("firstName") === people2("firstName"))
peopleNamesBcast.explain()

// COMMAND ----------

peopleNamesBcast.count()

// COMMAND ----------

// MAGIC %md
// MAGIC We should also see a performance benefit of broadcasting.<br/> The `names` DataFrame is over the 10MB limit but thanks to the `broadcast` function, the optimization can be achieved.

// COMMAND ----------

peopleNames.count()

// COMMAND ----------

peopleNamesBcast.count()

// COMMAND ----------

// MAGIC %md
// MAGIC In Spark 2.2 and later after <a target="blank" href="https://issues.apache.org/jira/browse/SPARK-16475">SPARK-16475</a>, a broadcast hint function has been introduced to Spark SQL.

// COMMAND ----------

names.createOrReplaceTempView("names")
people2.createOrReplaceTempView("people2")
var broadcastedSQLDF = spark.sql("SELECT /*+ BROADCASTJOIN(names) */* FROM names INNER JOIN people2 on  names.firstName=people2.firstName")
broadcastedSQLDF.explain()

// COMMAND ----------

broadcastedSQLDF.count()

// COMMAND ----------

// MAGIC %md
// MAGIC #### Broadcast Cleanup
// MAGIC 
// MAGIC We can  clean up memory used by broadcast variables. There are two different options:
// MAGIC - `unpersist` - cleans up the broadcasted variable from all executors, keeps a copy in the driver.
// MAGIC - `destroy` - cleans broadcast variable from driver and executors.

// COMMAND ----------

// MAGIC %md
// MAGIC ## Exercises
// MAGIC 
// MAGIC ### Exercise 1: Broadcast variables
// MAGIC 
// MAGIC Let's begin by creating a variable to hold some data to be used with a Spark Dataset within a closure. Remember, don't make the data too large, the driver JVM has a default size of **1GB**. 
// MAGIC 
// MAGIC **NOTE**: Because we're using the Dataset API for Exercise 1, we cannot use Python.

// COMMAND ----------

// MAGIC %scala
// MAGIC val limit = 50 * 1000 * 1000
// MAGIC val data = (1 to limit).toArray

// COMMAND ----------

// MAGIC %scala
// MAGIC // TODO
// MAGIC val dataSet = (1 to 5).map(x => (x, x*x)).toArray
// MAGIC val df = spark.createDataFrame(dataSet).toDF("numb", "squared")
// MAGIC // Convert the DataFrame to a Dataset so that lambdas and anonymous functions can be used.
// MAGIC val ds = <FILL_IN>

// COMMAND ----------

// MAGIC %md
// MAGIC We can now use the data we declared earlier to manipulate the newly created Dataset when using lambdas / anonymous functions.

// COMMAND ----------

// MAGIC %scala
// MAGIC for (i <- 1 to 5) ds.map { x => data.length * x.numb }.count()

// COMMAND ----------

// MAGIC %md
// MAGIC Update the above code that carries out a map on the Dataset to use a broadcast variable for efficiency. 

// COMMAND ----------

// MAGIC %scala
// MAGIC // TODO
// MAGIC for (i <- 1 to 5) ds.map { /*TODO*/ }.count()

// COMMAND ----------

// MAGIC %md
// MAGIC Make sure to clean the broadcast variable from the cluster once it's no longer needed.

// COMMAND ----------

// MAGIC %scala
// MAGIC // TODO 
// MAGIC remove broadcast variable from memory of executors and driver.

// COMMAND ----------

// MAGIC %md
// MAGIC What happens if we try to use the broadcast variable after garbage collecting it from memory?

// COMMAND ----------

// MAGIC %scala
// MAGIC broadcastVar.value

// COMMAND ----------

// MAGIC %md
// MAGIC ### Exercise 2: Using broadcasting to optimize joins
// MAGIC 
// MAGIC Earlier in the course we saw that using the `broadcast` hint can help in situations where a DataFrame is larger than `spark.sql.autoBroadcastJoinThreshold`. Another option is to simply increase `autoBroadcastJoinThreshold`. But what if we want to prevent broadcasting? One sure way to prevent broadcasts from happening is to set `autoBroadcastJoinThreshold` to **-1**. Update the threshold below to prevent any broadcasting from happening and verify by looking at the selected physical plan for execution.

// COMMAND ----------

// TODO
spark.conf.get("spark.sql.autoBroadcastJoinThreshold")
spark.conf.set(<<FILL_IN>>)

var names = spark.read.parquet("/mnt/training/ssn/names.parquet")
people = (
  spark
    .read
    .option("delimiter", ":")
    .option("header", "true") 
    .option("inferSchema", "true")
    .csv("/mnt/training/dataframes/people-with-header.txt")
)

var joinedDF = names.join(people, Seq("firstName"))
// verify that neither of the tables was broadcasted.
joinedDF.<<FILL_IN>>

// COMMAND ----------

// MAGIC %md
// MAGIC Even though we've set `autoBroadcastJoinThreshold` to **-1**, this configuration can be circumvented by using the `broadcast` hint option. Use it below to force broadcasting of the people dataframe.

// COMMAND ----------

// TODO
print("autoBroadcastJoinThreshold: " + spark.conf.get("spark.sql.autoBroadcastJoinThreshold"))

var joinedDF2 = <<FILL_IN>> 
joinedDF2.explain()


// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>