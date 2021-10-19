# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Transformations & Actions
# MAGIC 
# MAGIC **Technical Accomplishments:**
# MAGIC * Review the Lazy vs. Eager design
# MAGIC * Quick review of Transformations
# MAGIC * Quick review of Actions
# MAGIC * Introduce the Catalyst Optimizer
# MAGIC * Wide vs. Narrow Transformations

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Getting Started
# MAGIC 
# MAGIC Run the following cell to configure our "classroom."

# COMMAND ----------

# MAGIC %run "../Includes/Classroom Setup"

# COMMAND ----------

# Because we will need it later...
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Laziness By Design
# MAGIC 
# MAGIC Fundamental to Apache Spark are the notions that
# MAGIC * Transformations are **LAZY**
# MAGIC * Actions are **EAGER**
# MAGIC 
# MAGIC We see this play out when we run multiple transformations back-to-back, and no job is triggered:

# COMMAND ----------

schema = StructType(
  [
    StructField("project", StringType(), False),
    StructField("article", StringType(), False),
    StructField("requests", IntegerType(), False),
    StructField("bytes_served", LongType(), False)
  ]
)

parquetFile = "/mnt/training/wikipedia/pagecounts/staging_parquet_en_only_clean/"

topTenDF = (spark                                          # Our SparkSession & Entry Point
  .read                                                    # DataFrameReader
  .schema(schema)                                          # DataFrameReader (config)
  .parquet(parquetFile)                                    # Transformation (initial)
  .where( "project = 'en'" )                               # Transformation
  .drop("bytes_served")                                    # Transformation
  .filter( col("article") != "Main_Page")                  # Transformation
  .filter( col("article") != "-")                          # Transformation
  .filter( col("article").startswith("Special:") == False) # Transformation
)
print(topTenDF) # Python hack to print the data type

# COMMAND ----------

# MAGIC %md
# MAGIC There is one exception to this.
# MAGIC 
# MAGIC When you are reading data, Apache Spark needs to know the schema of the `DataFrame`.
# MAGIC 
# MAGIC In this case, the schema was specified
# MAGIC 
# MAGIC However, if the `DataFrameReader` had to infer the schema, it would trigger a one-time job:

# COMMAND ----------

# schema = StructType(
#   [
#     StructField("project", StringType(), False),
#     StructField("article", StringType(), False),
#     StructField("requests", IntegerType(), False),
#     StructField("bytes_served", LongType(), False)
#   ]
# )

parquetFile = "/mnt/training/wikipedia/pagecounts/staging_parquet_en_only_clean/"

(spark                                                      # Our SparkSession & Entry Point
  .read                                                     # DataFrameReader
  #.schema(schema)                                          # DataFrameReader (config)
  .parquet(parquetFile)                                     # Transformation (initial)
  .where( "project = 'en'" )                                # Transformation
  .drop("bytes_served")                                     # Transformation
  .filter( col("article") != "Main_Page")                   # Transformation
  .filter( col("article") != "-")                           # Transformation
  .filter( col("article").startswith("Special:") == False)  # Transformation
)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Back to our instance of `topTenDF`...
# MAGIC 
# MAGIC With it, we can trigger a job by calling an action such as `count()`

# COMMAND ----------

topTenDF.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Why is Laziness So Important?
# MAGIC 
# MAGIC This is a common pattern in functional programming as well as with Big Data specific languages.
# MAGIC * We see it in Scala as part of its core design.
# MAGIC * Java 8 introduced the concept with its Streams API.
# MAGIC * And many functional programming languages have similar APIs.
# MAGIC 
# MAGIC It has a number of benefits
# MAGIC * Not forced to load all data at step #1 
# MAGIC   * Technically impossible with **REALLY** large datasets.
# MAGIC * Easier to parallelize operations 
# MAGIC   * N different transformations can be processed on a single data element, on a single thread, on a single machine. 
# MAGIC * Most importantly, it allows the framework to automatically apply various optimizations

# COMMAND ----------

# MAGIC %md
# MAGIC ### Catalyst Optimizer
# MAGIC 
# MAGIC Because our API is declarative a large number of optimizations are available to us.
# MAGIC 
# MAGIC Some of the examples include:
# MAGIC   * Optimizing data type for storage
# MAGIC   * Rewriting queries for performance
# MAGIC   * Predicate push downs
# MAGIC 
# MAGIC ![Catalyst](https://files.training.databricks.com/images/105/catalyst-diagram.png)

# COMMAND ----------

# MAGIC %md
# MAGIC Additional Articles:
# MAGIC * <a href="https://databricks.com/session/deep-dive-into-catalyst-apache-spark-2-0s-optimizer" target="_blank">Deep Dive into Catalyst: Apache Spark 2.0's Optimizer</a>, Yin Huai's Spark Summit 2016 presentation.
# MAGIC * <a href="https://www.youtube.com/watch?v=6bCpISym_0w" target="_blank">Catalyst: A Functional Query Optimizer for Spark and Shark</a>, Michael Armbrust's presentation at ScalaDays 2016.
# MAGIC * <a href="https://databricks.com/blog/2015/04/13/deep-dive-into-spark-sqls-catalyst-optimizer.html" target="_blank">Deep Dive into Spark SQL's Catalyst Optimizer</a>, Databricks Blog, April 13, 2015.
# MAGIC * <a href="http://people.csail.mit.edu/matei/papers/2015/sigmod_spark_sql.pdf" target="_blank">Spark SQL: Relational Data Processing in Spark</a>, Michael Armbrust, Reynold S. Xin, Cheng Lian, Yin Huai, Davies Liu, Joseph K. Bradley, Xiangrui Meng, Tomer Kaftan, Michael J. Franklin, Ali Ghodsi, Matei Zaharia,<br/>_Proceedings of the 2015 ACM SIGMOD International Conference on Management of Data_.

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Actions
# MAGIC 
# MAGIC Transformations always return a `DataFrame` (or in some cases, such as Scala & Java, `Dataset[Row]`).
# MAGIC 
# MAGIC In contrast, Actions either return a result or write to disc. For example:
# MAGIC * The number of records in the case of `count()` 
# MAGIC * An array of objects in the case of `collect()` or `take(n)`
# MAGIC 
# MAGIC We've seen a good number of the actions - most of them are listed below. 
# MAGIC 
# MAGIC For the complete list, one needs to review the API docs.
# MAGIC 
# MAGIC | Method | Return | Description |
# MAGIC |--------|--------|-------------|
# MAGIC | `collect()` | Collection | Returns an array that contains all of Rows in this Dataset. |
# MAGIC | `count()` | Long | Returns the number of rows in the Dataset. |
# MAGIC | `first()` | Row | Returns the first row. |
# MAGIC | `foreach(f)` | - | Applies a function f to all rows. |
# MAGIC | `foreachPartition(f)` | - | Applies a function f to each partition of this Dataset. |
# MAGIC | `head()` | Row | Returns the first row. |
# MAGIC | `reduce(f)` | Row | Reduces the elements of this Dataset using the specified binary function. |
# MAGIC | `show(..)` | - | Displays the top 20 rows of Dataset in a tabular form. |
# MAGIC | `take(n)` | Collection | Returns the first n rows in the Dataset. |
# MAGIC | `toLocalIterator()` | Iterator | Return an iterator that contains all of Rows in this Dataset. |
# MAGIC 
# MAGIC ** *Note #1:* ** *There are some variations in the methods from language to language *
# MAGIC 
# MAGIC ** *Note #2:* ** *The command `display(..)` is not included here because it's not part of the Spark API, even though it ultimately calls an action. *

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Transformations
# MAGIC 
# MAGIC Transformations have the following key characteristics:
# MAGIC * They eventually return another `DataFrame` (or in the case of Scala and Java, `Dataset[T]`).
# MAGIC * They are immutable - that is each instance of a `DataFrame` cannot be altered once it's instantiated.
# MAGIC   * This means other optimizations are possible - such as the use of shuffle files (to be discussed in detail later)
# MAGIC * Are classified as either a Wide or Narrow operation
# MAGIC * In Scala & Java come in two flavors: Typed & Untyped
# MAGIC 
# MAGIC Let's take a look at the API docs - in this case, looking at the Scala doc is a little easier to decipher.
# MAGIC 
# MAGIC ** *Note:* ** The list of transformations varies significantly between each language.<br/>
# MAGIC Mostly because Java & Scala are strictly typed languages compared Python & R which are loosed typed.

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Typed vs. Untyped Transformations
# MAGIC 
# MAGIC The difference between **Typed** and **Untyped** transformations is specific to Scala and Java.
# MAGIC 
# MAGIC In short, it means the difference between returning a `DataFrame` vs. a `Dataset[T]`.
# MAGIC 
# MAGIC | Typed Transformations <br/> returns *Dataset[T]*  | Untyped Transformations <br/> returns *DataFrame* | 
# MAGIC |:----------------------------:|:-----------------------------:|
# MAGIC | `alias(..)`                  | `agg(..)`                     |
# MAGIC | `as(..)`                     | `apply(..)`                   |
# MAGIC | `coalesce(..)`               | `col(..)`                     |
# MAGIC | `distinct(..)`               | `crossJoin(..)`               |
# MAGIC | `dropDuplicates(..)`         | `cube(..)`                    |
# MAGIC | `except(..)`                 | `drop(..)`                    |
# MAGIC | `filter(..)`                 | `groupBy(..)`                 |
# MAGIC | `flatMap(..)`                | `join(..)`                    |
# MAGIC | `groupByKey(..)`             | `rollup(..)`                  | 
# MAGIC | `intersect(..)`              | `select(..)`                  |
# MAGIC | `join(..)`                   | `selectExpr(..)`              |                    
# MAGIC | `limit(..)`                  | `stat(..)`                    |                    
# MAGIC | `map(..)`                    | `withColumn(..)`              |                    
# MAGIC | `mapPartitions(..)`          | `withColumnRenamed(..)`       |                     
# MAGIC | `orderBy(..)`                |                               |                     
# MAGIC | `randomSplit(..)`            |                               |                     
# MAGIC | `repartition(..)`            |                               |                     
# MAGIC | `sample(..)`                 |                               |                     
# MAGIC | `select(..)`                 |                               |                     
# MAGIC | `sort(..)`                   |                               |                     
# MAGIC | `sortWithinPartitions(..)`   |                               |                     
# MAGIC | `union(..)`                  |                               |                     
# MAGIC | `where(..)`                  |                               |                     

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Wide vs. Narrow Transformations
# MAGIC 
# MAGIC Regardless of language, transformations break down into two broad categories: wide and narrow.
# MAGIC 
# MAGIC **Narrow Transformations**: The data required to compute the records in a single partition reside in at most one partition of the parent RDD.
# MAGIC 
# MAGIC Examples include:
# MAGIC * `filter(..)`
# MAGIC * `drop(..)`
# MAGIC * `coalesce()`
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/105/transformations-narrow.png" alt="Narrow Transformations" style="height:300px"/>
# MAGIC 
# MAGIC <br/>
# MAGIC 
# MAGIC **Wide Transformations**: The data required to compute the records in a single partition may reside in many partitions of the parent RDD. 
# MAGIC 
# MAGIC Examples include:
# MAGIC * `distinct()` 
# MAGIC * `groupBy(..).sum()` 
# MAGIC * `repartition(n)` 
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/105/transformations-wide.png" alt="Wide Transformations" style="height:300px"/>

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Shuffles
# MAGIC 
# MAGIC A shuffle operation is triggered when data needs to move between executors.
# MAGIC 
# MAGIC For example, to group by color, it will serve us best if...
# MAGIC   * All the reds are in one partitions
# MAGIC   * All the blues are in a second partition
# MAGIC   * All the greens are in a third
# MAGIC 
# MAGIC From there we can easily sum/count/average all of the reds, blues, and greens.
# MAGIC 
# MAGIC To carry out the shuffle operation Spark needs to
# MAGIC * Convert the data to the UnsafeRow (if it isn't already), commonly refered to as Tungsten Binary Format.
# MAGIC * Write that data to disk on the local node - at this point the slot is free for the next task.
# MAGIC * Send that data across the wire to another executor
# MAGIC   * Technically the Driver decides which executor gets which piece of data.
# MAGIC   * Then the executor pulls the data it needs from the other executor's shuffle files.
# MAGIC * Copy the data back into RAM on the new executor
# MAGIC   * The concept, if not the action, is just like the initial read "every" `DataFrame` starts with.
# MAGIC   * The main difference being it's the 2nd+ stage.
# MAGIC 
# MAGIC As we will see in a moment, this amounts to a free cache from what is effectively temp files.
# MAGIC 
# MAGIC ** *Note:* ** *Some actions induce in a shuffle.*<br/>
# MAGIC *Good examples would include the operations `count()` and `reduce(..)`.*

# COMMAND ----------

# MAGIC %md
# MAGIC ### UnsafeRow (aka Tungsten Binary Format)
# MAGIC 
# MAGIC As a quick side note, the data that is "shuffled" is in a format known as `UnsafeRow`, or more commonly, the Tungsten Binary Format.
# MAGIC 
# MAGIC `UnsafeRow` is the in-memory storage format for Spark SQL, DataFrames & Datasets. 
# MAGIC 
# MAGIC Advantages include:
# MAGIC 
# MAGIC * Compactness: 
# MAGIC   * Column values are encoded using custom encoders, not as JVM objects (as with RDDs). 
# MAGIC   * The benefit of using Spark 2.x's custom encoders is that you get almost the same compactness as Java serialization, but significantly faster encoding/decoding speeds. 
# MAGIC   * Also, for custom data types, it is possible to write custom encoders from scratch.
# MAGIC 
# MAGIC * Efficiency: Spark can operate _directly out of Tungsten_, without first deserializing Tungsten data into JVM objects.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center"><img src="https://files.training.databricks.com/images/unsafe-row-format.png"></div>

# COMMAND ----------

# MAGIC %md
# MAGIC ### How UnsafeRow works
# MAGIC * The first field, "123", is stored in place as its primitive.
# MAGIC * The next 2 fields, "data" and "bricks", are strings and are of variable length. 
# MAGIC * An offset for these two strings is stored in place (32L and 48L respectively shown in the picture below).
# MAGIC * The data stored in these two offset’s are of format “length + data”. 
# MAGIC * At offset 32L, we store 4 + "data" and likewise at offset 48L we store 6 + "bricks".

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Lightning-fast Serialization with Encoders
# MAGIC <div style="text-align:center"><img src="https://files.training.databricks.com/images/encoders-vs-serialization-benchmark.png"></div>

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Stages
# MAGIC * When we shuffle data, it creates what is known as a stage boundary.
# MAGIC * Stage boundaries represent a process bottleneck.
# MAGIC 
# MAGIC Take for example the following transformations:
# MAGIC 
# MAGIC |Step |Transformation|
# MAGIC |----:|--------------|
# MAGIC | 1   | Read    |
# MAGIC | 2   | Select  |
# MAGIC | 3   | Filter  |
# MAGIC | 4   | GroupBy |
# MAGIC | 5   | Select  |
# MAGIC | 6   | Filter  |
# MAGIC | 7   | Write   |
# MAGIC   

# COMMAND ----------

# MAGIC %md
# MAGIC Spark will break this one job into two stages (steps 1-4b and steps 4c-8):
# MAGIC 
# MAGIC **Stage #1**
# MAGIC 
# MAGIC |Step |Transformation|
# MAGIC |----:|--------------|
# MAGIC | 1   | Read |
# MAGIC | 2   | Select |
# MAGIC | 3   | Filter |
# MAGIC | 4a | GroupBy 1/2 |
# MAGIC | 4b | shuffle write |
# MAGIC 
# MAGIC **Stage #2**
# MAGIC 
# MAGIC |Step |Transformation|
# MAGIC |----:|--------------|
# MAGIC | 4c | shuffle read |
# MAGIC | 4d | GroupBy  2/2 |
# MAGIC | 5   | Select |
# MAGIC | 6   | Filter |
# MAGIC | 7   | Write |

# COMMAND ----------

# MAGIC %md
# MAGIC In **Stage #1**, Spark will create a pipeline of transformations in which the data is read into RAM (Step #1), and then perform steps #2, #3, #4a & #4b
# MAGIC 
# MAGIC All partitions must complete **Stage #1** before continuing to **Stage #2**
# MAGIC * It's not possible to group all records across all partitions until every task is completed.
# MAGIC * This is the point at which all the tasks must synchronize.
# MAGIC * This creates our bottleneck.
# MAGIC * Besides the bottleneck, this is also a significant performance hit: disk IO, network IO and more disk IO.
# MAGIC 
# MAGIC Once the data is shuffled, we can resume execution...
# MAGIC 
# MAGIC For **Stage #2**, Spark will again create a pipeline of transformations in which the shuffle data is read into RAM (Step #4c) and then perform transformations #4d, #5, #6 and finally the write action, step #7.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Pipelining
# MAGIC <img src="https://files.training.databricks.com/images/pipelining-2.png" style="float: right"/>
# MAGIC 
# MAGIC <ul>
# MAGIC   <li>Pipelining is the idea of executing as many operations as possible on a single partition of data.</li>
# MAGIC   <li>Once a single partition of data is read into RAM, Spark will combine as many narrow operations as it can into a single **Task**</li>
# MAGIC   <li>Wide operations force a shuffle, conclude, a stage and end a pipeline.</li>
# MAGIC   <li>Compare to MapReduce where: </li>
# MAGIC   <ol>
# MAGIC     <li>Data is read from disk</li>
# MAGIC     <li>A single transformation takes place</li>
# MAGIC     <li>Data is written to disk</li>
# MAGIC     <li>Repeat steps 1-3 until all transformations are completed</li>
# MAGIC   </ol>
# MAGIC   <li>By avoiding all the extra network and disk IO, Spark can easily out perform traditional MapReduce applications.</li>
# MAGIC </ul>
# MAGIC   

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Lineage
# MAGIC From the developer's perspective, we start with a read and conclude (in this case) with a write
# MAGIC 
# MAGIC |Step |Transformation|
# MAGIC |----:|--------------|
# MAGIC | 1   | Read    |
# MAGIC | 2   | Select  |
# MAGIC | 3   | Filter  |
# MAGIC | 4   | GroupBy |
# MAGIC | 5   | Select  |
# MAGIC | 6   | Filter  |
# MAGIC | 7   | Write   |
# MAGIC   
# MAGIC However, Spark starts with the action (`write(..)` in this case).
# MAGIC 
# MAGIC Next, it asks the question, what do I need to do first?
# MAGIC 
# MAGIC It then proceeds to determine which transformation precedes this step until it identifies the first transformation.
# MAGIC 
# MAGIC |Step |Transformation| |
# MAGIC |----:|--------------|-|
# MAGIC | 7   | Write   | Depends on #6 |
# MAGIC | 6   | Filter  | Depends on #5 |
# MAGIC | 5   | Select  | Depends on #4 |
# MAGIC | 4   | GroupBy | Depends on #3 |
# MAGIC | 3   | Filter  | Depends on #2 |
# MAGIC | 2   | Select  | Depends on #1 |
# MAGIC | 1   | Read    | First |
# MAGIC 
# MAGIC This would be equivalent to understanding your own lineage. 
# MAGIC * You don't ask if you are related to Genghis Khan and then work through the ancestry of all his children (5% of all people in Asia).
# MAGIC * You start with your mother.
# MAGIC * Then your grandmother
# MAGIC * Then your great-grandmother
# MAGIC * ... and so on
# MAGIC * Until you discover you are actually related to Catherine Parr, the last queen of Henry the VIII.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Why Work Backwards?
# MAGIC **Question:** So what is the benefit of working backward through your action's lineage?<br/>
# MAGIC **Answer:** It allows Spark to determine if it is necessary to execute every transformation.
# MAGIC 
# MAGIC Take another look at our example:
# MAGIC * Say we've executed this once already
# MAGIC * On the first execution, step #4 resulted in a shuffle
# MAGIC * Those shuffle files are on the various executors (src & dst)
# MAGIC * Because the transformations are immutable, no aspect of our lineage can change.
# MAGIC * That means the results of our last shuffle (if still available) can be reused.
# MAGIC 
# MAGIC |Step |Transformation| |
# MAGIC |----:|--------------|-|
# MAGIC | 7   | Write   | Depends on #6 |
# MAGIC | 6   | Filter  | Depends on #5 |
# MAGIC | 5   | Select  | Depends on #4 |
# MAGIC | 4   | GroupBy | <<< shuffle |
# MAGIC | 3   | Filter  | *don't care* |
# MAGIC | 2   | Select  | *don't care* |
# MAGIC | 1   | Read    | *don't care* |
# MAGIC 
# MAGIC In this case, what we end up executing is only the operations from **Stage #2**.
# MAGIC 
# MAGIC This saves us the initial network read and all the transformations in **Stage #1**
# MAGIC 
# MAGIC |Step |Transformation|   |
# MAGIC |----:|---------------|:-:|
# MAGIC | 1   | Read          | *skipped* |
# MAGIC | 2   | Select        | *skipped* |
# MAGIC | 3   | Filter        | *skipped* |
# MAGIC | 4a  | GroupBy 1/2   | *skipped* |
# MAGIC | 4b  | shuffle write | *skipped* |
# MAGIC | 4c  | shuffle read  | - |
# MAGIC | 4d  | GroupBy  2/2  | - |
# MAGIC | 5   | Select        | - |
# MAGIC | 6   | Filter        | - |
# MAGIC | 7   | Write         | - |

# COMMAND ----------

# MAGIC %md
# MAGIC ### And Caching...
# MAGIC 
# MAGIC The reuse of shuffle files (aka our temp files) is just one example of Spark optimizing queries anywhere it can.
# MAGIC 
# MAGIC We cannot assume this will be available to us. 
# MAGIC 
# MAGIC Shuffle files are by definition temporary files and will eventually be removed.
# MAGIC 
# MAGIC However, we cache data to explicitly accomplish the same thing that happens inadvertently with shuffle files.
# MAGIC 
# MAGIC In this case, the lineage plays the same role. Take for example:
# MAGIC 
# MAGIC |Step |Transformation| |
# MAGIC |----:|--------------|-|
# MAGIC | 7   | Write   | Depends on #6 |
# MAGIC | 6   | Filter  | Depends on #5 |
# MAGIC | 5   | Select  | <<< cache |
# MAGIC | 4   | GroupBy | <<< shuffle files |
# MAGIC | 3   | Filter  | ? |
# MAGIC | 2   | Select  | ? |
# MAGIC | 1   | Read    | ? |
# MAGIC 
# MAGIC In this case we cached the result of the `select(..)`. 
# MAGIC 
# MAGIC We never even get to the part of the lineage that involves the shuffle, let alone **Stage #1**.
# MAGIC 
# MAGIC Instead, we pick up with the cache and resume execution from there:
# MAGIC 
# MAGIC |Step |Transformation|   |
# MAGIC |----:|---------------|:-:|
# MAGIC | 1   | Read          | *skipped* |
# MAGIC | 2   | Select        | *skipped* |
# MAGIC | 3   | Filter        | *skipped* |
# MAGIC | 4a  | GroupBy 1/2   | *skipped* |
# MAGIC | 4b  | shuffle write | *skipped* |
# MAGIC | 4c  | shuffle read  | *skipped* |
# MAGIC | 4d  | GroupBy  2/2  | *skipped* |
# MAGIC | 5a  | cache read    | - |
# MAGIC | 5b  | Select        | - |
# MAGIC | 6   | Filter        | - |
# MAGIC | 7   | Write         | - |

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/labs.png) Transformations & Actions Lab
# MAGIC It's time to put what we learned to practice.
# MAGIC 
# MAGIC Go ahead and open the notebook [Transformations And Actions Lab]($./Transformations And Actions Lab) and complete the exercises.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>