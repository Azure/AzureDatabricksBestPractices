# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC #Partitioning
# MAGIC 
# MAGIC ** Data Source **
# MAGIC * English Wikipedia pageviews by second
# MAGIC * Size on Disk: ~255 MB
# MAGIC * Type: Tab Separated Text File & Parquet files
# MAGIC * More Info: <a href="https://old.datahub.io/dataset/english-wikipedia-pageviews-by-second" target="_blank">https&#58;old.datahub.io/dataset/english-wikipedia-pageviews-by-second</a>
# MAGIC 
# MAGIC **Technical Accomplishments:**
# MAGIC * Understand the relationship between partitions and slots/cores
# MAGIC * Review `repartition(n)` and `coalesce(n)`
# MAGIC * Review one key side effect of shuffle partitions

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Getting Started
# MAGIC 
# MAGIC Run the following cell to configure our "classroom."

# COMMAND ----------

# MAGIC %run "../Includes/Classroom Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) **The Data Source**
# MAGIC 
# MAGIC This data uses the **Pageviews By Seconds** data set.
# MAGIC 
# MAGIC The file is located on the DBFS at **dbfs:/mnt/training/wikipedia/pageviews/pageviews_by_second.tsv**.

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# Use a schema to avoid the overhead of inferring the schema
# In the case of CSV/TSV it requires a full scan of the file.
schema = StructType(
  [
    StructField("timestamp", StringType(), False),
    StructField("site", StringType(), False),
    StructField("requests", IntegerType(), False)
  ]
)

fileName = "/mnt/training/wikipedia/pageviews/pageviews_by_second.tsv"

# Create our initial DataFrame
initialDF = (spark.read
  .option("header", "true")
  .option("sep", "\t")
  .schema(schema)
  .csv(fileName)
)

# COMMAND ----------

# MAGIC %md
# MAGIC We can see below that our data consists of...
# MAGIC * when the record was created
# MAGIC * the site (mobile or desktop) 
# MAGIC * and the number of requests
# MAGIC 
# MAGIC This amounts to one record per site, per second, and captures the number of requests made in that one second. 
# MAGIC 
# MAGIC That means for every second of the day, there are two records.

# COMMAND ----------

display(initialDF)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) My First Steps
# MAGIC 
# MAGIC Before processing any data, there are normally several steps to simply prepare the data for analysis such as
# MAGIC 0. <div style="text-decoration:line-through">Read the data in</div>
# MAGIC 0. Balance the number of partitions to the number of slots
# MAGIC 0. Cache the data
# MAGIC 0. Adjust the `spark.sql.shuffle.partitions`
# MAGIC 0. Perform some basic ETL (ie convert strings to timestamp)
# MAGIC 0. Possibly re-cache the data if the ETL was costly

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Partitions vs Slots
# MAGIC 
# MAGIC * We have our `initialDF` (**Step #1**) which amounts to nothing more than reading in the data.
# MAGIC * For **Step #2** we have to ask the question, what is the relationship between partitions and slots.
# MAGIC 
# MAGIC 
# MAGIC ** *Note:* ** *The Spark API uses the term **core** meaning a thread available for parallel execution.*<br/>*Here we refer to it as **slot** to avoid confusion with the number of cores in the underlying CPU(s)*<br/>*to which there isn't necessarily an equal number.*

# COMMAND ----------

# MAGIC %md
# MAGIC ### Slots/Cores
# MAGIC 
# MAGIC In most cases, if you created your cluster, you should know how many cores you have.
# MAGIC 
# MAGIC However, to check programatically, you can use `SparkContext.defaultParallelism`
# MAGIC 
# MAGIC For more information, see the doc <a href="https://spark.apache.org/docs/latest/configuration.html#execution-behavior" target="_blank">Spark Configuration, Execution Behavior</a>
# MAGIC > For operations like parallelize with no parent RDDs, it depends on the cluster manager:
# MAGIC > * Local mode: number of cores on the local machine
# MAGIC > * Mesos fine grained mode: 8
# MAGIC > * **Others: total number of cores on all executor nodes or 2, whichever is larger**

# COMMAND ----------

cores = sc.defaultParallelism

print("You have {} cores, or slots.".format(cores))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Partitions
# MAGIC 
# MAGIC * The second 1/2 of this question is how many partitions of data do I have?
# MAGIC * With that we have two subsequent questions:
# MAGIC   0. Why do I have that many?
# MAGIC   0. What is a partition?
# MAGIC 
# MAGIC For the last question, a **partition** is a small piece of the total data set.
# MAGIC 
# MAGIC Google defines it like this:
# MAGIC > the action or state of dividing or being divided into parts.
# MAGIC 
# MAGIC If our goal is to process all our data (say 1M records) in parallel, we need to divide that data up.
# MAGIC 
# MAGIC If I have 8 **slots** for parallel execution, it would stand to reason that I want 1M / 8 or 125,000 records per partition.

# COMMAND ----------

# MAGIC %md
# MAGIC Back to the first question, we can answer it by running the following command which
# MAGIC * takes the `initialDF`
# MAGIC * converts it to an `RDD`
# MAGIC * and then asks the `RDD` for the number of partitions

# COMMAND ----------

partitions = initialDF.rdd.getNumPartitions()
print("Partitions: {0:,}".format( partitions ))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC * It is **NOT** coincidental that we have **8 slots** and **8 partitions**
# MAGIC * In Spark 2.0 a lot of optimizations have been added to the readers.
# MAGIC * Namely the readers looks at **the number of slots**, the **size of the data**, and makes a best guess at how many partitions **should be created**.
# MAGIC * You can actually double the size of the data several times over and Spark will still read in **only 8 partitions**.
# MAGIC * Eventually it will get so big that Spark will forgo optimization and read it in as 10 partitions, in that case.
# MAGIC 
# MAGIC But 8 partitions and 8 slots is just too easy.
# MAGIC   * Let's read in another copy of this same data.
# MAGIC   * A parquet file that was saved in 5 partitions.
# MAGIC   * This gives us an excuse to reason about the **relationship between slots and partitions**

# COMMAND ----------

# Create our initial DataFrame. We can let it infer the 
# schema because the cost for parquet files is really low.
alternateDF = (spark.read
  .parquet("/mnt/training/wikipedia/pageviews/pageviews_by_second.parquet")
)

print("Partitions: {0:,}".format( alternateDF.rdd.getNumPartitions() ))

# COMMAND ----------

# MAGIC %md
# MAGIC Now that we have only 5 partitions we have to ask...
# MAGIC 
# MAGIC What is going to happen when I perform and action like `count()` **with 8 slots and only 5 partitions?**

# COMMAND ----------

alternateDF.count()

# COMMAND ----------

# MAGIC %md
# MAGIC **Question #1:** Is it OK to let my code continue to run this way?
# MAGIC 
# MAGIC **Question #2:** What if it was a **REALLY** big file that read in as **200 partitions** and we had **256 slots**?
# MAGIC 
# MAGIC **Question #3:** What if it was a **REALLY** big file that read in as **200 partitions** and we had only **8 slots**, how long would it take compared to a dataset that has only 8 partitions?
# MAGIC 
# MAGIC **Question #4:** Given the previous example (**200 partitions** vs **8 slots**) what are our options (given that we cannot increase the number of partitions)?

# COMMAND ----------

# MAGIC %md
# MAGIC ### Use Every Slot/Core
# MAGIC 
# MAGIC With some very few exceptions, you always want the number of partitions to be **a factor of the number of slots**.
# MAGIC 
# MAGIC That way **every slot is used**.
# MAGIC 
# MAGIC That is, every slots is being assigned a task.
# MAGIC 
# MAGIC With 5 partitions & 8 slots we are **under-utilizing three of the eight slots**.
# MAGIC 
# MAGIC With 9 partitions & 8 slots we just guaranteed our **job will take 2x** as long as it may need to.
# MAGIC * 10 seconds, for example, to process the first 8.
# MAGIC * Then as soon as one of the first 8 is done, another 10 seconds to process the last partition.

# COMMAND ----------

# MAGIC %md
# MAGIC ### More or Less Partitions?
# MAGIC 
# MAGIC As a **general guideline** it is advised that each partition (when cached) is roughly around 200MB.
# MAGIC * Size on disk is not a good gauge. For example...
# MAGIC * CSV files are large on disk but small in RAM - consider the string "12345" which is 10 bytes compared to the integer 12345 which is only 4 bytes.
# MAGIC * Parquet files are highly compressed but uncompressed in RAM.
# MAGIC * In a relational database... well... who knows?
# MAGIC 
# MAGIC The **200 comes from** the real-world-experience of Databricks's engineers and is **based largely on efficiency** and not so much resource limitations. 
# MAGIC 
# MAGIC On an executor with a reduced amount of RAM (such as our CE JVMs with 6GB) you might need to lower that.
# MAGIC 
# MAGIC For example, at 8 partitions (corresponding to our max number of slots) & 200MB per partition
# MAGIC * That will use roughly **1.5GB**
# MAGIC * We **might** get away with that on CE.
# MAGIC * If you have transformations that balloon the data size (such as Natural Language Processing) you are sure to run into problems.
# MAGIC 
# MAGIC **Question:** If I read in my data and it comes in as 10 partitions should I...
# MAGIC * reduce my partitions down to 8 (1x number of slots)
# MAGIC * or increase my partitions up to 16 (2x number of slots)
# MAGIC 
# MAGIC **Answer:** It depends on the size of each partition
# MAGIC * Read the data in. 
# MAGIC * Cache it. 
# MAGIC * Look at the size per partition.
# MAGIC * If you are near or over 200MB consider increasing the number of partitions.
# MAGIC * If you are under 200MB consider decreasing the number of partitions.
# MAGIC 
# MAGIC The goal will **ALWAYS** be to use as few partitions as possible while maintaining at least 1 x number-of-slots.

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) repartition(n) or coalesce(n)
# MAGIC 
# MAGIC We have two operations that can help address this problem: `repartition(n)` and `coalesce(n)`.
# MAGIC 
# MAGIC If you look at the API docs, `coalesce(n)` is described like this:
# MAGIC > Returns a new Dataset that has exactly numPartitions partitions, when fewer partitions are requested.<br/>
# MAGIC > If a larger number of partitions is requested, it will stay at the current number of partitions.
# MAGIC 
# MAGIC If you look at the API docs, `repartition(n)` is described like this:
# MAGIC > Returns a new Dataset that has exactly numPartitions partitions.
# MAGIC 
# MAGIC The key differences between the two are
# MAGIC * `coalesce(n)` is a **narrow** transformation and can only be used to reduce the number of partitions.
# MAGIC * `repartition(n)` is a **wide** transformation and can be used to reduce or increase the number of partitions.
# MAGIC 
# MAGIC So, if I'm increasing the number of partitions I have only one choice: `repartition(n)`
# MAGIC 
# MAGIC If I'm reducing the number of partitions I can use either one, so how do I decide?
# MAGIC * First off, `coalesce(n)` is a **narrow** transformation and performs better because it avoids a shuffle.
# MAGIC * However, `coalesce(n)` cannot guarantee even **distribution of records** across all partitions.
# MAGIC * For example, with `coalesce(n)` you might end up with **a few partitions containing 80%** of all the data.
# MAGIC * On the other hand, `repartition(n)` will give us a relatively **uniform distribution**.
# MAGIC * And `repartition(n)` is a **wide** transformation meaning we have the added cost of a **shuffle operation**.
# MAGIC 
# MAGIC In our case, we "need" to go form 5 partitions up to 8 partitions - our only option here is `repartition(n)`. 

# COMMAND ----------

repartitionedDF = alternateDF.repartition(8)

print("Partitions: {0:,}".format( repartitionedDF.rdd.getNumPartitions() ))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Cache, Again?
# MAGIC 
# MAGIC Back to list...
# MAGIC 0. <div style="text-decoration:line-through">Read the data in</div>
# MAGIC 0. <div style="text-decoration:line-through">Balance the number of partitions to the number of slots</div>
# MAGIC 0. Cache the data
# MAGIC 0. Adjust the `spark.sql.shuffle.partitions`
# MAGIC 0. Perform some basic ETL (i.e., convert strings to timestamp)
# MAGIC 0. Possibly re-cache the data if the ETL was costly
# MAGIC 
# MAGIC We just balanced the number of partitions to the number of slots.
# MAGIC 
# MAGIC Depending on the size of the data and the number of partitions, the shuffle operation can be fairly expensive (though necessary).
# MAGIC 
# MAGIC Let's cache the result of the `repartition(n)` call..
# MAGIC * Or more specifically, let's mark it for caching.
# MAGIC * The actual cache will occur later once an action is performed
# MAGIC * Or you could just execute a count to force materialization of the cache.

# COMMAND ----------

repartitionedDF.cache()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) spark.sql.shuffle.partitions
# MAGIC 
# MAGIC 0. <div style="text-decoration:line-through">Read the data in</div>
# MAGIC 0. <div style="text-decoration:line-through">Balance the number of partitions to the number of slots</div>
# MAGIC 0. <div style="text-decoration:line-through">Cache the data</div>
# MAGIC 0. Adjust the `spark.sql.shuffle.partitions`
# MAGIC 0. Perform some basic ETL (i.e., convert strings to timestamp)
# MAGIC 0. Possibly re-cache the data if the ETL was costly
# MAGIC 
# MAGIC The next problem has to do with a side effect of certain **wide** transformations.
# MAGIC 
# MAGIC So far, we haven't hit any **wide** transformations other than `repartition(n)`
# MAGIC * But eventually we will... 
# MAGIC * Let's illustrate the problem that we will **eventually** hit
# MAGIC * We can do this by simply sorting our data.

# COMMAND ----------

(repartitionedDF
  .orderBy(col("timestamp"), col("site")) # sort the data
   .foreach(lambda x: None)               # litterally does nothing except trigger a job
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Quick Detour
# MAGIC Something isn't right here...
# MAGIC * We only executed one action.
# MAGIC * But two jobs were triggered.
# MAGIC * If we look at the physical plan we can see the reason for the extra job.
# MAGIC * The answer lies in the step **Exchange rangepartitioning**

# COMMAND ----------

(repartitionedDF
  .orderBy(col("timestamp"), col("site"))
  .explain()
)
print("-"*80)

(repartitionedDF
  .orderBy(col("timestamp"), col("site"))
  .limit(3000000)
  .explain()
)
print("-"*80)

# COMMAND ----------

# MAGIC %md
# MAGIC And just to prove that the extra job is due to the number of records in our DataFrame, re-run it with only 3M records:

# COMMAND ----------

(repartitionedDF
  .orderBy(col("timestamp"), col("site")) # sort the data
  .limit(3000000)                         # only 3 million please    
  .foreach(lambda x: None)                # litterally does nothing except trigger a job
)

# COMMAND ----------

# MAGIC %md
# MAGIC Only 1 job.
# MAGIC 
# MAGIC Spark's Catalyst Optimizer is optimizing our jobs for us!

# COMMAND ----------

# MAGIC %md
# MAGIC ### The Real Problem
# MAGIC 
# MAGIC Back to the original issue...
# MAGIC * Rerun the original job (below).
# MAGIC * Take a look at the second job.
# MAGIC * Look at the 3rd Stage.
# MAGIC * Notice that it has 200 partitions!
# MAGIC * And this is our problem.

# COMMAND ----------

funkyDF = (repartitionedDF
  .orderBy(col("timestamp"), col("site")) # sorts the data
)                                         #
funkyDF.foreach(lambda x: None)           # litterally does nothing except trigger a job

# COMMAND ----------

# MAGIC %md
# MAGIC The problem is the number of partitions we ended up with.
# MAGIC 
# MAGIC Besides looking at the number of tasks in the final stage, we can simply print out the number of partitions

# COMMAND ----------

print("Partitions: {0:,}".format( funkyDF.rdd.getNumPartitions() ))

# COMMAND ----------

# MAGIC %md
# MAGIC The engineers building Apache Spark chose a default value, 200, for the new partition size.
# MAGIC 
# MAGIC After all our work to determine the right number of partitions they go and undo it on us.
# MAGIC 
# MAGIC The value 200 is actually based on practical experience, attempting to account for the most common scenarios to date.
# MAGIC 
# MAGIC Work is being done to intelligently determine this new value but that is still in progress.
# MAGIC 
# MAGIC For now, we can tweak it with the configuration value `spark.sql.shuffle.partitions`
# MAGIC 
# MAGIC We can see below that it is actually configured for 200 partitions

# COMMAND ----------

spark.conf.get("spark.sql.shuffle.partitions")

# COMMAND ----------

# MAGIC %md
# MAGIC We can change the config setting with the following command

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", "8")

# COMMAND ----------

# MAGIC %md
# MAGIC Now, if we re-run our query, we will see that we end up with the 8 partitions we want post-shuffle.

# COMMAND ----------

betterDF = (repartitionedDF
  .orderBy(col("timestamp"), col("site")) # sort the data
)                                         #
betterDF.foreach(lambda x: None)          # litterally does nothing except trigger a job

print("Partitions: {0:,}".format( betterDF.rdd.getNumPartitions() ))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Initial ETL
# MAGIC 
# MAGIC 0. <div style="text-decoration:line-through">Read the data in</div>
# MAGIC 0. <div style="text-decoration:line-through">Balance the number of partitions to the number of slots</div>
# MAGIC 0. <div style="text-decoration:line-through">Cache the data</div>
# MAGIC 0. <div style="text-decoration:line-through">Adjust the `spark.sql.shuffle.partitions`</div>
# MAGIC 0. Perform some basic ETL (i.e., convert strings to timestamp)
# MAGIC 0. Possibly re-cache the data if the ETL was costly
# MAGIC 
# MAGIC We may have some standard ETL.
# MAGIC 
# MAGIC In this case we will want to do something like convert the `timestamp` column from a **string** to a data type more appropriate for **date & time**.
# MAGIC 
# MAGIC We are not going to do that here, instead will will cover that specific case in a future notebook when we look at all the date & time functions.
# MAGIC 
# MAGIC But so as to not leave you in suspense...

# COMMAND ----------

pageviewsDF = (repartitionedDF
  .select(
    unix_timestamp( col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss").cast("timestamp").alias("createdAt"), 
    col("site"), 
    col("requests") 
  )
)

print("****BEFORE****")
repartitionedDF.printSchema()

print("****AFTER****")
pageviewsDF.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC And assuming that initial ETL was expensive... we would want to finish up by caching our final `DataFrame`

# COMMAND ----------

# mark it as cached.
pageviewsDF.cache() 

# materialize the cache.
pageviewsDF.count() 

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) All Done
# MAGIC 
# MAGIC 0. <div style="text-decoration:line-through">Read the data in</div>
# MAGIC 0. <div style="text-decoration:line-through">Balance the number of partitions to the number of slots</div>
# MAGIC 0. <div style="text-decoration:line-through">Cache the data</div>
# MAGIC 0. <div style="text-decoration:line-through">Adjust the `spark.sql.shuffle.partitions`</div>
# MAGIC 0. <div style="text-decoration:line-through">Perform some basic ETL (i.e., convert strings to timestamp)</div>
# MAGIC 0. <div style="text-decoration:line-through">Possibly re-cache the data if the ETL was costly</div>

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/labs.png) Partitioning Lab
# MAGIC It's time to put what we learned to practice.
# MAGIC 
# MAGIC Go ahead and open the notebook [Partitioning Lab]($./Partitioning Lab) and complete the exercises.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>