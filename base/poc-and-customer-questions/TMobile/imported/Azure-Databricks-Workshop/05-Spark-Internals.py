# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://cdn2.hubspot.net/hubfs/438089/docs/training/dblearning-banner.png" alt="Databricks Learning" width="555" height="64">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spark Internals

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## The Driver and Executors
# MAGIC 
# MAGIC Broadly speaking, a Spark application is broken into two areas of responsibility:
# MAGIC 
# MAGIC 1. The **Driver**, a single JVM process, is responsible for analyzing your Spark program, optimizing your DataFrame queries, determining how your Spark jobs should
# MAGIC    be parallelized, and distributing the work.
# MAGIC 2. The **Executors**, which actually perform the distributed work.
# MAGIC 
# MAGIC Each Spark program has a single Driver and one or more Executors.
# MAGIC 
# MAGIC <img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/spark-shell-driver.png" style="border: 1px solid #aaa; float: right; margin-left: 20px"/>
# MAGIC 
# MAGIC Consider the diagram to the right. In this example:
# MAGIC 
# MAGIC * The `spark-shell` interpreter (Scala) or `pyspark` (Python) interpreter is running on a laptop and connected to a cluster. In this case,
# MAGIC   the interpreter is the Driver.
# MAGIC   
# MAGIC * There are three nodes in this cluster, and the Driver has been allocated an Executor on each node.
# MAGIC 
# MAGIC The user enters commands (like DataFrame or SQL queries) in the interpreter. 
# MAGIC 
# MAGIC The **Driver** (on the laptop):
# MAGIC 
# MAGIC * analyzes the queries
# MAGIC * optimizes them
# MAGIC * "compiles" them down to RDDs
# MAGIC * determines how the work is to be parallelized (e.g., how the file is to be partitioned and which Executors get which partitions)
# MAGIC * breaks each Spark job into stages of execution
# MAGIC * directs the work of the Executors
# MAGIC 
# MAGIC The **Executors** on each node:
# MAGIC 
# MAGIC * read the data from the file
# MAGIC * use one or more threads to process the partitions they are responsible for reading
# MAGIC * send shuffled data to each other
# MAGIC * in short, do the actual work on the data

# COMMAND ----------

# MAGIC %md
# MAGIC ### The Cluster Manager
# MAGIC 
# MAGIC There are three cluster managers:
# MAGIC 
# MAGIC * **Spark Standalone**: Designed to run _only_ Spark jobs, this cluster manager is the simplest to configure.
# MAGIC * **YARN**: Designed for Hadoop jobs, YARN will also run Spark jobs. While a little more complex, it's ideal for shops that need to run both Spark and Hadoop jobs.
# MAGIC * **Mesos**: Designed to run pretty much any kind of distributed jobs. Somewhat complicated to configure, but highly flexible.
# MAGIC 
# MAGIC #### The _Master_
# MAGIC 
# MAGIC In all cases, the Driver's interface to a cluster manager is a _Master_.
# MAGIC 
# MAGIC * The Driver comes up and requests resources (Executors) from a master process.
# MAGIC * No matter what kind of cluster it is, the master always looks the same to Spark.
# MAGIC * The master uses the resources of the cluster manager to allocate Executors for the Driver.
# MAGIC * Each Executor, when it comes up, connects back to the Driver. At that point, all communication is directly between the Driver and its Executors.
# MAGIC * The cluster manager is responsible for allocating resources and ensuring that they are restarted on failure.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Local Mode
# MAGIC 
# MAGIC Spark can also run in a special non-connected mode called _local mode_. 
# MAGIC 
# MAGIC When you fire up `spark-shell` or `pyspark` without specifying a `--master` argument, you are running in local mode. 
# MAGIC 
# MAGIC In local mode, there are no Executor processes. Instead, the Driver also acts as (a single) Executor.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Units of Parallelism
# MAGIC 
# MAGIC <img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/executors-and-threads.png" style="border: 1px solid #aaa; float: right; margin-left: 20px"/>
# MAGIC 
# MAGIC There are, then, two units of parallelism:
# MAGIC 
# MAGIC * Work is distributed, across the cluster, to Executor _processes_ that can run in parallel.
# MAGIC * _Within_ each executor process, there are typically multiple _threads_ that can also run in parallel (depending on how many physical CPU cores are available).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scheduling: Jobs, Stages and Tasks
# MAGIC 
# MAGIC ### Terminology
# MAGIC 
# MAGIC In Spark:
# MAGIC 
# MAGIC * When we speak about a **job**, we're generally referring to the distributed execution that Spark launches when the Driver executes an _action_.
# MAGIC * A job consists of multiple **stages**, defined by _shuffle boundaries_. Stages affect scheduling.
# MAGIC * A **task** is something Spark does in a single thread. A typical task is processing a single partition.

# COMMAND ----------

# MAGIC %md
# MAGIC Let's start by reading in our data.

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup

# COMMAND ----------

df = (spark
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", ":")
      .csv(blobStoreBaseURL + "people-with-header-10m.txt"))

# COMMAND ----------

# MAGIC %md
# MAGIC Now let's select all the records where the gender is female and salary is greater than $50,000

# COMMAND ----------

femaleDF = df.filter("gender = 'F' AND salary > 50000")

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Query Plan
# MAGIC We can take a look at the resulting plan Catalyst generated for us.

# COMMAND ----------

femaleDF.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC What is the `*` in front of those steps? Let's dive into Whole-stage code gen!

# COMMAND ----------

# MAGIC %md
# MAGIC ## Whole-stage Code Generation
# MAGIC 
# MAGIC Whole-stage Code Generation fuses multiple DataFrame operations together into a single Java function, generated on the fly, to improve execution performance. It collapses a query into a single optimized function, compiled with the embedded [Janino](https://janino-compiler.github.io/janino/) compiler, which helps optimize access to intermediate data. 
# MAGIC 
# MAGIC ### The old way of doing things
# MAGIC 
# MAGIC Prior to this feature, Spark used a general-purpose iterator-based query processing approach, commonly called the "Volcano model." This approach is general-purpose. To use the example from  
# MAGIC the [Databricks Blog post](https://databricks.com/blog/2016/05/23/apache-spark-as-a-compiler-joining-a-billion-rows-per-second-on-a-laptop.html) cited below, consider this query:
# MAGIC 
# MAGIC ```
# MAGIC -- SQL
# MAGIC SELECT count(*) FROM store_sales WHERE ss_item_sk = 1000
# MAGIC 
# MAGIC // Scala
# MAGIC store_sales_df.filter($"ss_item_sk" === 1000).select(count("*")).show()
# MAGIC 
# MAGIC # Python
# MAGIC store_sales_df.filter(col("ss_item_sk") == 1000).select(count("*")).show()
# MAGIC ```
# MAGIC 
# MAGIC Versions of Spark prior to 2.x would capture the `filter` operation (the `WHERE` clause) in a _predicate function_, and would then
# MAGIC evaluate the predicate on each row using general-purpose code something like this:
# MAGIC 
# MAGIC ```
# MAGIC class Filter(child: Operator, predicate: (Row => Boolean)) extends Operator {
# MAGIC   def next(): Row = {
# MAGIC     var current = child.next
# MAGIC     while (current == null || predicate(current))
# MAGIC       current = child.next
# MAGIC     return current;
# MAGIC   }
# MAGIC }
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### The new way
# MAGIC 
# MAGIC **But**... What would happen if, instead, Spark somehow wrote code _specific_ to that filter operation? Something,
# MAGIC perhaps, more like this:
# MAGIC 
# MAGIC ```
# MAGIC var count = 0
# MAGIC for (ss-item_sk <- store_sales) {
# MAGIC   if (ss_item_sk == 1000) count += 1
# MAGIC }
# MAGIC ```
# MAGIC 
# MAGIC This second piece of code is highly specific to the where clause condition. But, is it any faster?
# MAGIC 
# MAGIC The Spark team ran a simple benchmark comparing the two approaches, using a single thread against a Parquet file. The results are striking:
# MAGIC 
# MAGIC ![](https://s3-us-west-2.amazonaws.com/curriculum-release/images/volcano-vs-whole-stage-codegen.png)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Specific improvements
# MAGIC 
# MAGIC Hand-crafted, per-predicate code is faster, for a few reasons:
# MAGIC 
# MAGIC * **No virtual function dispatches**. If you're not familiar with virtual function dispatches, don't worry about it. All you need to know is that they are slower, because they
# MAGIC   involve address indirection. The hand-written (or whole stage code-generated) code doesn't have them
# MAGIC * **Intermediate data is in CPU registers, not memory.** See the blog post for details, but the short version is: If the intermediate data is in registers, memory fetches are elimiinated.
# MAGIC * **Other CPU optimizations**. Modern compilers and CPUs are _incredibly_ efficient at compiling and executing simple _for_ loops, like the second code. They're far less efficient at handling
# MAGIC   complex function call graphs, like the first version.
# MAGIC   
# MAGIC To quote the blog post:
# MAGIC   
# MAGIC > The key take-away here is that the **hand-written code is written specifically to run that query and nothing else, and as a result it can take advantage of all the information that is known**,
# MAGIC > leading to optimized code that eliminates virtual function dispatches, keeps intermediate data in CPU registers, and can be optimized by the underlying hardware.
# MAGIC 
# MAGIC **Note**: Whole stage code generation only works on certain parts of your query. The blog post outlines, in greater detail, how it works. 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### More information on whole stage code generation
# MAGIC 
# MAGIC Some additional references on Spark's whole-stage code generation:
# MAGIC 
# MAGIC * "Apache Spark as a Compiler: Joining a Billion Rows per Second on a Laptop: Deep dive into the new Tungsten execution engine," Databricks Blog, May 23, 2016. <https://databricks.com/blog/2016/05/23/apache-spark-as-a-compiler-joining-a-billion-rows-per-second-on-a-laptop.html>
# MAGIC * The short ["Whole-Stage Code Generation (aka Whole-Stage CodeGen)"](https://jaceklaskowski.gitbooks.io/mastering-apache-spark/spark-sql-whole-stage-codegen.html) chapter in Jacek Laskowski's _Mastering Spark_ GitBook.
# MAGIC * The Apache Spark team's [JIRA Epic](https://issues.apache.org/jira/browse/SPARK-12795) (SPARK-12795) that captures all the work done to implement this feature.

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Shuffle Partitions
# MAGIC 
# MAGIC Let's go ahead and `orderBy` into our query, and look at the resulting query plan.

# COMMAND ----------

firstNameDF = femaleDF.orderBy("firstName")

display(firstNameDF)

# COMMAND ----------

firstNameDF.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC Note that some operations are prefixed with `*`, such as `*Sort`, and some are not (`Exchange`). The ones with the asterisk prefixes are subject to whole stage code generation.
# MAGIC 
# MAGIC What exactly is that `Exchange rangepartitioning`? And the 200?

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Shuffles
# MAGIC 
# MAGIC A shuffle operation is triggered when data needs to move between executors.
# MAGIC 
# MAGIC In order to carry out the shuffle operation Spark needs to
# MAGIC * Convert the data to the Tungsten Binary Format (if it isn't already)
# MAGIC * Write that data to disk on the local node - at this point the slot is free for the next task.
# MAGIC * Send that data across the wire to another executor
# MAGIC   * Technically the Driver decides which executor gets which piece of data.
# MAGIC   * Then the executor pulls the data it needs from the other executor's shuffle files.
# MAGIC * Copy the data back into RAM on the new executor
# MAGIC   * The concept, if not the action, is just like the initial read "every" `DataFrame` starts with.
# MAGIC   * The main difference being it's the 2nd+ stage.
# MAGIC 
# MAGIC As we will see in a moment, this amounts to a free cache from what is effectively temp files.

# COMMAND ----------

firstNameDF.rdd.getNumPartitions()

# COMMAND ----------

# MAGIC %md
# MAGIC Whenever there is a shuffle of data across the network, the default for the new partition size is set to 200.
# MAGIC 
# MAGIC The value 200 is actually based on practical experience, attempting to account for the most common scenarios to date.
# MAGIC 
# MAGIC Work is being done to intelligently determine this new value but that is still in progress.
# MAGIC 
# MAGIC For now, we can tweak it with the configuration value `spark.sql.shuffle.partitions`
# MAGIC 
# MAGIC We can see below that it is actually configured for 200 partitions.

# COMMAND ----------

spark.conf.get("spark.sql.shuffle.partitions")

# COMMAND ----------

# MAGIC %md
# MAGIC We can change the config setting with the following command.

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", "16")

# COMMAND ----------

# MAGIC %md
# MAGIC Now, if we re-run our query, we will see that we end up with the 16 partitions we want post-shuffle.

# COMMAND ----------

firstNameDF = femaleDF.orderBy("firstName")

firstNameDF.rdd.getNumPartitions()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tungsten Binary Format
# MAGIC 
# MAGIC As a quick side note, the data that is "shuffled" is in a format know as the Tungsten Binary Format.
# MAGIC 
# MAGIC This is also the same format that is stored in RAM when cached.
# MAGIC 
# MAGIC This one format means we can move data from memory, to disk, across the network and back into memory in one single, highly-optimized, format.
# MAGIC 
# MAGIC ![Catalyst](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/tungsten-binary-format.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Stages
# MAGIC * When we shuffle data, it creates what is know as a stage boundary.
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
# MAGIC * This creates our bottle neck.
# MAGIC * Besides the bottle neck, this is also a significant performance hit: disk IO, network IO and more disk IO.
# MAGIC 
# MAGIC Once the data is shuffled we can resume execution...
# MAGIC 
# MAGIC For **Stage #2**, Spark will again create a pipeline of transformations in which the shuffle data is read into RAM (Step #4c) and then perform transformations #4d, #5, #6 and finally the write action, step #7.

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Lineage
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
# MAGIC However, Spark actually starts with the action (`write(..)` in this case).
# MAGIC 
# MAGIC Next it asks the question, what do I need to do first?
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
# MAGIC **Question:** So what is the benefit of working backwards through your action's lineage?<br/>
# MAGIC **Answer:** It allows Spark to determine if it is actually necessary to execute every transformation.
# MAGIC 
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

# MAGIC %md-sandbox
# MAGIC &copy; 2018 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>