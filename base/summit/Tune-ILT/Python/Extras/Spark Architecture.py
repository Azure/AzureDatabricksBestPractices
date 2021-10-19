# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC # Overview of Apache Spark Architecture
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/title-diagram.png" style="margin-left: 20px; width: 300px; float: right"/>

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Getting Started
# MAGIC 
# MAGIC Run the following cell to configure our "classroom."

# COMMAND ----------

# MAGIC %run "../Includes/Classroom Setup"

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
# MAGIC <img src="https://files.training.databricks.com/images/spark-shell-driver.png" style="border: 1px solid #aaa; float: right; margin-left: 20px"/>
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

# MAGIC %md-sandbox
# MAGIC <img src="https://files.training.databricks.com/images/spark-submit-driver.png" style="border: 1px solid #aaa; float: right; margin-left: 20px"/>
# MAGIC 
# MAGIC In this diagram, the only difference from the above is that the Spark application was started via `spark-submit`.
# MAGIC 
# MAGIC In that case, the **Driver** runs on some node in the cluster. In every other respect, the behavior is the same.

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
# MAGIC When you fire up `spark-shell` or `pyspark` without specifying a `--master` argument, you are running in local mode. Databricks Community Edition also uses local mode.
# MAGIC 
# MAGIC In local mode, there are no Executor processes. Instead, the Driver also acts as (a single) Executor.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Units of Parallelism
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/executors-and-threads.png" style="border: 1px solid #aaa; float: right; margin-left: 20px"/>
# MAGIC 
# MAGIC There are, then, two units of parallelism:
# MAGIC 
# MAGIC * Work is distributed, across the cluster, to Executor _processes_ that can run in parallel.
# MAGIC * _Within_ each executor process, there are typically multiple _threads_ that can also run in parallel (depending on how many physical CPU cores are available).

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Partitions
# MAGIC 
# MAGIC What is a partition?
# MAGIC 
# MAGIC (Discuss.)

# COMMAND ----------

# MAGIC %md
# MAGIC Let's start with a file containing 10 million records about people.

# COMMAND ----------

# MAGIC %fs head dbfs:/mnt/training/dataframes/people-with-header-10m.txt

# COMMAND ----------

# MAGIC %md
# MAGIC As you can see, this is, essentially, a CSV file with a ":" delimiter. Let's open it and see how many partitions we get.

# COMMAND ----------

people_df = (
  spark
    .read
    .option("header", "true")
    .option("sep", ":")
    .csv("dbfs:/mnt/training/dataframes/people-with-header-10m.txt")
)

# COMMAND ----------

people_df.rdd.getNumPartitions()

# COMMAND ----------

# MAGIC %md
# MAGIC Why did we get the number of partitions we got?
# MAGIC 
# MAGIC It turns out that Catalyst, the Spark DataFrame query optimizer, has examined both the DataFrame and the number of threads we have available, to determine what it considers to be the optimal number of partitions. The exact same file will be partitioned differently on different sized clusters.

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
# MAGIC 
# MAGIC ### Spark kicks off jobs when you create a DataFrame
# MAGIC 
# MAGIC (This behavior is different from the RDD API.)
# MAGIC 
# MAGIC Let's start our 10 million record people file. 
# MAGIC 
# MAGIC Recall that the file has a header. Let's use the CSV reader to read it. Initially, we'll do no schema inference.

# COMMAND ----------

people_df = (
  spark
    .read
    .option("header", "true")
    .option("sep", ":")
    .csv("dbfs:/mnt/training/dataframes/people-with-header-10m.txt")
)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC Open the "Spark Jobs" drop-down:
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/spark-jobs-databricks-1.png" alt="Jobs drop-down" style="border: 1px solid #aaa"/>
# MAGIC 
# MAGIC You should see 1 job with 1 _stage_ and one _task_ within the stage.
# MAGIC 
# MAGIC Why did Spark run a job? The answer lies with the following code:

# COMMAND ----------

people_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC When you open a DataFrame, Spark _always_ runs an initial job to try to determine the schema.
# MAGIC 
# MAGIC In the case of data files that have an existing schema (e.g., Parquet, which records the schema in its metadata), that operation is quick. For files, such as JSON and CSV, that don't have a schema, Spark reads the first record to try to get some idea of the schema.
# MAGIC 
# MAGIC For this file, Spark only needs to process data from the first partition to gather the information it needs. 
# MAGIC 
# MAGIC **NOTE**: The schema above is incomplete. Spark assumes all columns are string. But, we can make it do a little more work to infer actual column types, if we want.
# MAGIC 
# MAGIC In the following cell, copy and paste the initialization of the "people" DataFrame, and add `option("inferSchema", "true")`.

# COMMAND ----------

# TODO
Your modified DataFrame read goes here

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Note that, this time, the initialization took quite a bit longer—and, that Spark ran a different number of jobs. Why?

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC Take a look at the jobs again.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/spark-jobs-databricks-2.png" alt="Jobs drop-down" style="border: 1px solid #aaa; width: 300px"/>
# MAGIC 
# MAGIC The circle numbers in the diagram represent the number of _tasks_ in each stage. Recall that a _task_ is allocated to do a unit of work. In this case, the work is processing a partition.
# MAGIC 
# MAGIC **Discuss**: Why does the first job have only one task, while the second job has many?

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Let's take a look at a slightly more complicated example.

# COMMAND ----------

people_df_last_name = people_df.orderBy("lastName").select("lastName")

people_df_last_name.count()

# COMMAND ----------

# MAGIC %md
# MAGIC We get 2 jobs.
# MAGIC 
# MAGIC But what if we artificially limit the amount of data?

# COMMAND ----------

people_df_last_name.limit(10000).count()

# COMMAND ----------

# MAGIC %md
# MAGIC What's going on? Why did we get two separate jobs with the full data set, but only one job when we pare things down to 10,000 records?
# MAGIC 
# MAGIC Here, the query plan can help us. You can see the query plan in the Spark UI, but it's just as easy to look at it here.

# COMMAND ----------

people_df_last_name.explain()

# COMMAND ----------

people_df_last_name.limit(10000).explain()

# COMMAND ----------

# MAGIC %md
# MAGIC Note that the `orderBy` on the full data set uses the 
# MAGIC [RangePartitioner](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.RangePartitioner),
# MAGIC which "partitions sortable records by range into roughly equal ranges." The ranges are determined by sampling the content.
# MAGIC 
# MAGIC Because of the size of the data being processed in the first case, the query optimizer (Catalyst) decided to preprocess the data by sampling it, so it could choose more efficient partition sizes for processing. That sampling operation requires a second job.
# MAGIC 
# MAGIC In the second case, where the data set is relatively small, Catalyst decided it didn't need that extra overhead.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Digging a little deeper
# MAGIC 
# MAGIC How many partitions did we get in each case? How many bytes are in each partition?
# MAGIC 
# MAGIC The Spark UI can often help answer those two questions, but the Databricks Spark UI currently doesn't show the number of bytes read into each partition. (This is a bug that is due to be fixed. Vanilla Spark doesn't exhibit that problem.)
# MAGIC 
# MAGIC We can use the following code to determine that information.

# COMMAND ----------

def check_partitions(df):
  print("Number of partitions = {0}\n".format(df.rdd.getNumPartitions()))

  def count_partition(index, iterator):
    yield (index, len(list(iterator)))

  data = (
    df
      .rdd
      .mapPartitionsWithIndex(count_partition, True)
      .collect()
  )

  for index, count in data:
    print("Partition {0:2d}: {1} bytes".format(index, count))
    
check_partitions(people_df_last_name)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### A Wrinkle in Partitioning, plus Shuffles
# MAGIC 
# MAGIC Why did we get 200 partitions above?
# MAGIC 
# MAGIC The answer: `orderBy` causes a _shuffle_, and, after a shuffle, Spark _always_ sets the number of post-shuffle partitions to 200. This is controlled by a special parameter.

# COMMAND ----------

spark.conf.get("spark.sql.shuffle.partitions")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Sometimes, depending on the configuration of the cluster running your job, it can be beneficial to change that configuration parameter. You can do so on a _per-job_ basis (where "job" means
# MAGIC "action"), right in your code. 200 is a bit ridiculous for Community Edition, so let's try a more appropriate number.

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", "8")

# COMMAND ----------

# MAGIC %md
# MAGIC Let's see if this change has any effect on our previous job.

# COMMAND ----------

# MAGIC %md
# MAGIC Hmmm. Maybe we have to create the query again.

# COMMAND ----------

# MAGIC %md
# MAGIC It didn't change because Catalyst looked at the value of `spark.sql.shuffle.partitions` when we *created* the DataFrame. So the change doesn't get picked up until we create a new DataFrame.

# COMMAND ----------

# MAGIC %md
# MAGIC ... and this brings us to ...
# MAGIC 
# MAGIC ## What is shuffling?
# MAGIC 
# MAGIC A shuffle is when an Executor must hand some data off to another Executor.
# MAGIC 
# MAGIC Before we continue, let's load the Parquet version of the 10-million-people data set.

# COMMAND ----------

people_df = spark.read.parquet("dbfs:/mnt/training/dataframes/people-10m.parquet")

# COMMAND ----------

# MAGIC %md
# MAGIC Now, consider the following job:

# COMMAND ----------

from pyspark.sql.functions import *

display(people_df.filter(col("lastName").like("Z%")).select("lastName", "firstName"))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC For that job, each partition can be processed _independently_ of the other partitions.
# MAGIC 
# MAGIC Processing the `filter` and the `select` on a particular partition doesn't require information from _any_ other partition. 
# MAGIC 
# MAGIC The entire job can be completed without a shuffle.
# MAGIC 
# MAGIC Contrast that job with the following job:

# COMMAND ----------

people_df.select(substring(col("lastName"), 1, 1).alias("lastInitial")).groupBy("lastInitial").count().collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise
# MAGIC 
# MAGIC Change the query to aggregate by the first _two_ letters of the last name (thus, enlarging the key space), and replace the `collect()` in the above query to a `show(10000)`, run the query, and check the number of jobs.
# MAGIC 
# MAGIC **Discuss**: What caused the change?

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC With that job, it is impossible to complete the `groupBy` entirely in parallel. The way Spark parallelizes that operation is in stages.
# MAGIC 
# MAGIC **First**, it arranges do a partial `groupBy` _within_ each partition.
# MAGIC 
# MAGIC When a thread finishes processing a partition, it saves the partial grouped results to files local to the Executor node. (These are called _shuffle files_.) At this point, the thread is free to be reallocated to another partition.
# MAGIC 
# MAGIC _But_: Spark can't complete the `groupBy` operation until _all_ the partial `groupBy`'s are finished.
# MAGIC 
# MAGIC Once all those partial grouping operations are done, and every thread has finished saving its partial results, the stage is complete.
# MAGIC 
# MAGIC **Next**, the Driver tells the Executors to start the second stage. The Executors then send requests to each other to pull data across the network to complete the `groupBy` and move on with the execution.
# MAGIC 
# MAGIC For example, the Driver tells Executor 1 to complete the aggregation (the `groupBy`) for key "Z", Executor 1 might have to contact Executor 3 and Executor 4 to receive _their_ partial aggregations for "Z", before it can complete the aggregation.
# MAGIC 
# MAGIC **Within a stage**, partitions can truly be done in parallel. Running these operations in parallel, within a stage and without interacting with any other thread, is called **Pipelining**.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/wiki-book/pageviews/pipelining.png" style="float: right"/>
# MAGIC 
# MAGIC #### Shuffles can be expensive
# MAGIC 
# MAGIC Operations that require shuffling impose several kinds of performance overhead
# MAGIC 
# MAGIC * They force I/O operations to the temporary shuffle files.
# MAGIC * They require that *all* partitions reach the same state (end of partial aggregation, for instance), before execution can resume again. They introduce a synchronization point in the parallel computation.
# MAGIC * They can cause network I/O.
# MAGIC 
# MAGIC Consequently, while shuffles are often unavoidable, you should try hard to limit them.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Catalyst and Query Optimization
# MAGIC 
# MAGIC ### What is Catalyst?
# MAGIC 
# MAGIC Catalyst is Spark's extensible query optimizer.
# MAGIC 
# MAGIC When you're writing your queries in SQL or using the DataFrame API, you're creating a _logical query plan_, bit by bit. Unlike when you use the RDD API,
# MAGIC you're using a high-level query language, instead of expressing your operations as opaque lambdas (code). Spark _knows_ what your query looks like, and
# MAGIC it can optimize the query before running it, much the way a traditional RDBMS (like Oracle, PostgreSQL, or SQL Server) can.
# MAGIC 
# MAGIC Here's a high level view of Catalyst:
# MAGIC 
# MAGIC ![Catalyst Optimizer](https://files.training.databricks.com/images/wiki-book/pageviews/catalyst.png)

# COMMAND ----------

# MAGIC %md
# MAGIC 1. You build the query with SQL (which Spark must parse), the DataFrame API, or a combination of both. The result is a _parsed logical plan_. A logical plan describes
# MAGIC    _what_ is to be done **without** defining _how_ to do it. The query typically contains references to columns, but, at this point, Spark hasn't even validated that the
# MAGIC    columns exist.
# MAGIC  
# MAGIC 2. Spark maintains an internal catalog that tracks the various data sources being used in the query. Catalyst uses that catalog to resolve column names (as well as other things),
# MAGIC    yielding an _analyzed logical plan_.
# MAGIC    
# MAGIC 3. Catalyst then optimizes the logical plan (more on that in a minute).
# MAGIC 
# MAGIC 4. Finally, Catalyst generates one or more _physical execution plans_ (combinations underlying operations) and selects the most efficient one, using
# MAGIC    a cost model.
# MAGIC    
# MAGIC 5. Once the appropriate physical plan is chosen, Catalyst transforms it into iternal, specialized RDDs for execution.

# COMMAND ----------

# MAGIC %md
# MAGIC You can see the final physical plan with the `explain()` function. (It's also available in the Spark UI's SQL tab, for queries you've already run.)

# COMMAND ----------

people_df.select("firstName", "lastName").orderBy("lastName", "firstName").limit(100).explain()

# COMMAND ----------

# MAGIC %md
# MAGIC You can see all the stages by passing `true` (Scala) or `True` (Python) to the `explain()` function.

# COMMAND ----------

# TODO
Try it:

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Examples of Catalyst optimizations
# MAGIC 
# MAGIC #### Predicate pushdown
# MAGIC 
# MAGIC This join is _expensive_ (why?):
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/predicate-pushdown-1.png" style="border: 1px solid #aaa; margin: 20px; width: 600px"/>
# MAGIC 
# MAGIC But, Catalyst will fix that for us automatically:
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/predicate-pushdown-2.png" style="border: 1px solid #aaa; margin: 20px; width: 600px"/>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Catalyst will also:
# MAGIC 
# MAGIC * Push filter operations down into the data source, if possible. For instance, if a data source is a database, a `filter` operation 
# MAGIC   can become a `WHERE` clause _in the database_, so the data can be filtered before it ever gets to Spark.
# MAGIC   
# MAGIC * Push `SELECT` operations into the data source, if possible. For columnar file formats, like Parquet, Spark can read only the columns
# MAGIC   you want, leaving the rest untouched, on disk. (Contrast that with CSV.)
# MAGIC   
# MAGIC * Convert decimals into more efficient long integers for certain aggregation functions.
# MAGIC 
# MAGIC * and others.

# COMMAND ----------

# MAGIC %md
# MAGIC ### More information on Catalyst
# MAGIC 
# MAGIC * "Deep Dive into Catalyst: Apache Spark 2.0's Optimizer," Yin Huai's Spark Summit 2016 presentation. <https://spark-summit.org/2016/events/deep-dive-into-catalyst-apache-spark-20s-optimizer/>
# MAGIC * "Catalyst: A Functional Query Optimizer for Spark and Shark", Michael Armbrust's presentation at ScalaDays 2016. <https://www.youtube.com/watch?v=6bCpISym_0w>
# MAGIC * "Deep Dive into Spark SQL's Catalyst Optimizer," Databricks Blog, April 13, 2015. <https://databricks.com/blog/2015/04/13/deep-dive-into-spark-sqls-catalyst-optimizer.html>
# MAGIC * Michael Armbrust, Reynold S. Xin, Cheng Lian, Yin Huai, Davies Liu, Joseph K. Bradley, Xiangrui Meng, Tomer Kaftan, Michael J. Franklin, Ali Ghodsi, Matei Zaharia,
# MAGIC   "Spark SQL: Relational Data Processing in Spark," _Proceedings of the 2015 ACM SIGMOD International Conference on Management of Data_. Also available at 
# MAGIC   <http://people.csail.mit.edu/matei/papers/2015/sigmod_spark_sql.pdf>

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
# MAGIC 
# MAGIC There are definite advantages to this approach, and they're outlined in the blog post.

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
# MAGIC ![](https://files.training.databricks.com/images/volcano-vs-whole-stage-codegen.png)

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
# MAGIC **Note**: Whole stage code generation only works on certain parts of your query. The blog post outlines, in greater detail, how it works. But, take a look at `explain` output for the following query:

# COMMAND ----------

people_df.groupBy(substring(col("lastName"), 1, 1)).count().limit(1000).explain()

# COMMAND ----------

# MAGIC %md
# MAGIC Note that some operations are prefixed with `*`, such as `*HashAggregate`, and some are not (`Exchange`). The ones with the asterisk prefixes are subject to whole stage code generation.

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
# MAGIC ## Tungsten (briefly)
# MAGIC 
# MAGIC Tungsten is the in-memory storage format for Spark SQL / DataFrames.
# MAGIC 
# MAGIC Tungsten is a _column-oriented_ store. Advantages:
# MAGIC 
# MAGIC * Efficiency: Accessing successive values of a particular column is efficient, because they're adjacent in memory. And, under the covers, Spark can operate _directly out of Tungsten_, without 
# MAGIC   deserializing Tungsten data into JVM objects first.
# MAGIC 
# MAGIC * Compactness: Column values are stored as raw data, not as JVM objects (as with RDDs). **There's no object overhead.** So, the in-memory data uses much less memory.
# MAGIC 
# MAGIC * No garbage collection: Data is stored in a way that makes it not subject to JVM garbage collection.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Caching Introduction
# MAGIC 
# MAGIC Caching can speed up repeated queries against the same data by storing that data "next to" the Executor that's processing it.
# MAGIC 
# MAGIC * Caching is done on a per-partition basis.
# MAGIC * When you cache a DataFrame, Spark will capture the data _at that point_ and store as much as it can in memory, flushing what doesn't fit to disk.
# MAGIC * Any disk storage _is local to the Executors_, not distributed storage, so it's likely to be faster than going back to the original data.
# MAGIC 
# MAGIC Let's look at an example. We'll start with our 10 million person CSV file. We'll create a set of transformations that winnow the data down
# MAGIC to people born within the last 30 years.

# COMMAND ----------

from pyspark.sql.functions import *
people_df = spark.read.option("header", "true").option("inferSchema", "true").option("sep", ":").csv("dbfs:/mnt/training/dataframes/people-with-header-10m.txt") # first DataFrame
people_df2 = people_df.select(col("*"), year(col("birthDate")).alias("birthYear"), year(current_date()).alias("thisYear")) # second DataFrame
people_df3 = people_df2.filter((col("thisYear") - col("birthYear")) <= 30) # third DataFrame
people_df4 = people_df3.drop("thisYear") # fourth DataFrame

# COMMAND ----------

# MAGIC %md
# MAGIC Okay, let's run an aggregation on the result.

# COMMAND ----------

display(people_df4.groupBy("birthYear").count())

# COMMAND ----------

# MAGIC %md
# MAGIC What if we cache the fourth DataFrame before doing the `groupBy`?

# COMMAND ----------

people_df4.cache()

# COMMAND ----------

# MAGIC %md
# MAGIC Note that nothing seemed to happen. 
# MAGIC 
# MAGIC **`cache()` is _lazy_.**

# COMMAND ----------

people_df4.count()

# COMMAND ----------

# MAGIC %md
# MAGIC Now, let's rerun our `groupBy`.

# COMMAND ----------

display(people_df4.groupBy("birthYear").count())

# COMMAND ----------

# MAGIC %md
# MAGIC The entire query ran significantly faster, because it did not have to read the Parquet file.

# COMMAND ----------

# MAGIC %md
# MAGIC Finally, let's clean things up by uncaching our DataFrame.

# COMMAND ----------

people_df4.unpersist()


# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>