// Databricks notebook source
// MAGIC 
// MAGIC %md-sandbox
// MAGIC 
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC # Review Questions
// MAGIC The following questions are intended to be reviewed the following day and are simply suggestions based on the material covered in the corresponding notebooks.

// COMMAND ----------

// MAGIC %md
// MAGIC ## Spark Programming
// MAGIC * What is Apache Spark?
// MAGIC   * A database
// MAGIC   * A processing engine
// MAGIC   * A design pattern for processing big data
// MAGIC   * A no-sql database with a computation engine
// MAGIC   * A multi-threaded, in-memory, storage engine
// MAGIC * Who is Databricks?
// MAGIC   * The invinters of Apache Spark
// MAGIC   * The #1 contributor to Apache Spark
// MAGIC   * Sponors of Spark+AI Summit
// MAGIC   * Host to Spark Meetups world wide
// MAGIC   * Providers of the fastest, hosted version of Apache Spark
// MAGIC   * All of the above
// MAGIC * What environment can Spark run on?
// MAGIC   * OpenStack
// MAGIC   * Yarn
// MAGIC   * Mesos
// MAGIC   * EC2
// MAGIC   * Docker
// MAGIC   * Kubernetties
// MAGIC   * Databricks
// MAGIC   * All of the above
// MAGIC   * All of the above and more
// MAGIC * Which environment does Spark run best on?
// MAGIC   * OpenStack
// MAGIC   * Yarn
// MAGIC   * Mesos
// MAGIC   * EC2
// MAGIC   * Docker
// MAGIC   * Kubernetties
// MAGIC   * Databricks
// MAGIC * What is the relationship between a shard, workspace and a cluster?
// MAGIC   * open-ended
// MAGIC * Name five cluster managers:
// MAGIC   * open-ended (Spark Standalone, Mesos, Yarn, Kuberneties & Databricks)
// MAGIC * Why did we have to create a new cluster (next day)?
// MAGIC   * Clusters are short lived processes
// MAGIC   * The cluster auto-terminated due to inactivity
// MAGIC   * Someone shut down the cluster for you
// MAGIC   * The cluster crashed
// MAGIC * What is the relationship between a core, thread & slot
// MAGIC   * They are all the same thing
// MAGIC   * There are N threads per core, subdivided into slots
// MAGIC   * Cores refer to the CPU cores, one thread are multi-plexed to the CPU cores and slots are a Spark conceptualization of a thread.
// MAGIC   * Slots are the space on the motherboard in which the cores is installed with multiple threads per core
// MAGIC * What is the relationship between a job & a stage
// MAGIC   * A job is a request to do some work in the executor, stage refers to the staging area in which that job executes
// MAGIC   * A job is typically triggered by an action and further subdivided in to stages/units of work
// MAGIC   * Stages refer to the sequence of work to be performed in parallel, each sequence consisting of multiple jobs
// MAGIC   * None of the above
// MAGIC * RDDs are the ? of the DataFrames API
// MAGIC   * assembly language
// MAGIC   * deprecated predecessors
// MAGIC   * successors
// MAGIC   * none of the above

// COMMAND ----------

// MAGIC %md
// MAGIC ## The Databricks Environment
// MAGIC * What is the DBFS?
// MAGIC   * Stands for DistriButed File System
// MAGIC   * A virtual drive over an object store such as Azure Blog or Amazon S3
// MAGIC   * A virtualized hard drive backed by a NAS device
// MAGIC   * An ephemeral storage device
// MAGIC * What is the name of the utility class for interacting with the DBFS?
// MAGIC   * db_utils
// MAGIC   * utils
// MAGIC   * dbutils
// MAGIC   * dbfs
// MAGIC   * dbfs_utils
// MAGIC   * databricks
// MAGIC * What does the `display()` command do?
// MAGIC   * A utility method for graphically rendering ML models
// MAGIC   * A Databricks specific function for presenting information in a notebook
// MAGIC   * A utility method for displaying Numpy visualizations
// MAGIC   * A utility method for presenting up to 1000 records of a dataframe
// MAGIC   * All of the above
// MAGIC * Name some of the magic commands covered on day one.
// MAGIC   * open-ended (%run, %fs, %sh, %r, %python, %scala, %sql, %md, %md-sandbox

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## Reading Data
// MAGIC * What does it mean to partition data?
// MAGIC   * To merge two chunks of data into one
// MAGIC   * To divide a chucnk of data in half
// MAGIC   * To divide a single dataset into smaller manageable chunks
// MAGIC   * To combine many small chunks of data into one large manageable chunk
// MAGIC * How do the `DataFrameReader`s decide how to partition data?
// MAGIC   * Always specified by the optional parameter `partitions`
// MAGIC   * Implementation is left to the sole discresion of the reader
// MAGIC   * Controled by the config parameter `spark.sql.shuffle.partitions`
// MAGIC   * Controled by the config parameter `spark.sql.read.partitions`
// MAGIC   * None of the above
// MAGIC * Which technique for reading data provided control over the number of partitions being created & what was so special about it?
// MAGIC   * CSV files
// MAGIC   * Parquet files
// MAGIC   * JDBC connections
// MAGIC   * ORC files
// MAGIC   * TCP/IP Connections 
// MAGIC * What would be the ramifications of creating 1,000, 10,000 or even 100,000 partitions from a JDBC data source?
// MAGIC   * Acheive extreamly high parallelization
// MAGIC   * Saturate the network connection between Spark and the SQL server
// MAGIC   * Exceede the SQL server's maximum number of connections
// MAGIC   * Crash the SQL server
// MAGIC   * All of the above
// MAGIC   * None of the above
// MAGIC * What is the side effect of infering the schema when reading in CSV or JSON data?
// MAGIC   * Performance is increased for every subsequent query
// MAGIC   * Performance is increased for the initialization of the DataFrame only
// MAGIC   * Performance is decreased on every subsequent query
// MAGIC   * Performance is decreased only when the DataFrame is initialized
// MAGIC * Why is the Parquet file format better than CSV, JSON, ORC, JDBC?
// MAGIC   * It is splittable allowing for parallel reads
// MAGIC   * It supports multiple compression algorithims
// MAGIC   * Columns of data can be read without reading the enitre row of data
// MAGIC   * The schema is stored as part of the file allowing for fast schema reads
// MAGIC   * The data can be partitioned on disk allowing for selective reads
// MAGIC   * It's harder to spell
// MAGIC   * All of the above

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## Introduction to DataFrames, Part #1
// MAGIC * In Spark 2.x, what is the name of the class for the "entry point" for the DataFrames API.
// MAGIC   * SparkContext
// MAGIC   * SparkSession
// MAGIC   * SQLContext
// MAGIC   * DataFrame
// MAGIC   * Dataset
// MAGIC * In the Scala API, what class do I need to search for in order to load the DataFrames API docs?
// MAGIC   * Dataframe
// MAGIC   * DataFrame
// MAGIC   * DataFrameReader
// MAGIC   * Dataset
// MAGIC   * DataSet
// MAGIC   * DataSetReader
// MAGIC * What is the difference between a `DataFrame` and a `Dataset[Row]`?
// MAGIC   * DataFrames are in Python only and Datasets are in Scala only
// MAGIC   * Dataset[Row] is a subclass of DataFrame specifically for row-based data
// MAGIC   * DataFrames work with most data sources where Dataset[Row] is specifically for relational databases
// MAGIC   * None of the above
// MAGIC * What DataFame operations did we cover so far?
// MAGIC   * open-ended (limit, select, drop, distinct, dropDuplicates, show, display, count)
// MAGIC * DataFrame operations generally fall into one of two categories. 
// MAGIC   * What category do operations like `limit()`, `select()`, `drop()` and `distinct()` fall into?
// MAGIC     * Transformaions
// MAGIC     * Readers
// MAGIC     * Utilities
// MAGIC     * Actions
// MAGIC     
// MAGIC   * What category do operations like `show()`, `display()` and `count()` fall into?
// MAGIC     * Transformaions
// MAGIC     * Readers
// MAGIC     * Utilities
// MAGIC     * Actions
// MAGIC 
// MAGIC * Which type of operation **always** triggers a job?
// MAGIC   * Transformaions
// MAGIC   * Readers
// MAGIC   * Utilities
// MAGIC   * Actions
// MAGIC 
// MAGIC * In what case **might** a transformation trigger a job?
// MAGIC   * open-ended: When readers need to touch the data so as to determine the schema
// MAGIC * What is the difference between `display()` and `show()`?
// MAGIC   * display is pretty
// MAGIC   * show is ugly
// MAGIC   * display is Databricks specific function
// MAGIC   * show is part of core spark
// MAGIC   * show is an action
// MAGIC   * display calls an action
// MAGIC * What is the difference between `distinct()` and `dropDuplicates()`?
// MAGIC   * There is no difference
// MAGIC   * distinct() only works on DataFrames & dropDuplicates() only works on Datasets
// MAGIC   * dropDuplicates(..) accepts additional parameters
// MAGIC   * All of the above

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## Transformations & Actions
// MAGIC * What set of operations are considered "lazy"?
// MAGIC   * Utility methods
// MAGIC   * Actions
// MAGIC   * Transformations
// MAGIC   * Readers
// MAGIC   
// MAGIC * What set of operations are considered "eager"?
// MAGIC   * Utility methods
// MAGIC   * Actions
// MAGIC   * Transformations
// MAGIC   * Readers
// MAGIC   
// MAGIC * Why do transformations not trigger a job (aside from some of the initial read operations)?
// MAGIC   * Because the transformaion is executed on the driver
// MAGIC   * So that the transformaions can be optimized
// MAGIC   * Because the spark config `spark.sql.transformations.eager` is set to false
// MAGIC   * Because the server crashed
// MAGIC * What bennifits does lazy execution aford us?
// MAGIC   * All the data is not loaded before processing it
// MAGIC   * They can be optimized before the data is processed
// MAGIC   * It makes parallelization easier
// MAGIC   * It allows the reader to decide how much data to pull from disk
// MAGIC   * None of the above
// MAGIC   * All of the above
// MAGIC * What is the name of the entity responsible for optimizing our DataFrames and SQL and converting them into RDDs?
// MAGIC   * The Tungsten Optimizer
// MAGIC   * The Catalyst Optimizer
// MAGIC   * The Cost Based Optimizer
// MAGIC   * RDDs
// MAGIC * What is the difference between a wide and narrow transformation?
// MAGIC   * Wide operations process more data than narrow transformaions
// MAGIC   * Narrow transformations work on partitions less than 200 MB
// MAGIC   * Narrow transformations are pipelined together and wide operations create a stage boundry
// MAGIC   * Wide transformations are pipelined together and narrow operations create a stage boundry
// MAGIC   * Narrow transformations reduce the number of partitions in a dataset and wide transformations increase them
// MAGIC * What triggers a shuffle operation?
// MAGIC   * Any action
// MAGIC   * Any transformaion
// MAGIC   * Wide transformations
// MAGIC   * Narrow transformaions
// MAGIC   * Wide Actions
// MAGIC   * Narrow Actions
// MAGIC * When are stages created?
// MAGIC   * After a job is completed but before control is returned to the driver
// MAGIC   * Between any wide transformation
// MAGIC   * Between any narrow transformaion
// MAGIC   * When the partition size is > 200 MB
// MAGIC * Can we begin executing tasks in Stage #2 if other tasks on other executors are still running in Stage #1?
// MAGIC   * Yes
// MAGIC   * No
// MAGIC   * Yes, but only if all transformations in the previous stage were narrow
// MAGIC   * Yes, but only if all transformations in the previous stage were wide
// MAGIC * Why must we wait for all operations in a given stage to complete before moving on to the next stage?
// MAGIC   * open-ended
// MAGIC * What is the term used for the scenario when data is read into RAM and several transformations are sequentially executed against that data?
// MAGIC   * Shuffling
// MAGIC   * Tuning
// MAGIC   * Optimizing
// MAGIC   * Pipelining
// MAGIC   * None of the above
// MAGIC * How is Pipelining in Apache Spark different when processing transformations compared to MapReduce?
// MAGIC   * Spark pipelines results in faster processing than MapReduce
// MAGIC   * MapReduce doesn't pipline multiple transformations together
// MAGIC   * MapReduce writes each result back to disk
// MAGIC   * All of the above
// MAGIC   * None of the above
// MAGIC * Describe how Apache Spark can determine that certain stages can be skipped when re-executing a query?
// MAGIC   * Because the DataFrame was specifically cached
// MAGIC   * Because the transformation results in a net-zero change
// MAGIC   * Because the shuffle operation let temp files on the executor that can be resused
// MAGIC   * Because the leniage of transformations is immutable
// MAGIC * Where in the Spark UI can I see how long it took to execute a task?
// MAGIC   * In the Job Details
// MAGIC   * In the Stage Details
// MAGIC   * In the Task Details
// MAGIC   * In the Storage Tab
// MAGIC   * In the Executor Tab
// MAGIC * Where in the Spark UI can I see if my data is evenly distributed amongst each partition?
// MAGIC   * In the Job Details
// MAGIC   * In the Stage Details
// MAGIC   * In the Task Details
// MAGIC   * In the Storage Tab
// MAGIC   * In the Executor Tab

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## Introduction to DataFrames, Part #2
// MAGIC * What are some of the transformations we reviewed yesterday?
// MAGIC * What are some of the actions we reviewed yesterday?
// MAGIC * What two new classes did we discuss yesterday?
// MAGIC   * Hint: The first class is used in methods like `filter()` and `where()`
// MAGIC   * Hint: The second method is the object backing every row of a `DataFrame`.
// MAGIC * What potential problem might I run into by calling `collect()`?
// MAGIC * What type of object does `collect()` and `take(n)` return?
// MAGIC * What type of object does `head()` and `first()` return?
// MAGIC * What are some of the operations we can perform on a `Column` object?
// MAGIC * With the Scala API, how do you access the 3rd column of a `DataFrame`'s `Row` object which happens to be a `Boolean` value?
// MAGIC * What happens if you do not use the correct accessor method on a `Row` object?
// MAGIC   * Hint: What happens if you ask for a `Boolean` when the backing object is a `String`?
// MAGIC * Why don't we have these problems with the Scala API?
// MAGIC * What is different about the transformations `orderBy(String)` and `filter(String)`?
// MAGIC * What methods do you call on a `Column` object to test for equality, in-equality and null-safe equality?

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## Partitioning
// MAGIC * On a non-autoscaling cluster, what is the magic ration between slots and partitions?
// MAGIC * If you have 125 partitions and 200 cores, what type of problems might you run into?
// MAGIC * What about 101 partitions and 100 cores?
// MAGIC * How do I find out how many cores my clsuter has?
// MAGIC * What is the recomended size in MB for each partition (in RAM, i.e. cached)?
// MAGIC * If I need to increase the number of partions to match my cores, should I use `repartition(n)` or `coalesce(n)`?
// MAGIC * What side affect might I run into by using `coalesce(n)`?
// MAGIC * Which of the two operations is a wide transformation, `repartition(n)` or `coalesce(n)`?
// MAGIC * What is the significance of the configuration setting `spark.sql.shuffle.partitions`?

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## Introduction to DataFrames, Part #3
// MAGIC * What is the name of the class returned by the `groupBy()` transformation?
// MAGIC   * For Python: `GroupedData`
// MAGIC   * For Scala: `RelationalGroupedDataset`
// MAGIC * What are some of the aggregate functions available for a `groupBy()` operation?
// MAGIC * With the various aggregate functions for `groubBy()`, what happens if you do not specify the column to `sum(..)`, for example?
// MAGIC * How many different ways can you think of to rename/add a column?
// MAGIC   * `withColumn()`
// MAGIC   * `select()`
// MAGIC   * `as()`
// MAGIC   * `alias()`
// MAGIC   * `withColumnRenamed()`
// MAGIC * What are some of the `...sql.functions` methods we saw yesterday?
// MAGIC   * `unix_timestamp()`
// MAGIC   * `cast()`
// MAGIC   * `year()`
// MAGIC   * `month()`
// MAGIC   * `dayofyear()`
// MAGIC   * `sum()`
// MAGIC   * `count()`
// MAGIC   * `avg()`
// MAGIC   * `min()`
// MAGIC   * `max()`
// MAGIC * In Python, can I use the `as()` to rename a column?

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## Caching
// MAGIC * Should I cache data?
// MAGIC * Why might it be better to convert a CSV file to Parquet instead of caching it?
// MAGIC * What is the default storage level for the `DataFrame` API?
// MAGIC * What is the default storage level for the `RDD` API?
// MAGIC * When would I use the storage level `DISK_ONLY`?
// MAGIC * When would I use any of the `XXXX_2` storage levels?
// MAGIC * When would I use any of the `XXXX_SER` storage levels?

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## Introduction to DataFrames, Part #4
// MAGIC * What is a UDF?
// MAGIC * Whare are some the gotcha's with UDFs?
// MAGIC   * Cannot be optimized
// MAGIC   * Have to be careful about serialization
// MAGIC   * Slower than built-in functions
// MAGIC   * There is almost always a built in function that alrady does what you want.
// MAGIC * How do you avoid problems with UDFs?
// MAGIC * How does the performance with UDFs compare with Python vs Scala?
// MAGIC * How is a join operation in the DataFrames API different than in MySQL or Oracle?
// MAGIC * Is the join operation wide or narrow?

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## Broadcasting
// MAGIC * What is a broadcast join?
// MAGIC * What is the threshold for automatically triggering a broadcast join (at least on Databricks)
// MAGIC * How do we change the threshold?
// MAGIC * spark.sql.autoBroadcastJoinThreshold

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## Catalyst Optimizer
// MAGIC * Does it matter which language I use?
// MAGIC * Does it matter which API I use? SQL / DataFrame / Dataset?
// MAGIC * What is the difference between the logical and physical plans?
// MAGIC * Why does the Catalyst Opimizer produce multiple physical models?
// MAGIC * What is the last step/stage for the Catalyst Optimizer?

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>