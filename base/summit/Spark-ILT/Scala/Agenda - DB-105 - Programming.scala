// Databricks notebook source
// MAGIC 
// MAGIC %md-sandbox
// MAGIC 
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC # Apache&reg; Spark&trade; Programming
// MAGIC ## Databricks Spark 105 (2-Day or 3-Day)
// MAGIC See **<a href="https://databricks.com/training/instructor-led-training/courses/apache-spark-programming" target="_blank">https&#58;//databricks.com/training/instructor-led-training/courses/apache-spark-programming</a>**

// COMMAND ----------

// MAGIC %md
// MAGIC ## Day #1 AM
// MAGIC | 3-Day | 2-Day | Topic &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; | Description &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; |
// MAGIC |:----:|:----:|-------|-------------|
// MAGIC | 50m  | 50m  | **Introductions**                                                                ||
// MAGIC | 10m  | 10m  | **Break**                                                                        ||
// MAGIC | 20m  | 20m  | **Setup**                                                                        | *Registration, Courseware & Q&As* |
// MAGIC | 30m  | 30m  | **[Apache Spark Overview]($./Apache Spark Overview)**                            | *About Databricks, Spark & Spark Architecture* |
// MAGIC | 10m  | 10m  | **Break**                                                                        || 
// MAGIC | 20m  | 20m  | **[The Databricks Environment]($./Other Topics/Databricks Environment)**         | DBFS, dbutils, Magic Commands: %run, %sh, %md, %fs
// MAGIC | 30m  | 30m  | **[Reading Data - CSV]($./Reading & Writing Data/Reading Data 1 - CSV)**         | *Spark Entry Point, Reading Data, Inferring Schemas, API Docs* |
// MAGIC | 10M  | 10M  | **[Reading Data - Parquet]($./Reading & Writing Data/Reading Data 2 - Parquet)** | *The #1 recomended format for storing big-data* |

// COMMAND ----------

// MAGIC %md
// MAGIC ## Day #1 PM
// MAGIC | 3-Day | 2-Day | Topic &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; | Description &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; |
// MAGIC |:----:|:----:|-------|-------------|
// MAGIC | 20M  | -    | Reading Data - More Examples                                                     | *We will cover as many as time permits* |
// MAGIC |      | -    | **[Reading Data - Tables]($./Reading & Writing Data/Reading Data 3 - Tables)**   | *See how Databricks makes registering datasets easy with the Tables UI* | 
// MAGIC |      | -    | **[Reading Data - JSON]($./Reading & Writing Data/Reading Data 4 - JSON)**       | *Complex Data Types, JSON-Lines & Multi-Line JSON* |
// MAGIC |      | -    | **[Reading Data - Text]($./Reading & Writing Data/Reading Data 5 - Text)**       | *Simple Text Files* |
// MAGIC |      | -    | **[Reading Data - JDBC]($./Reading & Writing Data/Reading Data 6 - JDBC)**       | *Partition Strides & JDBC Sources: MySQL, Postgres, Oracle.* |
// MAGIC |      | -    | **[Reading Data - Summary]($./Reading & Writing Data/Reading Data 7 - Summary)** | *2-minute review of the various readers and data sources* | 
// MAGIC |      | -    | **[Writing Data]($./Reading & Writing Data/Writing Data)**                       | *Short example of how to use DataFrameWriters* |
// MAGIC | 30m  | 30m  | **[Lab: Reading Data]($./Reading & Writing Data/Reading Data 8 - Lab)**          | *Putting to practice what we just learned* |
// MAGIC | 10m  | 10m  | **Break**                                                                        || 
// MAGIC | 50m  | 50m  | **[Intro To DataFrames Part-1]($./Intro To DataFrames/Intro To DF Part 1)**       | *DataFrames, cache(), show(), display(), limit(), count(), select(), drop(), distinct(), SQL, Temp Views* |
// MAGIC | 10m  | 10m  | **Break**                                                                        || 
// MAGIC | 30m  | 30m  | **[Lab: Distinct Articles]($./Intro To DataFrames/Intro To DF Part 1 Lab)**      | *Putting to practice what we just learned* |
// MAGIC | 10m  | 10m  | **Break**                                                                        || 
// MAGIC | 50m  | 50m  | **[Transformations And Actions]($./Other Topics/Transformations And Actions)**   | *Lazy, Catalyst, Actions, Transformations, Wide vs. Narrow, Shuffling, Stages, Pipelining, Lineage* |
// MAGIC | 30m  | 30m  | **[Lab: T&A in the Spark UI]($./Other Topics/Transformations And Actions Lab)**  | *Explore the effects of Transformations and Actions while exploring the Spark UI.* |

// COMMAND ----------

// MAGIC %md
// MAGIC ## Day #2 AM
// MAGIC | 3-Day | 2-Day | Topic &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; | Description &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; |
// MAGIC |:----:|:----:|-------|-------------|
// MAGIC | 30m  | 15m  | **Review**                                                                       | *What did we discover yesterday?* |      
// MAGIC | 20m  | 20m  | **[Caching]($./Other Topics/Caching)**                                           | *cache(), persist(), unpersist(), RDD Name, don't cache!* |
// MAGIC | 10m  | 10m  | **Break** || 
// MAGIC | 50m  | 50m  | **[Intro To DataFrames Part-2]($./Intro To DataFrames/Intro To DF Part 2)**      | *orderBy(), Column, filter(), first(), Row, collect(), take(n), Dataframe vs. DataSet* |
// MAGIC | 10m  | 10m  | **Break** || 
// MAGIC | 30m  | 30m  | **[Lab: Washingtons and Marthas]($./Intro To DataFrames/Intro To DF Part 2 Lab)**       | *Counting & summing Washingtons and Adams - how many different ways can you solve the same problem?* |
// MAGIC | 30m  | 30m  | **[Catalyst Optimizer]($./Other Topics/Catalyst Optimizer)**                      | *Logical, Optimized & Physical Plan, Cost Model, WholeStageCodegen, Predicate Pushdown* |

// COMMAND ----------

// MAGIC %md
// MAGIC ## Day #2 PM
// MAGIC | 3-Day | 2-Day | Topic &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; | Description &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; |
// MAGIC |:----:|:----:|-------|-------------|
// MAGIC | 60m  | 60m  | **[Partitioning]($./Other Topics/Partitioning)**                                  | *Partitions vs. Slots, repartition(n), coalesce(n), spark.sql.shuffle.partitions* |
// MAGIC | 10m  | 10m  | **Break** || 
// MAGIC | 20m  | 20m  | **[Lab: Exploring Partitions]($./Other Topics/Partitioning Lab)**                 | *Explore the real-word consequences of partition sizes* |
// MAGIC | 30m  | 30m  | **[Intro To DataFrames Part-3, 1/2]($./Intro To DataFrames/Intro To DF Part 3)**   | *withColumnRenamed(), withColumn(), unix_timestamp() & cast()* |
// MAGIC | 10m  | 10m  | **Break** || 
// MAGIC | 30m  | 30m  | **[Intro To DataFrames Part-3, 2/2]($./Intro To DataFrames/Intro To DF Part 3)**   | *year(), month(), dayofyear(), RelationalGroupedDataset, sum(), count(), avg(), min(), max()* |
// MAGIC | 50m  | 30m  | **[Lab: De-Duping Data]($./Intro To DataFrames/Intro To DF Part 3 Lab)**          | *Real world problem solving - removing duplicate records* |
// MAGIC | 10m  | 10m  | **Break** || 
// MAGIC | 20m  | -    | **Q&A** | *Someting we forgot to cover? Now's your chance* | 

// COMMAND ----------

// MAGIC %md
// MAGIC ## Day #3 AM
// MAGIC | 3-Day | 2-Day | Topic &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; | Description &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; |
// MAGIC |:----:|:----:|-------|-------------|
// MAGIC | 30m  | -    | **Review**                                                                  | *What did we discover yesterday?* |      
// MAGIC | 40m  | 40m  | **[Intro To DataFrames Part-4]($./Intro To DataFrames/Intro To DF Part 4)** | *date_format(), User Defined Functions, Mondays, join()* |
// MAGIC | 10m  | -    | **Break**||
// MAGIC | 50m  | 20m  | **[Lab: What-The-Monday?]($./Intro To DataFrames/Intro To DF Part 4 Lab)** | *What is going on with Mondays?* |
// MAGIC | 10m  | -    | **Break**||
// MAGIC | 40m  | -    | **[Intro To DataFrames Part-5]($./Intro To DataFrames/Intro To DF Part 5)** | *Broadcast Joins, broadcast()* |

// COMMAND ----------

// MAGIC %md
// MAGIC ## Day #3 PM
// MAGIC | 3-Day | 2-Day | Topic &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; | Description &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; |
// MAGIC |:----:|:----:|-------|-------------|
// MAGIC | 15m  | 15m  | **House Keeping**|| 
// MAGIC | 45m  | -    | **Structured Streaming Hour #1** | &nbsp; |
// MAGIC | 10m  | -    | **Break**||
// MAGIC | 50m  | -    | **Structured Streaming Hour #2** | &nbsp; |
// MAGIC | 10m  | -    | **Break**||
// MAGIC | 60m  | -    | **[Machine Learning Pipeline]($./Other Topics/Machine Learning Pipeline Demo)** | *ML Pipelines, Feature Extractors, Random Forests, Evaluators, Param Grids, Cross Validation* |
// MAGIC | 10m  | -    | **Break**||
// MAGIC | 40m  | -    | **Bonus Topics / Q&A** | *Want to see something else? Any last minute questions?* | 

// COMMAND ----------

// MAGIC %md
// MAGIC General Notes:
// MAGIC * The times indicated here *are approximate*
// MAGIC * Actual times will vary by class size, class participation, and other unforeseen factors
// MAGIC 
// MAGIC 2-Day Variance Notes:
// MAGIC * Caching is taught on day #1 instead of day #2			
// MAGIC * ITDF-4 is taught at the end of day #2			
// MAGIC * Other topics are obmitted to make time for ITDF-4			
// MAGIC * Some labs & reviews are shortened to make time for ITDF-4			

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>