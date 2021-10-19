// Databricks notebook source
// MAGIC 
// MAGIC %md-sandbox
// MAGIC 
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ## In this lesson you:
// MAGIC * Learn about when to use Databricks Delta
// MAGIC * Log into Databricks
// MAGIC * Create a notebook inside your home folder in Databricks
// MAGIC * Create, or attach to, a Spark cluster
// MAGIC * Import the course files into your home folder
// MAGIC * <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> Make sure to go through the lessons in order because they build upon each other
// MAGIC 
// MAGIC ## Audience
// MAGIC * Primary Audience: Data Engineers
// MAGIC * Secondary Audience: Data Analysts, and Data Scientists
// MAGIC 
// MAGIC ## Prerequisites
// MAGIC * Web browser: current versions of Google Chrome, Firefox, Safari, Microsoft Edge and 
// MAGIC Internet Explorer 11 on Windows 7, 8, or 10 (see <a href="https://docs.databricks.com/user-guide/supported-browsers.html#supported-browsers#" target="_blank">Supported Web Browsers</a>)
// MAGIC * Databricks Runtime 4.2 or greater
// MAGIC * Completed courses Spark-SQL, DataFrames or ETL-Part 1 from <a href="https://academy.databricks.com/" target="_blank">Databricks Academy</a>, or have similar knowledge

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC # The Challenge with Data Lakes
// MAGIC ### Or, it's not a Data Lake, it's a Data CESSPOOL
// MAGIC 
// MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/Delta/cesspool.jpg" style="height: 200px; border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa"/></div>
// MAGIC Image: iStock/Alexmia
// MAGIC 
// MAGIC A <b>Data Lake</b>: 
// MAGIC * Is a storage repository that inexpensively stores a vast amount of raw data in its native format.
// MAGIC * Consists of current and historical data dumps in various formats including XML, JSON, CSV, Parquet, etc.
// MAGIC * May contain operational relational databases with live transactional data.
// MAGIC * In effect, it's a dumping ground of amorphous data.
// MAGIC 
// MAGIC To extract meaningful information out of a Data Lake, we need to resolve problems like:
// MAGIC * Schema enforcement when new tables are introduced 
// MAGIC * Table repairs when any new data is inserted into the data lake
// MAGIC * Frequent refreshes of metadata 
// MAGIC * Bottlenecks of small file sizes for distributed computations
// MAGIC * Difficulty re-sorting data by an index (i.e. userID) if data is spread across many files and partitioned by i.e. eventTime

// COMMAND ----------

// MAGIC %md
// MAGIC # The Solution: Databricks Delta
// MAGIC 
// MAGIC Databricks Delta is a unified data management system that brings reliability and performance (10-100x faster than Spark on Parquet) to cloud data lakes.  Delta's core abstraction is a Spark table with built-in reliability and performance optimizations.
// MAGIC 
// MAGIC You can read and write data stored in Databricks Delta using the same familiar Apache Spark SQL batch and streaming APIs you use to work with Hive tables or DBFS directories. Databricks Delta provides the following functionality:<br><br>
// MAGIC 
// MAGIC * <b>ACID transactions</b> - Multiple writers can simultaneously modify a data set and see consistent views.
// MAGIC * <b>DELETES/UPDATES/UPSERTS</b> - Writers can modify a data set without interfering with jobs reading the data set.
// MAGIC * <b>Automatic file management</b> - Data access speeds up by organizing data into large files that can be read efficiently.
// MAGIC * <b>Statistics and data skipping</b> - Reads are 10-100x faster when statistics are tracked about the data in each file, allowing Delta to avoid reading irrelevant information.

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## Additional Topics & Resources
// MAGIC **Q:** Where can I find documentation on Databricks Delta?  
// MAGIC **A:** See <a href="https://docs.databricks.com/delta/delta-intro.html#what-is-databricks-delta" target="_blank">Databricks Delta Guide</a>.
// MAGIC 
// MAGIC **Q:** Are there additional docs I can reference to find my way around Databricks?  
// MAGIC **A:** See <a href="https://docs.databricks.com/user-guide/getting-started.html" target="_blank">Getting Started with Databricks</a>.
// MAGIC 
// MAGIC **Q:** Where can I learn more about the cluster configuration options?  
// MAGIC **A:** See <a href="https://docs.databricks.com/user-guide/clusters/index.html" target="_blank">Spark Clusters on Databricks</a>.
// MAGIC 
// MAGIC **Q:** Can I import formats other than .dbc files?  
// MAGIC **A:** Yes, see <a href="https://docs.databricks.com/user-guide/notebooks/index.html#importing-notebooks" target="_blank">Importing notebooks</a>.
// MAGIC 
// MAGIC **Q:** Can I use browsers other than Chrome or Firefox?  
// MAGIC **A:** Databricks is tested for Chrome and Firefox.  It does work on Internet Explorer 11 and Safari however, it is possible some user-interface features may not work properly.
// MAGIC 
// MAGIC **Q:** Can I install the courseware notebooks into a non-Databricks distribution of Spark?  
// MAGIC **A:** No, the files that contain the courseware are in a Databricks specific format (DBC).
// MAGIC 
// MAGIC **Q:** Do I have to have a paid Databricks subscription to complete this course?  
// MAGIC **A:** No, you can sign up for a free <a href="https://databricks.com/try-databricks" target="_blank">Community Edition</a> account from Databricks.

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>