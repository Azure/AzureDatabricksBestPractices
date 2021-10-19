// Databricks notebook source
// MAGIC 
// MAGIC %md-sandbox
// MAGIC 
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC # Apache&reg; Spark&trade; Tuning and Best Practices
// MAGIC ## Databricks Spark 110 (3 Day)
// MAGIC See **<a href="https://databricks.com/training/courses/apache-spark-tuning-and-best-practices" target="_blank">https&#58;//databricks.com/training/courses/apache-spark-tuning-and-best-practices</a>**

// COMMAND ----------

// MAGIC %md
// MAGIC | Time | Topic &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; | Description &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; |
// MAGIC |:----:|-------|-------------|
// MAGIC | 50m  | **Introductions**                  ||
// MAGIC | 10m  | **Break**                          ||
// MAGIC | 20m  | **Setup**                          | *Registration, Courseware & Q&As* |
// MAGIC | -    | **[Getting Started]($./Tuning/01 Getting Started)**              ||
// MAGIC | -    | **[Utility Functions]($./Tuning/02 Utility Functions)**            ||
// MAGIC | -    | **[Developer Productivity]($./Tuning/03 Developer Productivity)**       ||
// MAGIC | -    | **[Comparing 2014 vs 2017]($./Tuning/04 Comparing 2014 vs 2017)**       ||
// MAGIC | 30m  | **[Lab: T&A in the Spark UI]($./Other Topics/Transformations And Actions Lab)**  | *Explore the effects of Transformations and Actions while exploring the Spark UI.* |
// MAGIC | -    | **[Reusability - Part 1]($./Tuning/05 Reusability - Part 1)**         ||
// MAGIC | 30m  | **[Catalyst Optimizer]($./Other Topics/Catalyst Optimizer)**                      | *Logical, Optimized & Physical Plan, Cost Model, WholeStageCodegen, Predicate Pushdown* |
// MAGIC | -    | **[Reusability - Part 2]($./Tuning/06 Reusability - Part 2)**         ||
// MAGIC | -    | **[Comparing 2014 vs 2018]($./Tuning/07 Comparing 2014 vs 2018)**       ||
// MAGIC | -    | **[StatFiles]($./Tuning/08 StatFiles)**                    ||
// MAGIC | -    | **[From 9x to 300x]($./Tuning/09 From 9x to 300x)**             ||
// MAGIC | -    | **[Join Optimizations]($./Tuning/10 Join Optimizations)**           ||
// MAGIC | -    | **[The Overhead of Caching]($./Tuning/11 The Overhead of Caching)**      ||
// MAGIC | -    | **[Extracting More Information]($./Tuning/12 Extracting More Information)**   ||
// MAGIC | -    | **[Cache Differentiation]($./Tuning/13 Cache Differentiation)**        ||
// MAGIC | -    | **[Data Types & Data Structures]($./Tuning/14 Data Types & Data Structures)** ||

// COMMAND ----------

// MAGIC %md
// MAGIC ## Electives
// MAGIC | Time | Topic &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; | Description &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; |
// MAGIC |:----:|-------|-------------|
// MAGIC | -    | **[Coding Challenge #1]($./Tuning/30 Coding Challenge 1)** | Optimize a "standard" query for the fastest execution time |
// MAGIC | -    | **[Coding Challenge #2]($./Tuning/31 Coding Challenge 2)** | Optimize the use of UDFs for the fastest execution time |
// MAGIC | -    | **[Coding Challenge #3]($./Tuning/99 Capstone Project)**    | A multi-day project to clean up a multitude of datasets |

// COMMAND ----------

// MAGIC %md
// MAGIC General Notes:
// MAGIC * The times indicated here *are approximate*
// MAGIC * Actual times will vary by class size, class participation, and other unforeseen factors

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>