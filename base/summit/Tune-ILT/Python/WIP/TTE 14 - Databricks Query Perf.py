# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Databricks - Optimized Big Data Query Engine

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS partitioned_people_by_year
# MAGIC USING parquet
# MAGIC OPTIONS (
# MAGIC   path '/mnt/training/dataframes/tuning/people-10m-partitioned.parquet'
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE partitioned_people_by_year

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT min(birthYear) 
# MAGIC FROM partitioned_people_by_year 

# COMMAND ----------

# MAGIC %sql
# MAGIC MSCK REPAIR TABLE partitioned_people_by_year 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT min(birthYear) 
# MAGIC FROM partitioned_people_by_year 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Table with 10 Million Records

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) 
# MAGIC FROM partitioned_people_by_year

# COMMAND ----------

# MAGIC %md
# MAGIC #### Disable Optimizations

# COMMAND ----------

# MAGIC %sql
# MAGIC set spark.databricks.io.skipping.enabled=false;
# MAGIC set spark.databricks.io.cache.enabled=false;
# MAGIC set spark.databricks.optimizer.dynamicPartitionPruning=false;
# MAGIC set spark.databricks.optimizer.aggregatePushdown.enabled=false;

# COMMAND ----------

# MAGIC %md
# MAGIC ##Data Skipping Index
# MAGIC * Centralize Parquet Statistics
# MAGIC * Don't Read Files Unless Useful

# COMMAND ----------

# MAGIC %md
# MAGIC #### Run with data skipping index OFF

# COMMAND ----------

# MAGIC %sql
# MAGIC set spark.databricks.io.skipping.enabled=false;
# MAGIC 
# MAGIC SELECT count(distinct cast(birthDate as date)) 
# MAGIC FROM partitioned_people_by_year 
# MAGIC WHERE birthYear > 1951;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Run with data skipping index ON

# COMMAND ----------

# MAGIC %sql
# MAGIC set spark.databricks.io.skipping.enabled=true;
# MAGIC 
# MAGIC SELECT count(distinct cast(birthDate as date)) 
# MAGIC FROM partitioned_people_by_year 
# MAGIC WHERE birthYear > 1951;

# COMMAND ----------

# MAGIC %md
# MAGIC ##Transparent Caching
# MAGIC * Automatically cache parquet data (that matches predicate)
# MAGIC * Use SSD of cluster
# MAGIC * Use LRU for eviction

# COMMAND ----------

# MAGIC %md
# MAGIC #### Run with data skipping & data caching OFF

# COMMAND ----------

# MAGIC %sql
# MAGIC set spark.databricks.io.cache.enabled=false;
# MAGIC set spark.databricks.io.skipping.enabled=false;
# MAGIC 
# MAGIC SELECT birthDate, avg(salary) 
# MAGIC FROM partitioned_people_by_year 
# MAGIC GROUP BY birthDate 
# MAGIC HAVING avg(salary) > 40000

# COMMAND ----------

# MAGIC %md
# MAGIC #### Run with data skipping & data caching ON

# COMMAND ----------

# MAGIC %sql
# MAGIC set spark.databricks.io.cache.enabled=true;
# MAGIC set spark.databricks.io.skipping.enabled=true;
# MAGIC 
# MAGIC SELECT birthDate, avg(salary) 
# MAGIC FROM partitioned_people_by_year 
# MAGIC GROUP BY birthDate 
# MAGIC HAVING avg(salary) > 40000

# COMMAND ----------

# MAGIC %md
# MAGIC #### Run it Again (after cache)

# COMMAND ----------

# MAGIC %sql
# MAGIC set spark.databricks.io.cache.enabled=true;
# MAGIC set spark.databricks.io.skipping.enabled=true;
# MAGIC 
# MAGIC SELECT birthDate, avg(salary) 
# MAGIC FROM partitioned_people_by_year 
# MAGIC GROUP BY birthDate 
# MAGIC HAVING avg(salary) > 40000

# COMMAND ----------

# MAGIC %md
# MAGIC ##More Advanced Queries
# MAGIC * Push down aggregates below joins
# MAGIC * Dynamic Partition Pruning will automatically run sub-selects and replan

# COMMAND ----------

# MAGIC %md
# MAGIC #### Run with Optimizations OFF

# COMMAND ----------

# MAGIC %sql
# MAGIC set spark.databricks.io.skipping.enabled=false;
# MAGIC set spark.databricks.io.cache.enabled=false;
# MAGIC set spark.databricks.optimizer.dynamicPartitionPruning=false;
# MAGIC set spark.databricks.optimizer.aggregatePushdown.enabled=false;
# MAGIC set spark.sql.cbo.enabled=false;
# MAGIC 
# MAGIC SELECT p1.birthDate, avg(p2.salary) 
# MAGIC FROM partitioned_people_by_year p1, partitioned_people_by_year p2
# MAGIC WHERE p1.birthDate = p2.birthDate
# MAGIC GROUP BY p1.birthDate 
# MAGIC HAVING avg(p2.salary) > 40000

# COMMAND ----------

# MAGIC %md
# MAGIC #### Run with Optimizations ON

# COMMAND ----------

# MAGIC %sql
# MAGIC set spark.databricks.io.skipping.enabled=true;
# MAGIC set spark.databricks.io.cache.enabled=true;
# MAGIC set spark.databricks.optimizer.dynamicPartitionPruning=true;
# MAGIC set spark.databricks.optimizer.aggregatePushdown.enabled=true;
# MAGIC set spark.sql.cbo.enabled=true;
# MAGIC 
# MAGIC 
# MAGIC SELECT p1.birthDate, avg(p2.salary) 
# MAGIC FROM partitioned_people_by_year p1, partitioned_people_by_year p2
# MAGIC WHERE p1.birthDate = p2.birthDate
# MAGIC GROUP BY p1.birthDate 
# MAGIC HAVING avg(p2.salary) > 40000

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>