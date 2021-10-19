// Databricks notebook source
// MAGIC 
// MAGIC %md-sandbox
// MAGIC 
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC # Transformations & Actions Lab
// MAGIC ## Exploring T&As in the Spark UI

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Getting Started
// MAGIC 
// MAGIC Run the following cell to configure our "classroom."

// COMMAND ----------

// MAGIC %run "../Includes/Classroom Setup"

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Instructions
// MAGIC 0. Run the cell below.<br/>** *Note:* ** *There is no real rhyme or reason to this code.*<br/>*It simply includes a couple of actions and a handful of narrow and wide transformations.*
// MAGIC 0. Answer each of the questions.
// MAGIC   * All the answers can be found in the **Spark UI**.
// MAGIC   * All aspects of the **Spark UI** may or may not have been reviewed with you - that's OK.
// MAGIC   * The goal is to get familiar with diagnosing applications.
// MAGIC 0. Submit your answers for review.
// MAGIC 
// MAGIC **WARNING:** Run the following cell only once. Running it multiple times will change some of the answers and make validation a little harder.

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val initialDF = spark                                                       
  .read                                                                     
  .parquet("/mnt/training/wikipedia/pagecounts/staging_parquet_en_only_clean/")   
  .cache()

initialDF.foreach(x => ()) // materialize the cache

displayHTML("All done<br/><br/>")

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Round #1 Questions
// MAGIC 0. How many jobs were triggered?
// MAGIC 0. Open the Spark UI and select the **Jobs** tab.
// MAGIC   0. What action triggered the first job?
// MAGIC   0. What action triggered the second job?
// MAGIC 0. Open the details for the second job, how many MB of data was read in? Hint: Look at the **Input** column.
// MAGIC 0. Open the details for the first stage of the second job, how many records were read in? Hint: Look at the **Input Size / Records** column.

// COMMAND ----------

val someDF = initialDF
  .withColumn("first", upper($"article".substr(0,1)) )
  .where( $"first".isin("A","B","C","D","E","F","G","H","I","J","K","L","M","N","O","P","Q","R","S","T","U","V","W","X","Y","Z") )
  .groupBy($"project", $"first").sum()
  .drop("sum(bytes_served)")
  .orderBy($"first", $"project")
  .select($"first", $"project", $"sum(requests)".as("total"))
  .filter($"total" > 10000)

val total = someDF.count().toInt

displayHTML("All done<br/><br/>")

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Round #2 Questions
// MAGIC 0. How many jobs were triggered?
// MAGIC 0. How many actions were executed?
// MAGIC 0. Open the **Spark UI** and select the **Jobs** tab.
// MAGIC   0. What action triggered the first job?
// MAGIC   0. What action triggered the second job?
// MAGIC 0. Open the **SQL** tab - what is the relationship between these two jobs?
// MAGIC 0. For the first job...
// MAGIC   0. How many stages are there?
// MAGIC   0. Open the **DAG Visualization**. What do you suppose the green dot refers to?
// MAGIC 0. For the second job...
// MAGIC   0. How many stages are there?
// MAGIC   0. Open the **DAG Visualization**. Why do you suppose the first stage is grey?
// MAGIC   0. Can you figure out what transformation is triggering the shuffle at the end of 
// MAGIC     0. The first stage?
// MAGIC     0. The second stage?
// MAGIC     0. The third stage? HINT: It's not a transformation but an action.
// MAGIC 0. For the second job, the second stage, how many records (total) 
// MAGIC   0. Were read in as a result of the previous shuffle operation?
// MAGIC   0. Were written out as a result of this shuffle operation?  
// MAGIC   Hint: look for the **Aggregated Metrics by Executor**
// MAGIC 0. Open the **Event Timeline** for the second stage of the second job.
// MAGIC   * Make sure to turn on all metrics under **Show Additional Metrics**.
// MAGIC   * Note that there were 200 tasks executed.
// MAGIC   * Visually compare the **Scheduler Delay** to the **Executor Computing Time**
// MAGIC   * Then in the **Summary Metrics**, compare the median **Scheduler Delay** to the median **Duration** (aka **Executor Computing Time**)
// MAGIC   * What is taking longer? scheduling, execution, task deserialization, garbage collection, result serialization or getting the result?

// COMMAND ----------

someDF.take(total)

displayHTML("All done<br/><br/>")

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Round #3 Questions
// MAGIC 0. Collectively, `someDF.count()` produced 2 jobs and 6 stages.  
// MAGIC However, `someDF.take(total)` produced only 1 job and 2 stages.  
// MAGIC   0. Why did it only produce 1 job?
// MAGIC   0. Why did the last job only produce 2 stages?
// MAGIC 0. Look at the **Storage** tab. How many partitions were cached?
// MAGIC 0. True or False: The cached data is fairly evenly distributed.
// MAGIC 0. How many MB of data is being used by our cache?
// MAGIC 0. How many total MB of data is available for caching?
// MAGIC 0. Go to the **Executors** tab. How many executors do you have?
// MAGIC 0. Go to the **Executors** tab. How many total cores do you have available?
// MAGIC 0. Go to the **Executors** tab. What is the IP Address of your first executor?
// MAGIC 0. How many tasks is your cluster able to execute simultaneously?
// MAGIC 0. What is the path to your **Java Home**?

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>