# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Review Questions

# COMMAND ----------

# MAGIC %md
# MAGIC ## Introducing Delta
# MAGIC 
# MAGIC **Question:** What is Databricks Delta?<br>
# MAGIC **Answer:** Databricks Delta is a mechanism of effectively managing the flow of data (<b>data pipeline</b>) to and from a <b>Data Lake</b>.
# MAGIC 
# MAGIC **Question:** What are some of the pain points of existing data pipelines?<br>
# MAGIC **Answer:** 
# MAGIC * Introduction of new tables requires schema creation 
# MAGIC * Whenever any new data is inserted into the data lake, table repairs are required
# MAGIC * Metadata must be frequently refreshed
# MAGIC * Small file sizes become a bottleneck for distributed computations
# MAGIC * If data is sorted by a particular index (i.e. eventTime), it is very difficult to re-sort the data by a different index (i.e. userID)
# MAGIC 
# MAGIC **Question:** How do you create a notebook?  
# MAGIC **Answer:** Sign into Databricks, select the **Home** icon from the sidebar, right-click your home-folder, select **Create**, and then **Notebook**. In the **Create Notebook** dialog, specify the name of your notebook and the default programming language.
# MAGIC 
# MAGIC **Question:** How do you create a cluster?  
# MAGIC **Answer:** Select the **Clusters** icon on the sidebar, click the **Create Cluster** button, specify the specific settings for your cluster and then click **Create Cluster**.
# MAGIC 
# MAGIC **Question:** How do you attach a notebook to a cluster?  
# MAGIC **Answer:** If you run a command while detached, you may be prompted to connect to a cluster. To connect to a specific cluster, open the cluster menu by clicking the **Attached/Detached** menu item and then selecting your desired cluster.

# COMMAND ----------

# MAGIC %md
# MAGIC ##Create
# MAGIC **Q:** What is the Databricks Delta command to display metadata?<br>
# MAGIC **A:** Metadata is displayed through `DESCRIBE DETAIL tableName`.
# MAGIC 
# MAGIC **Q:** Where does the schema for a Databricks Delta data set reside?<br>
# MAGIC **A:** The table name, path, database info are stored in Hive metastore, the actual schema is stored in the `_delta_logs` directory.
# MAGIC 
# MAGIC **Q:** What is the general rule about partitioning and the cardinality of a set?<br>
# MAGIC **A:** We should partition on sets that are of small cardinality to avoid penalties incurred with managing large quantities of partition info meta-data.
# MAGIC 
# MAGIC **Q:** What is schema-on-read?<br>
# MAGIC **A:** It stems from Hive and roughly means: the schema for a data set is unknown until you perform a read operation.
# MAGIC 
# MAGIC **Q:** How does this problem manifest in Databricks assuming a `parquet` based data lake?<br>
# MAGIC **A:** It shows up as missing data upon load into a table in Databricks.
# MAGIC 
# MAGIC **Q:** How do you remedy this problem in Databricks above?<br>
# MAGIC **A:** To remedy, you repair the table using `MSCK REPAIR TABLE` or switch to Databricks Delta!

# COMMAND ----------

# MAGIC %md
# MAGIC ##Append
# MAGIC 
# MAGIC **Q:** What parameter do you need to add to an existing dataset in a Delta table?<br>
# MAGIC **A:** 
# MAGIC `df.write...mode("append").save("..")`
# MAGIC 
# MAGIC **Q:** What's the difference between `.mode("append")` and `.mode("overwrite")` ?<br>
# MAGIC **A:** `append` atomically adds new data to an existing Databricks Delta table and `overwrite` atomically replaces all of the data in a table.
# MAGIC 
# MAGIC **Q:** I've just repaired `myTable` using `MSCK REPAIR TABLE myTable;`
# MAGIC How do I verify that the repair worked ?<br>
# MAGIC **A:** `SELECT count(*) FROM myTable` and make sure the count is what I expected
# MAGIC 
# MAGIC 
# MAGIC   
# MAGIC **Q:** In exercise 2, why did we use `.withColumn(.. cast(rand(5) ..)` i.e. pass a seed to the `rand()` function ?<br>
# MAGIC **A:** In order to ensure we get the SAME set of pseudo-random numbers every time, on every cluster.

# COMMAND ----------

# MAGIC %md
# MAGIC ##Upsert
# MAGIC 
# MAGIC **Q:** What does it mean to UPSERT?<br>
# MAGIC **A:** To UPSERT is to either INSERT a row, or if the row already exists, UPDATE the row.
# MAGIC 
# MAGIC **Q:** What happens if you try to UPSERT in a parquet-based data set?<br>
# MAGIC **A:** That's not possible due to the schema-on-read paradigm, you will get an error until you refresh the table.
# MAGIC 
# MAGIC **Q:** What is schema-on-read?<br>
# MAGIC **A:** It stems from Hive and roughly means: the schema for a data set is unknown until you perform a read operation.
# MAGIC 
# MAGIC **Q:** How to you perform UPSERT in a Databricks Delta dataset?<br>
# MAGIC **A:** Using the `MERGE INTO my-table USING data-to-upsert`.
# MAGIC 
# MAGIC **Q:** What is the caveat to `USING data-to-upsert`?<br>
# MAGIC **A:** Your source data has ALL the data you want to replace: in other words, you create a new dataframe that has the source data you want to replace in the Databricks Delta table.

# COMMAND ----------

# MAGIC %md
# MAGIC ##Streaming
# MAGIC **Q:** Why is Databricks Delta so important for a data lake that incorporates streaming data?<br>
# MAGIC **A:** Frequent meta data refreshes, table repairs and accumulation of small files on a secondly- or minutely-basis!
# MAGIC 
# MAGIC **Q:** What happens if you shut off your stream before it has fully initialized and started and you try to `CREATE TABLE .. USING DELTA` ? <br>
# MAGIC **A:** You will get this: `Error in SQL statement: AnalysisException: The user specified schema is empty;`.
# MAGIC 
# MAGIC **Q:** When you do a write stream command, what does this option do `outputMode("append")` ?<br>
# MAGIC **A:** This option takes on the following values and their respective meanings:
# MAGIC * <b>append</b>: add only new records to output sink
# MAGIC * <b>complete</b>: rewrite full output - applicable to aggregations operations
# MAGIC * <b>update</b>: update changed records in place
# MAGIC 
# MAGIC **Q:** What happens if you do not specify `option("checkpointLocation", pointer-to-checkpoint directory)`?<br>
# MAGIC **A:** When the streaming job stops, you lose all state around your streaming job and upon restart, you start from scratch.
# MAGIC 
# MAGIC **Q:** How do you view the list of active streams?<br>
# MAGIC **A:** Invoke `spark.streams.active`.
# MAGIC 
# MAGIC **Q:** How do you verify whether `streamingQuery` is running (boolean output)?<br>
# MAGIC **A:** Invoke `spark.streams.get(streamingQuery.id).isActive`.

# COMMAND ----------

# MAGIC %md
# MAGIC ##Optimization
# MAGIC **Q:** Why are many small files problematic when doing queries on data backed by these?<br>
# MAGIC **A:** If there are many files, some of whom may not be co-located the principal sources of slowdown are
# MAGIC * network latency 
# MAGIC * (volume of) file metatadata 
# MAGIC 
# MAGIC **Q:** What do `OPTIMIZE` and `VACUUM` do?<br>
# MAGIC **A:** `OPTIMIZE` creates the larger file from a collection of smaller files and `VACUUM` deletes the invalid small files that were used in compaction.
# MAGIC 
# MAGIC **Q:** What size files does `OPTIMIZE` compact to and why that value?<br>
# MAGIC **A:** Small files are compacted to around 1GB; this value was determined by the Spark optimization team as a good compromise between speed and performace.
# MAGIC 
# MAGIC **Q:** What should one be careful of when using `VACUUM`?<br>
# MAGIC **A:** Don't set a retention interval shorter than seven days because old snapshots and uncommitted files can still be in use by concurrent readers or writers to the table.
# MAGIC 
# MAGIC **Q:** What does `ZORDER` do?<br>
# MAGIC **A:** It is a technique to colocate related information in the same set of files. 

# COMMAND ----------

# MAGIC %md
# MAGIC ##Architecture
# MAGIC **Q:** Why are many small files problematic when doing queries on data backed by these?<br>
# MAGIC **A:** If there are many files, some of whom may not be co-located the principal sources of slowdown are
# MAGIC * network latency 
# MAGIC * (volume of) file metatadata 
# MAGIC 
# MAGIC **Q:** What do `OPTIMIZE` and `VACUUM` do?<br>
# MAGIC **A:** `OPTIMIZE` creates the larger file from a collection of smaller files and `VACUUM` deletes the invalid small files that were used in compaction.
# MAGIC 
# MAGIC **Q:** What size files does `OPTIMIZE` compact to and why that value?<br>
# MAGIC **A:** Small files are compacted to around 1GB; this value was determined by the Spark optimization team as a good compromise between speed and performace.
# MAGIC 
# MAGIC **Q:** What should one be careful of when using `VACUUM`?<br>
# MAGIC **A:** Don't set a retention interval shorter than seven days because old snapshots and uncommitted files can still be in use by concurrent readers or writers to the table.
# MAGIC 
# MAGIC **Q:** What does `ZORDER` do?<br>
# MAGIC **A:** It is a technique to colocate related information in the same set of files. 

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>