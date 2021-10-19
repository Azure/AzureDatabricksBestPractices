-- Databricks notebook source
-- MAGIC %md
-- MAGIC 
-- MAGIC # **Log Analysis in SQL**
-- MAGIC This notebook analyzes Apache access logs with SQL.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### **Step 1:** Import the data into a Spark SQL table.
-- MAGIC 
-- MAGIC In this example, we will use the sample file that has already been saved on DBFS. To generate your own sample logs, run the **Create Sample Logs** notebook.

-- COMMAND ----------

-- MAGIC %fs ls /databricks-datasets/sample_logs

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC This example will use the RegexSerDe in the hive-contrib library which is not installed by default.
-- MAGIC 
-- MAGIC See the [Libraries Notebook](/#workspace/databricks_guide/02 Product Overview/04 Libraries) and install the Maven Coordinate: **org.apache.hive:hive-contrib:0.13.0**

-- COMMAND ----------

-- MAGIC %md  #### Use a **CREATE EXTERNAL TABLE** command with the RegexSerDe format.

-- COMMAND ----------

-- MAGIC %sql show tables

-- COMMAND ----------

DROP TABLE IF EXISTS SQLApacheAccessLogsTable

-- COMMAND ----------

CREATE EXTERNAL TABLE SQLApacheAccessLogsTable (
  ipaddress STRING,
  clientidentd STRING,
  userid STRING,
  datetime STRING,
  method STRING,
  endpoint STRING,
  protocol STRING,
  responseCode INT,
  contentSize BIGINT
)
ROW FORMAT
  SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
WITH SERDEPROPERTIES (
  'input.regex' = '^(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \\"(\\S+) (\\S+) (\\S+)\\" (\\d{3}) (\\d+)'
)
LOCATION 
  -- NOTE: Replace with your data location if you have this somewhere else.
  '/databricks-datasets/sample_logs'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC #### **Tip:** If you collect logs on a daily or hourly basis, consider using partitions on your table.  
-- MAGIC See the **[Partitioned Tables Notebook](/#workspace/databricks_guide/06 Spark SQL & DataFrames/04 Partitioned Tables)** for more details.

-- COMMAND ----------

SELECT * from SQLApacheAccessLogsTable limit 10

-- COMMAND ----------

-- MAGIC %md #### Cache the table for faster analysis - since we are querying the table many times.

-- COMMAND ----------

CACHE TABLE SQLApacheAccessLogsTable

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC 
-- MAGIC ### **Step 2:** Calculate statistics on the Content Sizes returned.

-- COMMAND ----------

-- MAGIC %sql SELECT (SUM(contentsize) / COUNT(*)) as avg FROM SQLApacheAccessLogsTable

-- COMMAND ----------

-- MAGIC %sql SELECT MIN(contentsize) as min FROM SQLApacheAccessLogsTable

-- COMMAND ----------

-- MAGIC %sql SELECT MAX(contentsize) as max FROM SQLApacheAccessLogsTable

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Click on this above cell to see the SQL statements that are embedded in that Markdown cell.

-- COMMAND ----------

-- MAGIC %md ### **Step 3:** Display the response code to count statistics.

-- COMMAND ----------

-- Results of running a select statement in SQL are automatically graphed.
--   For this display, a Pie Chart is chosen by selecting that icon after running this cell.
--   Then, "Plot Options..." was used to make the response_code the key and the_count as the value.
select responsecode, count(*) as the_count from SQLApacheAccessLogsTable group by responsecode order by the_count desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### **Step 4:** View a list of IPAddresses that has accessed the server more than N times.

-- COMMAND ----------

-- Use the parameterized query option to allow a viewer to dynamically specify a value for N.
-- Note how it's not necessary to worry about limiting the number of results.
-- The number of values returned are automatically limited to 1000.
-- But there are options to view a plot that would contain all the data to view the trends.
SELECT ipaddress, COUNT(*) AS total FROM SQLApacheAccessLogsTable GROUP BY ipaddress HAVING total > $N 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC Change N in the cell above, hit enter, and watch the chart update dynamically.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### **Step 5:** Explore Statistics about the endpoints. 

-- COMMAND ----------

-- Display a plot of the distribution of the number of hits across the endpoints.
SELECT endpoint, count(*) as num_hits FROM SQLApacheAccessLogsTable GROUP BY endpoint ORDER BY num_hits DESC


-- COMMAND ----------

-- Or select the table view to see the names of the most popular endpoints.
select endpoint, count(*) AS num_hits FROM SQLApacheAccessLogsTable GROUP BY endpoint order by num_hits desc