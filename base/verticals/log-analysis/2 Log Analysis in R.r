# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # **Log Analysis in R**
# MAGIC This notebook analyzes Apache access logs with R, using SparkR dataframes.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### **Step 1:** Change this parameter to your sample logs directory.
# MAGIC 
# MAGIC In this example, we will use the sample file that has already been saved on DBFS.

# COMMAND ----------

# MAGIC %fs ls /databricks-datasets/sample_logs

# COMMAND ----------

# MAGIC %sql DROP TABLE IF EXISTS RApacheAccessLogsTable

# COMMAND ----------

# MAGIC %sql CREATE EXTERNAL TABLE RApacheAccessLogsTable (
# MAGIC   ipaddress STRING,
# MAGIC   clientidentd STRING,
# MAGIC   userid STRING,
# MAGIC   datetime STRING,
# MAGIC   method STRING,
# MAGIC   endpoint STRING,
# MAGIC   protocol STRING,
# MAGIC   responseCode INT,
# MAGIC   contentSize BIGINT
# MAGIC )
# MAGIC ROW FORMAT
# MAGIC   SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
# MAGIC WITH SERDEPROPERTIES (
# MAGIC   "input.regex" = '^(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \\"(\\S+) (\\S+) (\\S+)\\" (\\d{3}) (\\d+)'
# MAGIC )
# MAGIC LOCATION 
# MAGIC   -- Replace with your data location if you have this somewhere else.
# MAGIC   "/databricks-datasets/sample_logs"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Create a SparkR dataframe from the table.

# COMMAND ----------

df <- sql(sqlContext, "select * from RApacheAccessLogsTable")

# COMMAND ----------

head(df)

# COMMAND ----------

# MAGIC %md ### **Step 2:** Calculate statistics on the Content Sizes returned.

# COMMAND ----------

head(agg(df, contentsize = "avg"))

# COMMAND ----------

head(agg(df, contentsize = "min"))

# COMMAND ----------

head(agg(df, contentsize = "max"))

# COMMAND ----------

# MAGIC %md ### **Step 3:** Display the response code to count statistics.

# COMMAND ----------

counts <- agg(group_by(df, df$responsecode), count = n(df$responsecode))
responseCodeCount <- arrange(counts, desc(counts$count))
head(responseCodeCount)

# COMMAND ----------

# Now, display can be called on the resulting DataFrame.
# For this display, a Pie Chart is chosen by selecting that icon.
# Then, "Plot Options..." was used to make the response_code the key and count as the value.
display(responseCodeCount)

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Step 4:** View a list of IPAddresses that has accessed the server more than N times.

# COMMAND ----------

# Change this number as it makes sense for your data set.
n = 100

# COMMAND ----------

# Count number of accesses by IP Address
ipCount <- agg(group_by(df, df$ipaddress), count = n(df$ipaddress))

# Filter for IP Addresses that has accessed the server more than N times
display(filter(ipCount, ipCount$count > n))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### **Step 5:** Explore statistics about the endpoints. 

# COMMAND ----------

# Calculate the number of hits for each endpoint
endptCount <- agg(group_by(df, df$endpoint), num_hits = n(df$endpoint))

# Display a plot of the distribution of the number of hits across the endpoints
display(endptCount)


# COMMAND ----------

# View top 10 endpoints
endptCount_desc <- arrange(endptCount, desc(endptCount$num_hits))
take(endptCount_desc, 10)