# Databricks notebook source
# MAGIC %md
# MAGIC ### Medicare Industry Analysis
# MAGIC We will use public datasets to attempt to define anti-fraud strategies to understand the key demographics and regions that exploit our programs:
# MAGIC * medications
# MAGIC * wellness programs
# MAGIC * health tracking tools
# MAGIC 
# MAGIC This notebook will cover the following:
# MAGIC * Define ETL Code to Fetch Public Healthcare Data
# MAGIC * Analyze the average costs around the US and identify trends, e.g. age, geography, population density
# MAGIC 
# MAGIC ![FraudTherapy](https://s3-us-west-2.amazonaws.com/dbc-mwc/images/medicare-fraud-graph-medicare-fraud-therapy.jpg)
# MAGIC 
# MAGIC 
# MAGIC #### Let's use Apache Spark to programmatically find these anomalies

# COMMAND ----------

# MAGIC %md
# MAGIC #### Medicare doles out nearly $600 billion each year
# MAGIC Its estimated that 10% ($98 billion of Medicare and Medicaid spending) and up to $272 billion across the entire health system.  
# MAGIC Ref: [Economist 2014: The $272 billion swindle](http://www.economist.com/news/united-states/21603078-why-thieves-love-americas-health-care-system-272-billion-swindle).
# MAGIC 
# MAGIC If we could use Apache Spark to save .1% of $100 billion, we could save the government $100M.  
# MAGIC 
# MAGIC Let's looking into the public Medicare Claims data to do exploratory data analysis. We can look at other public data sets to link link these datasets together to find trends across the US. 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Medicare Hospital Spending by Claim
# MAGIC Organization: **Centers for Medicare &amp; Medicaid Services**  
# MAGIC Source page: https://catalog.data.gov/dataset/medicare-hospital-spending-by-claim-61b57
# MAGIC 
# MAGIC Data file:
# MAGIC https://data.medicare.gov/api/views/nrth-mfg3/rows.json?accessType=DOWNLOAD
# MAGIC 
# MAGIC [Part A Covers](https://www.medicare.gov/what-medicare-covers/part-a/what-part-a-covers.html):
# MAGIC * Hospital care
# MAGIC * Skilled nursing facility care
# MAGIC * Nursing home care (as long as custodial care isn't the only care you need)
# MAGIC * Hospice
# MAGIC * Home health services
# MAGIC 
# MAGIC [Part B Covers](https://www.medicare.gov/what-medicare-covers/part-b/what-medicare-part-b-covers.html):
# MAGIC * Clinical research  
# MAGIC * Ambulance services
# MAGIC * Durable medical equipment (DME)
# MAGIC * Mental health
# MAGIC   * Inpatient
# MAGIC   * Outpatient
# MAGIC   * Partial hospitalization
# MAGIC * Getting a second opinion before surgery
# MAGIC * Limited outpatient prescription drugs

# COMMAND ----------

# MAGIC %md
# MAGIC The `%run "/Users/mwc@databricks.com/demo/Medicare Analysis/_etl_helper.py"` won't work since this is relative to my home workspace, but here's the relevant code that loads the copy_to_dbfs() function. 
# MAGIC ```
# MAGIC # Fetch the dataset 
# MAGIC import urllib
# MAGIC # Save the data locally to an instance, then move to dbfs
# MAGIC def removeNonAscii(s): return "".join(c for c in s if ord(c)<128)
# MAGIC 
# MAGIC def get_url_and_strip(url, path):
# MAGIC   """ 
# MAGIC   url: URL to source file
# MAGIC   filename: full path to file
# MAGIC   Opens a socket, reads line by line parsing non-ascii characters, then writing to local FS
# MAGIC   """
# MAGIC   data_socket = urllib.urlopen(url)
# MAGIC   data_file = path + "data.json" if (path[-1] == '/') else path
# MAGIC   with open(path + "/data.json", "w") as fd:
# MAGIC     for line in data_socket.readlines():
# MAGIC       fd.write(line.strip())
# MAGIC   fd.close()
# MAGIC 
# MAGIC def copy_to_dbfs(url, path="/tmp/"):
# MAGIC   # Save the data locally to an instance, then move to dbfs 
# MAGIC   dbutils.fs.mkdirs("file:{0}".format(path))
# MAGIC   get_url_and_strip(url, path)
# MAGIC   # Copy over to DBFS
# MAGIC   dbutils.fs.mv("file:{0}".format(path), "dbfs:{0}".format(path), True)
# MAGIC ```

# COMMAND ----------

# MAGIC %run "/Users/mwc@databricks.com/demo/Medicare Analysis/_etl_helper.py"

# COMMAND ----------

copy_to_dbfs("https://data.medicare.gov/api/views/nrth-mfg3/rows.json?accessType=DOWNLOAD", "/tmp/medicare_claims")

# COMMAND ----------

df = spark.read.json("/tmp/medicare_claims")

tmp_table_name = "mwc_tmp"
def get_col_names(df, table_name = "mwc_tmp"):
  df.createOrReplaceTempView(table_name)
  return spark.sql("select the_columns.fieldName, the_columns.position + 7 as idx from {0}\
                  lateral view explode({0}.meta.view.columns) c as the_columns \
                  where the_columns.position > 0".format("mwc_tmp"))
  
df_cols = get_col_names(df, tmp_table_name)
display(df_cols)

# COMMAND ----------

# Collect the DataFrame column information and craft a SQL Query to fetch the relevant data 
df_cols.collect()
column_index = [(x.fieldName, x.idx) for x in df_cols.collect()]
select_stmnt = ""
for (c, i) in column_index:
  select_stmnt += "the_data[{0}] as {1}, ".format(i, c)  
  
data_df = sql("select {0} from {1} lateral view explode({1}.data) d as the_data".format(select_stmnt[:-2], tmp_table_name))

try:
  dbutils.fs.ls("/mnt/mwc/medicare_claims")
  data_df.repartition(12).write.parquet("/mnt/mwc/medicare_claims")
except:
  print "Medicare claims exist. Cleanup directory if re-write is needed." 

# COMMAND ----------

# DBTITLE 1,Reload the datasets as SparkSQL Tables
spark.read.parquet("/mnt/mwc/medicare_claims").createOrReplaceTempView("medicare_claims")
spark.read.parquet("/mnt/mwc/drg_payments").createOrReplaceTempView("drg_payments")

# COMMAND ----------

table("medicare_claims").printSchema()

# COMMAND ----------

# Get the column names directly 
[i for i in table("medicare_claims").columns]

# COMMAND ----------

# MAGIC %sql 
# MAGIC select count(*) from medicare_claims

# COMMAND ----------

# MAGIC %sql
# MAGIC -- How many hospitals within the dataset
# MAGIC select count(distinct(hospital_name)) as num_of_hospitals from medicare_claims

# COMMAND ----------

# MAGIC %sql
# MAGIC -- What are the various claim types
# MAGIC select distinct(claim_type) from medicare_claims

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- How many claim type records are there per state
# MAGIC select state, claim_type, count(1) as total_claims from medicare_claims 
# MAGIC   group by claim_type, state
# MAGIC   order by total_claims desc
# MAGIC   LIMIT 200

# COMMAND ----------

# MAGIC %md
# MAGIC Interesting that the dataset shows CA and TX with similar record types given a difference in the population
# MAGIC > California is still the most populous state in the nation with 38.8 million residents. Texas is the second most populous state with a population of 27.0 million people.

# COMMAND ----------

# MAGIC %sql
# MAGIC select hospital_name,
# MAGIC provider_number,
# MAGIC state,
# MAGIC period,
# MAGIC claim_type,
# MAGIC avg_spending_per_episode_hospital,
# MAGIC avg_spending_per_episode_state,
# MAGIC avg_spending_per_episode_nation  from medicare_claims
# MAGIC where state in ("TX" , "CA")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(distinct(provider_number)) as unique_providers, max(avg_spending_per_episode_hospital), max(avg_spending_per_episode_state), state from medicare_claims where state in ("CA", "TX") group by state

# COMMAND ----------

# MAGIC %sql 
# MAGIC select hospital_name, provider_number, claim_type, cast(avg_spending_per_episode_hospital as int) 
# MAGIC   from medicare_claims 
# MAGIC   where state = "TX" 
# MAGIC   sort by avg_spending_per_episode_hospital desc 

# COMMAND ----------

# MAGIC %run "/Users/mwc@databricks.com/demo/Medicare Analysis/USA Population"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from usa_population 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Conclusion
# MAGIC The exploratory work helped increasing trends between different states and types of claims across hospitals. Given that this was a sample of the dataset, the same logic applies to the complete dataset to identify these trends and anomalies at a larger scale.  
# MAGIC We can look at another public dataset to see if we can identify possible fraudulent claims, and run ML algorithms to identify these trends. 

# COMMAND ----------

# MAGIC %md
# MAGIC We will attempt to gather more public data to find relationships between the different claim information types. The ML algorithms will be implemented on the [Medicare Fraud Analysis Notebook](#workspace/Users/mwc@databricks.com/demo/Medicare Analysis/3. Medicare Fraud Analysis)

# COMMAND ----------

