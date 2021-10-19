# Databricks notebook source
# MAGIC %md
# MAGIC ### Dataset 2 
# MAGIC 
# MAGIC Source Page: https://www.cms.gov/Research-Statistics-Data-and-Systems/Statistics-Trends-and-Reports/Medicare-Provider-Charge-Data/Inpatient.html
# MAGIC 
# MAGIC Data Link: https://data.cms.gov/api/views/97k6-zzx3/rows.json?accessType=DOWNLOAD
# MAGIC 
# MAGIC Inpatient Prospective Payment System (IPPS) Provider Summary for the Top 100 Diagnosis-Related Groups (DRG) - FY2011
# MAGIC 
# MAGIC > 6/2/14 UPDATE: Original FY2011 data file has been updated to include a new column, "Average Medicare Payment." 
# MAGIC > The data provided here include hospital-specific charges for the more than 3,000 U.S. hospitals that receive Medicare Inpatient Prospective Payment System (IPPS) payments for the top 100 most frequently billed discharges
# MAGIC > Paid under Medicare based on a rate per discharge using the Medicare Severity Diagnosis Related Group (MS-DRG) for Fiscal Year (FY) 2011. 
# MAGIC > These DRGs represent more than 7 million discharges or 60 percent of total Medicare IPPS discharges.

# COMMAND ----------

copy_to_dbfs("https://data.cms.gov/api/views/97k6-zzx3/rows.json?accessType=DOWNLOAD", "/tmp/drg_payments")

# COMMAND ----------

df = spark.read.json("/tmp/drg_payments")

tmp_table_name = "mwc_tmp"
def get_col_names(df, table_name = "mwc_tmp"):
  df.createOrReplaceTempView(table_name)
  return sqlContext.sql("select the_columns.fieldName, the_columns.position + 7 as idx from {0}\
                  lateral view explode({0}.meta.view.columns) c as the_columns \
                  where the_columns.position > 0".format("mwc_tmp"))
  
df_cols = get_col_names(df)
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
  dbutils.fs.ls("/mnt/mwc/drg_payments")
  data_df.repartition(12).write.parquet("/mnt/mwc/drg_payments")
except:
  print "Medicare claims exist. Cleanup directory if re-write is needed." 

# COMMAND ----------

df_drg = spark.read.parquet("/mnt/mwc/drg_payments")
df_drg.createOrReplaceTempView("drg_payments")
df_drg.printSchema()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from drg_payments limit 20 

# COMMAND ----------

# MAGIC %sql
# MAGIC select provider_state, sum(total_discharges) as total_discharged from drg_payments group by provider_state order by total_discharged desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select drg_definition, count(1) as cnts from drg_payments group by drg_definition order by cnts desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select drg_definition, 
# MAGIC   provider_state, 
# MAGIC   (average_medicare_payments + average_medicare_payments_2) - average_covered_charges as diff
# MAGIC   from drg_payments 
# MAGIC   where average_covered_charges < average_medicare_payments + average_medicare_payments_2 
# MAGIC   order by diff desc 
# MAGIC   limit 100

# COMMAND ----------

# MAGIC %sql
# MAGIC select max(average_covered_charges), max(average_medicare_payments), max(average_medicare_payments_2) from drg_payments

# COMMAND ----------

# MAGIC %sql
# MAGIC select provider_name, provider_state, 
# MAGIC   average_medicare_payments,
# MAGIC   average_covered_charges,
# MAGIC   average_medicare_payments / average_covered_charges as pct_covered, 
# MAGIC   average_covered_charges  
# MAGIC   from drg_payments
# MAGIC   order by pct_covered desc

# COMMAND ----------

