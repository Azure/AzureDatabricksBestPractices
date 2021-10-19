# Databricks notebook source
# Fetch the dataset 
import urllib
# Save the data locally to an instance, then move to dbfs
def removeNonAscii(s): return "".join(c for c in s if ord(c)<128)

def get_url_and_strip(url, path):
  """ 
  url: URL to source file
  filename: full path to file
  Opens a socket, reads line by line parsing non-ascii characters, then writing to local FS
  """
  data_socket = urllib.urlopen(url)
  data_file = path + "data.json" if (path[-1] == '/') else path
  with open(path + "/data.json", "w") as fd:
    for line in data_socket.readlines():
      fd.write(line.strip())
  fd.close()

def copy_to_dbfs(url, path="/tmp/"):
  # Save the data locally to an instance, then move to dbfs 
  dbutils.fs.mkdirs("file:{0}".format(path))
  get_url_and_strip(url, path)
  # Copy over to DBFS
  dbutils.fs.mv("file:{0}".format(path), "dbfs:{0}".format(path), True)

# COMMAND ----------

copy_to_dbfs("https://data.cms.gov/api/views/97k6-zzx3/rows.json?accessType=DOWNLOAD", "/tmp/drg_payments")
copy_to_dbfs("https://data.medicare.gov/api/views/nrth-mfg3/rows.json?accessType=DOWNLOAD", "/tmp/medicare_claims")

# COMMAND ----------

df1 = spark.read.json("/tmp/drg_payments")

tmp_table_name1 = "mwc_tmp1"
def get_col_names(df, table_name = "mwc_tmp1"):
  df.createOrReplaceTempView(table_name)
  return sqlContext.sql("select the_columns.fieldName, the_columns.position + 7 as idx from {0}\
                  lateral view explode({0}.meta.view.columns) c as the_columns \
                  where the_columns.position > 0".format("mwc_tmp1"))
  
df_cols1 = get_col_names(df1)
display(df_cols1)

df2 = spark.read.json("/tmp/medicare_claims")

tmp_table_name2 = "mwc_tmp2"
def get_col_names(df, table_name = "mwc_tmp2"):
  df.createOrReplaceTempView(table_name)
  return spark.sql("select the_columns.fieldName, the_columns.position + 7 as idx from {0}\
                  lateral view explode({0}.meta.view.columns) c as the_columns \
                  where the_columns.position > 0".format("mwc_tmp2"))
  
df_cols2 = get_col_names(df2, tmp_table_name)
display(df_cols2)

# COMMAND ----------

# Collect the DataFrame column information and craft a SQL Query to fetch the relevant data 
df_cols1.collect()
column_index1 = [(x.fieldName, x.idx) for x in df_cols1.collect()]
select_stmnt = ""
for (c, i) in column_index1:
  select_stmnt += "the_data[{0}] as {1}, ".format(i, c)  
  
data_df1 = sql("select {0} from {1} lateral view explode({1}.data) d as the_data".format(select_stmnt[:-2], tmp_table_name1))

try:
  data_df1.repartition(12).write.parquet("/mnt/wesley/drg_payments")
except:
  print "Drug Payments exist. Cleanup directory if re-write is needed." 

# COMMAND ----------

# MAGIC %fs 
# MAGIC ls /mnt/wesley/drg_payments

# COMMAND ----------

# Collect the DataFrame column information and craft a SQL Query to fetch the relevant data 
df_cols2.collect()
column_index2 = [(x.fieldName, x.idx) for x in df_cols2.collect()]
select_stmnt = ""
for (c, i) in column_index2:
  select_stmnt += "the_data[{0}] as {1}, ".format(i, c)  
  
data_df2 = sql("select {0} from {1} lateral view explode({1}.data) d as the_data".format(select_stmnt[:-2], tmp_table_name2))

try:
  data_df2.repartition(12).write.parquet("/mnt/wesley/medicare_claims")
except:
  print "Medicare claims exist. Cleanup directory if re-write is needed." 

# COMMAND ----------

spark.read.parquet("/mnt/wesley/medicare_claims").createOrReplaceTempView("medicare_claims")
spark.read.parquet("/mnt/wesley/drg_payments").createOrReplaceTempView("drg_payments")

# COMMAND ----------

table("medicare_claims").printSchema()

# COMMAND ----------

table("drg_payments").printSchema()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from drg_payments limit 20 

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from medicare_claims limit 20

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from medicare_claims a join drg_payments b on a.provider_number=b.provider_id