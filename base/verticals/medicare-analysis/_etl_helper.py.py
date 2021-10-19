# Databricks notebook source
# DBTITLE 1,Extract-Transform-Load Functions
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

def get_col_names(df, table_name = "mwc_tmp"):
  df.createOrReplaceTempView(table_name)
  return spark.sql("select the_columns.fieldName, the_columns.position + 7 as idx from {0}\
                  lateral view explode({0}.meta.view.columns) c as the_columns \
                  where the_columns.position > 0".format("mwc_tmp"))

def fetch_and_create_table(url, folder_name):
  # Define temp location 
  tmp_loc = "/tmp/" + folder_name
  dst_mnt_loc = "/mnt/mwc/" + folder_name
  
  # Load the data 
  copy_to_dbfs(url, tmp_loc) 
  
  # Read the source json file
  tmp_table_name = "mwc_tmp"
  df = spark.read.json(tmp_loc)
  # Get the column names 
  df_cols = get_col_names(df, tmp_table_name)
  # Collect the DataFrame column information and craft a SQL Query to fetch the relevant data 
  df_cols.collect()
  column_index = [(x.fieldName, x.idx) for x in df_cols.collect()]
  select_stmnt = ""
  for (c, i) in column_index:
    select_stmnt += "the_data[{0}] as {1}, ".format(i, c)  

  data_df = sql("select {0} from {1} lateral view explode({1}.data) d as the_data".format(select_stmnt[:-2], tmp_table_name))

  try:
    dbutils.fs.ls(dst_mnt_loc)
    print "Destination file exist. Cleanup directory if re-write is needed." 
  except:
    print "Writing dataset out to: " + dst_mnt_loc
    data_df.repartition(12).write.parquet(dst_mnt_loc)

# COMMAND ----------

