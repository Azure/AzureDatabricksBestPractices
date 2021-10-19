# Databricks notebook source
dbutils.widgets.text("mypath","/dbfs/mnt/landing/MDM/20181213131900_000100_MDM.zip")

# COMMAND ----------

#Allows us to connect to Azure Blob Storage #######################################
#Azure Blob Storage Name
storage_account_name = "devasaanalytica2"
#Azure Blob Storage Access Key
storage_account_access_key = "fcvBvFdqIhV5viN98jnaUhskBRRHY7jZXUGS/KOumT0Dtl3H3XyI/h2T9psBw/YH8G0FSBRK+ssr+LxwkJ0Kxg=="

# COMMAND ----------

strRootBlobName_SourceFolder = "landing"
wasbs_string_Source = "wasbs://" + strRootBlobName_SourceFolder + "@"+ storage_account_name +".blob.core.windows.net" 
mount = "/mnt/landing"
config_key = "fs.azure.account.key." + storage_account_name + ".blob.core.windows.net"

# COMMAND ----------

dbutils.fs.mount(
  source = wasbs_string_Source,
  mount_point = mount,
  extra_configs = {config_key:storage_account_access_key})

# COMMAND ----------

# MAGIC %sh
# MAGIC mypath = dbutils.widgets.get("mypath")

# COMMAND ----------

dbutils.fs.unmount(mount)

# COMMAND ----------

foreach 
./run notebookname  %param1