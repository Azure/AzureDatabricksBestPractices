# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Working with ADLS
# MAGIC 
# MAGIC [ADLS Gen2 support in ADB](https://docs.microsoft.com/en-us/azure/databricks/data/data-sources/azure/azure-datalake-gen2?toc=https%3A%2F%2Fdocs.microsoft.com%2Fen-us%2Fazure%2Fazure-databricks%2Ftoc.json&bc=https%3A%2F%2Fdocs.microsoft.com%2Fen-us%2Fazure%2Fbread%2Ftoc.json)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ###There are 4 ways to connect to ADLS Gen2
# MAGIC 1. Using the ADLS Gen2 storage account access key directly
# MAGIC 2. Mounting an ADLS Gen2 filesystem to DBFS using a service principal (OAuth 2.0)
# MAGIC 3. Using a service principal directly (OAuth 2.0)
# MAGIC 4. Azure Active Directory (AAD) credential passthrough

# COMMAND ----------

# MAGIC %md
# MAGIC ##1. Using the ADLS Gen2 storage account access key directly

# COMMAND ----------

# MAGIC %md
# MAGIC ####1. Get Access Key from ADLS
# MAGIC 
# MAGIC <img src="https://publicimg.blob.core.windows.net/images/ADLS Access Key.png" width="600">

# COMMAND ----------

# MAGIC %scala
# MAGIC val accesskey = "mFVCLXVfFrNMlmnuHbGNKJP2nLkiINGdYarCA9kHrwN5OmKk2344TxWcMiLW3PL2WTpTLTNLqrRUZ0QFLIt6aA=="

# COMMAND ----------

# MAGIC %scala
# MAGIC spark.conf.set(
# MAGIC   "fs.azure.account.key.nikhilg.dfs.core.windows.net",accesskey)

# COMMAND ----------

# MAGIC %scala
# MAGIC val df = spark.read.csv("abfss://epldata@nikhilg.dfs.core.windows.net/")
# MAGIC display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Setting up Service Principal

# COMMAND ----------

# MAGIC %md 
# MAGIC Please use the below notebook to create a Service Principal
# MAGIC 
# MAGIC ## <div><a href="$./Create Service Principal">Create a Service Principal</a> <b style="font-size: 160%; color:#1CA0C2;"></b></div>

# COMMAND ----------

# MAGIC %md
# MAGIC From Service Principal get the following parameters
# MAGIC 1. Client ID
# MAGIC 2. Tenant ID
# MAGIC 3. Secret

# COMMAND ----------

# MAGIC %md
# MAGIC ##Give Access to ADLS Folders to the Service Principal

# COMMAND ----------

# MAGIC %md
# MAGIC ####1. Go to Azure Explorer - Select the ADLS account and the folder - Click Manage Access
# MAGIC 
# MAGIC <img src="https://publicimg.blob.core.windows.net/images/1.Ex.png" width="800">

# COMMAND ----------

# MAGIC %md
# MAGIC ####2. Select the Service Principal you want to give access to the folder
# MAGIC 
# MAGIC <img src="https://publicimg.blob.core.windows.net/images/2.Ex.png" width="800">

# COMMAND ----------

# MAGIC %md
# MAGIC ####3. Give require access - Minimum Level of Access is Execute
# MAGIC 
# MAGIC <img src="https://publicimg.blob.core.windows.net/images/3.Ex.png" width="800">

# COMMAND ----------

# MAGIC %md
# MAGIC ####1. Go to the Storage Account -> Access Control -> View Role Assignment
# MAGIC 
# MAGIC <img src="https://publicimg.blob.core.windows.net/images/1.SPA.png" width="800">

# COMMAND ----------

# MAGIC %md
# MAGIC ####2. Add Role Assigment
# MAGIC 
# MAGIC <img src="https://publicimg.blob.core.windows.net/images/2.SPA.png" width="800">

# COMMAND ----------

# MAGIC %md
# MAGIC ####3. Give Blob Data Contributor Role to the SP
# MAGIC 
# MAGIC <img src="https://publicimg.blob.core.windows.net/images/3.SPA.png" width="500">

# COMMAND ----------

# MAGIC %md
# MAGIC ####4. Confirm the access has been given
# MAGIC 
# MAGIC <img src="https://publicimg.blob.core.windows.net/images/4.SPA.png" width="800">

# COMMAND ----------

# MAGIC %md
# MAGIC #2.Mounting an ADLS Gen2 filesystem to DBFS using a service principal (OAuth 2.0)

# COMMAND ----------

clientID = "ee21d703-7554-4368-a8f4-818dbedc5a71"
tenantID = "9f37a392-f0ae-4280-9796-f1864a10effc"
secret = "a2dKRWTXDS1iMbThoc.C7e:yVsUdL3[:"

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": clientID,
           "fs.azure.account.oauth2.client.secret": secret,
           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/9f37a392-f0ae-4280-9796-f1864a10effc/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://epldata@nikhilg.dfs.core.windows.net/",
  mount_point = "/mnt/nik-mount",
  extra_configs = configs)

# COMMAND ----------

dbutils.fs.ls("/mnt/nik-mount")

# COMMAND ----------

dbutils.fs.unmount("/mnt/nik-mount") 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imp
# MAGIC - Mounting an Azure Data Lake Storage Gen2 is supported only using OAuth credentials. Mounting with an account access key is not supported.
# MAGIC - All users in the Azure Databricks workspace have access to the mounted Azure Data Lake Storage Gen2 account. The service client that you use to access the Azure Data Lake Storage Gen2 account should be granted access only to that Azure Data Lake Storage Gen2 account; it should not be granted access to other resources in Azure.
# MAGIC - Once a mount point is created through a cluster, users of that cluster can immediately access the mount point. To use the mount point in another running cluster, you must run dbutils.fs.refreshMounts() on that running cluster to make the newly created mount point available for use.

# COMMAND ----------

# MAGIC %md
# MAGIC #3. Using a service principal directly (OAuth 2.0)

# COMMAND ----------

# MAGIC %scala
# MAGIC val clientID = "ee21d703-7554-4368-a8f4-818dbedc5a71"
# MAGIC val tenantID = "9f37a392-f0ae-4280-9796-f1864a10effc"
# MAGIC val secret = "a2dKRWTXDS1iMbThoc.C7e:yVsUdL3[:"

# COMMAND ----------

# MAGIC %scala
# MAGIC spark.conf.set("fs.azure.account.auth.type.nikhilg.dfs.core.windows.net", "OAuth")
# MAGIC spark.conf.set("fs.azure.account.oauth.provider.type.nikhilg.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
# MAGIC spark.conf.set("fs.azure.account.oauth2.client.id.nikhilg.dfs.core.windows.net", clientID)
# MAGIC spark.conf.set("fs.azure.account.oauth2.client.secret.nikhilg.dfs.core.windows.net",secret)
# MAGIC spark.conf.set("fs.azure.account.oauth2.client.endpoint.nikhilg.dfs.core.windows.net", "https://login.microsoftonline.com/9f37a392-f0ae-4280-9796-f1864a10effc/oauth2/token")

# COMMAND ----------

# MAGIC %scala
# MAGIC val df = spark.read.csv("abfss://epldata@nikhilg.dfs.core.windows.net/")
# MAGIC display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #4.Using credential Passthrough (ADLS Passthrough)

# COMMAND ----------

# MAGIC %md
# MAGIC ###You can use ADLS Passthrough through both cluster types:
# MAGIC 1. High Concurrency Cluster : When Multiple users want to access the data source credentail passthrough will ensure that each user will able to query data based on permissions set on folder/tables
# MAGIC 2. Standard Cluster : Single User Clusters

# COMMAND ----------

# MAGIC %md
# MAGIC ####Standard Cluster
# MAGIC 
# MAGIC <img src="https://publicimg.blob.core.windows.net/images/1.SD.png" width="400">

# COMMAND ----------

# MAGIC %md
# MAGIC #### Set Permissions Using Azure Explorer
# MAGIC 
# MAGIC <img src="https://publicimg.blob.core.windows.net/images/1.ADP.png" width="700">

# COMMAND ----------

# MAGIC %md
# MAGIC #### Set Blob Contributor Data Role
# MAGIC 
# MAGIC <img src="https://publicimg.blob.core.windows.net/images/2.ADP.png" width="700">

# COMMAND ----------

df = spark.read.csv("abfss://epldata@nikhilg.dfs.core.windows.net/")
display(df)