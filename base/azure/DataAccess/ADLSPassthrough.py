# Databricks notebook source
configs = { 
"fs.azure.account.auth.type": "CustomAccessToken",
"fs.azure.account.custom.token.provider.class": spark.conf.get("spark.databricks.passthrough.adls.gen2.tokenProviderClassName")
}

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://demographics-us-county@joeladlsgen2.dfs.core.windows.net/",
  mount_point = "/mnt/joel-demographics-us-county",
  extra_configs = configs)

# COMMAND ----------

dbutils.fs.ls("/mnt/joel-demographics-us-county")

# COMMAND ----------

dbutils.fs.ls("abfss://covid-us-county@joeladlsgen2.dfs.core.windows.net/")

# COMMAND ----------

dbutils.fs.ls("abfss://demographics-us-county@joeladlsgen2.dfs.core.windows.net/")

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

clientID = "55594358-53b6-4238-a8b3-d68b8305ad06"
tenantID = "9f37a392-f0ae-4280-9796-f1864a10effc"
secret = "s.1_56j~5jmRDm23A.dTg5_XjTAcRjCbH."

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": clientID,
           "fs.azure.account.oauth2.client.secret": secret,
           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/9f37a392-f0ae-4280-9796-f1864a10effc/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://covid-us-county@joeladlsgen2.dfs.core.windows.net/",
  mount_point = "/mnt/joel/covid-us-county",
  extra_configs = configs)

# COMMAND ----------

dbutils.fs.ls("/mnt/joel/covid-us-county")

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://demographics-us-county@joeladlsgen2.dfs.core.windows.net/",
  mount_point = "/mnt/joel/demographics-us-county",
  extra_configs = configs)

# COMMAND ----------

dbutils.fs.ls("/mnt/joel/demographics-us-county")

# COMMAND ----------

