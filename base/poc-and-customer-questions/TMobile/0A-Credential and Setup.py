# Databricks notebook source
configs = {"dfs.adls.oauth2.access.token.provider.type": "ClientCredential",
           "dfs.adls.oauth2.client.id": "20046851-fa30-4349-8c8b-f7a0a0460151",
           "dfs.adls.oauth2.credential": "RLgl4/ycrhPKZD0tkMlzFi7vYgF6eHwxrwHNpmj5Rq0=",
           "dfs.adls.oauth2.refresh.url": "https://login.microsoftonline.com/be413eec-6262-4083-97c8-8c2a817c2fe1/oauth2/token"}

adls_base = "dbfs:/mnt/ndwpocdl/"
incoming_dir = "incoming/"

# COMMAND ----------

dbutils.fs.unmount("/mnt/ndwpocdl")

# COMMAND ----------

dbutils.fs.mount(
  source = "adl://ndwpocdl.azuredatalakestore.net",
  mount_point = "/mnt/ndwpocdl",
  extra_configs = configs
)