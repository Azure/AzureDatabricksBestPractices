# Databricks notebook source
storage_account_name = "joelsimpleblobstore"
storage_account_access_key = "XuKm58WHURoiBqbxGuKFvLDQwDu4ctbjNgphY14qhRsUmsiFMdBmw2UbIIBYQvawsydVSiqZVaRPaWv/v2l/7g=="
spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
  storage_account_access_key)