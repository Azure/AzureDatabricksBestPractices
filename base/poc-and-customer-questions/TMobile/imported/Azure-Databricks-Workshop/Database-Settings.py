# Databricks notebook source
# SQL Database & Data Warehouse Location
user = ""
password = ""
databaseHost = ""

if not user:
  raise Exception("Please complete the Database-Settings notebook with the database login settings")

jdbc_url_db = "jdbc:sqlserver://{}:1433;database=AdventureWorks;user={}@databricks-azure-training;password={};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;".format(databaseHost, user, password)
jdbc_url_dw = "jdbc:sqlserver://{}:1433;database=AdventureWorks-DW;user={}@databricks-azure-training;password={};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;".format(databaseHost, user, password)

None # Suppress any output

# COMMAND ----------

# Blob store for the Polybase SQL Data Warehouse connector's staging area 
polybaseBlobStorageAccount=""
polybaseBlobName="polybase"
polybaseBlobAccessKey=""

if not polybaseBlobStorageAccount:
  raise Exception("Please complete the Database-Settings notebook with the polybase blob access settings")

None # Suppress any output

# COMMAND ----------

print "Setup Complete."

None # Suppress any output