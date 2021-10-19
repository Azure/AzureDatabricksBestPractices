# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Clean Up Workspace
# MAGIC 
# MAGIC This script should be run after a workshop is finished. This will (a) empty all user folders, (b) delete and recreate the "Workshop" path, (c) delete all jobs (except our "Terminate Clusters" job), and (d) terminate all currently running clusters.

# COMMAND ----------

# MAGIC %run "./0 - Settings"

# COMMAND ----------

from azure_databricks_api import AzureDatabricksRESTClient

# Instantiate Client
client = AzureDatabricksRESTClient(AZURE_REGION, API_TOKEN)

TOTAL_USERS = NUM_GROUPS * NUM_USERS_PER_GROUP

# COMMAND ----------

# DBTITLE 1,Empty User Folders
# Loop through users and delete 

for i in range(1, TOTAL_USERS+1):
  print("{0}..".format(i)),
  for object in client.workspace.list('/Users/' + USER_EMAIL_TEMPLATE.format(number=i)):
    if not object.path.endswith("Trash"):
      client.workspace.delete(object.path, recursive=True)

# COMMAND ----------

# DBTITLE 1,Delete Main Workshop Folder and Reimport
client.workspace.delete('/Workshop', recursive=True, not_exists_ok=True)
client.workspace.import_file(dbx_path='/Workshop', file_format='DBC', url=WORKSHOP_MATERIALS_URL)