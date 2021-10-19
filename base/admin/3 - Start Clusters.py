# Databricks notebook source
# MAGIC %md
# MAGIC # Start Clusters
# MAGIC 
# MAGIC By default, each cluster has 5 assigned users - however, this number may be overridden during cluster creation. The cluster creation script uses the name format `user_{x}_to_{y}` - where x and y are the first and last user number assigned to the cluster
# MAGIC 
# MAGIC This script loops through all of the clusters to determine if they conform to the workshop attendee format and, if so, if the beginning user is greater than the number specified. If so, then the cluster is started.

# COMMAND ----------

dbutils.widgets.text(name='users', label="Number of Users", defaultValue='30')

# COMMAND ----------

users = dbutils.widgets.get('users')
users

# COMMAND ----------

# MAGIC %run "./0 - Settings"

# COMMAND ----------

from azure_databricks_api import AzureDatabricksRESTClient

# Instantiate Client
client = AzureDatabricksRESTClient(AZURE_REGION, API_TOKEN)

# Cluster names are in format users_x_to_y and we are comparing x to the required number of users
for cluster in client.clusters.list():
  tags = cluster.get('custom_tags')
  if tags:
    if tags.get('Workshop') == 'AttendeeCluster':
      if int(cluster['cluster_name'].split('_')[1]) <= int(users):
        print("Starting cluster {0}".format(cluster['cluster_name']))
        client.clusters.start(cluster_id=cluster['cluster_id'])