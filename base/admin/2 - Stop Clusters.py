# Databricks notebook source
# MAGIC %md
# MAGIC # Stop Clusters - Nightly Run
# MAGIC This will terminate clusters on a nightly basis - unless the cluster is the transient cluster currently running the jobs.
# MAGIC 
# MAGIC This is meant to be schedule to run on a 0 worker node cluster on a nightly basis.

# COMMAND ----------

# MAGIC %run './0 - Settings'

# COMMAND ----------

from azure_databricks_api import AzureDatabricksRESTClient

# Instantiate Client - using secret stored in Azure Key Vault
client = AzureDatabricksRESTClient(AZURE_REGION, API_TOKEN)

# COMMAND ----------

# Get all clusters where the current state is not either TERMINATING or TERMINATED and the cluster was not created by a job
running_clusters = [cluster for cluster in client.clusters.list() 
                    if cluster['state'] not in ['TERMINATING', 'TERMINATED'] 
                    and cluster['cluster_source'] != 'JOB']

# For these remaining clusters, shut down each cluster
for cluster in running_clusters:
  print(client.clusters.terminate(cluster_id=cluster['cluster_id']))

# COMMAND ----------

# After these clusters are shut down, print the details of any remaining clusters
[cluster for cluster in client.clusters.list() if cluster['state'] not in ['TERMINATING', 'TERMINATED']]