# Databricks notebook source
# MAGIC %md
# MAGIC # Workspace Setup for New Workshops
# MAGIC This will setup your workspace for new workshops. It will use the [azure-databricks-api Python package](https://github.com/ezwiefel/azure-databricks-api) It is done in 5 steps:
# MAGIC 
# MAGIC 1. Import AD Users into Databricks
# MAGIC 1. Create clusters, create groups, and associate users to groups given the number of groups and users per group.
# MAGIC 1. Associate groups with a cluster
# MAGIC 1. Import the workshop materials into the top level of the environment
# MAGIC 1. Set appropriate permissions on the workshop materials

# COMMAND ----------

# MAGIC %md # 0. Load Settings
# MAGIC Load the settings that are stored in '0 - Settings' notebook.

# COMMAND ----------

# MAGIC %run "./0 - Settings"

# COMMAND ----------

# MAGIC %md # 1. Import New Users into the Workspace
# MAGIC To import new users into the workspace, we'll be using the [SCIM API](https://docs.databricks.com/api/latest/scim.html) (in preview as of October 5th, 2018). 
# MAGIC 
# MAGIC Since it's in preview it hasn't been built into the azure-databricks-api package, so we'll be using [requests](http://docs.python-requests.org/en/master/) directly.

# COMMAND ----------

import requests
from azure_databricks_api.exceptions import APIError, AuthorizationError

SCIM_URL = "https://{region}.azuredatabricks.net/api/2.0/preview/scim/v2/Users".format(region=AZURE_REGION)
SCIM_HEADERS = {'Authorization': "Bearer {0}".format(API_TOKEN),
                'Content-Type': 'application/scim+json'}
SCIM_PAYLOAD = {"schemas":["urn:ietf:params:scim:schemas:core:2.0:User"],
                 "userName":""}

for user in range(1, ((NUM_GROUPS * NUM_USERS_PER_GROUP)+1)):
 
  data = SCIM_PAYLOAD.copy()
  data['userName'] = USER_EMAIL_TEMPLATE.format(number=user)
  
  resp = requests.post(url=SCIM_URL, json=data, headers=SCIM_HEADERS)
  
  if resp.status_code in [200, 201]:
    print('User {0} created.'.format(user))
  elif resp.status_code == 403:
    raise AuthorizationError("User is not authorized or token is incorrect.")
  elif resp.status_code == 409:
    print('User {0} already exists in workspace'.format(user))
  else:
      raise APIError("Response code {0}: {1} {2}".format(resp.status_code,
                                                     resp.json().get('error_code'),
                                                     resp.json().get('message')))
    

# COMMAND ----------

# MAGIC %md
# MAGIC # 2. Create New Clusters, New Groups and Assign Users to each group
# MAGIC This is using the azure-databricks-api package.

# COMMAND ----------

from azure_databricks_api import AzureDatabricksRESTClient
from azure_databricks_api.exceptions import ResourceDoesNotExist

# Instantiate Client
client = AzureDatabricksRESTClient(AZURE_REGION, API_TOKEN)

# COMMAND ----------

current_user = 1

all_current_groups = client.groups.list()

# Iterate over the required number of groups
for g in range(NUM_GROUPS):
  group_name = GROUP_NAME_TEMPLATE.format(start=(NUM_USERS_PER_GROUP*g)+1, end=(NUM_USERS_PER_GROUP*(g+1)))
  cluster_name = CLUSTER_NAME_TEMPLATE.format(start=(NUM_USERS_PER_GROUP*g)+1,  end=(NUM_USERS_PER_GROUP*(g+1)))
  
  print("Group '{0}'".format(group_name))
  
  if group_name in all_current_groups:
    print('  Group already exists')
  else:
    print("  Creating group".format(group_name))
    client.groups.create(group_name)
  
  try:
    client.clusters.get(cluster_name)
    print("  Cluster '{0}' already exists".format(cluster_name))
  except ResourceDoesNotExist:
    print("Creating Cluster '{0}'".format(cluster_name))
    # Create and instantly terminiate the cluster
    client.clusters.create(cluster_name=cluster_name, **CLUSTER_DEFAULTS)
    client.clusters.terminate(cluster_name)
    
  client.clusters.pin(cluster_name)
  print()
  print('  Users')
  for i in range(NUM_USERS_PER_GROUP):
    user_name = USER_EMAIL_TEMPLATE.format(number = current_user)
    
    print("  - Adding {0} to '{1}'".format(user_name, group_name))
    client.groups.add_member(group_name, user_name=user_name)
    current_user += 1
  
  print()

# COMMAND ----------

# MAGIC %md
# MAGIC # 3. Associate groups to clusters
# MAGIC This is currently done manually using the 'Clusters' interface within Databricks
# MAGIC 
# MAGIC <img src="https://github.com/azeltov/adb_workshop/raw/master/imgs/cluster_permissions.gif" alt="Cluster Permissions Animation" width="900">

# COMMAND ----------

# MAGIC %md
# MAGIC # 4. Import Workshop materials to top level of environment

# COMMAND ----------

client.workspace.delete('/Workshop', recursive=True, not_exists_ok=True)
client.workspace.import_file(dbx_path='/Workshop', file_format='DBC', url=WORKSHOP_MATERIALS_URL)

# COMMAND ----------

# MAGIC %md
# MAGIC #5. Set appropriate permissions on the workshop materials
# MAGIC Same as step 3, this is currently a manual process. It's recommended that you give read access to the 'Labs' folder under the workshop for all users.
# MAGIC 
# MAGIC <img src="https://github.com/azeltov/adb_workshop/raw/master/imgs/workshop_permissions.gif" alt="Workspace Permissions Animation" width="900">