# Databricks notebook source
# MAGIC %md 
# MAGIC Load the constants/settings that will be used in the script - such as `USER_EMAIL_TEMPLATE` and `GROUP_NAME_TEMPLATE`.
# MAGIC 
# MAGIC Please note - if you change these, you need to make sure the `{}` sections in your string have the included names:
# MAGIC 
# MAGIC * `USER_EMAIL_TEMPLATE` must include `{number}` where the user number goes.
# MAGIC * `GROUP_NAME_TEMPLATE` must include both `{start}` and `{end}` where the user number for the starting and end user are defined.
# MAGIC * `CLUSTER_NAME_TEMPLATE` must include both `{start}` and `{end}` also

# COMMAND ----------

# Region that the Databricks workspace is in.
AZURE_REGION = 'centralus'

# Databricks API Token
# Here, we've stored in Key Vault backed Secrets Scope, but
# you can also just pass a string equal to the Databricks Personal Access Token
API_TOKEN = dbutils.secrets.get('key-vault', 'databricks-api-token')

# Define cluster settings and the number of groups and users per group
NUM_GROUPS = 2
NUM_USERS_PER_GROUP = 5

# If you override these values - please include {number} in the email_template and both {start} and {end} in the name templates
# PLEASE NOTE - other scripts may need to be changed if CLUSTER_NAME_TEMPLATE is overridden.
USER_EMAIL_TEMPLATE = "user_{number}@entropydata.net"
GROUP_NAME_TEMPLATE = "Users {start} to {end}"
CLUSTER_NAME_TEMPLATE = "users_{start}_to_{end}"

# Specify your own defaults to use here.
CLUSTER_DEFAULTS = {
  'num_workers': 4,
  'spark_version': '4.3.x-scala2.11',
  'node_type_id': 'Standard_DS3_v2',
  'python_version': 3,
  'autotermination_minutes': 120,
  'custom_tags': {'Workshop': 'AttendeeCluster'}
}

WORKSHOP_MATERIALS_URL = 'https://aka.ms/gbb/databricks/workshop/materials'