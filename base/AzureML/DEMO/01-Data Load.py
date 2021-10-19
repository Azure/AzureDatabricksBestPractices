# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ### Step 1: Setup storage access keys

# COMMAND ----------

storage_account_name = "dbxsummitstg"
storage_account_access_key=dbutils.secrets.get('aml','mykeyforstorageacct')

# COMMAND ----------

spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".dfs.core.windows.net",
  storage_account_access_key)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Step 2: Get data and store in Parquet file format in 'Silver Zone'

# COMMAND ----------

import os
import numpy as np
import pandas as pd


# COMMAND ----------

# MAGIC %sh wget https://azmlworkshopdata.blob.core.windows.net/safedriverdata/porto_seguro_safe_driver_prediction_train.csv -P /tmp

# COMMAND ----------

# Directly load from file into Pandas DataFrame
DATA_DIR = "/tmp"
pandas_df = pd.read_csv(os.path.join(DATA_DIR, 'porto_seguro_safe_driver_prediction_train.csv'))

print(pandas_df.shape)
pandas_df.head(5)

# COMMAND ----------

safedriverdata_df = spark.createDataFrame(pandas_df)

# Add the spark data frame to the catalog
safedriverdata_df.createOrReplaceTempView('safedriverdata')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from safedriverdata

# COMMAND ----------

silver= "abfss://silver@dbxsummitstg.dfs.core.windows.net/safedriverdata"
safedriverdata_df.write.mode('overwrite').parquet(silver)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Step 3: Register Parquet data in AML Datastore/Dataset using SPN

# COMMAND ----------

from azureml.core.authentication import ServicePrincipalAuthentication

#svcdbxsummit service principle
tenant_id="72f988bf-86f1-41af-91ab-2d7cd011db47"
app_id="b13e47be-a9b6-4431-8e29-9b32bf505206"
svc_pr_password=dbutils.secrets.get('aml','mykeyforsp')

#ServicePrincipalAuthentication(tenant_id, service_principal_id, service_principal_password, cloud='AzureCloud', _enable_caching=True)
svc_pr = ServicePrincipalAuthentication(
       tenant_id=tenant_id,
       service_principal_id=app_id,
       service_principal_password=svc_pr_password) 

# COMMAND ----------

from azureml.core import Workspace, Dataset

subscription_id = "5763fde3-4253-480c-928f-dfe1e8888a57"  #you should be owner or contributor
resource_group = "dbx_tech_summit" #you should be owner or contributor
workspace_name = "aml_dbx_summit"
workspace_region = "eastus2"

ws = Workspace.get(name = workspace_name,
                      subscription_id = subscription_id,
                      resource_group = resource_group,
               auth=svc_pr)

# COMMAND ----------

from azureml.core.datastore import Datastore
safedriver_datastore= "safedriverdatastore"
file_system = "silver"
# register a new datastore in the workspace by name
mydatastore = Datastore.register_azure_data_lake_gen2(ws, safedriver_datastore, file_system, storage_account_name, tenant_id, app_id, svc_pr_password, resource_url=None, authority_url=None, protocol=None, endpoint=None, overwrite=False)

# COMMAND ----------

ds = Dataset.Tabular.from_parquet_files([(mydatastore,'safedriverdata/*.parquet')])
ds.register(workspace = ws,
           name = 'safedriverdataset',
           description = 'safedriver training data',
           create_new_version = True)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Step 4: Validate dataset from AML

# COMMAND ----------

dataset = Dataset.get_by_name(ws, name='safedriverdataset')
p= dataset.to_pandas_dataframe()
p

# COMMAND ----------

p.count()

# COMMAND ----------

