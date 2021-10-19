# Databricks notebook source
# MAGIC %md ### This notebook is used to analyze audit logs for field-eng workspace

# COMMAND ----------

# DBTITLE 1,Mount the storage account container - once only
'''
dbutils.fs.mount(
  source = "wasbs://logs@fieldengauditlogs.blob.core.windows.net",
  mount_point = "/mnt/fieldengauditlogs",
  extra_configs = {"fs.azure.account.key.fieldengauditlogs.blob.core.windows.net":dbutils.secrets.get(scope = "audit-logs-blob", key = "accesskey")})
'''

# COMMAND ----------

# DBTITLE 1,field-eng workspace folder
# MAGIC %fs ls /mnt/fieldengauditlogs/workspaceId=984752964297111/

# COMMAND ----------

# DBTITLE 1,Create the table for field-eng audit logs - once only
# MAGIC %sql 
# MAGIC --DROP TABLE IF EXISTS FIELD_ENG_AUDIT_LOGS;
# MAGIC 
# MAGIC --CREATE TABLE IF NOT EXISTS FIELD_ENG_AUDIT_LOGS
# MAGIC --USING JSON
# MAGIC --    OPTIONS (
# MAGIC --      path "/mnt/fieldengauditlogs/workspaceId=984752964297111/"
# MAGIC --    );

# COMMAND ----------

# DBTITLE 1,Run MSCK REPAIR to read latest audit logs
# MAGIC %sql MSCK REPAIR TABLE FIELD_ENG_AUDIT_LOGS;

# COMMAND ----------

# MAGIC %sql describe table FIELD_ENG_AUDIT_LOGS

# COMMAND ----------

# MAGIC %sql select * from FIELD_ENG_AUDIT_LOGS order by date desc

# COMMAND ----------

# DBTITLE 1,Distinct services and actions supported
# MAGIC %sql 
# MAGIC 
# MAGIC select distinct serviceName, actionName
# MAGIC   from FIELD_ENG_AUDIT_LOGS 
# MAGIC   where date > to_date('2019-01-01')
# MAGIC   order by serviceName, actionName

# COMMAND ----------

# DBTITLE 1,Objects created on DBFS
# MAGIC %sql 
# MAGIC 
# MAGIC select *
# MAGIC   from FIELD_ENG_AUDIT_LOGS 
# MAGIC   where date > to_date('2019-01-01')
# MAGIC   and serviceName = "dbfs"
# MAGIC   and actionName in ("create", "addBlock")

# COMMAND ----------

# MAGIC %md ### Cluster actions

# COMMAND ----------

# DBTITLE 1,Clusters created by users - by decreasing count
# MAGIC %sql 
# MAGIC 
# MAGIC select userIdentity.email, count(*) as cnt
# MAGIC from FIELD_ENG_AUDIT_LOGS 
# MAGIC where serviceName="clusters" and actionName="create"
# MAGIC group by userIdentity.email
# MAGIC order by cnt desc

# COMMAND ----------

# DBTITLE 1,Clusters created by Jobs - Unknown email
# MAGIC %sql 
# MAGIC 
# MAGIC select requestParams.cluster_creator, count(*)
# MAGIC from FIELD_ENG_AUDIT_LOGS 
# MAGIC where serviceName="clusters" and actionName="create" and userIdentity.email="Unknown"
# MAGIC group by requestParams.cluster_creator

# COMMAND ----------

# DBTITLE 1,Details for clusters created by Jobs
# MAGIC %sql
# MAGIC 
# MAGIC select userIdentity, requestParams, response 
# MAGIC   from FIELD_ENG_AUDIT_LOGS 
# MAGIC   where serviceName="clusters" 
# MAGIC   and actionName="create"
# MAGIC   and !(userIdentity.email="Unknown" and requestParams.cluster_creator="JOB_LAUNCHER")

# COMMAND ----------

# MAGIC %md ## Job actions

# COMMAND ----------

# DBTITLE 1,Jobs info
# MAGIC %sql 
# MAGIC 
# MAGIC select requestParams.jobId, count(*) as cnt
# MAGIC   from FIELD_ENG_AUDIT_LOGS 
# MAGIC   where serviceName="jobs"
# MAGIC   group by requestParams.jobId
# MAGIC   order by cnt desc

# COMMAND ----------

# MAGIC %sql SET spark.sql.crossJoin.enabled=true

# COMMAND ----------

# DBTITLE 1,Failed jobs with cluster information
# MAGIC %sql 
# MAGIC 
# MAGIC select distinct job_tmp.jobId, cluster_tmp.node_type_id, cluster_tmp.spark_version
# MAGIC from
# MAGIC (
# MAGIC   select requestParams.jobId, requestParams.idInJob
# MAGIC   from FIELD_ENG_AUDIT_LOGS 
# MAGIC   where serviceName="jobs" and actionName='runFailed'
# MAGIC ) job_tmp 
# MAGIC   join
# MAGIC (
# MAGIC   select requestParams.cluster_name, requestParams.node_type_id, requestParams.spark_version
# MAGIC   from FIELD_ENG_AUDIT_LOGS 
# MAGIC   where serviceName="clusters" and actionName="create" and requestParams.cluster_creator="JOB_LAUNCHER"
# MAGIC ) cluster_tmp
# MAGIC on ('job-' || CAST(job_tmp.jobId AS STRING) || '-run-' || CAST(job_tmp.idInJob AS STRING)) = cluster_tmp.cluster_name

# COMMAND ----------

# MAGIC %md ## Others - Accounts, SQL ACLs, Notebooks

# COMMAND ----------

# DBTITLE 1,Unsuccessful logins
# MAGIC %sql
# MAGIC 
# MAGIC select distinct userIdentity.email, date, requestParams.user, response
# MAGIC from FIELD_ENG_AUDIT_LOGS
# MAGIC where serviceName = "accounts" and actionName = "login" and response.statusCode != 200
# MAGIC order by date desc

# COMMAND ----------

# DBTITLE 1,SQL ACLs actions
# MAGIC %sql
# MAGIC 
# MAGIC select userIdentity.email, date, *
# MAGIC from FIELD_ENG_AUDIT_LOGS
# MAGIC where serviceName = "sqlPermissions" and actionName = 'grantPermission' 
# MAGIC order by date desc

# COMMAND ----------

# DBTITLE 1,Clusters with maximum attached notebooks
# MAGIC %sql
# MAGIC 
# MAGIC Select cluster_tmp.clusterName, count(*) as cnt
# MAGIC from
# MAGIC (
# MAGIC   select distinct requestParams.clusterId, requestParams.notebookId
# MAGIC   from FIELD_ENG_AUDIT_LOGS
# MAGIC   where serviceName = "notebook" and actionName = 'attachNotebook'
# MAGIC ) nb_tmp
# MAGIC   inner join
# MAGIC (
# MAGIC   select requestParams.clusterId, requestParams.clusterName
# MAGIC   from FIELD_ENG_AUDIT_LOGS
# MAGIC   where serviceName = "clusters" and actionName = 'createResult'
# MAGIC ) cluster_tmp
# MAGIC on nb_tmp.clusterId = cluster_tmp.clusterId
# MAGIC group by clusterName
# MAGIC order by cnt desc