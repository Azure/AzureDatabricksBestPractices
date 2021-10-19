// Databricks notebook source
// MAGIC 
// MAGIC %md-sandbox
// MAGIC 
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC # Azure Data Lake Storage, Gen 2

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Getting Started
// MAGIC 
// MAGIC Run the following cell to configure our "classroom."

// COMMAND ----------

// MAGIC %run "../Includes/Classroom Setup"

// COMMAND ----------

val (source, configMap) = getAzureMapping(getAzureRegion)
val SASentity = source.replace("wasbs://", "fs.azure.sas.").replace("@", ".").replace("/", "")
val SAStoken = configMap(SASentity)
spark.conf.set(SASentity, SAStoken)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC  1. Create Service Principal
// MAGIC     * In Azure Active Directory, go to Properties. Make note of the **Directory ID**.
// MAGIC     * Go to App Registrations and create a New application registration
// MAGIC        * example: airlift-app-registration, Web app/API, https://can-be-literally-anything.com
// MAGIC     * Make note of the **Application ID**.
// MAGIC     * Under Settings > Keys, create and copy a new key. Make note of the **Key Value**.
// MAGIC  1. Create Storage Account
// MAGIC     * On the Advanced Tab (1), make sure to enable Hierarchal NameSpace (2).
// MAGIC        <img src="https://www.evernote.com/l/AAFW89nF7OtKb4j798yshtao-a4SVE2vUk4B/image.png" width=300px>
// MAGIC     * Make note of the **Storage Account Name**.
// MAGIC     * Create a Data Lake Gen2 file system on the storage account and make note of the **File System Name**.
// MAGIC     * Under Access control (IAM) add a *Role assignment*, where the role is *Storage Blob Data Contributor (Preview)* assigned to the App Registration previously created.

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png)Requirements
// MAGIC 
// MAGIC - Databricks Runtime 5.2 or above. 
// MAGIC - ADLS Gen2 storage account in the same region as your Azure Databricks workspace.
// MAGIC - A service principal with delegated permissions.

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png)Configure Authentication
// MAGIC 
// MAGIC 
// MAGIC Use the **directoryID**, **applicationID**, **keyValue**, **storageAccountName**, and **fileSystemName** to configure the connection to this resource. Read more about configuring this resource [here](https://docs.azuredatabricks.net/spark/latest/data-sources/azure/azure-datalake-gen2.html#requirements-azure-data-lake).

// COMMAND ----------

val directoryID = "12606c46-81a2-4b95-b67d-adb806a15441"
val applicationID = "f0c7e2ae-a260-435a-88c6-a6c7d61a446a"
val keyValue = "JzPTDfxhB2HGyIBWBErdzhvt3ZxVKM+8+CAUFRMGSag="
val storageAccountName = "airliftadls"
val fileSystemName = "adlsfs"

val configs = Map(
  "fs.azure.account.auth.type" -> "OAuth",
  "fs.azure.account.oauth.provider.type" -> "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
  "fs.azure.account.oauth2.client.id" -> applicationID,
  "fs.azure.account.oauth2.client.secret" -> keyValue,
  "fs.azure.account.oauth2.client.endpoint" -> s"https://login.microsoftonline.com/$directoryID/oauth2/token")

// COMMAND ----------

// Optionally, you can add <your-directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = s"abfss://$fileSystemName@$storageAccountName.dfs.core.windows.net/",
  mountPoint = s"/mnt/$fileSystemName",
  extraConfigs = configs)

// COMMAND ----------

s"/mnt/$fileSystemName"

// COMMAND ----------

// MAGIC %fs ls /mnt/adlsfs

// COMMAND ----------

val wikiEditsDF = spark.read.json(source + "wikipedia/edits/snapshot-2016-05-26.json")
display(wikiEditsDF)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png)Write to ADLS

// COMMAND ----------

wikiEditsDF.write
  .mode("overwrite")
  .format("delta")
  .partitionBy("channel")
  .save("/mnt/adlsfs/wikiEdits")


// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>