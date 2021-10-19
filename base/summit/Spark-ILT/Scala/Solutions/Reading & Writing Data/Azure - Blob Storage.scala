// Databricks notebook source
// MAGIC 
// MAGIC %md-sandbox
// MAGIC 
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC # Connecting to Azure Resources
// MAGIC ## Azure Storage Account

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Getting Started
// MAGIC 
// MAGIC Run the following cell to configure our "classroom."

// COMMAND ----------

// MAGIC %run "../Includes/Classroom Setup"

// COMMAND ----------

val SASentity = "fs.azure.sas.training.dbtrainwestus2.blob.core.windows.net"
val SAStoken = "?sv=2018-03-28&ss=bfqt&srt=sco&sp=rl&se=3000-01-01T05:59:59Z&st=2019-01-18T01:16:38Z&spr=https&sig=vY4rsDZhyQ9eq5NblfqVDiPIOmTEQquRIEHB4MH4BTA%3D"

spark.conf.set(SASentity, SAStoken)

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Read from a Storage Account using SAS Token
// MAGIC 
// MAGIC It is possible to read directly from Azure Blob Storage using the [Spark API and Databricks APIs](https://docs.databricks.com/spark/latest/data-sources/azure/azure-storage.html#access-azure-blob-storage-using-the-dataframe-api).
// MAGIC 
// MAGIC Note that this method can not be used for writing.
// MAGIC 
// MAGIC #### Configure Access to a Container using a Shared Access Signature
// MAGIC 
// MAGIC A [shared access signature (SAS)](https://docs.microsoft.com/en-us/azure/storage/common/storage-dotnet-shared-access-signature-part-1) provides you with a way to grant limited access to objects in your storage account to other clients, without exposing your account key. 

// COMMAND ----------

// MAGIC %fs ls wasbs://training@dbtrainwestus2.blob.core.windows.net/

// COMMAND ----------

val wikiEditsDF = spark.read.json(source + "wikipedia/edits/snapshot-2016-05-26.json")
display(wikiEditsDF)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Creating an Azure Storage Account
// MAGIC 
// MAGIC Follow these steps to [create an Azure Storage Account](https://docs.microsoft.com/en-us/azure/storage/common/storage-quickstart-create-account?tabs=azure-portal#regenerate-storage-access-keys) and Container. This method will use a [Shared Key](https://docs.microsoft.com/en-us/rest/api/storageservices/authorize-with-shared-key) to authorize requests to the the Storage Account. Be sure to make note of the **Storage Account Name**, the **Container Name**, and the **Access Key** while working through these steps.
// MAGIC 
// MAGIC 
// MAGIC ### Create Resource
// MAGIC 1. Access the Azure Portal
// MAGIC 2. Create a New Resource
// MAGIC 
// MAGIC    <img src="https://www.evernote.com/l/AAFkIbQio4tBrJxef3AIBnClisn7T_cIQ9AB/image.png" width=300px>
// MAGIC    
// MAGIC 3. Create a Storage account
// MAGIC 
// MAGIC    <img src="https://www.evernote.com/l/AAHja5wI6zFO_LMD_V0-ELVnbP0hWgSYXXMB/image.png" width=300px>
// MAGIC 
// MAGIC 4. Make sure to specify the correct *Resource Group* and *Region*. Use any unique string as the  for the **Storage Account Name**
// MAGIC 
// MAGIC    <img src="https://www.evernote.com/l/AAE9rEuHvNNE-4UKUV8OKNUhCjRrNeNZZasB/image.png" width=300px>
// MAGIC    
// MAGIC 5. Access Blobs
// MAGIC 
// MAGIC    <img src="https://www.evernote.com/l/AAEy8WWFvU1I1YCJBElTtZpowLreFzsckp4B/image.png" width=300px>
// MAGIC 
// MAGIC 6. Create a New Container using any unique string for the **Container Name**
// MAGIC 
// MAGIC    <img src="https://www.evernote.com/l/AAEieyEA2JdF75lfNeDfbFlAmoRf2NH1tZEB/image.png" width=300px>
// MAGIC    
// MAGIC 7. Retrieve the primary **Access Key** for the new Storage Account
// MAGIC 
// MAGIC    <img src="https://www.evernote.com/l/AAHXWYaCFwVGa50lAAh8sAqC4eZEjjHE0UwB/image.png" width=300px>

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Configure Authentication
// MAGIC 
// MAGIC Use the **Storage Account Name**, **Container Name**, and **Access Key** to configure the connection to this resource. This is done by configuring the [*SparkSession*](https://databricks.com/blog/2016/08/15/how-to-use-sparksession-in-apache-spark-2-0.html) with the appropriate values. 

// COMMAND ----------

val storageAccountName = "correcthorse"
val containerName = "anystring"
val writeSource = s"wasbs://$containerName@$storageAccountName.blob.core.windows.net/"
val storageEntity = s"fs.azure.account.key.$storageAccountName.blob.core.windows.net"
val accessKey = "x9+so4uR360OBbSOUBNtH1qHys36Lbx8/xwp+xk4vZCJ/oSOi7TRivF62VrjLjVfyP2HsXuevL23WWHKVs7Hug=="
val mountLocation = "/mnt/mycontainer"

dbutils.fs.mount(writeSource, mountLocation, extraConfigs = Map(storageEntity -> accessKey))

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Write to Storage Account using Access Key

// COMMAND ----------

// MAGIC %fs ls /mnt/mycontainer

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