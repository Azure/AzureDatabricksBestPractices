# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://cdn2.hubspot.net/hubfs/438089/docs/training/dblearning-banner.png" alt="Databricks Learning" width="555" height="64">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Getting Started on Blob Store and ADLS
# MAGIC 
# MAGIC Azure Databricks&reg; provides a notebook-oriented Apache Spark&trade; as-a-service workspace environment, making it easy to manage clusters and explore data interactively.
# MAGIC 
# MAGIC ### Use cases for Apache Spark 
# MAGIC * Read and process huge files and data sets
# MAGIC * Query, explore, and visualize data sets
# MAGIC * Join disparate data sets found in data lakes
# MAGIC * Train and evaluate machine learning models
# MAGIC * Process live streams of data
# MAGIC * Perform analysis on large graph data sets and social networks

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC **Technical Accomplishments:**
# MAGIC - Connect to our Blob store
# MAGIC - Create your own ADLS Store

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/wiki-book/general/logo_spark_tiny.png) Connect to our Blob store
# MAGIC 
# MAGIC Next, let's connect to the read-only Blob store you'll have access to for data needed in this course.  We can easily mount data in blob stores to Azure Databricks for fast and scalable data storage
# MAGIC 
# MAGIC *Note:* You will have to have a cluster running to execute this code

# COMMAND ----------

# MAGIC %md
# MAGIC <h2 style="color:red">IMPORTANT!</h2> This notebook must be run using Azure Databricks runtime 4.0 or better.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 1. Run the following cell

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC try{
# MAGIC   dbutils.fs.mount(
# MAGIC     source = "wasbs://training-primary@databrickstraining.blob.core.windows.net/",
# MAGIC     mountPoint = "/mnt/training-msft",
# MAGIC     extraConfigs = Map("fs.azure.account.key.databrickstraining.blob.core.windows.net" ->
# MAGIC                        "BXOG8lPEcgSjjlmsOgoPdVCpPDM/RwfN1QTrlXEX3oq0sSbNZmNPyE8By/7l9J1Z7SVa8hsKHc48qBY1tA/mgQ=="))
# MAGIC } catch {
# MAGIC   case e: Exception => println(e.getCause().getMessage())
# MAGIC }

# COMMAND ----------

extra_configs = {"{confKey}": "{confValue}"})

# COMMAND ----------

# MAGIC %fs unmount /mnt/training-msft

# COMMAND ----------

  dbutils.fs.mount(
    source = "wasbs://training-primary@databrickstraining.blob.core.windows.net/",
    mount_point = "/mnt/training-msft/",
    extra_configs = {"fs.azure.account.key.databrickstraining.blob.core.windows.net" :
                       "BXOG8lPEcgSjjlmsOgoPdVCpPDM/RwfN1QTrlXEX3oq0sSbNZmNPyE8By/7l9J1Z7SVa8hsKHc48qBY1tA/mgQ=="})

# COMMAND ----------

  dbutils.fs.mount(
    source = "wasbs://training-primary@databrickstraining.blob.core.windows.net/",
    mount_point = "/mnt/training-msft",
    extra_configs = {("fs.azure.account.key.databrickstraining.blob.core.windows.net" ->
                       "BXOG8lPEcgSjjlmsOgoPdVCpPDM/RwfN1QTrlXEX3oq0sSbNZmNPyE8By/7l9J1Z7SVa8hsKHc48qBY1tA/mgQ=="))

# COMMAND ----------

# MAGIC %md
# MAGIC <span>2.</span> You can check to see what's in the mount by using `%fs ls <mount name>` <br>

# COMMAND ----------

# MAGIC %fs ls /mnt/training-msft

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/wiki-book/general/logo_spark_tiny.png) Create your own ADLS Store
# MAGIC 
# MAGIC We'll be using your own ADLS store to write our data back to.  Follow the instructions below to set this up

# COMMAND ----------

# MAGIC %md
# MAGIC <span>1.</span> Choose "Azure Active Directory" in the Azure portal<br>
# MAGIC <span>2.</span> Choose "App Registrations"<br>
# MAGIC <span>3.</span> Choose "New Application Registration<br>
# MAGIC <div><img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/eLearning/MSFT/MSFT-ADLS-setup-1.png" style="height: 200px"/></div><br/>

# COMMAND ----------

# MAGIC %md
# MAGIC <span>4.</span> Enter a name for your app<br>
# MAGIC <span>5.</span> Choose "Web app / API"<br>
# MAGIC <span>6.</span> Enter a valid URL.  This could be anything<br>
# MAGIC <span>7.</span> Click "Create"<br>
# MAGIC <div><img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/eLearning/MSFT/MSFT-ADLS-setup-2.png" style="height: 200px"/></div><br/>

# COMMAND ----------

# MAGIC %md
# MAGIC <span>8.</span> Click on the App you just created
# MAGIC <div><img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/eLearning/MSFT/MSFT-ADLS-setup-3.png" style="height: 200px"/></div><br/>

# COMMAND ----------

# MAGIC %md
# MAGIC <span>9.</span> Copy the Application ID, paste it between the quotes in the cell below, and then run it<br>
# MAGIC <span>10.</span> Click "Keys"<br>
# MAGIC 
# MAGIC <div><img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/eLearning/MSFT/MSFT-ADLS-setup-4.png" style="height: 200px"/></div><br/>

# COMMAND ----------

# MAGIC %scala
# MAGIC val applicationId="" // fill this in with your Application ID

# COMMAND ----------

# MAGIC %md
# MAGIC <span>11.</span> Name your key `training-dbfs` <br>
# MAGIC <span>12.</span> Set the expiration date to one year<br>
# MAGIC <span>13.</span> Save the result and it will generate a key<br>
# MAGIC <div><img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/eLearning/MSFT/MSFT-ADLS-setup-5.png" style="height: 200px"/></div><br/>

# COMMAND ----------

# MAGIC %md
# MAGIC <span>14.</span> Copy the new key and paste it between the quotes in the cell below.  Run the cell
# MAGIC <div><img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/eLearning/MSFT/MSFT-ADLS-setup-6.png" style="height: 200px"/></div><br/>

# COMMAND ----------

# MAGIC %scala
# MAGIC val applicationKey="" // fill this in with your Application Key

# COMMAND ----------

# MAGIC %md
# MAGIC <span>15.</span> Click "Azure Data Directory"<br>
# MAGIC <span>16.</span> Click "Properties"<br>
# MAGIC <span>17.</span> Copy the Directory ID, paste it in the cell below, and Run it<br>
# MAGIC <div><img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/eLearning/MSFT/MSFT-ADLS-setup-7.png" style="height: 200px"/></div><br/>

# COMMAND ----------

# MAGIC %scala
# MAGIC val directoryId="" // fill this in with your Directory ID

# COMMAND ----------

# MAGIC %md
# MAGIC <span>18.</span> Choose "New"<br>
# MAGIC <span>19.</span> Enter "data lake store"<br>
# MAGIC <div><img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/eLearning/MSFT/MSFT-ADLS-setup-8.png" style="height: 200px"/></div><br/>

# COMMAND ----------

# MAGIC %md
# MAGIC <span>20.</span> Choose "Data Lake Store" and click "Create"
# MAGIC <div><img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/eLearning/MSFT/MSFT-ADLS-setup-9.png" style="height: 200px"/></div><br/>

# COMMAND ----------

# MAGIC %md
# MAGIC <span>21.</span> Enter a name for your data lake.  **This must be a gobally unique name**<br>
# MAGIC <span>22.</span> Choose your subscription<br>
# MAGIC <span>23.</span> Choose your resource group<br>
# MAGIC <span>24.</span> Choose "US East 2" as your location<br>
# MAGIC <span>25.</span> Choose "Create"<br>
# MAGIC <div><img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/eLearning/MSFT/MSFT-ADLS-setup-10.png" style="height: 200px"/></div><br/>

# COMMAND ----------

# MAGIC %md
# MAGIC <span>26.</span> Choose "Access Control (IAM)"
# MAGIC <div><img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/eLearning/MSFT/MSFT-ADLS-setup-11.png" style="height: 200px"/></div><br/>

# COMMAND ----------

# MAGIC %md
# MAGIC <span>28.</span> Click "Add" <br>
# MAGIC <span>29.</span> Choose "Owner" as the role <br>
# MAGIC <span>30.</span> Choose "Azure AD user, group, or application" <br>
# MAGIC <span>31.</span> Select your data lake <br>
# MAGIC <span>32.</span> Click "Save" <br>
# MAGIC <div><img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/eLearning/MSFT/MSFT-ADLS-setup-13.png" style="height: 200px"/></div><br/>

# COMMAND ----------

# MAGIC %md
# MAGIC <span>33.</span> In the overview tab in the console for the Data Lake Store, copy the ADL URI, paste it into the cell below between the quotes, and run it
# MAGIC <div><img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/eLearning/MSFT/MSFT-ADLS-setup-14.png" style="height: 200px"/></div><br/>

# COMMAND ----------

# MAGIC %scala
# MAGIC val adlURI="" // fill this in with your ADL URI

# COMMAND ----------

# MAGIC %md
# MAGIC Finally, run the cell below to mount your data

# COMMAND ----------

# MAGIC %scala
# MAGIC //HOW-TO Instructions can be found here: https://docs.databricks.com/spark/latest/data-sources/azure/azure-datalake.html
# MAGIC 
# MAGIC // You should have filled in these variables above
# MAGIC 
# MAGIC val applicationId="adl-kyle"
# MAGIC val applicationKey="VxkR8Uqo3qufXQ7GoEA8JK3HD5bUh23lstJL9TdtleA="
# MAGIC val directoryId="9f37a392-f0ae-4280-9796-f1864a10effc"
# MAGIC val adlURI="https://kyle-adl"
# MAGIC 
# MAGIC // And then run this cell.
# MAGIC val dbfsMountPoint="/mnt/training-adl"
# MAGIC try{
# MAGIC   dbutils.fs.mount(
# MAGIC     mountPoint = dbfsMountPoint,
# MAGIC     source = adlURI,
# MAGIC     extraConfigs = Map(
# MAGIC       "dfs.adls.oauth2.access.token.provider.type" -> "ClientCredential",
# MAGIC       "dfs.adls.oauth2.client.id" -> applicationId,
# MAGIC       "dfs.adls.oauth2.credential" -> applicationKey,
# MAGIC       "dfs.adls.oauth2.refresh.url" -> s"https://login.microsoftonline.com/$directoryId/oauth2/token"
# MAGIC     )
# MAGIC   )
# MAGIC   println("Success")
# MAGIC } catch {
# MAGIC   case e: Exception => println(e.getCause().getMessage())
# MAGIC }

# COMMAND ----------

