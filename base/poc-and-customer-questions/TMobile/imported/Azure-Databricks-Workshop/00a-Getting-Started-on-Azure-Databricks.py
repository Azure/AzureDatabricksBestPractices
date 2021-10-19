# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://cdn2.hubspot.net/hubfs/438089/docs/training/dblearning-banner.png" alt="Databricks Learning" width="555" height="64">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Getting Started on Azure Databricks 
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
# MAGIC - Create a Azure DataBricks Shard
# MAGIC - Spin up your First Azure DataBricks Cluster
# MAGIC - Import the Notebooks for this Course

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/wiki-book/general/logo_spark_tiny.png) Create an Azure DataBricks Shard
# MAGIC 
# MAGIC First, let's get working on Azure Databricks.  A shard is the deployment of Azure Databricks that you'll be using.  This exercise will have you create a Azure Databricks shard

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Once logged into Azure, click the "New" button
# MAGIC 2. Type "databricks" into the search field
# MAGIC <div><img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/eLearning/MSFT/MSFT-DB-setup-1.png" style="height: 200px"/></div><br/>

# COMMAND ----------

# MAGIC %md
# MAGIC <span>3.</span> Choose "Azure Databricks (preview)"
# MAGIC <div><img style="width:55%" src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/eLearning/MSFT/MSFT-DB-setup-2.png" style="height: 200px"/></div><br/>

# COMMAND ----------

# MAGIC %md
# MAGIC <span>4.</span> Click "Create"
# MAGIC <div><img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/eLearning/MSFT/MSFT-DB-setup-3.png" style="height: 200px"/></div><br/>

# COMMAND ----------

# MAGIC %md
# MAGIC <span>5.</span> Enter your workspace name as `databricks-training` <br>
# MAGIC <span>6.</span> Choose your subscription <br>
# MAGIC <span>7.</span> Either create a new resource group or use an existing one <br>
# MAGIC <span>8.</span> Choose your resource group <br>
# MAGIC <span>9.</span> Set your location as "East US 2" <br>
# MAGIC 
# MAGIC <div><img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/eLearning/MSFT/MSFT-DB-setup-4.png" style="height: 200px"/></div><br/>

# COMMAND ----------

# MAGIC %md
# MAGIC <span>10.</span> Click "Create" <br>
# MAGIC 
# MAGIC <div><img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/eLearning/MSFT/MSFT-DB-setup-5.png" style="height: 200px"/></div><br/>

# COMMAND ----------

# MAGIC %md
# MAGIC <span>11.</span> Click on the shard you just created <br>
# MAGIC 
# MAGIC <div><img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/eLearning/MSFT/MSFT-DB-setup-6.png" style="height: 200px"/></div><br/>

# COMMAND ----------

# MAGIC %md
# MAGIC <span>12.</span> Click on the icon to enter into your shard <br>
# MAGIC 
# MAGIC <div><img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/eLearning/MSFT/MSFT-DB-setup-7.png" style="height: 200px"/></div><br/>

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/wiki-book/general/logo_spark_tiny.png) Spin up your First Azure DataBricks Cluster

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Step 1
# MAGIC 
# MAGIC Sign in to your Azure Databricks workspace using Active Directory.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Step 2
# MAGIC Azure Databricks notebooks are backed by clusters, or networked computers that work together to process your data. Create a Spark cluster (*if you already have a running cluster, skip to **Step 3** *):
# MAGIC 1. Select the **Clusters** icon in the sidebar.
# MAGIC <div><img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/eLearning/create-cluster-4.png" style="height: 200px"/></div><br/>
# MAGIC 2. Click the **Create Cluster** button.
# MAGIC <div><img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/eLearning/create-cluster-5.png" style="border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa"/></div><br/>
# MAGIC 3. Name your cluster. Use your name or initials to easily differentiate your cluster from your coworkers.
# MAGIC 4. Select the cluster type. This class requires the latest runtime (**4.0**, or newer) and Scala **2.11**.
# MAGIC 5. Specify your cluster configuration.
# MAGIC   * Storage Optimized Standard_L4s (if you accidentally chose any other type, it will be fine for this class)
# MAGIC   * Autoscaling 2-8 clusters.
# MAGIC   * Timeout at 180 minutes. (This esures it won't shut-down over lunch during class.)
# MAGIC 6. Click the **Create Cluster** button.
# MAGIC <div><img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/eLearning/create-cluster-2.png" style="height: 300px; border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa"/></div>
# MAGIC 
# MAGIC 
# MAGIC :HINT: Check with your local system administrator to see if there is a recommended default cluster at your company to use for the rest of the class. This could save you some money!

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Step 3
# MAGIC 
# MAGIC Create a new notebook in your home folder:
# MAGIC 1. Select the **Home** icon in the sidebar.
# MAGIC <div><img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/eLearning/home.png" style="height: 200px"/></div><br/>
# MAGIC 2. Right-click your home folder.
# MAGIC 3. Select **Create**.
# MAGIC 4. Select **Notebook**.
# MAGIC <div><img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/eLearning/create-notebook-1.png" style="height: 150px; border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa"/></div><br/>
# MAGIC 5. Name your notebook `First Notebook`.<br/>
# MAGIC 6. Set the language to **Python**.<br/>
# MAGIC 7. Select the cluster to which to attach this Notebook.  
# MAGIC :SIDENOTE: If a cluster is not currently running, this option will not exist.
# MAGIC 8. Click **Create**.
# MAGIC <div>
# MAGIC   <div style="float:left"><img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/eLearning/create-notebook-2b.png" style="width:400px; border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa"/></div>
# MAGIC   <div style="float:left">&nbsp;&nbsp;&nbsp;or&nbsp;&nbsp;&nbsp;</div>
# MAGIC   <div style="float:left"><img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/eLearning/create-notebook-2.png" style="width:400px; border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa"/></div>
# MAGIC   <div style="clear:both"></div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Step 4
# MAGIC 
# MAGIC Now that you have a notebook, we can use it to run some code.
# MAGIC 1. In the first cell of your notebook, type `1 + 1`. 
# MAGIC 2. Run the cell by clicking the run icon and selecting **Run Cell**.
# MAGIC <div><img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/eLearning/run-notebook-1.png" style="width:600px; margin-bottom:1em; border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa"/></div>
# MAGIC :SIDENOTE: You can also run a cell by typing **Ctrl-Enter**.

# COMMAND ----------

# MAGIC %python
# MAGIC 1 + 1

# COMMAND ----------

# MAGIC %scala
# MAGIC 1 + 1

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ### Attach and Run
# MAGIC 
# MAGIC If your notebook was not previously attached to a cluster you might receive the following prompt: 
# MAGIC <div><img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/eLearning/run-notebook-2.png" style="margin-bottom:1em; border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa"/></div>
# MAGIC 
# MAGIC If you click **Attach and Run**, first make sure that you are attaching to the correct cluster.
# MAGIC 
# MAGIC If it is not the correct cluster, click **Cancel** instead see the next cell, **Attach & Detach**.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Attach & Detach
# MAGIC 
# MAGIC If your notebook is detached you can attach it to another cluster:  
# MAGIC <img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/eLearning/attach-to-cluster.png" style="border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa"/>
# MAGIC <br/>
# MAGIC <br/>
# MAGIC <br/>
# MAGIC If your notebook is attached to a cluster you can:
# MAGIC * Detach your notebook from the cluster.
# MAGIC * Restart the cluster.
# MAGIC * Attach to another cluster.
# MAGIC * Open the Spark UI.
# MAGIC * View the Driver's log files.
# MAGIC 
# MAGIC <img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/eLearning/detach-from-cluster.png" style="margin-bottom:1em; border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa"/>

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/wiki-book/general/logo_spark_tiny.png)Import the Notebooks for this Course

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Step 1
# MAGIC 
# MAGIC The labs are located in an Azure Databricks zip-file called a DBC file.
# MAGIC 
# MAGIC The DBC file for this course is located at:
# MAGIC 
# MAGIC <input type="text" value="http://files.training.databricks.com/courses/azure-databricks/latest/course.dbc" style="width: 40em" onDblClick="event.stopPropagation()"></input>
# MAGIC 
# MAGIC Copy the URL to your clipboard but don't download it yet.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Step 2
# MAGIC 
# MAGIC Import the lab files into your Azure Databricks Workspace:
# MAGIC 0. Click the **Home** icon in the left sidebar.
# MAGIC <div><img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/eLearning/home.png" style="height: 200px"/></div><br/>
# MAGIC 0. Right-click your home folder, then click **Import**.
# MAGIC <div><img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/eLearning/import-labs-1.png" style="height: 175px; border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa"/></div><br/>
# MAGIC 0. In the popup, click **URL**.
# MAGIC <div><img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/eLearning/import-labs-2.png" style="height: 125px; border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa"/></div><br/>
# MAGIC 0. Paste the URL you copied in step 1 into the text box.  Here is the URL again:<br/>
# MAGIC <input type="text" value="http://files.training.databricks.com/courses/azure-databricks/latest/course.dbc" style="width: 40em" onDblClick="event.stopPropagation()" />
# MAGIC 0. Click the **Import** button.  The import process may take a couple of minutes to complete.
# MAGIC <br/>
# MAGIC 0. Wait for the import to finish. This can take a minute or so to complete.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Step 3
# MAGIC 
# MAGIC Verify that the notebook was properly imported:
# MAGIC 1. Click the **Home** icon in the left sidebar.
# MAGIC 2. Select your home folder.
# MAGIC 3. Select the course folder