# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Course Overview and Setup
# MAGIC ## Machine Learning in Production
# MAGIC ### MLflow and Model Deployment
# MAGIC 
# MAGIC In this course data scientists and data engineers learn the best practices for putting machine learning models into production.  It starts with managing experiments, projects, and models using MLflow.  It then explores various deployment options including batch predictions, real time with Spark Streaming, and on demand with RESTful and containerized services.  Finally, it covers monitoring machine learning models once they have been deployed into production.
# MAGIC 
# MAGIC By the end of this course, you will have built the infrastructure to log, deploy, and monitor machine learning models.
# MAGIC 
# MAGIC ** Prerequisites:**<br><br>
# MAGIC 
# MAGIC - Python (`pandas`, `sklearn`, `numpy`)
# MAGIC - Background in machine learning and data science
# MAGIC - Basic knowledge of Spark DataFrames (recommended)
# MAGIC 
# MAGIC ** The course consists of the following lessons:**<br><br>
# MAGIC 
# MAGIC 0. Course Overview and Setup
# MAGIC 0. Experiment Tracking
# MAGIC 0. Packaging ML Projects
# MAGIC 0. Model Management 
# MAGIC 0. Production Issues
# MAGIC 0. Batch Deployment
# MAGIC 0. Streaming Deployment
# MAGIC 0. Real Time Deployment
# MAGIC 0. Drift Monitoring
# MAGIC 0. Alerting
# MAGIC 0. Delta Time Travel
# MAGIC 
# MAGIC Open questions:
# MAGIC - Debugging and provenance (when you have poor performance in production, you need to reproduce it. Can do this with a link to data in MLflow, copy and paste the MLflow project run command)
# MAGIC 
# MAGIC Out of scope?:
# MAGIC  - Warm starts?

# COMMAND ----------

# MAGIC %md
# MAGIC -- INSTRUCTOR_ONLY
# MAGIC 
# MAGIC Internal Resources:
# MAGIC   - [Go to market strategy](https://docs.google.com/document/d/11TY2A6MNPvFy1O-XmKalp59vC0vDbqN5-MAZXsJ1Pe4/edit#)
# MAGIC   - [Golden pitch deck](https://docs.google.com/presentation/d/1wkqmBxFTBCTf_GwD46W77-wFgXKWFStmEwYIpyBA7zI/edit#slide=id.p16)
# MAGIC   - [MLflow strategy](https://docs.google.com/presentation/d/1jqL54c-usY5SZyyL9C6AxISBrArSGLlHSnmNV6LWdOs/edit#slide=id.g41fc7cf04d_0_117)
# MAGIC   - [Demo from ILT](https://databricks-prod-cloudfront.cloud.databricks.com/public/793177bc53e528530b06c78a4fa0e086/0/5769800/100020/latest.html)
# MAGIC   - [Andy's talk on multistep workflows](https://drive.google.com/drive/u/0/folders/0B86uHPNtT6KsflAzRGZwcmVCTkZ5cEhqRjhuVWhpb2lDSElGVzdWcmlVX1hkb1hCOUd0cnM)
# MAGIC   - [Andre's links of available resources](https://databricks.atlassian.net/wiki/spaces/~andre.mesarovic/pages/571244676/MLflow+Links#MLflowLinks-DatabricksInternal)
# MAGIC   - [Andre's links of available notebooks](https://demo.cloud.databricks.com/#notebook/1564377/command/2395991) 
# MAGIC   
# MAGIC External Resources:
# MAGIC   - [Docs](https://mlflow.org/docs/latest/index.html)
# MAGIC   - [github](https://github.com/mlflow/mlflow/)
# MAGIC   - [Richard's talk](https://www.datasciencecentral.com/video/apache-sparkm-mllib-2-x-productionize-your-machine-learning-model)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Schedule
# MAGIC 
# MAGIC - Morning 9-12 - MLflow
# MAGIC   - 15m break
# MAGIC - Lunch 12-1
# MAGIC - Afternoon 1-5 - Deployment
# MAGIC   - 30m break

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Getting Started on Databricks
# MAGIC 
# MAGIC ## In this lesson you:
# MAGIC * Introduce the Machine Learning in Production
# MAGIC * Log into Databricks
# MAGIC * Create a notebook inside your home folder in Databricks
# MAGIC * Create, or attach to, a Spark cluster
# MAGIC * Import the course files into your home folder
# MAGIC 
# MAGIC ## Audience
# MAGIC * Primary Audience: Data Scientists and Data Analysts
# MAGIC * Additional Audiences: Data Engineers
# MAGIC 
# MAGIC ## Prerequisites
# MAGIC * Web browser: current versions of Google Chrome, Firefox, Safari, Microsoft Edge and 
# MAGIC Internet Explorer 11 on Windows 7, 8, or 10 (see <a href="https://docs.databricks.com/user-guide/supported-browsers.html#supported-browsers#" target="_blank">Supported Web Browsers</a>)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 1
# MAGIC 
# MAGIC Create a notebook and Spark cluster.

# COMMAND ----------

# MAGIC %md
# MAGIC <iframe  
# MAGIC src="//fast.wistia.net/embed/iframe/46ztztgeuq?videoFoam=true"
# MAGIC style="border:1px solid #1cb1c2;"
# MAGIC allowtransparency="true" scrolling="no" class="wistia_embed"
# MAGIC name="wistia_embed" allowfullscreen mozallowfullscreen webkitallowfullscreen
# MAGIC oallowfullscreen msallowfullscreen width="640" height="360" ></iframe>
# MAGIC <div>
# MAGIC <a target="_blank" href="https://fast.wistia.net/embed/iframe/46ztztgeuq?seo=false">
# MAGIC   <img alt="Opens in new tab" src="https://files.training.databricks.com/static/images/external-link-icon-16x16.png"/>&nbsp;Watch full-screen.</a>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Step 1
# MAGIC Databricks notebooks are backed by clusters, or networked computers, that process data. Create a Spark cluster (*if you already have a running cluster, skip to **Step 3** *):
# MAGIC 1. Select the **Clusters** icon in the sidebar.
# MAGIC <div><img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/eLearning/create-cluster-4.png" style="height: 200px; margin: 20px"/></div>
# MAGIC 2. Click the **Create Cluster** button.
# MAGIC <div><img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/eLearning/create-cluster-5.png" style="border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa; margin: 20px"/></div>
# MAGIC 3. Name your cluster. Use your name or initials to easily differentiate your cluster from your coworkers.
# MAGIC 4. Select the cluster type. We recommend the latest Databricks runtime (**3.3**, **3.4**, etc.) and Scala **2.11**.
# MAGIC 5. Specify your cluster configuration.
# MAGIC 
# MAGIC   * For all other shards, please refer to your company's policy on private clusters.</br></br>
# MAGIC 6. Click the **Create Cluster** button.
# MAGIC <div><img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/eLearning/create-cluster-2.png" style="height: 300px; border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa; margin: 20px"/></div>
# MAGIC 
# MAGIC 
# MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** Check with your local system administrator to see if there is a recommended default cluster at your company to use for the rest of the class. This could save you  money!

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Step 2
# MAGIC 
# MAGIC Create a new notebook in your home folder:
# MAGIC 1. Select the **Home** icon in the sidebar.
# MAGIC <div><img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/eLearning/home.png" style="height: 200px; margin: 20px"/></div>
# MAGIC 2. Right-click your home folder.
# MAGIC 3. Select **Create**.
# MAGIC 4. Select **Notebook**.
# MAGIC <div><img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/eLearning/create-notebook-1.png" style="height: 150px; border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa; margin: 20px"/></div>
# MAGIC 5. Name your notebook `My Notebook`.<br/>
# MAGIC 6. Set the language to **Python**.<br/>
# MAGIC 7. Select the cluster to attach this Notebook.  
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> If a cluster is not currently running, this option will not exist.
# MAGIC 8. Click **Create**.
# MAGIC <div>
# MAGIC   <div style="float:left"><img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/eLearning/create-notebook-2b.png" style="width:400px; border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa; margin: 20px"/></div>
# MAGIC   <div style="float:left; margin-left: e3m; margin-right: 3em">or</div>
# MAGIC   <div style="float:left"><img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/eLearning/create-notebook-2.png" style="width:400px; border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa; margin: 20px"/></div>
# MAGIC   <div style="clear:both"></div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Step 3
# MAGIC 
# MAGIC Now  you have a notebook, use it to run code.
# MAGIC 1. In the first cell of your notebook, type `1 + 1`. 
# MAGIC 2. Run the cell: Click the **Run** icon and then select **Run Cell**.
# MAGIC <div><img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/eLearning/run-notebook-1.png" style="width:600px; margin-bottom:1em; border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa; margin: 20px"/></div>
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> **Ctrl-Enter** also runs a cell.

# COMMAND ----------

1 + 1

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ### Attach and Run
# MAGIC 
# MAGIC If your notebook was not previously attached to a cluster you might receive the following prompt: 
# MAGIC <div><img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/eLearning/run-notebook-2.png" style="margin-bottom:1em; border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa; margin: 20px"/></div>
# MAGIC 
# MAGIC If you click **Attach and Run**, first make sure that you are attaching to the correct cluster.
# MAGIC 
# MAGIC If it is not the correct cluster, click **Cancel** instead see the next cell, **Attach & Detach**.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Attach & Detach
# MAGIC 
# MAGIC If your notebook is detached you can attach it to another cluster:  
# MAGIC <img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/eLearning/attach-to-cluster.png" style="border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa; margin: 20px"/>
# MAGIC 
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
# MAGIC ## Summary
# MAGIC * Click the down arrow on a folder and select the **Create Notebook** option to create notebooks.
# MAGIC * Click the down arrow on a folder and select the **Import** option to import notebooks.
# MAGIC * Select the **Attached/Detached** option directly below the notebook title to attach to a spark cluster 
# MAGIC * Create clusters using the Clusters button on the left sidebar.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Review
# MAGIC 
# MAGIC **Question:** How do you create a Notebook?  
# MAGIC **Answer:** Sign into Databricks, select the **Home** icon from the sidebar, right-click your home-folder, select **Create**, and then **Notebook**. In the **Create Notebook** dialog, specify the name of your notebook and the default programming language.
# MAGIC 
# MAGIC **Question:** How do you create a cluster?  
# MAGIC **Answer:** Select the **Clusters** icon on the sidebar, click the **Create Cluster** button, specify the specific settings for your cluster and then click **Create Cluster**.
# MAGIC 
# MAGIC **Question:** How do you attach a notebook to a cluster?  
# MAGIC **Answer:** If you run a command while detached, you may be prompted to connect to a cluster. To connect to a specific cluster, open the cluster menu by clicking the **Attached/Detached** menu item and then selecting your desired cluster.
# MAGIC 
# MAGIC **Question:** What does it mean to "attach" to a cluster?  
# MAGIC **Answer:** Attaching a notebook to a cluster means that the  current notebook will execute its code on that cluster. You could also say that when attached, the notebook is using the resources of that cluster.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Next Steps
# MAGIC 
# MAGIC This course is available in Python and Scala.  Start the next lesson, **Experiment Tracking**.
# MAGIC 1. Click the **Home** icon in the left sidebar.
# MAGIC 2. Select your home folder.
# MAGIC 3. Select the folder **Machine-Learning-in-Production**
# MAGIC 4. Open the notebook **02-Experiment-Tracking** in either the Python or Scala folder
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/eLearning/ML-Part-1/course-intro-next-steps.png" style="margin-bottom: 5px; border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa; width: auto; height: auto; max-height: 383px"/>
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> The Python and Scala content is identical except for the language used.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Topics & Resources
# MAGIC **Q:** Are there additional docs I can reference to find my way around Databricks?  
# MAGIC **A:** See <a href="https://docs.databricks.com/user-guide/getting-started.html" target="_blank">Getting Started with Databricks</a>.
# MAGIC 
# MAGIC **Q:** Where can I learn more about the cluster configuration options?  
# MAGIC **A:** See <a href="https://docs.databricks.com/user-guide/clusters/index.html" target="_blank">Spark Clusters on Databricks</a>.
# MAGIC 
# MAGIC **Q:** Can I import formats other than .dbc files?  
# MAGIC **A:** Yes, see <a href="https://docs.databricks.com/user-guide/notebooks/index.html#importing-notebooks" target="_blank">Importing Notebooks</a>.
# MAGIC 
# MAGIC **Q:** Can I use browsers other than Chrome or Firefox?  
# MAGIC **A:** Databricks is tested for Chrome and Firefox.  It does work on Internet Explorer 11 and Safari however, it is possible some user-interface features may not work properly.
# MAGIC 
# MAGIC **Q:** Can I install the courseware notebooks into a non-Databricks distribution of Spark?  
# MAGIC **A:** No, the files that contain the courseware are in a Databricks specific format (DBC).
# MAGIC 
# MAGIC **Q:** Do I have to have a paid Databricks subscription to complete this course?  
# MAGIC **A:** No, you can sign up for a free <a href="https://databricks.com/try-databricks" target="_blank">Community Edition</a> account from Databricks.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>