# Databricks notebook source
# MAGIC %md
# MAGIC # Machine Learning with Azure Databricks
# MAGIC Easy to get started collection of Machine Learning Examples in Azure Databricks
# MAGIC 
# MAGIC Example Notebooks: [HTML format](https://joelcthomas.github.io/ml-azuredatabricks/), [Github](https://github.com/joelcthomas/ml-azuredatabricks)  
# MAGIC 
# MAGIC ## Azure Databricks Reference Architecture - Machine Learning & Advanced Analytics
# MAGIC 
# MAGIC <img src="https://joelcthomas.github.io/ml-azuredatabricks/img/azure_databricks_reference_architecture.png" width="1300">
# MAGIC 
# MAGIC ## Key Benefits:
# MAGIC - Built for enterprise with security, reliability, and scalability
# MAGIC - End to end integration from data access (ADLS, SQL DW, EventHub, Kafka, etc.), data prep, feature engineering, model building in single node or distributed, MLops with MLflow, integration with AzureML, Synapse, & other Azure services.
# MAGIC - Delta Lake to set the data foundation with higher data quality, reliability and performance for downstream ML & AI use cases 
# MAGIC - ML Runtime Optimizations
# MAGIC     - Reliable and secure distribution of open source ML frameworks
# MAGIC     - Packages and optimizes most common ML frameworks
# MAGIC     - Built-in optimization for distributed deep learning
# MAGIC     - Built-in AutoML and Experiment tracking
# MAGIC     - Customized environments using conda for reproducibility
# MAGIC - Distributed Machine Learning
# MAGIC     - Spark MLlib
# MAGIC     - Migrate Single Node to distributed with just a few lines of code changes:
# MAGIC         - Distributed hyperparameter search (Hyperopt, Gridsearch)
# MAGIC         - PandasUDF to distribute models over different subsets of data or hyperparameters
# MAGIC         - Koalas: Pandas DataFrame API on Spark
# MAGIC     - Distributed Deep Learning training with Horovod
# MAGIC - Use your own tools
# MAGIC     - Multiple languages in same Databricks notebooks (Python, R, Scala, SQL)
# MAGIC     - Databricks Connect: connect external tools with Azure databricks (IDEs, RStudio, Jupyter,...)
# MAGIC 
# MAGIC ## Machine Learning & MLops Examples using Azure Databricks:
# MAGIC To reproduce in a notebook, see instructions below.
# MAGIC 
# MAGIC - [PyTorch on a single node with MNIST dataset](https://joelcthomas.github.io/ml-azuredatabricks/#PyTorch-SingleNode.html)
# MAGIC - [PyTorch, distributed with Horovod with MNIST dataset](https://joelcthomas.github.io/ml-azuredatabricks/#PyTorch-Horovod.html)
# MAGIC - [Using MLflow to track hyperparameters, metrics, log models/artifacts in AzureML](https://joelcthomas.github.io/ml-azuredatabricks/#PyTorch-SingleNode.html)
# MAGIC - [Using MLflow to deploy a scoing server (REST endpoint) with ACI](https://joelcthomas.github.io/ml-azuredatabricks/#PyTorch-SingleNode.html)  
# MAGIC 
# MAGIC Adding soon:
# MAGIC - Single node scikit-learn to distributed hyperparamter search using Hyperopt 
# MAGIC - Single node pandas to distributed using Koalas
# MAGIC - PandasUDF to distribute models over different subsets of data or hyperparameters
# MAGIC - Using databricks automl-toolkit in Azure Databricks
# MAGIC - Using automl from AzureML in Azure Databricks
# MAGIC 
# MAGIC Other:
# MAGIC - [Model Drift](https://github.com/joelcthomas/modeldrift)

# COMMAND ----------

# MAGIC %md
# MAGIC ## How to run this example?
# MAGIC To reproduce examples provided here, please import `ml-azuredatabricks.dbc` file in git root directory to databricks workspace.
# MAGIC 
# MAGIC [Instructions on how to import notebooks in databricks](https://docs.microsoft.com/en-us/azure/databricks/notebooks/notebooks-manage#--import-a-notebook)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup Cluster
# MAGIC Create a cluster - https://docs.microsoft.com/en-us/azure/databricks/clusters/create  
# MAGIC GPU enabled Clusters - https://docs.microsoft.com/en-us/azure/databricks/clusters/gpu  
# MAGIC Install a library/package - https://docs.microsoft.com/en-us/azure/databricks/libraries  
# MAGIC Machine Learning Runtime - https://docs.microsoft.com/en-us/azure/databricks/runtime/mlruntime  
# MAGIC To see list of already available package in each runtime - https://docs.microsoft.com/en-us/azure/databricks/release-notes/runtime/releases

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Information
# MAGIC For more information on using Azure Databricks  
# MAGIC https://docs.microsoft.com/en-us/azure/azure-databricks/