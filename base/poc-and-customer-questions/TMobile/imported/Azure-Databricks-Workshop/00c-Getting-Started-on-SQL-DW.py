# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://cdn2.hubspot.net/hubfs/438089/docs/training/dblearning-banner.png" alt="Databricks Learning" width="555" height="64">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Getting Started on SQL and SQL DW
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
# MAGIC - Deploy Adventure Works SQL
# MAGIC - Deploy Adventure Works SQL DW

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/wiki-book/general/logo_spark_tiny.png) Deploy Adventure Works SQL

# COMMAND ----------

# MAGIC %md
# MAGIC <span>1.</span> Click on "SQL Databases" on the Azure home screen  <br>
# MAGIC 
# MAGIC <div><img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/eLearning/MSFT/MSFT-SQL-setup-1.png" style="height: 200px"/></div><br/>

# COMMAND ----------

# MAGIC %md
# MAGIC <span>2.</span> Click on "Add"  <br>
# MAGIC 
# MAGIC <div><img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/eLearning/MSFT/MSFT-SQL-setup-2.png" style="height: 200px"/></div><br/>

# COMMAND ----------

# MAGIC %md
# MAGIC <span>3.</span> Click on "Create SQL Databases"  <br>
# MAGIC 
# MAGIC <div><img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/eLearning/MSFT/MSFT-SQL-setup-3.png" style="height: 200px"/></div><br/>

# COMMAND ----------

# MAGIC %md
# MAGIC <span>4.</span> Enter "AdventureWorksDB" as your database name  <br>
# MAGIC <span>5.</span> Choose your subscription  <br>
# MAGIC <span>6.</span> Choose your resource group  <br>
# MAGIC <span>7.</span> Choose "Sample (AdventureWorkdsLT)" as your source  <br>
# MAGIC <span>8.</span> Click on "Server"  <br>
# MAGIC <span>9.</span> Click on "Create a new server"  <br>
# MAGIC <span>10.</span> Enter a server name.  Note that this needs to be a globally unique name  <br>
# MAGIC <span>11.</span> Enter an admin login  <br>
# MAGIC <span>12.</span> Enter your password  <br>
# MAGIC <span>13.</span> Confirm your password  <br>
# MAGIC <span>14.</span> Choose "East US 2" as your location  <br>
# MAGIC <span>15.</span> Click "Select" and then "Create"  <br>
# MAGIC <div><img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/eLearning/MSFT/MSFT-SQL-setup-4.png" style="height: 200px"/></div><br/>

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/wiki-book/general/logo_spark_tiny.png) Deploy Adventure Works SQL DW

# COMMAND ----------

# MAGIC %md
# MAGIC <span>1.</span> Click on "New" on the Azure landing page  <br>
# MAGIC 
# MAGIC <div><img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/eLearning/MSFT/MSFT-SQL-DW-setup-1.png" style="height: 200px"/></div><br/>

# COMMAND ----------

# MAGIC %md
# MAGIC <span>2.</span> Type in "SQL Data Warehouse"  <br>
# MAGIC 
# MAGIC <div><img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/eLearning/MSFT/MSFT-SQL-DW-setup-2.png" style="height: 200px"/></div><br/>

# COMMAND ----------

# MAGIC %md
# MAGIC <span>3.</span> Choose "SQL Data Warehouse"  <br>
# MAGIC 
# MAGIC <div><img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/eLearning/MSFT/MSFT-SQL-DW-setup-3.png" style="height: 200px"/></div><br/>

# COMMAND ----------

# MAGIC %md
# MAGIC <span>4.</span> Click on "Create"  <br>
# MAGIC 
# MAGIC <div><img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/eLearning/MSFT/MSFT-SQL-DW-setup-4.png" style="height: 200px"/></div><br/>

# COMMAND ----------

# MAGIC %md
# MAGIC <span>5.</span> Enter "AdventureWorksDW" as your database name  <br>
# MAGIC <span>6.</span> Choose your subscription  <br>
# MAGIC <span>7.</span> Choose your resource group  <br>
# MAGIC <span>8.</span> Choose "Sample" as your source  <br>
# MAGIC <span>9.</span> Select "AdventureWorksDW" as your sample  <br>
# MAGIC <span>10.</span> Choose the server that you created earlier  <br>
# MAGIC <span>11.</span> Enter your admin login <br>
# MAGIC <span>12.</span> Enter your password  <br>
# MAGIC <span>13.</span> Click on "Create"  <br>
# MAGIC 
# MAGIC <div><img src="https://s3-us-west-2.amazonaws.com/curriculum-release/images/eLearning/MSFT/MSFT-SQL-DW-setup-5.png" style="height: 200px"/></div><br/>