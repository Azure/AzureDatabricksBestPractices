// Databricks notebook source
// MAGIC 
// MAGIC %md-sandbox
// MAGIC 
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC # Data Science with Apache&reg; Spark&trade;

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC -- INTRUCTOR_NOTE
// MAGIC 
// MAGIC 1. Intro to Databricks 
// MAGIC    1. Sign in
// MAGIC    1. Differentiate between Apache Spark and Databricks
// MAGIC    1. Especially Databricks Runtime for ML
// MAGIC 1. Intro to Spark
// MAGIC    1. how it differs from single node sklearn and pandas
// MAGIC 1. Machine Learning Overview
// MAGIC    - https://brookewenig.github.io/MLOverview.html
// MAGIC    - only through Data Cleansing
// MAGIC 1. Data Cleansing
// MAGIC 1. Data Exploration
// MAGIC 1. Linear Regression 
// MAGIC 1. Decision Trees
// MAGIC 1. Hyperparameter Tuning
// MAGIC 1. Random Forest Hyperparameter Tuning
// MAGIC 1. MLflow

// COMMAND ----------

// MAGIC %md
// MAGIC ## AM
// MAGIC | Time | Topic &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; | Description &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp;  |
// MAGIC |:-:|-|-|
// MAGIC |  9:00 am  | Sign In ||
// MAGIC |  9:30 am  | Intro to Databricks | |
// MAGIC |  9:50 am  | [Apache Spark Overview]($./00a-Spark-Overview) | |
// MAGIC | 10:10 am  | Machine Learning Overview | Slides available [here](https://brookewenig.github.io/MLOverview.html#/). |
// MAGIC | 10:30 am  | **break** | |
// MAGIC | 11:00 am  | [Data Cleansing]($./01-Data-Cleansing)  | |
// MAGIC | 11:30 am  | [Data Exploration Lab]($./01L-Data-Exploration) | &nbsp; |
// MAGIC 
// MAGIC 
// MAGIC ## PM
// MAGIC | Time | Topic &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; | Description &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp;  |
// MAGIC |:-:|-|-|
// MAGIC | 12:00 pm  | **lunch** | You can review [this slide deck](https://brookewenig.github.io/LinearRegression.html#/) on linear regression during lunch. |
// MAGIC |  1:00 pm  | [Linear Regression I]($./02-Linear-Regression-I) ||
// MAGIC |  1:25 pm  | [Linear Regression II]($./03-Linear-Regression-II) ||
// MAGIC |  1:45 pm  | [Linear Regression II Lab]($./03L-Linear-Regression-II) ||
// MAGIC |  2:15 pm  | [Decision Trees]($./05-Decision-Trees) ||
// MAGIC |  2:35 pm  | [Hyperparameter Tuning]($./06-Hyperparameter-Tuning) ||
// MAGIC |  3:00 pm  | **break** ||
// MAGIC |  3:30 pm  | [Random Forest Hyperparameter Tuning Lab]($./06L-RF-Hyperparameter-Tuning) ||
// MAGIC |  4:00 pm  | MLflow | MLflow (track experiments/mention, MLeap & deployment options)| 

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>