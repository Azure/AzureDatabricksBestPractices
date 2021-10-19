# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Use Databricks Delta Time Travel and MLflow to Analyze Power Plant Data
# MAGIC 
# MAGIC In this notebook, we 
# MAGIC 0. Stream power plant data to a Databricks Delta table
# MAGIC 0. Train our model on a current version of our data
# MAGIC 0. Post some results to MLflow
# MAGIC 0. Rewind to an older version of our data
# MAGIC 0. Re-train our model on an older version of our data
# MAGIC 0. Evaluate our (rewound) data 
# MAGIC 0. Make predictions on the streaming data
# MAGIC 
# MAGIC The focus isn't necessarily on Machine Learning here, but, it is to show you how we may integrate the latest Databricks features in Machine Learning.
# MAGIC 
# MAGIC Our schema definition from UCI appears below:
# MAGIC 
# MAGIC - AT = Atmospheric Temperature [1.81-37.11]°C
# MAGIC - V = Exhaust Vaccum Speed [25.36-81.56] cm Hg
# MAGIC - AP = Atmospheric Pressure in [992.89-1033.30] milibar
# MAGIC - RH = Relative Humidity [0-100]%
# MAGIC - PE = Power Output [420.26-495.76] MW
# MAGIC 
# MAGIC PE is our label or target. This is the value we are trying to predict given the measurements.
# MAGIC 
# MAGIC *Reference [UCI Machine Learning Repository Combined Cycle Power Plant Data Set](https://archive.ics.uci.edu/ml/datasets/Combined+Cycle+Power+Plant)*

# COMMAND ----------

# MAGIC %fs head /databricks-datasets/power-plant/README.md

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Slow Stream of Files
# MAGIC 
# MAGIC Our stream source is a repository of many small files.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Append to a Databricks Delta Table
# MAGIC 
# MAGIC Use this to create `powerTable`

# COMMAND ----------

# MAGIC %md
# MAGIC Cell below is to keep the stream running in case we do a RunAll

# COMMAND ----------

# MAGIC %md
# MAGIC Create a DataFrame out of the Delta stream so we can get a scatterplot.
# MAGIC 
# MAGIC This will be a "snapshot" of the data at an instant in time, so, a static table.

# COMMAND ----------

# MAGIC %md
# MAGIC ##Use Scatter Plot show intution
# MAGIC 
# MAGIC Let's plot `PE` versus other fields to see if there are any relationships.
# MAGIC 
# MAGIC You can toggle between fields by adjusting Plot Options.
# MAGIC 
# MAGIC Couple observations
# MAGIC * It looks like there is strong linear correlation between Atmospheric Temperature and Power Output
# MAGIC * Maybe a bit of correlation between Atmospheric Pressure and Power Output

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##Train LR model on static DataFrame
# MAGIC <br>
# MAGIC 0. Split `staticPowerDF` into training and test set
# MAGIC 0. Use all features: AT, AP, RH and V
# MAGIC 0. Reshape training set
# MAGIC 0. Do linear regression
# MAGIC 0. Predict power output (PE)
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> data is changing under neath us

# COMMAND ----------

# MAGIC %md
# MAGIC ## Use MLFlow 
# MAGIC 
# MAGIC MLflow is an open source platform for managing the end-to-end machine learning lifecycle. 
# MAGIC 
# MAGIC In this notebook, we use MLflow to track experiments to record and compare parameters and results.
# MAGIC 
# MAGIC https://www.mlflow.org/docs/latest/index.html

# COMMAND ----------

# MAGIC %md
# MAGIC ### Post results to MLflow
# MAGIC 
# MAGIC In this notebook, we would like to keep track of the Root Mean Squared Error (RMSE).
# MAGIC 
# MAGIC This line actually does the work of posting the RMSE to MLflow.
# MAGIC 
# MAGIC `mlflowClient.logMetric(runId, "RMSE", rmse)`
# MAGIC 
# MAGIC If you rerun the below cell multiple times, you will see new runs are posted to MLflow, with different RMSE!

# COMMAND ----------

# MAGIC %md
# MAGIC ### Question to Ponder:
# MAGIC 
# MAGIC Why is `RMSE` changing under our feet? We are working with "static" DataFrames..
# MAGIC 
# MAGIC :INSTRUCTOR_NOTE: You might want to discuss this in class.

# COMMAND ----------

# MAGIC %md
# MAGIC ##Introducing Delta Time Travel
# MAGIC 
# MAGIC Delta time travel allows you to query an older snapshot of a Delta table.
# MAGIC 
# MAGIC At the beginning of this lesson, we timestamped our stream i.e.
# MAGIC 
# MAGIC `.option("timestampAsOf", now)` 
# MAGIC 
# MAGIC where `now` is the current timestamp
# MAGIC 
# MAGIC Let's wind back to a version of our table we had several hours ago & fit our data to that version.
# MAGIC 
# MAGIC Maybe some pattern we were looking at became apparent for the first time a few hours ago.
# MAGIC 
# MAGIC https://docs.databricks.com/delta/delta-batch.html#deltatimetravel
# MAGIC 
# MAGIC This query shows the timestamps of the Delta writes as they were happening.

# COMMAND ----------

# MAGIC %md
# MAGIC Let's rewind back to almost the beginning (where we had just a handful of rows), let's say the 5th write.
# MAGIC 
# MAGIC Maybe we started noticing a pattern at this point.

# COMMAND ----------

# MAGIC %md
# MAGIC We had this many (few) rows back then.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Train Model Based on Data from a Few Hours Ago
# MAGIC 
# MAGIC * Use `rewoundDF`
# MAGIC * Write to MLflow
# MAGIC 
# MAGIC Notice the only change from what we did earlier is the use of `rewoundDF`
# MAGIC 
# MAGIC `val Array(trainDF, testDF) = rewoundDF.randomSplit(Array(0.80, 0.20), seed=42)`

# COMMAND ----------

# MAGIC %md
# MAGIC ### Evaluate Using Test Set
# MAGIC 
# MAGIC 0. Reshape data via `assembler.transform()`
# MAGIC 0. Apply linear regression model 
# MAGIC 0. Record metrics in MLflow

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Final Model
# MAGIC 
# MAGIC The stats from the test set are pretty good so we've done a decent job coming up with the model.

# COMMAND ----------

# MAGIC %md
# MAGIC We are pretty happy with the model we developed.
# MAGIC 
# MAGIC Let's save the model.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Make real-time predictions using the data from the stream.
# MAGIC 
# MAGIC Let's apply the model we saved to the rest of the streaming data!

# COMMAND ----------

# MAGIC %md
# MAGIC Time to make some predictions!!

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean Up

# COMMAND ----------

# MAGIC %md
# MAGIC Stop all remaining streams.

# COMMAND ----------

# MAGIC %md
# MAGIC Drop the table we are using.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>