# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Batch Deployment
# MAGIC 
# MAGIC Batch inference is the most common way of deploying machine learning models.  This lesson introduces various strategies for deploying models using batch including in pure Python, Spark, and on the JVM.
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lesson you:<br>
# MAGIC  - Explore batch deployment options
# MAGIC  - Predict on a Pandas DataFrame and save the results
# MAGIC  - Predict on a Spark DataFrame and save the results
# MAGIC  - Compare other batch deployment options

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Inference in Batch
# MAGIC 
# MAGIC Batch deployment represents the vast majority of use cases for deploying machine learning models.  At a high level, this normally means running the predictions from a model and saving them somewhere for later use.  If the model is needed to be served live--for instance as part of website--then the results are often saved to a database that will serve the saved prediction quickly.  In other cases, such as populating emails, they can be stored in less performant data stores such as a blob store.
# MAGIC 
# MAGIC Writing the results of your inference can be optimized in a number of ways.  For large sums of data, writes should be performed in parallel.  **The access pattern for the saved predicitons should also be kept in mind in how the data is written.**  In the case of writing to static files or data warehouses, partitioning speeds up data reads.  In the case of databases, indexing the database on the relavant query generally improves performance.  In either case, the index is working similar to an index in a book: it allows you to skip ahead to the relavant content.
# MAGIC 
# MAGIC There are a few other considerations to ensure the accuracy of your model.  First is to make sure that your model generally matches expectations.  We'll cover this in further detail in the model drift section.  Second is to **retrain your model on the entirety of your dataset.**  While making a train/test split is a good method in tuning hyperparameters and estimating how the model will perform on unseen data, retraining the model on the entirety of your data ensures that you have as much information as possible factored into the model.

# COMMAND ----------

# MAGIC %md
# MAGIC Run the following cell to set up our environment.

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Inference in Pure Python
# MAGIC 
# MAGIC Inference in Python leverages the predict functionality of the machine learning package or MLflow wrapper.

# COMMAND ----------

# MAGIC %md
# MAGIC First create a new experiment for this lesson

# COMMAND ----------

import mlflow
from mlflow.exceptions import MlflowException
from  mlflow.tracking import MlflowClient

experimentPath = "/Users/" + username + "/experiment-L4"

try:
  experimentID = mlflow.create_experiment(experimentPath)
except MlflowException:
  experimentID = MlflowClient().get_experiment_by_name(experimentPath).experiment_id
  mlflow.set_experiment(experimentPath)

print("The experiment can be found at the path `{}` and has an experiment_id of `{}`".format(experimentPath, experimentID))

# COMMAND ----------

# MAGIC %md
# MAGIC Import the data.  **Do not perform a train/test split.**

# COMMAND ----------

import pandas as pd
from sklearn.model_selection import train_test_split

df = pd.read_csv("/dbfs/mnt/conor-work/airbnb/airbnb-cleaned-mlflow.csv")

X = df.drop(["price"], axis=1)
y = df["price"]

# COMMAND ----------

# MAGIC %md
# MAGIC Train a final model

# COMMAND ----------

from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error

rf = RandomForestRegressor(n_estimators=100, max_depth=5)
rf.fit(X, y)

predictions = X.copy()
predictions["prediction"] = rf.predict(X)

mse = mean_squared_error(y, predictions["prediction"]) # This is on the same data the model was trained

# COMMAND ----------

# MAGIC %md
# MAGIC Save the results and partition by zipcode.  Note that zip code was indexed.

# COMMAND ----------

import os

path = "/dbfs/tmp/" + username + "/predictions/"

dbutils.fs.rm(path.replace("/dbfs", "dbfs:"), True)

for i, partition in predictions.groupby(predictions["zipcode"]):
  dirpath = path + str(i)
  print("Writing to {}".format(dirpath))
  os.makedirs(dirpath)
  
  partition.to_csv(dirpath + "/predictions.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC Log the model and predictions.

# COMMAND ----------

import mlflow.sklearn

# mlflow.set_experiment(experiment_name=experimentPath)

with mlflow.start_run(experiment_id=experimentID, run_name="Final RF Model") as run: 
  mlflow.sklearn.log_model(rf, "random-forest-model")
  mlflow.log_metric("mse on training data", rf_mse)
  
  mlflow.log_artifacts(path, "predictions")
  
  run_info = run.info

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Inference in Spark
# MAGIC 
# MAGIC Models trained in various machine learning libraries can be applied at scale using Spark.  To do this, use `mlflow.pyfunc.spark_udf` and pass in the `SparkSession`, name of the model, and run id.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Using UDF's in Spark means that supporting libraries must be installed on every node in the cluster.  In the case of `sklearn`, this is installed in Databricks clusters by default.  With using other libraries, install them using the UI in order to ensure that they will work as a UDF.

# COMMAND ----------

# MAGIC %md
# MAGIC Create a Spark DataFrame from the Pandas DataFrame.

# COMMAND ----------

XDF = spark.createDataFrame(X)

display(XDF)

# COMMAND ----------

# MAGIC %md
# MAGIC MLflow easily produces a Spark user defined function (UDF).  This bridges the gap between Python environments and applying models at scale using Spark.

# COMMAND ----------

pyfunc_udf = mlflow.pyfunc.spark_udf(spark, "random-forest-model", run_id=run_info.run_uuid)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC Apply the model as a standard UDF using the column names as the input to the function.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Python has an internal limit to the maximum number of arguments you can pass to a funciton.  The maximum number of features in a model applied in this way is therefore 255.  This limit will be changed in Python 3.7.

# COMMAND ----------

predictionDF = XDF.withColumn("prediction", pyfunc_udf(*X.columns))

display(predictionDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Other Deployment Options
# MAGIC 
# MAGIC There are a number of other common batch deployment options.  One common use case is going from a Python environment for training to a Java environment.  Here are a few tools:<br><br>
# MAGIC 
# MAGIC  - **An Easy Port to Java:** In certain models, such as linear regression, models can be trained the coefficients can be taken and implemented by hand in Java.  This can work with tree-based models as well
# MAGIC  - **Reserializing for Java:** Since Python uses Pickle by default to serialize, a library like <a href="https://github.com/jpmml/jpmml-sklearn" target="_blank">jpmml-sklearn</a> can deserialize `sklearn` libraries and reserialize them for use in Java environments
# MAGIC  - **Leveraging Library Functionality:** Some libraries include the ability to deploy to Java such as <a href="https://github.com/dmlc/xgboost/tree/master/jvm-packages" target="_blank">xgboost4j</a>
# MAGIC  - **Containers:** Using containerized solutions are becoming increasingly popular since they offer the encapsulation and reliability offered by jars while offering better more deployment options than just the Java environment.
# MAGIC  
# MAGIC Finally, MLeap is a common, open source serialization format and execution engine for Spark, `sklearn`, and `Tensorflow`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Review
# MAGIC **Question:** What are the main considerations in batch deployments?  
# MAGIC **Answer:** The following considerations help determine the best way to deploy batch inference results:
# MAGIC * How the data will be queried
# MAGIC * How the data will be written 
# MAGIC * The training and deployment environment
# MAGIC * What data the final model is trained on
# MAGIC 
# MAGIC **Question:** How can you optimize inference reads and writes?  
# MAGIC **Answer:** Writes can be optimized by managing parallelism.  In Spark, this would mean managing the partitions of a DataFrame such that work is evenly distributed and you have the most efficient connections back to the target database.
# MAGIC 
# MAGIC **Question:** How can I deploy models trained in Python in a Java environment?  
# MAGIC **Answer:** There are a number of ways to do this.  It's not unreasonable to just export model coefficients or trees in a random forest and parse them in Java.  This works well as a minimum viable product.  You can also look at different libraries that can serialize models in a way that the JVM can make use of them.  `jpmml-sklearn` and `xgboost4j` are two examples of this.  Finally, you can re-implement Python libraries in Java if needed.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC 
# MAGIC Start the next lesson, [Streaming Deployment]($./07-Streaming-Deployment ).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Topics & Resources
# MAGIC 
# MAGIC **Q:** Where can I find more information on UDF's created by MLflow?  
# MAGIC **A:** See the <a href="https://www.mlflow.org/docs/latest/python_api/mlflow.pyfunc.html" target="_blank">MLflow documentation for details</a>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>