# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Lab: Post-Processing on a Data Stream

# COMMAND ----------

# MAGIC %run "./../Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC Import the same Airbnb dataset.

# COMMAND ----------

airbnbDF = spark.read.parquet("/mnt/conor-work/airbnb/airbnb-cleaned-mlflow.parquet")
display(airbnbDF)

# COMMAND ----------

# MAGIC %md
# MAGIC Create a data stream to make predictions off of. Fill in the schema field with the appropriate airbnbDF schema.

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", "8")

# COMMAND ----------

# ANSWER
streamingData = (spark
                 .readStream
                 .schema(airbnbDF.schema)
                 .option("maxFilesPerTrigger", 1)
                 .parquet("/mnt/conor-work/airbnb/airbnb-cleaned-mlflow.parquet")
                 .drop("price"))

# COMMAND ----------

# MAGIC %md
# MAGIC Run the following cell to train a random forest model `rf` for making price predictions.

# COMMAND ----------

import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor

df = airbnbDF.toPandas()
X_train, X_test, y_train, y_test = train_test_split(df.drop(["price"], axis=1), df[["price"]].values.ravel(), random_state=42)

# new random forest model
rf = RandomForestRegressor(n_estimators=100, max_depth=25)

# fit and evaluate new rf model
rf.fit(X_train, y_train)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Adding Post-Processing Logic to Data Stream

# COMMAND ----------

# MAGIC %md
# MAGIC When processing our data stream, we are interested in seeing, for each data point, whether the predicted price is "High", "Medium", or "Low". To accomplish this, we are going to define a model class which will apply the desired post-processing step to our random forest `rf`'s results with a `.predict()` call.
# MAGIC 
# MAGIC Complete the `postprocess_result()` function to change the predicted value from a number to one of 3 catergorical labels, "High", "Medium", or "Low". Then finish the line in `predict()` to return the desired output.

# COMMAND ----------

# ANSWER
import mlflow

# Define the model class
class streaming_model(mlflow.pyfunc.PythonModel):

    def __init__(self, trained_rf):
        self.rf = trained_rf

    def postprocess_result(self, results):
        '''return post-processed results
        High: predicted price >= 120
        Medium: predicted price < 120 and >= 70
        Low: predicted price < 70'''
        output = []
        for result in results:
          if result >= 120:
            output.append("High")
          elif result >= 70:
            output.append("Medium")
          else:
            output.append("Low")
        return output
    
    def predict(self, context, model_input):
        results = self.rf.predict(model_input)
        return self.postprocess_result(results)

# COMMAND ----------

# MAGIC %md
# MAGIC Run the following cell to create and save your model at `model_path`.

# COMMAND ----------

# Construct and save the model
model_path = "/dbfs/tmp/07_streaming_model"
dbutils.fs.rm(model_path.replace("/dbfs", ""), True) # remove folder if already exists

model = streaming_model(trained_rf = rf)
mlflow.pyfunc.save_model(dst_path=model_path, python_model=model)

# COMMAND ----------

# MAGIC %md
# MAGIC The next cell will test your `streaming_model`'s `.predict()` function on fixed data `X_test` (not a data stream). You should see a list of price labels output underneath the cell.

# COMMAND ----------

# Load the model in `python_function` format
loaded_model = mlflow.pyfunc.load_pyfunc(model_path)

# Apply the model
loaded_model.predict(X_test)

# COMMAND ----------

# MAGIC %md
# MAGIC Finally, after confirming that your model works properly, apply it in parallel on all rows of `streamingData`.

# COMMAND ----------

# ANSWER
import mlflow.pyfunc

# Load the model in as a spark udf
pyfunc_udf = mlflow.pyfunc.spark_udf(spark, model_path, result_type="string")

# Apply UDF to data stream
predictionsDF = streamingData.withColumn("prediction", pyfunc_udf(*streamingData.columns))
display(predictionsDF)

# COMMAND ----------

# MAGIC %md
# MAGIC Now continuously write `predictionsDF` to a parquet file as they get created by the model.

# COMMAND ----------

checkpointLocation = userhome + "/academy/stream.checkpoint"
writePath = userhome + "/academy/predictions"

(streamingData
  .writeStream                                           # Write the stream
  .format("parquet")                                     # Use the delta format
  .partitionBy("zipcode")                                # Specify a feature to partition on
  .option("checkpointLocation", checkpointLocation)      # Specify where to log metadata
  .option("path", writePath)                             # Specify the output path
  .outputMode("append")                                  # Append new records to the output path
  .start()                                               # Start the operation
)

# COMMAND ----------

# MAGIC %md
# MAGIC Check that your predictions are indeed being written out to `writePath`.

# COMMAND ----------

dbutils.fs.ls(writePath)

# COMMAND ----------

# MAGIC %md
# MAGIC Run the following cell to terminate all active streams.

# COMMAND ----------

# stop streams
[q.stop() for q in spark.streams.active]


# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>