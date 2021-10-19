# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <html>
# MAGIC   <head>
# MAGIC     <script>
# MAGIC       function showhide() {
# MAGIC         var x = document.getElementById("showhide");
# MAGIC         if (x.style.display === "none") {
# MAGIC           x.style.display = "block";
# MAGIC         } else {
# MAGIC           x.style.display = "none";
# MAGIC         }
# MAGIC       }
# MAGIC     </script>
# MAGIC     <style>
# MAGIC       .button {
# MAGIC         background-color: #e7e7e7; color: black;
# MAGIC         border: none;
# MAGIC         color: black;
# MAGIC         padding: 15px 32px;
# MAGIC         text-align: center;
# MAGIC         text-decoration: none;
# MAGIC         display: inline-block;
# MAGIC         font-size: 16px;
# MAGIC         border-radius: 8px;
# MAGIC       }
# MAGIC     </style>
# MAGIC     
# MAGIC   </head>
# MAGIC   
# MAGIC   <body>
# MAGIC     <h1><img src="/files/tables/infinitypower_small-ad799.png"> InfinityPower - Active Room Identification</h1>
# MAGIC      <div style="text-align: left; line-height: 0; padding-top: 9px;">
# MAGIC       <img src="files/tables/Home_Appliance-2ebee.jpg" style="width: 800px;">
# MAGIC     </div>
# MAGIC     <p>
# MAGIC       <button class="button" onclick="showhide()">Info on Dataset</button>
# MAGIC     </p>
# MAGIC 
# MAGIC     <div id="showhide" style="display:none">
# MAGIC       <p>
# MAGIC       <h2>Individual household electric power consumption Data Set </h2>
# MAGIC       <a href="http://archive.ics.uci.edu/ml/datasets/Individual+household+electric+power+consumption#">From UCI Machine Learning Repository</a>
# MAGIC       <h3>Data Set Information:</h3>
# MAGIC 
# MAGIC       <p>
# MAGIC This archive contains 2075259 measurements gathered in a house located in Sceaux (7km of Paris, France) between December 2006 and November 2010 (47 months). <br>
# MAGIC Notes: <br>
# MAGIC 1.(global_active_power*1000/60 - sub_metering_1 - sub_metering_2 - sub_metering_3) represents the active energy consumed every minute (in watt hour) in the household by electrical equipment not measured in sub-meterings 1, 2 and 3. <br>
# MAGIC 2.The dataset contains some missing values in the measurements (nearly 1,25% of the rows). All calendar timestamps are present in the dataset but for some timestamps, the measurement values are missing: a missing value is represented by the absence of value between two consecutive semi-colon attribute separators. For instance, the dataset shows missing values on April 28, 2007.
# MAGIC </p>
# MAGIC 
# MAGIC       <h3>Attribute Information:</h3>
# MAGIC 
# MAGIC       <p>
# MAGIC 1.date: Date in format dd/mm/yyyy <br>
# MAGIC 2.time: time in format hh:mm:ss <br>
# MAGIC 3.global_active_power: household global minute-averaged active power (in kilowatt) <br>
# MAGIC 4.global_reactive_power: household global minute-averaged reactive power (in kilowatt) <br>
# MAGIC 5.voltage: minute-averaged voltage (in volt) <br>
# MAGIC 6.global_intensity: household global minute-averaged current intensity (in ampere) <br>
# MAGIC 7.sub_metering_1: energy sub-metering No. 1 (in watt-hour of active energy). It corresponds to the kitchen, containing mainly a dishwasher, an oven and a microwave (hot plates are not electric but gas powered). <br>
# MAGIC 8.sub_metering_2: energy sub-metering No. 2 (in watt-hour of active energy). It corresponds to the laundry room, containing a washing-machine, a tumble-drier, a refrigerator and a light. <br>
# MAGIC 9.sub_metering_3: energy sub-metering No. 3 (in watt-hour of active energy). It corresponds to an electric water-heater and an air-conditioner.
# MAGIC       </p>
# MAGIC       </p>
# MAGIC     </div>
# MAGIC   </body>
# MAGIC </html>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <h2><img src="/files/tables/infinitypower_small-ad799.png">Overview</h2>
# MAGIC 
# MAGIC Intent:  
# MAGIC To create a model that can be used to identify which rooms or units are active given only information on the global power.   
# MAGIC 
# MAGIC Data available:  
# MAGIC There are sub meter readings for 3 rooms (kitchen, laundry, heater+hvac) and from global power we can calculate the miscellaneous usage of power.  
# MAGIC Bin the meter readings as _active_ or _inactive_ by selecting a break point. 15 watt hour is chosen as break point based on exploratory data analysis.   
# MAGIC Multiple rooms states is converged to a single state for the purposes of model building

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <h2><img src="/files/tables/infinitypower_small-ad799.png">Data Preparation & Understanding</h2>

# COMMAND ----------

# DBTITLE 1,Read Pre-processed Data
df_transformed = spark.read.format("delta").load("/mnt/joel-blob-poweranalysis/parsed-delta")
df_transformed = df_transformed.na.drop()

display(df_transformed)

# COMMAND ----------

# DBTITLE 1,Summarize Data to observe Patterns
df_transformed.describe().toPandas().set_index("summary").transpose()

# COMMAND ----------

# DBTITLE 1,Combined Room State Summary
from pyspark.sql.functions import desc
display(df_transformed.groupBy("meter_state").count().sort(desc("count")))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <h2><img src="/files/tables/infinitypower_small-ad799.png">Data Modeling</h2>

# COMMAND ----------

# DBTITLE 1,Required ML Libs
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler, IndexToString, StringIndexer
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# COMMAND ----------

# DBTITLE 1,Initialize MLflow Settings
import mlflow
mlflow.set_experiment("/Users/joel.thomas@databricks.com/demo-ml")

# COMMAND ----------

(train_data, test_data) = df_transformed.randomSplit([0.7, 0.3])

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <h2><img src="/files/tables/infinitypower_small-ad799.png">Model Pipeline</h2>
# MAGIC 
# MAGIC ### Model -> Tune -> Evaluate -> MLflow

# COMMAND ----------

# Identify and index labels that could be fit through classification pipeline
labelIndexer = StringIndexer(inputCol="meter_state", outputCol="indexedLabel").fit(df_transformed)

# Incorporate all input fields as vector for classificaion pipeline
assembler = VectorAssembler(inputCols=['global_active_power','global_reactive_power','voltage'], outputCol="features_assembler").setHandleInvalid("skip")

# Scale input fields using standard scale
scaler = StandardScaler(inputCol="features_assembler", outputCol="features")

# Convert/Lookup prediction label index to actual label
labelConverter = IndexToString(inputCol="prediction", outputCol="predictedLabel", labels=labelIndexer.labels)


# COMMAND ----------

def classificationModel(stages, params, train, test):
  pipeline = Pipeline(stages=stages)
  
  with mlflow.start_run(run_name="InfinityPower - ARI") as ml_run:
    for k,v in params.items():
      mlflow.log_param(k, v)
      
    mlflow.set_tag("state", "dev")
      
    model = pipeline.fit(train)
    predictions = model.transform(test)

    evaluator = MulticlassClassificationEvaluator(
                labelCol="indexedLabel", predictionCol="prediction", metricName="accuracy")
    accuracy = evaluator.evaluate(predictions)
    predictions.select("predictedLabel", "meter_state").groupBy("predictedLabel", "meter_state").count().toPandas().to_pickle("confusion_matrix.pkl")
    
    mlflow.log_metric("accuracy", accuracy)
    mlflow.log_artifact("confusion_matrix.pkl")
    
    print("Documented with MLflow Run id %s" % ml_run.info.run_uuid)
  
  return predictions, accuracy, ml_run.info

# COMMAND ----------

numTreesList = [10, 25, 50]
maxDepthList = [5, 10, 20]
# numTreesList = [10]
# maxDepthList = [5]
for numTrees, maxDepth in [(numTrees,maxDepth) for numTrees in numTreesList for maxDepth in maxDepthList]:
  params = {"numTrees":numTrees, "maxDepth":maxDepth, "model": "RandomForest"}
  rf = RandomForestClassifier(labelCol="indexedLabel", featuresCol="features", numTrees=numTrees, maxDepth=maxDepth)
  predictions, accuracy, ml_run_info = classificationModel([labelIndexer, assembler, scaler, rf, labelConverter], params, train_data, test_data)
  print("Trees: %s, Depth: %s, Accuracy: %s\n" % (numTrees, maxDepth, accuracy))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <h2><img src="/files/tables/infinitypower_small-ad799.png">Retrieve Data from MLflow</h2>

# COMMAND ----------

runid = ml_run_info.run_uuid
mlflowclient = mlflow.tracking.MlflowClient()

# COMMAND ----------

mlflowclient.get_run(runid).to_dictionary()["data"]["params"]

# COMMAND ----------

mlflowclient.get_run(runid).to_dictionary()["data"]["metrics"]

# COMMAND ----------

artifact_uri = mlflowclient.get_run(runid).to_dictionary()["info"]["artifact_uri"]
confusion_matrix_uri = "/" + artifact_uri.replace(":","") + "/confusion_matrix.pkl"
confusion_matrix_uri

# COMMAND ----------

import pandas as pd
import numpy as np
confmat = pd.read_pickle(confusion_matrix_uri)

# COMMAND ----------

pd.set_option('display.width', 1000)

# COMMAND ----------

pd.pivot_table(confmat, values="count", index=["predictedLabel"], columns=["meter_state"], aggfunc=np.sum, fill_value=0)

# COMMAND ----------

