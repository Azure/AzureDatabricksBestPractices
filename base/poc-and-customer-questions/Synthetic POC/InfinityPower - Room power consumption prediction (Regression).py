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
# MAGIC     <h1><img src="/files/tables/infinitypower_small-ad799.png"> InfinityPower - Room Power Consumption Prediction</h1>
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
# MAGIC To create a model that can be used to identify how much power each room is consuming based on only usingthe global power.   
# MAGIC 
# MAGIC Data available:  
# MAGIC There are sub meter readings for 3 rooms (kitchen, laundry, heater+hvac) and from global power we can calculate the miscellaneous usage of power.  

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

# DBTITLE 1,Numerical Data Correlations - Visual Understanding
import pandas as pd
import matplotlib
import matplotlib.pyplot as plt

numeric_features = [i.name for i in df_transformed.schema.fields if "DoubleType" in str(i.dataType)]
sampled_data = df_transformed.select(numeric_features).sample(False, 0.10).toPandas()

axs = pd.scatter_matrix(sampled_data, alpha=0.2,  figsize=(14, 14))
n = len(sampled_data.columns)
for i in range(n):
    v = axs[i, 0]
    v.yaxis.label.set_rotation(0)
    v.yaxis.label.set_ha('right')
    v.yaxis.label.set_size(8)
    h = axs[n-1, i]
    h.xaxis.label.set_rotation(90)
    h.xaxis.label.set_size(8)
display(plt.show())

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <h2><img src="/files/tables/infinitypower_small-ad799.png">Data Modeling</h2>

# COMMAND ----------

# DBTITLE 1,Required ML Libs
from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer
from pyspark.ml.regression import LinearRegression, RandomForestRegressor, GBTRegressor
from pyspark.ml.regression import GeneralizedLinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml import Pipeline
from dbmlModelExport import ModelExport
from mlflow import spark as mlflow_spark 

# COMMAND ----------

# DBTITLE 1,Initialize MLflow Settings
import mlflow
import mlflow.mleap
# from mlflow import spark as mlflow_spark
import mlflow.spark
mlflow.set_experiment("/Users/joel.thomas@databricks.com/temp/demo-ml")

# COMMAND ----------

(train_data, test_data) = df_transformed.randomSplit([0.7, 0.3])

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <h2><img src="/files/tables/infinitypower_small-ad799.png">Model Pipeline</h2>
# MAGIC 
# MAGIC ### Model -> Tune -> Evaluate -> MLflow

# COMMAND ----------

labelList = ["sub_metering_1", "sub_metering_2", "sub_metering_3", "non_sub_metered"]

# COMMAND ----------

assembler = VectorAssembler(inputCols=['global_active_power','global_reactive_power','voltage'], outputCol="features_assembler").setHandleInvalid("skip")
scaler = StandardScaler(inputCol="features_assembler", outputCol="features")

# COMMAND ----------

def plot_prediction_target(predictions, label, image_name):
  global image
  fig = plt.figure(1)
  ax = plt.gca()
  predictions.select("prediction", label).toPandas().plot(x=label, y='prediction', kind='scatter', title='Actual Vs Predicted', ax=ax)
  
  image = fig
  fig.savefig(image_name)
  plt.close(fig)
  
  return image  

def regressionModel(stages, params, train, test, label):
  pipeline = Pipeline(stages=stages)
  
  with mlflow.start_run(run_name="InfinityPower - RPC") as ml_run:
    for k,v in params.items():
      mlflow.log_param(k, v)
      
    mlflow.set_tag("state", "dev")
      
    model = pipeline.fit(train)
    predictions = model.transform(test)
    
    evaluator = RegressionEvaluator()
    evaluator.setLabelCol(label)
    evaluator.setPredictionCol("prediction")

    rmse = evaluator.setMetricName("rmse").evaluate(predictions)
    r2 = evaluator.setMetricName("r2").evaluate(predictions)
    
    mlflow.log_metric("rmse", rmse)
    mlflow.log_metric("r2", r2)
    
    image_name = "regression-validation.png"
    image = plot_prediction_target(predictions, label, image_name)
    
    mlflow.log_artifact("regression-validation.png")
    
#     dbutils.fs.rm("/tmp/syn-poc", recurse=True)
#     ModelExport.exportModel(model, "/tmp/syn-poc")
#     mlflow.log_artifacts("/tmp/syn-poc")

#     mlflow.spark.save_model(model, "spark-model")
#     mlflow.log_artifacts("spark-model")
    
#     mlflow.mleap.log_model(model, train, "model")

    mlflow.spark.log_model(model, "model")
    
    print("Documented with MLflow Run id %s" % ml_run.info.run_uuid)
  
  return predictions, rmse, r2, ml_run.info

# COMMAND ----------

# numTreesList = [10, 25, 50]
# maxDepthList = [5, 10, 20]
numTreesList = [10]
maxDepthList = [5]
labelList = ["sub_metering_1"]
for label in labelList:
  for numTrees, maxDepth in [(numTrees,maxDepth) for numTrees in numTreesList for maxDepth in maxDepthList]:
    params = {"numTrees":numTrees, "maxDepth":maxDepth, "model": "RandomForest"}
    rf = RandomForestRegressor(labelCol=label, featuresCol="features", numTrees=numTrees, maxDepth=maxDepth)
    predictions, rmse, r2, ml_run_info = regressionModel([assembler, scaler, rf], params, train_data, test_data, label)
    print("Trees: %s, Depth: %s, rmse: %s, r2: %s\n" % (numTrees, maxDepth, rmse, r2))

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

mlflowclient.get_run(runid).to_dictionary()

# COMMAND ----------

artifact_uri = mlflowclient.get_run(runid).to_dictionary()["info"]["artifact_uri"]
image_uri = "/" + artifact_uri.replace(":","") + "/regression-validation.png"
image_uri

# COMMAND ----------

import matplotlib.pyplot as plt
import matplotlib.image as mpimg

img=mpimg.imread("/dbfs/databricks/mlflow/1711410111407627/5d3c37a2612543288726025782465c32/artifacts/regression-validation.png")

# COMMAND ----------

display(plt.imshow(img))

# COMMAND ----------

imgplot = plt.imshow(img)

# COMMAND ----------

plt.imshow(img)

# COMMAND ----------

  display("dbfs:/databricks/mlflow/1711410111407627/5d3c37a2612543288726025782465c32/artifacts/regression-validation.png")

# COMMAND ----------

display(image)

# COMMAND ----------



# COMMAND ----------

def plot_prediction_target(predictions, label_col, image_name):
  global image
  fig = plt.figure(1)
  ax = plt.gca()
  predictions.select("prediction", label_col).toPandas().plot(x=labelCol, y='prediction', kind='scatter', title='Prediction vs Target', ax=ax)
  
  image = fig
  fig.savefig(image_name)
  plt.close(fig)
  
  return image  

def cvRegressionEval(model, test_data, label_col, prediction_col):
  predictions = model.transform(test_data)
  evaluator = RegressionEvaluator()
  evaluator.setLabelCol(label_col)
  evaluator.setPredictionCol(prediction_col)

  rmse = evaluator.setMetricName("rmse").evaluate(predictions)
  r2 = evaluator.setMetricName("r2").evaluate(predictions)
  
  image_name = "demo-regression-validation.png"
  image = plot_prediction_target(predictions, label_col, image_name)
  
  return {"rmse":rmse, "r2":r2}

def cvRegressionModel(stages, param_grid, train_data, test_data, label_col, prediction_col="prediction"):
  pipeline = Pipeline(stages=stages)
  evaluator = RegressionEvaluator(
      labelCol=label_col, predictionCol=prediction_col, metricName="rmse")
  cv_folds = 3

  crossval = CrossValidator(
      estimator=pipeline,
      estimatorParamMaps=param_grid,
      evaluator=evaluator,
      numFolds=cv_folds)

  with mlflow.start_run() as ml_run:
    cvRegModel = crossval.fit(train_data)
    
    mlflow.spark.log_model(cvRegModel, "spark-model")
    
    metrics = list(zip(cvmodel.getEstimatorParamMaps(), cvRegModel.avgMetrics))
    bestMetric = max(map(lambda x: x[1], metrics))
    depth, trees = [i for i in results if i[1]== bestMetric][0][0].values()
      
    mlflow.log_param("depth", depth)
    mlflow.log_param("trees", trees)
    mlflow.log_param("cv_folds", cv_folds)
    
    mlflow.log_metric("train_rmse", bestMetric)
    
    test_metrics = cvRegressionEval(cvRegModel, test_data, label_col, prediction_col)
    
    for k,v in test_metrics.items():
      mlflow.log_metric(k, v)
    
    mlflow.log_artifact("demo-regression-validation.png")
    
    print("Inside MLflow Run with id %s" % ml_run.info.run_uuid)
  
    return ml_run.info



# COMMAND ----------



# COMMAND ----------

assembler = VectorAssembler(inputCols=['global_active_power','global_reactive_power','voltage'], outputCol="features_assembler").setHandleInvalid("skip")
scaler = StandardScaler(inputCol="features_assembler", outputCol="features")
lr = (LinearRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8)
      .setLabelCol(label)
      .setFeaturesCol("features"))

rf = (RandomForestRegressor(numTrees = 20)
      .setLabelCol(label)
      .setFeaturesCol("features"))

gbt = (GBTRegressor(maxIter=10)
      .setLabelCol(label)
      .setFeaturesCol("features"))


run_info_1 = cvRegressionModel([assembler, scaler, rf], rf_param_grid, train_data, test_data, label, prediction_col="prediction")

# COMMAND ----------

pipeline = Pipeline(stages=[assembler, scaler, rf])
paramGrid = ParamGridBuilder().addGrid(rf.numTrees, [10, 30]).addGrid(rf.maxDepth, [10, 20]).build()
evaluator = RegressionEvaluator(
    labelCol=labelCol, predictionCol="prediction", metricName="rmse")

crossval = CrossValidator(
    estimator=pipeline,
    estimatorParamMaps=paramGrid,
    evaluator=evaluator,
    numFolds=3)

cvmodel = crossval.fit(train_df)

# COMMAND ----------

bestMetric = max(map(lambda x: x[1], results))
numTrees, rmse = [i for i in results if i[1]== bestMetric][0][0].values()
print("Depth: %s, Trees: %s" % (numTrees, rmse))

# COMMAND ----------

# Zip the two lists together
results = list(zip(cvmodel.getEstimatorParamMaps(), cvmodel.avgMetrics))

# And pretty print 'em
for x in results:
  numTrees, rmse = list(x[0].values())
  print("Depth: %s, Trees: %s\nAverage: %s\n" % (numTrees, rmse, x[1]))
  
print("-"*80)

# COMMAND ----------



# COMMAND ----------

display(plot_prediction_target(cvPredictions, labelCol))

# COMMAND ----------

display(image)

# COMMAND ----------

fig, axs = plt.subplots(1,1,figsize=(3,3))
predictions.select("prediction", labelCol).toPandas().plot(x=labelCol, y='prediction', kind='scatter', title='Prediction vs Target', ax=axs)
display(fig)

# COMMAND ----------



# COMMAND ----------

rf = (RandomForestRegressor(numTrees = 20)
      .setLabelCol(label_col)
      .setFeaturesCol("features"))

# COMMAND ----------

rfPipeline = Pipeline()
rfPipeline.setStages([assembler, rf])
rfPipelineModel = rfPipeline.fit(train_df)
predictedDF = rfPipelineModel.transform(test_df)
evaluator = RegressionEvaluator()
print(evaluator.explainParams())

# COMMAND ----------

evaluator.setLabelCol(label_col)
evaluator.setPredictionCol("prediction")

metricName = evaluator.getMetricName()
metricVal = evaluator.evaluate(predictedDF)

print("{}: {}".format(metricName, metricVal))

# COMMAND ----------

printEval(predictedDF)

# COMMAND ----------

fig, axs = plt.subplots(1,1,figsize=(3,3))
predictedDF.select("prediction", label_col).toPandas().plot(x=label_col, y='prediction', kind='scatter', title='Prediction vs Target', ax=axs)
display(fig)

# COMMAND ----------

