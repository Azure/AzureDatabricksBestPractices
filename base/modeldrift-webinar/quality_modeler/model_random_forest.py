# Databricks notebook source
# DBTITLE 1,Required ML Libs
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler, IndexToString, StringIndexer
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# COMMAND ----------

def randomforest_model(stages, params, train, test):
  pipeline = Pipeline(stages=stages)
  
  with mlflow.start_run(run_name=mlflow_exp_name) as ml_run:
    for k,v in params.items():
      mlflow.log_param(k, v)
      
    mlflow.set_tag("state", "dev")
      
    model = pipeline.fit(train)
    predictions = model.transform(test)

    evaluator = MulticlassClassificationEvaluator(
                labelCol="indexedLabel", predictionCol="prediction", metricName="accuracy")
    accuracy = evaluator.evaluate(predictions)
    predictions.select("predicted_quality", "quality").groupBy("predicted_quality", "quality").count().toPandas().to_pickle("confusion_matrix.pkl")
    
    mlflow.log_metric("accuracy", accuracy)
    mlflow.log_artifact("confusion_matrix.pkl")
    mlflow.spark.log_model(model, "spark-model")
    
    print("Documented with MLflow Run id %s" % ml_run.info.run_uuid)
  
  return model, predictions, accuracy, ml_run.info

# COMMAND ----------

def run_randomforest(df):
  
  (train_data, test_data) = df.randomSplit([0.8, 0.2], 1234)
  
  labelIndexer = StringIndexer(inputCol="quality", outputCol="indexedLabel").fit(df)  # Identify and index labels that could be fit through classification pipeline
  assembler = VectorAssembler(inputCols=['temp', 'pressure', 'duration'], outputCol="features").setHandleInvalid("skip")  # Incorporate all input fields as vector for classificaion pipeline
  # scaler = StandardScaler(inputCol="features_assembler", outputCol="features")  # Scale input fields using standard scale
  labelConverter = IndexToString(inputCol="prediction", outputCol="predicted_quality", labels=labelIndexer.labels)  # Convert/Lookup prediction label index to actual label
  
  numTreesList = [10, 25, 50]
  maxDepthList = [3, 10, 5]
  
  for numTrees, maxDepth in [(numTrees,maxDepth) for numTrees in numTreesList for maxDepth in maxDepthList]:
    params = {"numTrees":numTrees, "maxDepth":maxDepth, "model": "RandomForest"}
    params.update({"model_data_date":model_data_date['start_date']+ ' - ' + model_data_date['end_date']})
    if run_exists(mlflow_exp_id, params):
      print("Trees: %s, Depth: %s, Run already exists"% (numTrees, maxDepth))
    else:
      rf = RandomForestClassifier(labelCol="indexedLabel", featuresCol="features", numTrees=numTrees, maxDepth=maxDepth, seed=512)
      model, predictions, accuracy, ml_run_info = randomforest_model([labelIndexer, assembler, rf, labelConverter], params, train_data, test_data)
      print("Trees: %s, Depth: %s, Accuracy: %s\n" % (numTrees, maxDepth, accuracy))

  mlflow_search_query = "params.model = 'RandomForest' and params.model_data_date = '"+ model_data_date['start_date']+ ' - ' + model_data_date['end_date']+"'"
  
  return best_run(mlflow_exp_id, mlflow_search_query)