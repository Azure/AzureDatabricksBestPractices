// Databricks notebook source
// MAGIC %md # XGBoost with Spark DataFrames
// MAGIC 
// MAGIC ![alt text](http://i.imgur.com/iFZNBVx.png "XGBoost")

// COMMAND ----------

import ml.dmlc.xgboost4j.scala.spark.{DataUtils, XGBoost,XGBoostEstimator}  
sql("drop table if exists power_plant")
case class PowerPlantTable(AT: Double, V : Double, AP : Double, RH : Double, PE : Double)
val powerPlantData = sc.textFile("dbfs:/databricks-datasets/power-plant/data/")
  .map(x => x.split("\t"))
  .filter(line => line(0) != "AT")
  .map(line => PowerPlantTable(line(0).toDouble, line(1).toDouble, line(2).toDouble, line(3).toDouble, line(4).toDouble))
  .toDF
  .write
  .saveAsTable("power_plant")

val dataset = sqlContext.table("power_plant")

// COMMAND ----------

display(dataset)

// COMMAND ----------

// MAGIC %md #### Step 3: Model creation

// COMMAND ----------

import org.apache.spark.ml.feature.VectorAssembler

val assembler =  new VectorAssembler()
  .setInputCols(Array("AT", "V", "AP", "RH"))
  .setOutputCol("features")

val vected = assembler.transform(dataset).withColumnRenamed("PE", "label").drop("AT","V","AP","RH")

val Array(split20, split80) = vected.randomSplit(Array(0.20, 0.80), 1800009193L)
val testSet = split20.cache()
val trainingSet = split80.cache()

// COMMAND ----------

// MAGIC %md Train XGBoost Model with Spark DataFrames

// COMMAND ----------

val paramMap = List(
  "eta" -> 0.3,
  "max_depth" -> 6,
  "objective" -> "reg:linear",
  "early_stopping_rounds" ->10).toMap

val xgboostModel = XGBoost.trainWithDataFrame(trainingSet, paramMap, 30, 10, useExternalMemory=true)

// COMMAND ----------

// MAGIC %md You can evaluate the XGBoost model using Evaluators from MLlib.

// COMMAND ----------

val predictions = xgboostModel.transform(testSet)
display(predictions)

// COMMAND ----------

import org.apache.spark.ml.evaluation.RegressionEvaluator

val evaluator = new RegressionEvaluator()
  .setLabelCol("label")
  .setPredictionCol("prediction")
  .setMetricName("rmse")

val rmse = evaluator.evaluate(predictions)

// COMMAND ----------

print ("Root mean squared error: " + rmse)