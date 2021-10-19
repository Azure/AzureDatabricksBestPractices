// Databricks notebook source
// MAGIC 
// MAGIC %md-sandbox
// MAGIC 
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC # Hyperparameter Tuning
// MAGIC 
// MAGIC Before we get to random forests, let's look at some of the options we have available to improve our decision tree model.
// MAGIC 
// MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lesson you:<br>
// MAGIC  - Tune hyperparameters using Grid Search
// MAGIC  - Optimize SparkML pipeline

// COMMAND ----------

// MAGIC %run "./Includes/Classroom-Setup"

// COMMAND ----------

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.ml.regression.DecisionTreeRegressor

val filePath = "mnt/training/airbnb/sf-listings/sf-listings-2018-12-06-clean.parquet/"
val airbnbDF = spark.read.parquet(filePath)
val Array(trainDF, testDF) = airbnbDF.randomSplit(Array(.8, .2), seed=42)

val categoricalColumns = for ((field, dataType) <- trainDF.dtypes if (dataType == "StringType")) yield field
val stages = ArrayBuffer[PipelineStage]()
for (categoricalCol <- categoricalColumns){
    val stringIndexer = new StringIndexer()
      .setInputCol(categoricalCol)
      .setOutputCol(categoricalCol + "Index")
      .setHandleInvalid("skip")
    stages += stringIndexer
}

val indexCols = for (c <- categoricalColumns ) yield c + "Index"
val numericCols = for ((field, dataType) <- trainDF.dtypes if (dataType == "DoubleType") & (field != "price")) yield field 
val assemblerInputs = indexCols ++ numericCols
val assembler = new VectorAssembler()
  .setInputCols(assemblerInputs)
  .setOutputCol("features")

stages += assembler
val dt = new DecisionTreeRegressor()
  .setLabelCol("price")
  .setMaxBins(40)

val stagesWithDT = stages.clone
stagesWithDT += dt

val pipeline = new Pipeline()
  .setStages(stagesWithDT.toArray)

// COMMAND ----------

// MAGIC %md
// MAGIC ## ParamGrid
// MAGIC 
// MAGIC There are a lot of hyperparamaters we could tune, and it would take a long time to manually configure.
// MAGIC 
// MAGIC Instead of a manual (ad-hoc) approach, let's use Spark's `ParamGridBuilder` to find the optimal hyperparameters in a more systematic approach 
// MAGIC [Python](https://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.tuning.ParamGridBuilder)/
// MAGIC [Scala](https://spark.apache.org/docs/latest/api/scala/#org.apache.spark.ml.tuning.ParamGridBuilder).
// MAGIC 
// MAGIC Let's define a grid of hyperparameters to test:
// MAGIC   - maxDepth: max depth of the decision tree (Use the values `2, 5, 10`)
// MAGIC 
// MAGIC `addGrid()` accepts the name of the parameter (e.g. `dt.maxDepth`), and a list of the possible values (e.g. `[2, 5, 10]`).

// COMMAND ----------

import org.apache.spark.ml.tuning.ParamGridBuilder

val paramGrid = (new ParamGridBuilder()
                    .addGrid(dt.maxDepth, Array(2, 5, 10))
                    .build())

// COMMAND ----------

// MAGIC %md
// MAGIC ## Cross Validation
// MAGIC 
// MAGIC We are also going to use 3-fold cross validation to identify the optimal maxDepth.
// MAGIC 
// MAGIC ![crossValidation](https://files.training.databricks.com/images/301/CrossValidation.png)
// MAGIC 
// MAGIC With 3-fold cross-validation, we train on 2/3 of the data, and evaluate with the remaining (held-out) 1/3. We repeat this process 3 times, so each fold gets the chance to act as the validation set. We then average the results of the three rounds.

// COMMAND ----------

// MAGIC %md
// MAGIC We pass in the `estimator` (pipeline), `evaluator`, and `estimatorParamMaps` to [CrossValidator](https://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.tuning.CrossValidator) so that it knows:
// MAGIC - Which model to use
// MAGIC - How to evaluate the model
// MAGIC - What hyperparamters to set for the model
// MAGIC 
// MAGIC We can also set the number of folds we want to split our data into (3), as well as setting a seed so we all have the same split in the data 
// MAGIC [Python](https://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.tuning.CrossValidator)/[Scala](https://spark.apache.org/docs/latest/api/scala/#org.apache.spark.ml.tuning.CrossValidator).

// COMMAND ----------

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.tuning.CrossValidator

val evaluator = new RegressionEvaluator()
                    .setLabelCol("price")
                    .setPredictionCol("prediction")

val cv = new CrossValidator()
 .setEstimator(pipeline)
 .setEvaluator(evaluator)
 .setEstimatorParamMaps(paramGrid)
 .setNumFolds(3)
 .setSeed(27)

// COMMAND ----------

// MAGIC %md
// MAGIC **Question**: How many models are we training right now?

// COMMAND ----------

val cvModel = cv.fit(trainDF)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Parallelism Parameter
// MAGIC 
// MAGIC Hmmm... that took a long time to run. That's because the models were being trained sequentially rather than in parellel!
// MAGIC 
// MAGIC In Spark 2.3, they introduced a [parallelism](https://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.tuning.CrossValidator.parallelism) parameter. From the docs: `the number of threads to use when running parallel algorithms (>= 1)`.
// MAGIC 
// MAGIC Let's set this value to 4 and see if we can train any faster.

// COMMAND ----------

val cvModel = cv.setParallelism(4).fit(trainDF)

// COMMAND ----------

// MAGIC %md
// MAGIC **Question**: Hmmm... that still took a long time to run. Should we put the pipeline in the cross validator, or the cross validator in the pipeline?
// MAGIC 
// MAGIC It depends if there are estimators or transformers in the pipeline. If you have things like OneHotEncodingEstimator (an estimator) in the pipeline, then you have to refit it every time if you put the entire pipeline in the cross validator.

// COMMAND ----------

val cv = new CrossValidator()
  .setEstimator(dt)
  .setEvaluator(evaluator)
  .setEstimatorParamMaps(paramGrid)
  .setNumFolds(3)
  .setParallelism(4)
  .setSeed(27)

val stagesWithCV = stages.clone
stagesWithCV.append(cv)

val pipeline = new Pipeline()
                   .setStages(stagesWithCV.toArray)

val pipelineModel = pipeline.fit(trainDF)

// COMMAND ----------

// MAGIC %md
// MAGIC ##Does Spark re-use existing results?
// MAGIC 
// MAGIC Unfortunately, Spark has no concept of "warm starts" which means if you train a model once, and want to go back and change it, you have to re-train the model entirely from scratch :(.

// COMMAND ----------

// MAGIC %md
// MAGIC Let's take a look at the model with the best hyperparameter configuration

// COMMAND ----------

cvModel.getEstimatorParamMaps.zip(cvModel.avgMetrics)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Overfitting
// MAGIC 
// MAGIC Fascinating! It appears to do better with a shallower tree instead of a deeper tree. This could be caused by overfitting!
// MAGIC 
// MAGIC Let's see how it does on the test dataset.

// COMMAND ----------

import org.apache.spark.ml.tuning.ParamGridBuilder

val predDF = pipelineModel.transform(testDF)

val regressionEvaluator = new RegressionEvaluator().setPredictionCol("prediction").setLabelCol("price").setMetricName("rmse")

val rmse = regressionEvaluator.evaluate(predDF)
val r2 = regressionEvaluator.setMetricName("r2").evaluate(predDF)
println(s"RMSE is $rmse")
println(s"R2 is $r2")
println("*-"*80)

// COMMAND ----------

// MAGIC %md
// MAGIC Alright, this is still a very bad model, but at least it was better than our v1 decision tree. We are now going to examine ensemble models of decision trees.

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>