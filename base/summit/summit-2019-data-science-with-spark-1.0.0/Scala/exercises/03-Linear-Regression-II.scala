// Databricks notebook source
// MAGIC 
// MAGIC %md-sandbox
// MAGIC 
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC # Linear Regression: Improving our model
// MAGIC 
// MAGIC In this notebook we will be adding additional features to our model, as well as discuss how to handle categorical features.
// MAGIC 
// MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lesson you:<br>
// MAGIC  - One Hot Encode categorical variables
// MAGIC  - Use the Pipeline API
// MAGIC  - Save and load models

// COMMAND ----------

// MAGIC %md
// MAGIC ## Machine Learning Pipelines
// MAGIC 
// MAGIC In this notebook, we will perform several feature engineering steps prior to building our model. The best practice for this is to use an [ML Pipeline](https://spark.apache.org/docs/latest/ml-pipeline.html).
// MAGIC 
// MAGIC ### Main Concepts in ML Pipelines
// MAGIC 
// MAGIC Before we get started building our model Pipeline, it is important to make sure we have a few concepts clear:
// MAGIC 
// MAGIC - `DataFrame`
// MAGIC    - the data structure for the ML dataset
// MAGIC    - typically will hold the final input as a column of type [`Vector`](https://spark.apache.org/docs/latest/mllib-data-types.html#local-vector).
// MAGIC - `Estimator`
// MAGIC    - defines a `.fit` method
// MAGIC    - is fit on a `DataFrame` to produce a `Transformer`
// MAGIC    - a learning algorithm is an `Estimator` that trains on a `DataFrame` and produces a model (`Transformer`)
// MAGIC - `Transformer`
// MAGIC    - defines a `.transform` method
// MAGIC    - transforms one DataFrame into another DataFrame
// MAGIC    - *an ML Model is Transformer which transforms a feature DataFrame to produce predictions*
// MAGIC - `Pipeline`
// MAGIC    - chains multiple `Estimators` and `Transformers` to specify an ML workflow
// MAGIC - `Parameter`
// MAGIC    - an attribute specifying the way in which a learning algorithm learns or a model produces new data
// MAGIC    - e.g. the number of means to identify in `KMeans`, the amount of regularization to apply in a `LogisticRegression`

// COMMAND ----------

// MAGIC %run "./Includes/Classroom-Setup"

// COMMAND ----------

val filePath = "mnt/training/airbnb/sf-listings/sf-listings-2018-12-06-clean.parquet/"
val airbnbDF = spark.read.parquet(filePath)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Train/Test Split
// MAGIC 
// MAGIC Let's use the same 80/20 split with the same seed as the previous notebook so we can compare our results apples to apples (unless you changed the cluster config!)

// COMMAND ----------

val Array(trainDF, testDF) = airbnbDF.randomSplit(Array(.8, .2), seed=42)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Encoding Categorical Features
// MAGIC 
// MAGIC There are a few ways to handle categorical features:
// MAGIC * Assign them a numeric value
// MAGIC * Use a One-Hot Encoding (also known as creating "dummy" variables)
// MAGIC * Generate embeddings (mainly used for textual data)
// MAGIC 
// MAGIC Here, we are going to apply a One-Hot Encoding (OHE) to our categorical variables. Spark doesn't have a `get_dummies`  function as in Pandas. Applying an OHE is a two step process. 
// MAGIC 
// MAGIC First, we use a `StringIndexer` to map a string column of labels to an ML column of label indices 
// MAGIC [[Python]](https://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.feature.StringIndexer)/
// MAGIC [[Scala]](https://spark.apache.org/docs/latest/api/scala/#org.apache.spark.ml.feature.StringIndexer). `StringIndexer` is an estimator.
// MAGIC 
// MAGIC Then, we can apply a `OneHotEncoderEstimator` to the output of each string indexed column
// MAGIC [[Python]](https://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.feature.OneHotEncoderEstimator)/[[Scala]](https://spark.apache.org/docs/latest/api/scala/#org.apache.spark.ml.feature.OneHotEncoderEstimator).

// COMMAND ----------

// MAGIC %md
// MAGIC ### Preparing an ML Pipeline
// MAGIC 
// MAGIC As previously discussed, we are going to be using the Estimators and Transformers in this notebook to build an ML Pipeline. We will not instantiate this `Pipeline` until we have defined all of the stages of the pipeline.

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC 
// MAGIC As we work toward this, we will store the stages in a series of intermediate `ArrayBuffer` objects. In this cell, we define the `ArrayBuffer[PipelineStage]`, `stages`. We then add a `StringIndexer` and `OneHotEncoderEstimator` for each categorical feature.

// COMMAND ----------

import org.apache.spark.ml.feature.{OneHotEncoderEstimator, StringIndexer}
import org.apache.spark.ml.PipelineStage
import scala.collection.mutable.ArrayBuffer

val categoricalColumns = for ((field, dataType) <- trainDF.dtypes if (dataType == "StringType")) yield field
val stages = ArrayBuffer[PipelineStage]()
for (categoricalCol <- categoricalColumns){
    val stringIndexer = new StringIndexer()
                            .setInputCol(categoricalCol)
                            .setOutputCol(categoricalCol + "Index")
                            .setHandleInvalid("skip")
    val ohe = new OneHotEncoderEstimator()
                    .setInputCols(Array(stringIndexer.getOutputCol))
                    .setOutputCols(Array(categoricalCol + "OHE"))
    stages += stringIndexer
    stages += ohe
}
println(stages)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Vector Assembler
// MAGIC 
// MAGIC As previously noted, SparkML models require features as a column of type `Vector`. For this reason, the penultimate step in a pipeline is typically to use a `VectorAssembler`

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC 
// MAGIC In this cell, we define a new `ArrayBuffer[PipelineStage]`, `stagesWithAssembler`. We then add a `VectorAssembler`.

// COMMAND ----------

import org.apache.spark.ml.feature.VectorAssembler

val oheCols = for (c <- categoricalColumns ) yield c + "OHE"  
val numericCols = for ((field, dataType) <- trainDF.dtypes if (dataType == "DoubleType") & (field!="price")) yield field 
val assemblerInputs = oheCols ++ numericCols
val assembler = new VectorAssembler()
                    .setInputCols(assemblerInputs)
                    .setOutputCol("features")

val stagesWithAssembler = stages.clone
stagesWithAssembler += assembler

// COMMAND ----------

// MAGIC %md
// MAGIC ## Linear Regression
// MAGIC 
// MAGIC Now that we have all of our features, let's build a linear regression model.

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC 
// MAGIC First, we define a new (and final) `ArrayBuffer[PipelineStage]`, `stagesComplete`. To this, we add our SparkML model, `LinearRegression`.

// COMMAND ----------

import org.apache.spark.ml.regression.LinearRegression
val lr = new LinearRegression()
            .setLabelCol("price")
            .setFeaturesCol("features")

val stagesComplete = stagesWithAssembler.clone
stagesComplete += lr

// COMMAND ----------

// MAGIC %md
// MAGIC ## Pipeline
// MAGIC 
// MAGIC At this point, we have assembled all of the stages in our pipeline. Let's put all these stages in a `Pipeline` object. Remember, a `Pipeline` is a way of organizing all of our transformers and estimators 
// MAGIC [Python](https://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.Pipeline)/
// MAGIC [Scala](https://spark.apache.org/docs/latest/api/scala/#org.apache.spark.ml.Pipeline).
// MAGIC 
// MAGIC This way, we don't have to worry about remembering the same ordering of transformations to apply to our test dataset.

// COMMAND ----------

import org.apache.spark.ml.Pipeline

val pipeline = new Pipeline()
  .setStages(stagesComplete.toArray)

val pipelineModel = pipeline.fit(trainDF)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Saving Models
// MAGIC 
// MAGIC We can save our models to persistent storage (e.g. DBFS) in case our cluster goes down so we don't have to recompute our results.

// COMMAND ----------

val pipelinePath = userhome + "/linear_regression/pipeline_model"
pipelineModel.write.overwrite().save(pipelinePath)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Loading models
// MAGIC 
// MAGIC When you load in models, you need to know the type of model you are loading back in (was it a linear regression or logistic regression model?).
// MAGIC 
// MAGIC For this reason, we recommend you always put your transformers/estimators into a Pipeline, so you can always load the generic PipelineModel back in.

// COMMAND ----------

import org.apache.spark.ml.PipelineModel

val savedPipelineModel = PipelineModel.load(pipelinePath)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Apply model to test set

// COMMAND ----------

val predDF = savedPipelineModel.transform(testDF)

display(predDF.select("features", "price", "prediction"))

// COMMAND ----------

// MAGIC %md
// MAGIC ## Evaluate model
// MAGIC 
// MAGIC How is our R2 doing?

// COMMAND ----------

import org.apache.spark.ml.evaluation.RegressionEvaluator

val regressionEvaluator = new RegressionEvaluator().setPredictionCol("prediction").setLabelCol("price").setMetricName("rmse")

val rmse = regressionEvaluator.evaluate(predDF)
val r2 = regressionEvaluator.setMetricName("r2").evaluate(predDF)
println(s"RMSE is $rmse")
println(s"R2 is $r2")
println("*-"*80)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Distributed Setting
// MAGIC 
// MAGIC If you are interested in how linear regression is implemented in the distributed setting and bottlenecks, check out these lecture slides:
// MAGIC * [distributed-linear-regression-1](https://files.training.databricks.com/static/docs/distributed-linear-regression-1.pdf)
// MAGIC * [distributed-linear-regression-2](https://files.training.databricks.com/static/docs/distributed-linear-regression-2.pdf)

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>