// Databricks notebook source
// MAGIC 
// MAGIC %md-sandbox
// MAGIC 
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC # Decision Trees
// MAGIC 
// MAGIC In the previous notebook, you were working with the parametric model, Linear Regression. We could do some more hyperparameter tuning with the linear regression model, but we're going to try tree based methods and see if our performance improves.
// MAGIC 
// MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lesson you:<br>
// MAGIC  - Identify the differences between single node and distributed decision tree implementations
// MAGIC  - Get the feature importance
// MAGIC  - Examine common pitfalls of decision trees

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### Decision Making
// MAGIC 
// MAGIC Decision trees in some ways mirror the way a human might make a decision.
// MAGIC 
// MAGIC ![](https://brookewenig.github.io/img/DecisionTrees/ml-decision-tree-job-offer.svg)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Creating Trees
// MAGIC 
// MAGIC Trees are created by splitting on data so that each split gives the maximum possible information gain.
// MAGIC 
// MAGIC These splits are greedy.
// MAGIC 
// MAGIC Splits are made until some stopping criteria is reached (maximum tree depth, minimum information gain).
// MAGIC 
// MAGIC ![](https://brookewenig.github.io/img/DecisionTrees/ml-splitspace.svg)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### Depth
// MAGIC 
// MAGIC The **depth** of the tree is the length of the longest path from root to leaf.
// MAGIC 
// MAGIC - if too deep, the tree can be overfit
// MAGIC - if too shallow, the tree can be underfit
// MAGIC 
// MAGIC 
// MAGIC ![](https://brookewenig.github.io/img/DecisionTrees/ml-decision-tree.svg)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Underfitting Vs. Overfitting
// MAGIC 
// MAGIC ![](https://brookewenig.github.io/img/DecisionTrees/underfitting-overfitting.png)

// COMMAND ----------

// MAGIC %run "./Includes/Classroom-Setup"

// COMMAND ----------

val filePath = "mnt/training/airbnb/sf-listings/sf-listings-2018-12-06-clean.parquet/"
val airbnbDF = spark.read.parquet(filePath)
val Array(trainDF, testDF) = airbnbDF.randomSplit(Array(.8, .2), seed=42)

// COMMAND ----------

// MAGIC %md
// MAGIC ## How to Handle Categorical Features?
// MAGIC 
// MAGIC We saw in the previous notebook that we can use StringIndexer/OneHotEncoderEstimator/VectorAssembler or RFormula.
// MAGIC 
// MAGIC **However, for decision trees, and in particular, random forests, we should not OHE our variables.**
// MAGIC 
// MAGIC Why is that? Well, how the splits are made is different (you can see this when you visualize the tree) and the feature importance scores are not correct.
// MAGIC 
// MAGIC For random forests (which we will discuss shortly), the result can change dramatically. So instead of using RFormula, we are going to use just StringIndexer/VectorAssembler.

// COMMAND ----------

import org.apache.spark.ml.PipelineStage
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.ml.feature.StringIndexer

val categoricalColumns = for ((field, dataType) <- trainDF.dtypes if (dataType == "StringType")) yield field
val stages = ArrayBuffer[PipelineStage]()
for (categoricalCol <- categoricalColumns){
    val stringIndexer = new StringIndexer()
                            .setInputCol(categoricalCol)
                            .setOutputCol(categoricalCol + "Index")
                            .setHandleInvalid("skip")
    stages += stringIndexer
}
println(stages)
println("*-"*80)

// COMMAND ----------

// MAGIC %md
// MAGIC ## VectorAssembler

// COMMAND ----------

import org.apache.spark.ml.feature.VectorAssembler

val indexCols = for (c <- categoricalColumns ) yield c+"Index" 
val numericCols = for ((field, dataType) <- trainDF.dtypes if (dataType == "DoubleType") & (field!="price")) yield field 
val assemblerInputs = indexCols ++ numericCols
val assembler = new VectorAssembler()
                    .setInputCols(assemblerInputs)
                    .setOutputCol("features")

val stagesWithAssembler = stages.clone
stagesWithAssembler += assembler

// COMMAND ----------

// MAGIC %md
// MAGIC ## Decision Tree
// MAGIC 
// MAGIC Now let's build a `DecisionTreeRegressor` with the default hyperparameters 
// MAGIC [Python](https://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.regression.DecisionTreeRegressor)/
// MAGIC [Scala](https://spark.apache.org/docs/latest/api/scala/#org.apache.spark.ml.regression.DecisionTreeRegressor).

// COMMAND ----------

import org.apache.spark.ml.regression.DecisionTreeRegressor

val dt = new DecisionTreeRegressor().setLabelCol("price")

val stagesComplete = stagesWithAssembler.clone
stagesComplete += dt

// COMMAND ----------

// MAGIC %md
// MAGIC ## Fit Pipeline

// COMMAND ----------

import org.apache.spark.ml.Pipeline

val pipeline = new Pipeline()
                  .setStages(stagesComplete.toArray)

// Uncomment to perform fit
// val pipelineModel = pipeline.fit(trainDF) 

// COMMAND ----------

// MAGIC %md
// MAGIC ## maxBins
// MAGIC 
// MAGIC What is this parameter [maxBins](https://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.regression.DecisionTreeRegressor.maxBins)? 

// COMMAND ----------

// MAGIC %md
// MAGIC In Spark, data is partitioned by row. So when it needs to make a split, each worker has to compute summary statistics for every feature for  each split point. Then these summary statistics have to be aggregated (via tree reduce) for a split to be made.
// MAGIC 
// MAGIC Think about it: What if worker 1 had the value `32` but none of the others had it. How could you communicate how good of a split that would be? So, Spark has a `maxBins` parameter for discretizing continuous variables into buckets, but the number of buckets has to be as large as the number of categorical variables.

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC <img src="https://brookewenig.github.io/img/DecisionTrees/DistDecisionTrees.png" width=400px>

// COMMAND ----------

// MAGIC %md
// MAGIC Let's go ahead and increase maxBins to `40`.

// COMMAND ----------

dt.setMaxBins(40)

// COMMAND ----------

// MAGIC %md
// MAGIC Take two.

// COMMAND ----------

import org.apache.spark.ml.Pipeline

val pipeline = new Pipeline()
                   .setStages(stagesComplete.toArray)

val pipelineModel = pipeline.fit(trainDF)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Feature Importance
// MAGIC 
// MAGIC Let's go ahead and get the fitted decision tree model, and look at the feature importance scores.

// COMMAND ----------

val dtModel = pipelineModel.stages(pipelineModel.stages.length-1).asInstanceOf[org.apache.spark.ml.regression.DecisionTreeRegressionModel]

display(dtModel)

// COMMAND ----------

dtModel.featureImportances

// COMMAND ----------

// MAGIC %md
// MAGIC ## Interpreting Feature Importance
// MAGIC 
// MAGIC Hmmm... it's a little hard to know what feature 4 vs 11 is. Given that the feature importance scores are "small data", let's use Pandas to help us recover the original column names.

// COMMAND ----------

val data = assembler.getInputCols.zip(dtModel.featureImportances.toArray)
val columns = Array("feature","Importance")
val df = spark.createDataFrame(data).toDF(columns: _*)

display(df)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Why so few features are non-zero?
// MAGIC 
// MAGIC With SparkML, the default `maxDepth` is 5, so there are only a few features we could consider (we can also split on the same feature many times at different split points).
// MAGIC 
// MAGIC Let's use a Databricks widget to get the top-K features.

// COMMAND ----------

dbutils.widgets.text("topK", "5")
val topK = dbutils.widgets.get("topK").toInt

val data = df.orderBy($"importance".desc)
val sortedData = data.select("feature").collect.map(_.toSeq).flatten
for (i <- 0 to topK - 1) println(sortedData(i))

println("*-"*80)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Scale Invariant
// MAGIC 
// MAGIC With decision trees, the scale of the features does not matter. For example, it will split 1/3 of the data if that split point is 100 or if it is normalized to be .33. The only thing that matters is how many data points fall left and right of that split point - not the absolute value of the split point.
// MAGIC 
// MAGIC This is not true for linear regression, and the default in Spark is to standardize first. Think about it: If you measure shoe sizes in American vs European sizing, the corresponding weight of those features will be very different even those those measures represent the same thing: the size of a person's foot!

// COMMAND ----------

// MAGIC %md
// MAGIC ## Apply model to test set

// COMMAND ----------

import org.apache.spark.sql.functions._

val predDF = pipelineModel.transform(testDF)

display(predDF.select("features", "price", "prediction").orderBy(desc("price")))

// COMMAND ----------

// MAGIC %md
// MAGIC ## Pitfall
// MAGIC 
// MAGIC What if we get a massive Airbnb rental? It was 20 bedrooms and 20 bathrooms. What will a decision tree predict?
// MAGIC 
// MAGIC It turns out decision trees cannot predict any values larger than they were trained on. The max value in our training set was $10,000, so we can't predict any values larger than that.

// COMMAND ----------

import org.apache.spark.ml.evaluation.RegressionEvaluator

val regressionEvaluator = new RegressionEvaluator()
                              .setPredictionCol("prediction")
                              .setLabelCol("price")
                              .setMetricName("rmse")

val rmse = regressionEvaluator.evaluate(predDF)
val r2 = regressionEvaluator.setMetricName("r2").evaluate(predDF)
println(s"RMSE is $rmse")
println(s"R2 is $r2")
println("*-"*80)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Uh oh!
// MAGIC 
// MAGIC This model is worse than the linear regression model.
// MAGIC 
// MAGIC In the next few notebooks, let's look at hyperparamter tuning and ensemble models to improve upon the performance of our singular decision tree.

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>