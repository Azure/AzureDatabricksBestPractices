# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Hyperparameter Tuning with Random Forests
# MAGIC 
# MAGIC In this lab, you will build a random forest and tune some hyperparameters of the random forest.
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lesson you:<br>
# MAGIC  - Perform grid search on a random forest
# MAGIC  - Get the feature importances across the forest
# MAGIC  - Save the model
# MAGIC  - Identify differences between Sklearn's Random Forest and SparkML's

# COMMAND ----------

# MAGIC %md
# MAGIC ### Decision Tree
# MAGIC 
# MAGIC An advantage of working with decision trees is that they are highly interpretable. As noted previously, in some ways they mirror human decision making. 
# MAGIC 
# MAGIC A disadvantange is that they tend to overfit, especially when they do not have their depth limited.
# MAGIC 
# MAGIC What would be the performance of a decision tree on its training data with no maximum depth constraint?

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bootstrap
# MAGIC 
# MAGIC A method for simulating new datasets. 
# MAGIC 
# MAGIC Take samples from the original training dataset with replacement. 
# MAGIC 
# MAGIC Repeat *n* times.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bootstrap Aggregation or Bagging
# MAGIC 
# MAGIC Aggregate over many bootstrapped sets toward reducing variance.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bagging to Improve Decision Tree Performance
# MAGIC 
# MAGIC We can train a tree on each bootstrap sample and aggregate their performance.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Random Forest
# MAGIC 
# MAGIC Random Forests extend the idea of bagging by building a tree using not only a bootstrap sample of data, but also a randomly selected subset of features.
# MAGIC 
# MAGIC ![](https://brookewenig.github.io/img/RandomForests/ml-random-forest.svg)

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Why can't we OHE?
# MAGIC 
# MAGIC **Question:** What would go wrong if we One Hot Encoded our variables before passing them into the random forest?
# MAGIC 
# MAGIC **HINT:** Think about what would happen to the "randomness" of feature selection.

# COMMAND ----------

from pyspark.ml import Pipeline
from pyspark.ml.feature import *

filePath = "mnt/training/airbnb/sf-listings/sf-listings-2018-12-06-clean.parquet/"
airbnbDF = spark.read.parquet(filePath)
(trainDF, testDF) = airbnbDF.randomSplit([.8, .2], seed=42)

categoricalColumns = [field for (field, dataType) in trainDF.dtypes if dataType == "string"]
stages = [] 
for categoricalCol in categoricalColumns:
    stringIndexer = StringIndexer(inputCol=categoricalCol, outputCol=categoricalCol + "Index", handleInvalid="skip")
    stages.append(stringIndexer)
    
indexCols = [c + "Index" for c in categoricalColumns]
numericCols = [field for (field, dataType) in trainDF.dtypes if ((dataType == "double") & (field != "price"))]
assemblerInputs = indexCols + numericCols
assembler = VectorAssembler(inputCols=assemblerInputs, outputCol="features")
stages.append(assembler)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Random Forest
# MAGIC 
# MAGIC Create a Random Forest estimator called `rf` with the `labelCol`=`price`, `maxBins`=`40`, and `seed`=`42` (for reproducibility).

# COMMAND ----------

# TODO

rf = <FILL_IN>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Grid Search
# MAGIC 
# MAGIC There are a lot of hyperparamaters we could tune, and it would take a long time to manually configure.
# MAGIC 
# MAGIC Let's use Spark's `ParamGridBuilder` to find the optimal hyperparameters in a more systematic approach 
# MAGIC [Python](https://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.tuning.ParamGridBuilder)/
# MAGIC [Scala](https://spark.apache.org/docs/latest/api/scala/#org.apache.spark.ml.tuning.ParamGridBuilder).
# MAGIC 
# MAGIC Let's define a grid of hyperparameters to test:
# MAGIC   - maxDepth: max depth of the decision tree (Use the values `2, 5, 10`)
# MAGIC   - numTrees: number of decision trees (Use the values `10, 20, 100`)
# MAGIC 
# MAGIC `addGrid()` accepts the name of the parameter (e.g. `rf.maxDepth`), and a list of the possible values (e.g. `[2, 5, 10]`).

# COMMAND ----------

# TODO

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cross Validation
# MAGIC 
# MAGIC We are going to do 3-Fold cross-validation, with `parallelism`=4, and set the `seed`=42 on the cross-validator for reproducibility.
# MAGIC 
# MAGIC Put the Random Forest in the CV to speed up the cross validation (as opposed to the pipeline in the CV) 
# MAGIC [Python](https://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.tuning.CrossValidator)/
# MAGIC [Scala](https://spark.apache.org/docs/latest/api/scala/#org.apache.spark.ml.tuning.CrossValidator).

# COMMAND ----------

# TODO

from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import CrossValidator

evaluator = RegressionEvaluator(labelCol = "price", predictionCol = "prediction")

# cv = <FILL_IN>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline
# MAGIC 
# MAGIC Let's fit the pipeline with our cross validator to our training data (this may take a few minutes).

# COMMAND ----------

stagesWithCV = stages.copy()
stagesWithCV.append(cv)

pipeline = Pipeline(stages=stagesWithCV)

pipelineModel = pipeline.fit(trainDF)
pipelineModel.stages

# COMMAND ----------

# MAGIC %md
# MAGIC ## Hyperparameter
# MAGIC 
# MAGIC Which hyperparameter combination performed the best?

# COMMAND ----------

cvModel = pipelineModel.stages[-1]
rfModel = cvModel.bestModel

# list(zip(cvModel.getEstimatorParamMaps(), cvModel.avgMetrics))

print(rfModel.explainParams())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Feature Selection
# MAGIC 
# MAGIC Since the trees in a random forest don't use all the features at once, we can see which features had the most impact when left out. Feature importance provides the mean decrease in purity or accuracy when the feature was not included. 

# COMMAND ----------

import pandas as pd

pandasDF = pd.DataFrame(list(zip(assembler.getInputCols(), rfModel.featureImportances)), columns=["feature", "importance"])
topFeatures = pandasDF.sort_values(["importance"], ascending=False)
topFeatures

# COMMAND ----------

# MAGIC %md
# MAGIC Do those features make sense? Would you use those features when picking an Airbnb rental?

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply Model to test set

# COMMAND ----------

# TODO

predDF = <FILL_IN>
rmse = <FILL_IN>
r2 = <FILL_IN>

print(f"RMSE is {rmse}")
print(f"R2 is {r2}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Model
# MAGIC 
# MAGIC Alright, our Random Forest only did slightly better.
# MAGIC 
# MAGIC Save the model to `<userhome>/airbnb/random_forest`

# COMMAND ----------

# TODO

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sklearn vs SparkML
# MAGIC 
# MAGIC [Sklearn RandomForestRegressor](https://scikit-learn.org/stable/modules/generated/sklearn.ensemble.RandomForestRegressor.html) vs `SparkML RandomForestRegressor` 
# MAGIC [Python](https://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.regression.RandomForestRegressor)/
# MAGIC [Scala](https://spark.apache.org/docs/latest/api/scala/#org.apache.spark.ml.regression.RandomForestRegressor).
# MAGIC 
# MAGIC Look at these params in particular:
# MAGIC * **n_estimators** (sklearn) vs **numTrees** (SparkML)
# MAGIC * **max_depth** (sklearn) vs **maxDepth** (SparkML)
# MAGIC * **max_features** (sklearn) vs **featureSubsetStrategy** (SparkML)
# MAGIC * **maxBins** (SparkML only)
# MAGIC 
# MAGIC What do you notice that is different? Is Sklearn's Random Forest default parameters really a random forest?

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>