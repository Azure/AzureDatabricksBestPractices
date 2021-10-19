// Databricks notebook source
// MAGIC 
// MAGIC %md-sandbox
// MAGIC 
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC # Machine Learning Pipeline
// MAGIC 
// MAGIC ** What you will learn:**
// MAGIC * How to create a Machine Learning Pipeline.
// MAGIC * How to train a Machine Learning model.
// MAGIC * How to save & read the model.
// MAGIC * How to make predictions with the model.

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Getting Started
// MAGIC 
// MAGIC Run the following cell to configure our "classroom."

// COMMAND ----------

// MAGIC %run "../../Includes/Classroom Setup"

// COMMAND ----------

// MAGIC %md
// MAGIC ## The Data
// MAGIC 
// MAGIC The dataset contains bike rental info from 2011 and 2012 in the Capital bikeshare system, plus additional relevant information such as weather.  
// MAGIC 
// MAGIC This dataset is from Fanaee-T and Gama (2013) and is hosted by the <a href="http://archive.ics.uci.edu/ml/datasets/Bike+Sharing+Dataset" target="_blank">UCI Machine Learning Repository</a>.

// COMMAND ----------

// MAGIC %md
// MAGIC ## The Goal
// MAGIC We want to learn to predict bike rental counts (per hour) from information such as day of the week, weather, month, etc.  
// MAGIC 
// MAGIC Having good predictions of customer demand allows a business or service to prepare and increase supply as needed.  

// COMMAND ----------

// MAGIC %md
// MAGIC ## Loading the data
// MAGIC 
// MAGIC We begin by loading our data, which is stored in the CSV format</a>.

// COMMAND ----------

val fileName = "/mnt/training/bikeSharing/data-001/hour.csv"

val initialDF = spark.read        // Our DataFrameReader
  .option("header", "true")       // Let Spark know we have a header
  .option("inferSchema", "true")  // Infering the schema (it is a small dataset)
  .csv(fileName)                  // Location of our data
  .cache()                        // Mark the DataFrame as cached.

initialDF.count()                 // Materialize the cache

initialDF.printSchema()           

// COMMAND ----------

// MAGIC %md
// MAGIC ## Understanding the data
// MAGIC 
// MAGIC According to the <a href="http://archive.ics.uci.edu/ml/datasets/Bike+Sharing+Dataset" target="_blank">UCI ML Repository description</a>, we have the following schema:
// MAGIC 
// MAGIC **Feature columns**:
// MAGIC * **dteday**: date
// MAGIC * **season**: season (1:spring, 2:summer, 3:fall, 4:winter)
// MAGIC * **yr**: year (0:2011, 1:2012)
// MAGIC * **mnth**: month (1 to 12)
// MAGIC * **hr**: hour (0 to 23)
// MAGIC * **holiday**: whether the day was a holiday or not
// MAGIC * **weekday**: day of the week
// MAGIC * **workingday**: `1` if the day is neither a weekend nor holiday, otherwise `0`.
// MAGIC * **weathersit**: 
// MAGIC   * 1: Clear, Few clouds, Partly cloudy, Partly cloudy
// MAGIC   * 2: Mist + Cloudy, Mist + Broken clouds, Mist + Few clouds, Mist
// MAGIC   * 3: Light Snow, Light Rain + Thunderstorm + Scattered clouds, Light Rain + Scattered clouds
// MAGIC   * 4: Heavy Rain + Ice Pallets + Thunderstorm + Mist, Snow + Fog
// MAGIC * **temp**: Normalized temperature in Celsius. The values are derived via `(t-t_min)/(t_max-t_min)`, `t_min=-8`, `t_max=+39` (only in hourly scale)
// MAGIC * **atemp**: Normalized feeling temperature in Celsius. The values are derived via `(t-t_min)/(t_max-t_min)`, `t_min=-16`, `t_max=+50` (only in hourly scale)
// MAGIC * **hum**: Normalized humidity. The values are divided to 100 (max)
// MAGIC * **windspeed**: Normalized wind speed. The values are divided to 67 (max)
// MAGIC 
// MAGIC **Label columns**:
// MAGIC * **casual**: count of casual users
// MAGIC * **registered**: count of registered users
// MAGIC * **cnt**: count of total rental bikes including both casual and registered
// MAGIC 
// MAGIC **Extraneous columns**:
// MAGIC * **instant**: record index
// MAGIC 
// MAGIC For example, the first row is a record of hour 0 on January 1, 2011---and apparently, 16 people rented bikes around midnight!

// COMMAND ----------

// MAGIC %md
// MAGIC ## Preprocessing the data
// MAGIC 
// MAGIC So what do we need to do to get our data ready for Machine Learning?
// MAGIC 
// MAGIC **Recall our goal**: We want to learn to predict the count of bike rentals (the `cnt` column).  We refer to the count as our target "label".
// MAGIC 
// MAGIC **Features**: What can we use as features to predict the `cnt` label?  
// MAGIC 
// MAGIC All the columns except `cnt`, and a few exceptions:
// MAGIC * `casual` & `registered`
// MAGIC   * The `cnt` column we want to predict equals the sum of the `casual` + `registered` columns.  We will remove the `casual` and `registered` columns from the data to make sure we do not use them to predict `cnt`.  (*Warning: This is a danger in careless Machine Learning.  Make sure you do not "cheat" by using information you will not have when making predictions*)
// MAGIC * `season` and the date column `dteday`: We could keep them, but they are well-represented by the other date-related columns like `yr`, `mnth`, and `weekday`.
// MAGIC * `holiday` and `weekday`: These features are highly correlated with the `workingday` column.
// MAGIC * row index column `instant`: This is a useless column to us.

// COMMAND ----------

// MAGIC %md
// MAGIC Let's drop the columns `instant`, `dteday`, `season`, `casual`, `holiday`, `weekday`, and `registered` from our DataFrame and then review our schema:

// COMMAND ----------

val preprocessedDF = initialDF.drop("instant", "dteday", "season", "casual", "registered", "holiday", "weekday")

preprocessedDF.printSchema()

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Train/Test Split
// MAGIC 
// MAGIC Our final data preparation step will be to split our dataset into separate training and test sets.
// MAGIC 
// MAGIC Using the `randomSplit()` function, we split the data such that 70% of the data is reserved for training and the remaining 30% for testing. 
// MAGIC 
// MAGIC For more information see:
// MAGIC * Scala: <a href="https://spark.apache.org/docs/latest/api/scala/#org.apache.spark.sql.Dataset" target="_blank">Dataset.randomSplit()</a>
// MAGIC * Python: <a href="https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrame.randomSplit" target="_blank">DataFrame.randomSplit()</a>

// COMMAND ----------

val Array(trainDF, testDF) = preprocessedDF.randomSplit( 
  Array(0.7, 0.3),  // 70-30 split
  seed=42)          // For reproducibility

println(s"We have ${trainDF.count} training examples and ${testDF.count} test examples.")
assert (trainDF.count() == 12197)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Visualize our data
// MAGIC 
// MAGIC Now that we have preprocessed our features, we can quickly visualize our data to get a sense of whether the features are meaningful.
// MAGIC 
// MAGIC We want to compare bike rental counts versus the hour of the day. 
// MAGIC 
// MAGIC To plot the data:
// MAGIC * Run the cell below
// MAGIC * From the list of plot types, select **Line**.
// MAGIC * Click the **Plot Options...** button.
// MAGIC * By dragging and dropping the fields, set the **Keys** to **hr** and the **Values** to **cnt**.
// MAGIC 
// MAGIC Once you've created the graph, go back and select different **Keys**. For example:
// MAGIC * **cnt** vs. **windspeed**
// MAGIC * **cnt** vs. **month**
// MAGIC * **cnt** vs. **workingday**
// MAGIC * **cnt** vs. **hum**
// MAGIC * **cnt** vs. **temp**
// MAGIC * ...etc.

// COMMAND ----------

display(trainDF)

// COMMAND ----------

// MAGIC %md
// MAGIC A couple of notes:
// MAGIC * Rentals are low during the night, and they peak in the morning (8 am) and in the early evening (5 pm).  
// MAGIC * Rentals are high during the summer and low in winter.
// MAGIC * Rentals are high on working days vs. non-working days
// MAGIC 
// MAGIC This indicates that the `hr`, `mnth` and `workingday` features are all useful and can help us predict our label `cnt`. 
// MAGIC 
// MAGIC But how do other features affect our prediction? 
// MAGIC 
// MAGIC Do combinations of those features matter? For example, high wind in summer is not going to have the same effect as high wind in winter.
// MAGIC 
// MAGIC As it turns out our features can be divided into two types:
// MAGIC  * **Numeric columns:**
// MAGIC    * `mnth`
// MAGIC    * `temp`
// MAGIC    * `hr`
// MAGIC    * `hum`
// MAGIC    * `atemp`
// MAGIC    * `windspeed`
// MAGIC 
// MAGIC * **Categorical Columns:**
// MAGIC   * `yr`
// MAGIC   * `workingday`
// MAGIC   * `weathersit`
// MAGIC   
// MAGIC We could treat both `mnth` and `hr` as categorical but we would lose the temporal relationships (e.g. 2:00 AM comes before 3:00 AM).

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) StringIndexer
// MAGIC 
// MAGIC For each of the categorical columns, we are going to create one `StringIndexer` where we
// MAGIC   * Set `inputCol` to something like `weathersit`
// MAGIC   * Set `outputCol` to something like `weathersitIndex`
// MAGIC 
// MAGIC This will have the effect of treating a value like `weathersit` not as number 1 through 4, but rather four categories: **light**, **mist**, **medium** & **heavy**, for example.
// MAGIC 
// MAGIC For more information see:
// MAGIC * Scala: <a href="https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.ml.feature.StringIndexer" target="_blank">StringIndexer</a>
// MAGIC * Python: <a href="https://spark.apache.org/docs/latest/api/python/pyspark.ml.html?highlight=stringindexer#pyspark.ml.feature.StringIndexer" target="_blank">StringIndexer</a>

// COMMAND ----------

// MAGIC %md
// MAGIC Before we get started, let's review our current schema:

// COMMAND ----------

trainDF.printSchema

// COMMAND ----------

// MAGIC %md
// MAGIC Let's create the first `StringIndexer` for the `workingday` column.
// MAGIC 
// MAGIC After we create it, we can run a sample through the indexer to see how it would affect our `DataFrame`.

// COMMAND ----------

import org.apache.spark.ml.feature.StringIndexer

val workingdayStringIndexer = new StringIndexer()
  .setInputCol("workingday")
  .setOutputCol("workingdayIndex")

// Just for demonstration purposes, we will use the StringIndexer to fit and
// then transform our training data set just to see how it affects the schema
workingdayStringIndexer.fit(trainDF).transform(trainDF).printSchema()

// COMMAND ----------

// MAGIC %md
// MAGIC Next we will create the `StringIndexer` for the `yr` column and preview its effect.

// COMMAND ----------

val yrStringIndexer = new StringIndexer()
  .setInputCol("yr")
  .setOutputCol("yrIndex")

yrStringIndexer.fit(trainDF).transform(trainDF).printSchema()

// COMMAND ----------

// MAGIC %md
// MAGIC And then create our last `StringIndexer` for the `weathersit` column.

// COMMAND ----------

val weathersitStringIndexer = new StringIndexer()
  .setInputCol("weathersit")
  .setOutputCol("weathersitIndex")

weathersitStringIndexer.fit(trainDF).transform(trainDF).printSchema()

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) VectorAssembler
// MAGIC 
// MAGIC The next step is to assemble the feature columns into a single feature vector.
// MAGIC 
// MAGIC To do that we will use the `VectorAssembler` where we
// MAGIC   * Set `inputCols` to the new list of feature columns
// MAGIC   * Set `outputCol` to `features`
// MAGIC   
// MAGIC   
// MAGIC For more information see:
// MAGIC * Scala: <a href="https://spark.apache.org/docs/latest/api/scala/#org.apache.spark.ml.feature.VectorAssembler" target="_blank">VectorAssembler</a>
// MAGIC * Python: <a href="https://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.feature.VectorAssembler" target="_blank">VectorAssembler</a>

// COMMAND ----------

import org.apache.spark.ml.feature.VectorAssembler

val assemblerInputs  = Array(
  "mnth", "temp", "hr", "hum", "atemp", "windspeed", // Our numerical features
  "yrIndex", "workingdayIndex", "weathersitIndex")   // Our new categorical features

val vectorAssembler = new VectorAssembler()
  .setInputCols(assemblerInputs)
  .setOutputCol("features")

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Random Forests
// MAGIC 
// MAGIC Random forests and ensembles of decision trees are more powerful than a single decision tree alone.
// MAGIC 
// MAGIC This is also the last step in our pipeline.
// MAGIC 
// MAGIC We will use the `RandomForestRegressor` where we
// MAGIC   * Set `labelCol` to the column that contains our label.
// MAGIC   * Set `seed` to ensure reproducibility.
// MAGIC   * Set `numTrees` to `3` so that we build 3 trees in our random forest.
// MAGIC   * Set `maxDepth` to `10` to control the depth/complexity of the tree.
// MAGIC 
// MAGIC For more information see:
// MAGIC * Scala: <a href="https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.ml.regression.RandomForestRegressor" target="_blank">RandomForestRegressor</a>
// MAGIC * Python: <a href="https://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.regression.RandomForestRegressor" target="_blank">RandomForestRegressor</a>

// COMMAND ----------

import org.apache.spark.ml.regression.RandomForestRegressor

val rfr = new RandomForestRegressor()
  .setLabelCol("cnt") // The column of our label
  .setSeed(27)        // Some seed value for consistency
  .setNumTrees(3)     // A guess at the number of trees
  .setMaxDepth(10)    // A guess at the depth of each tree

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Create a Machine Learning Pipeline
// MAGIC 
// MAGIC Now let's wrap all of these stages into a Pipeline.

// COMMAND ----------

import org.apache.spark.ml.Pipeline

val pipeline = new Pipeline().setStages(Array(
  workingdayStringIndexer, // categorize workingday
  weathersitStringIndexer, // categorize weathersit
  yrStringIndexer,         // categorize yr
  vectorAssembler,         // assemble the feature vector for all columns
  rfr))

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Train the model
// MAGIC 
// MAGIC Train the pipeline model to run all the steps in the pipeline.

// COMMAND ----------

val pipelineModel = pipeline.fit(trainDF)

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Evaluate the model
// MAGIC 
// MAGIC Now that we have fitted a model, we can evaluate it.
// MAGIC 
// MAGIC In the case of a random forest, one of the best things to look at is the `featureImportances`:

// COMMAND ----------

import org.apache.spark.ml.regression.RandomForestRegressionModel

val rfrm = pipelineModel.stages.last    // The RFRM is in the last stage of the model
  .asInstanceOf[RandomForestRegressionModel]

// Zip the list of features with their scores
val scores = assemblerInputs.zip(rfrm.featureImportances.toArray)

// And pretty print 'em
scores.foreach(x => println(f"${x._1}%-15s = ${x._2}"))

println("-"*80)

// COMMAND ----------

// MAGIC %md
// MAGIC Which features were most important?

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Making Predictions
// MAGIC 
// MAGIC Next, apply the trained pipeline model to the test set.

// COMMAND ----------

// Using the model, create our predictions from the test data
val predictionsDF = pipelineModel.transform(testDF)

// Reorder the columns for easier interpretation
val reorderedDF = predictionsDF.select("cnt", "prediction", "yr", "yrIndex", "mnth", "hr", "workingday", "workingdayIndex", "weathersit", "weathersitIndex", "temp", "atemp", "hum", "windspeed")

display(reorderedDF)

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Evaluate
// MAGIC 
// MAGIC Next, we'll use `RegressionEvaluator` to assess the results. The default regression metric is RMSE.
// MAGIC 
// MAGIC For more information see:
// MAGIC * Scala: <a href="https://spark.apache.org/docs/latest/api/scala/#org.apache.spark.ml.evaluation.RegressionEvaluator" target="_blank">RegressionEvaluator</a>
// MAGIC * Python: <a href="https://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.evaluation.RegressionEvaluator" target="_blank">RegressionEvaluator</a>

// COMMAND ----------

import org.apache.spark.ml.evaluation.RegressionEvaluator

val evaluator = new RegressionEvaluator().setLabelCol("cnt")

val rmse = evaluator.evaluate(predictionsDF)

println("Test Accuracy = " + rmse)

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) ParamGrid
// MAGIC 
// MAGIC There are a lot of hyperparamaters we could tune, and it would take a long time to manually configure.
// MAGIC 
// MAGIC Instead of a manual (ad-hoc) approach, let's use Spark's `ParamGridBuilder` to find the optimal hyperparameters in a more systematic approach.
// MAGIC 
// MAGIC In this example notebook, we keep these trees shallow and use a relatively small number of trees. Let's define a grid of hyperparameters to test:
// MAGIC   - maxDepth: max depth of each decision tree in the RF ensemble (Use the values `2, 5, 10`)
// MAGIC   - numTrees: number of trees in each RF ensemble (Use the values `10, 50`)
// MAGIC 
// MAGIC `addGrid()` accepts the name of the parameter (e.g. `rf.maxDepth`), and an Array of the possible values (e.g. `Array(2, 5, 10)`).
// MAGIC 
// MAGIC For more information see:
// MAGIC * Scala: <a href="https://spark.apache.org/docs/latest/api/scala/#org.apache.spark.ml.tuning.ParamGridBuilder" target="_blank">ParamGridBuilder</a>
// MAGIC * Python: <a href="https://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.tuning.ParamGridBuilder" target="_blank">ParamGridBuilder</a>

// COMMAND ----------

import org.apache.spark.ml.tuning.ParamGridBuilder

val paramGrid = new ParamGridBuilder()
                    .addGrid(rfr.maxDepth, Array(2, 5, 10))
                    .addGrid(rfr.numTrees, Array(10, 50))
                    .build()

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Cross-Validation
// MAGIC 
// MAGIC We are also going to use 3-fold cross-validation to identify the optimal maxDepth and numTrees combination.
// MAGIC 
// MAGIC ![crossValidation](https://files.training.databricks.com/images/301/CrossValidation.png)
// MAGIC 
// MAGIC With 3-fold cross-validation, we train on 2/3 of the data and evaluate with the remaining (held-out) 1/3. We repeat this process 3 times, so each fold gets the chance to act as the validation set. We then average the results of the three rounds.

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC We pass in the `estimator` (our original pipeline), an `evaluator`, and an `estimatorParamMaps` to the `CrossValidator` so that it knows:
// MAGIC - Which model to use
// MAGIC - How to evaluate the model
// MAGIC - What hyperparamters to set on the model
// MAGIC 
// MAGIC We can also set the number of folds we want to split our data into (3), as well as setting a seed so we all have the same split in the data.
// MAGIC 
// MAGIC For more information see:
// MAGIC * Scala: <a href="https://spark.apache.org/docs/latest/api/scala/#org.apache.spark.ml.tuning.CrossValidator" target="_blank">CrossValidator</a>
// MAGIC * Python: <a href="https://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.tuning.CrossValidator" target="_blank">CrossValidator</a>

// COMMAND ----------

import org.apache.spark.ml.tuning.CrossValidator
import org.apache.spark.ml.evaluation.RegressionEvaluator

val evaluator = new RegressionEvaluator()
  .setLabelCol("cnt")
  .setPredictionCol("prediction")

val cv = new CrossValidator()
  .setEstimator(pipeline)
  .setEvaluator(evaluator)
  .setEstimatorParamMaps(paramGrid)
  .setNumFolds(3)
  .setSeed(27)

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) A New Model
// MAGIC 
// MAGIC We can now use the `CrossValidator` to fit a new model - this could take several minutes on a small cluster.

// COMMAND ----------

val cvModel = cv.fit(trainDF)

// COMMAND ----------

// MAGIC %md
// MAGIC And now we can take a look at the model with the best hyperparameter configuration:

// COMMAND ----------

// Zip the two lists together
val results = cvModel.getEstimatorParamMaps.zip(cvModel.avgMetrics)

// And pretty print 'em
results.foreach(x => println(f"${x._1.toSeq(0).param} = ${x._1.toSeq(0).value}\n${x._1.toSeq(1).param} = ${x._1.toSeq(1).value}\nAverage: ${x._2}\n"))
println("-"*80)

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) One last set of predictions
// MAGIC 
// MAGIC Using our newest mode, let's make a final set of predictions:

// COMMAND ----------

// Using the model, create our predictions from the test data
val finalPredictionsDF = cvModel.transform(testDF)

// Reorder the columns for easier interpretation
val finalDF = finalPredictionsDF.select("cnt", "prediction", "yr", "yrIndex", "mnth", "hr", "workingday", "workingdayIndex", "weathersit", "weathersitIndex", "temp", "atemp", "hum", "windspeed")

display(finalDF)

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Evaluating the New Model
// MAGIC 
// MAGIC Let's see how our latest model does:

// COMMAND ----------

println("Test RMSE = " + evaluator.evaluate(finalPredictionsDF))


// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>