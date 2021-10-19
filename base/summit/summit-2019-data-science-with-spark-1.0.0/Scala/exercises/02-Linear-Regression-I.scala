// Databricks notebook source
// MAGIC 
// MAGIC %md-sandbox
// MAGIC 
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC # Regression: Predicting Rental Price
// MAGIC 
// MAGIC In this notebook, we will use the dataset we cleansed in the previous lab to predict Airbnb rental prices in San Francisco.
// MAGIC 
// MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lesson you:<br>
// MAGIC  - Use the SparkML API to build a linear regression model
// MAGIC  - Identify the differences between estimators and transformers

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## Linear Regression
// MAGIC 
// MAGIC Given two points, it is trivial to find the line connecting these two points.

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC import numpy as np
// MAGIC import matplotlib.pyplot as plt
// MAGIC 
// MAGIC fig, ax = plt.subplots()
// MAGIC 
// MAGIC points = [(1,2), (-2,-3)]
// MAGIC 
// MAGIC for point in points:
// MAGIC   ax.plot(*point, 'o',c='red', markersize=15)
// MAGIC 
// MAGIC slope = ((points[1][1] - points[0][1])/
// MAGIC          (points[1][0] - points[0][0]))
// MAGIC f = lambda x: point[1] + slope*(x - point[0])
// MAGIC 
// MAGIC xx = np.linspace(-5,5,100)
// MAGIC ax.plot(xx, f(xx))
// MAGIC 
// MAGIC ax.set_xlim(-5,5)
// MAGIC ax.set_ylim(-5,5)
// MAGIC display(fig)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC It is often the case in data science that we will have more points than variables. We generally refer to the number of instances or points as *n* and the number of features as *p*. Here we have *n > p*. A system with more *n* than *p* is known as an **over-determined system**.

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC fig, ax = plt.subplots()
// MAGIC 
// MAGIC points = np.array([(-2.9,4.5),
// MAGIC                   (-2.1,2.0),
// MAGIC                   (1.8,-2.7),
// MAGIC                   (0.5,-1.7),
// MAGIC                   (-1.1,1.9)])
// MAGIC 
// MAGIC for point in points:
// MAGIC   ax.plot(*point, 'o',c='red', markersize=15)
// MAGIC   
// MAGIC ax.set_xlim(-5,5)
// MAGIC ax.set_ylim(-5,5)
// MAGIC display(fig)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## The Line of Best Fit
// MAGIC 
// MAGIC In this case, there is single straight line that will pass through all of these points. We seek the **line of best fit**. Here, we use applied linear algebra using the Python library `numpy` to find the weights (in this 1-d case, the intercept and slope of the line) of the line of best fit. You can read more about this solution in Gilbert Strang's classic, [*Introduction to Linear Algebra*](http://math.mit.edu/~gs/linearalgebra/ila0403.pdf).

// COMMAND ----------

// MAGIC %python
// MAGIC fig, ax = plt.subplots()
// MAGIC 
// MAGIC X = np.array([(1, point[0]) for point in points])
// MAGIC x = np.array([point[0] for point in points])
// MAGIC y = np.array([point[1] for point in points])
// MAGIC 
// MAGIC XTX = X.T.dot(X)
// MAGIC XTy = X.T.dot(y) 
// MAGIC 
// MAGIC weights = np.linalg.inv(XTX).dot(XTy)
// MAGIC f = lambda x: weights[0] + weights[1]*x
// MAGIC 
// MAGIC for point in points:
// MAGIC   ax.plot(*point, 'o',c='red', markersize=15)
// MAGIC 
// MAGIC ax.plot(xx, f(xx))
// MAGIC   
// MAGIC ax.set_xlim(-5,5)
// MAGIC ax.set_ylim(-5,5)
// MAGIC display(fig)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## Supervised Machine Learning
// MAGIC 
// MAGIC Given some *input* data, *X*,  often called **feature** or **predictor** data and some *output* data, *y*, often called **target** or **response** data, we might seek a functional mapping from one set to the other. 
// MAGIC 
// MAGIC $$f: X \mapsto y$$
// MAGIC 
// MAGIC **Linear Regression** is a specific case of Supervised Learning in which we assume a linear form for this function. In the one-dimensional case
// MAGIC 
// MAGIC $$y \approx \widehat{y} = f(x) = w_0 + w_1x + \varepsilon$$
// MAGIC 
// MAGIC This form generalizes to a *p*-dimensional space, where *p* represents the number of features in the feature data set.
// MAGIC 
// MAGIC $$y \approx \widehat{y} = f(X) = w_0 + w_1x_1 + \dots + w_px_p + \varepsilon$$

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### Learn the Weights That Minimize the Residuals
// MAGIC 
// MAGIC We can think of the process tht identifies weights as one that minimizes the residual error (or residuals) between the line of best fit and the actual data. Consider the three lines below. From a visual inspection, we can see that the first (or the one generated above) has the smallest residual error between the line and the given data. 

// COMMAND ----------

// MAGIC %python
// MAGIC fig, axes = plt.subplots(1, 3, figsize=(20,5))
// MAGIC 
// MAGIC functions = [
// MAGIC   f,
// MAGIC   lambda x: -.25 -2*x,
// MAGIC   lambda x: .2 - 1.3*x
// MAGIC ]
// MAGIC 
// MAGIC for func, ax in zip(functions, axes):
// MAGIC   for point in points:
// MAGIC     ax.plot(*point, 'o',c='red', markersize=5)
// MAGIC 
// MAGIC   ax.plot(xx, func(xx))
// MAGIC   ax.vlines(x,y,func(x))
// MAGIC 
// MAGIC   ax.set_xlim(-5,5)
// MAGIC   ax.set_ylim(-5,5)
// MAGIC 
// MAGIC display(fig)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### Evaluating a Regression
// MAGIC 
// MAGIC We will need some way to measure this numerically i.e. to measure the "closeness" of a particular fit between the label and a predicted value.
// MAGIC 
// MAGIC Some common evaluation metrics are, where *y* is a vector of actual values/labels and "y hat" is a vector of predicted values.
// MAGIC 
// MAGIC #### The Loss
// MAGIC 
// MAGIC $$ y - \widehat{y} $$
// MAGIC 
// MAGIC #### The Absolute Loss
// MAGIC 
// MAGIC $$ \big\rvert y - \widehat{y} \big\rvert $$
// MAGIC 
// MAGIC #### The Squared Loss
// MAGIC 
// MAGIC $$ \left( y - \widehat{y} \right)^2$$
// MAGIC 
// MAGIC Each of these is useful under certain circumstances.

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### Spark Evaluation Metrics
// MAGIC 
// MAGIC In Spark, we use the evaluation metrics, the root-mean-squared error or RMSE, and the coefficient of determination or R2.
// MAGIC 
// MAGIC #### RMSE
// MAGIC 
// MAGIC $$\text{RMSE} = \sqrt{\frac1n \sum_{i=1}^n \left( y - \widehat{y} \right)^2}$$
// MAGIC 
// MAGIC Useful because the root-mean squared error can be directly interpreted in terms of the target variable. 
// MAGIC 
// MAGIC Note also that the squared error term is the **sqaured loss** metric above. 
// MAGIC 
// MAGIC #### R2
// MAGIC 
// MAGIC The R2 is a ratio. Consider a few squared sums that fall out of a given fit:
// MAGIC 
// MAGIC $$ \text{the }\textbf{RSS}\text{, or Residual Sum of Squares} = \sum\left(y_i-\widehat{y}_i\right)^2$$
// MAGIC $$ \text{the }\textbf{ESS}\text{, or Explained Sum of Squares} = \sum\left(\widehat{y}_i-\bar{y}\right)^2$$
// MAGIC $$ \text{the }\textbf{TSS}\text{, or Total Sum of Squares} = \sum\left(y_i-\bar{y}\right)^2$$
// MAGIC 
// MAGIC The R2 is then 
// MAGIC 
// MAGIC $$R^2 = 1 - \frac{\text{RSS}}{\text{TSS}} = \frac{\text{ESS}}{\text{TSS}} = \frac{\text{Var}(\widehat{y})}{\text{Var}(y)}$$
// MAGIC 
// MAGIC In other words, **the R2 is a ratio of the variance in the model to the variance in the data**.

// COMMAND ----------

// MAGIC %run "./Includes/Classroom-Setup"

// COMMAND ----------

val filePath = "mnt/training/airbnb/sf-listings/sf-listings-2018-12-06-clean.parquet/"
val airbnbDF = spark.read.parquet(filePath)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Train/Test Split
// MAGIC 
// MAGIC When we are building ML models, we don't want to look at our test data (why is that?). 
// MAGIC 
// MAGIC Let's keep 80% for the training set and set aside 20% of our data for the test set. We will use the `randomSplit` method 
// MAGIC [Python](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrame.randomSplit)/
// MAGIC [Scala](https://spark.apache.org/docs/latest/api/scala/#org.apache.spark.sql.Dataset).
// MAGIC 
// MAGIC **Question**: Why is it necessary to set a seed? What happens if I change my cluster configuration?

// COMMAND ----------

val Array(trainDF, testDF) = airbnbDF.randomSplit(Array(.8, .2), seed=42)
println(trainDF.cache().count)

// COMMAND ----------

// MAGIC %md
// MAGIC Let's change the # of partitions (to simulate a different cluster configuration), and see if we get the same number of data points in our training set. 

// COMMAND ----------

val Array(trainRepartitionDF, testRepartitionDF) = airbnbDF
  .repartition(24)
  .randomSplit(Array(.8, .2), seed=42)

println(trainRepartitionDF.count())

// COMMAND ----------

// MAGIC %md
// MAGIC ## Linear Regression
// MAGIC 
// MAGIC We are going to build a very simple model predicting `price` just given the number of `bedrooms`.
// MAGIC 
// MAGIC **Question**: What are some assumptions of the linear regression model?

// COMMAND ----------

display(trainDF.select("price", "bedrooms"))

// COMMAND ----------

display(trainDF.select("price", "bedrooms").summary())

// COMMAND ----------

display(trainDF)

// COMMAND ----------

// MAGIC %md
// MAGIC There do appear some outliers in our dataset for the price ($9,999 a night??). Just keep this in mind when we are building our models :).
// MAGIC 
// MAGIC We will use `LinearRegression` to build our first model 
// MAGIC [Python](https://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.regression.LinearRegression)/
// MAGIC [Scala](https://spark.apache.org/docs/latest/api/scala/#org.apache.spark.ml.regression.LinearRegression).

// COMMAND ----------

import org.apache.spark.ml.regression.LinearRegression;

val lr = new LinearRegression()
            .setFeaturesCol("bedrooms")
            .setLabelCol("price")

// Uncomment when running
// val lrModel = lr.fit(trainDF)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Explain Params
// MAGIC 
// MAGIC When you are unsure of the defaults or what a parameter does, you can call `.explainParams()`.

// COMMAND ----------

println(lr.explainParams())

// COMMAND ----------

// MAGIC %md
// MAGIC ## Vector Assembler
// MAGIC 
// MAGIC What went wrong? Turns out SparkML **models** expect a column of type `Vector` as input. 
// MAGIC 
// MAGIC We can easily get the values from the `bedrooms` column into a single vector using `VectorAssembler` 
// MAGIC [Python]((https://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.feature.VectorAssembler)/
// MAGIC [Scala](https://spark.apache.org/docs/latest/api/scala/#org.apache.spark.ml.feature.VectorAssembler). 
// MAGIC VectorAssembler is an example of a **transformer**. Transformers take in a DataFrame, and return a new DataFrame with one or more columns appended to it. They do not learn from your data, but apply rule based transformations.

// COMMAND ----------

import org.apache.spark.ml.feature.VectorAssembler

val vecAssembler = new VectorAssembler()
                       .setInputCols(Array("bedrooms"))
                       .setOutputCol("features")

val vecTrainDF = vecAssembler.transform(trainDF)
val lr = new LinearRegression()
             .setFeaturesCol("features")
             .setLabelCol("price")

val lrModel = lr.fit(vecTrainDF)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Inspect the model

// COMMAND ----------

val m = lrModel.coefficients(0)
val b = lrModel.intercept

println(s"The formula for the linear regression line is y = ${m}x + ${b}")
println("*-"*80)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Apply model to test set

// COMMAND ----------

val vecTestDF = vecAssembler.transform(testDF)

val predDF = lrModel.transform(vecTestDF)

predDF.select("bedrooms", "features", "price", "prediction").show()

// COMMAND ----------

// MAGIC %md
// MAGIC ## Evaluate Model
// MAGIC 
// MAGIC Let's see how our linear regression model with just one variable does. Does it beat our baseline model?

// COMMAND ----------

import org.apache.spark.ml.evaluation.RegressionEvaluator

val regressionEvaluator = new RegressionEvaluator()
                              .setPredictionCol("prediction")
                              .setLabelCol("price")
                              .setMetricName("rmse")

val rmse = regressionEvaluator.evaluate(predDF)
println(f"RMSE is ${rmse}")

// COMMAND ----------

// MAGIC %md
// MAGIC Wahoo! Our RMSE is better than our baseline model. However, it's still not that great. Let's see how we can further decrease it in future notebooks.

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>