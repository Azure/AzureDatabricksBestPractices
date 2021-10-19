// Databricks notebook source
// MAGIC %md
// MAGIC Visualization of Machine Learning Models
// MAGIC You can use the display command to visualize MLlib models in Databricks notebooks. This guide presents an example of how you can train models and display its results in Databricks.
// MAGIC Built-in visualizations are currently available for the following models in Scala:
// MAGIC Linear Regression (org.apache.spark.ml.regression.LinearRegressionModel).
// MAGIC Models from the older package org.apache.spark.mllib are also supported
// MAGIC K-Means Clustering (org.apache.spark.mllib.clustering.KMeansModel)
// MAGIC Logistic Regression (org.apache.spark.ml.classification.LogisticRegression). Note: Only available when running Spark 1.5

// COMMAND ----------

// MAGIC %md
// MAGIC Preview Sample Data

// COMMAND ----------

// MAGIC %sql select * from iris

// COMMAND ----------

// MAGIC %sql select * from diabetes

// COMMAND ----------

// MAGIC %md
// MAGIC Linear Regression in Scala
// MAGIC The command for visualizing a linear regression model is:
// MAGIC     display(
// MAGIC        model: LinearRegressionModel,
// MAGIC        data: DataFrame,
// MAGIC        plotType: String
// MAGIC     )
// MAGIC The optional parameter plotType specifies what is plotted and defaults to "fittedVsResiduals."
// MAGIC "fittedVsResiduals": Plots the residual values against the predicted label (target) values of the dataset. If the data is perfectly represented, the plot will be a straight horizontal line at zero (no residual error).
// MAGIC Parameters:
// MAGIC model: Linear Regression model
// MAGIC data: The points to display, stored as a Spark DataFrame. The dataframe must contain a features column of type Array[Double] or Vector, and a label column with the target values
// MAGIC plotType (optional): Option to select plot type, for e.g: fittedVsResiduals

// COMMAND ----------

import scala.util.Random
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}

// COMMAND ----------

// Train a Linear Regression model
val data = sqlContext.sql("select * from diabetes")
val lr = new LinearRegression()
 .setMaxIter(20)
 .setRegParam(0.3)
 .setElasticNetParam(1.0)
val lrModel = lr.fit(data)

// COMMAND ----------

display(lrModel, data, "fittedVsResiduals")

// COMMAND ----------

// MAGIC %md
// MAGIC K-Means in Scala
// MAGIC The command for visualizing clusters from a K-Means model is:
// MAGIC     display(
// MAGIC       model: KMeansModel,
// MAGIC       data: DataFrame
// MAGIC     )
// MAGIC This visualization creates a grid plot of numFeatures x numFeatures using a sample of the data. Each plot in the grid corresponds to 2 features, with data points colored by their cluster labels. If the feature vector has more than 10 dimensions, only the first ten features are displayed.
// MAGIC Parameters:
// MAGIC model: the cluster distribution
// MAGIC data: points that will be matched against the clusters. This dataframe is expected to have a features column that contains vectors of doubles (the feature representation of each point)

// COMMAND ----------

val data = sqlContext.sql("select * from iris")
// The MLLib package requires an RDD[Vector] instead of a dataframe. We need to manually extract the vector.
// This is not necessary when using the ml package instead.
val features = data.map(_.getAs[Vector]("features"))
val clusters = KMeans.train(features, 3, 10)

// COMMAND ----------

display(clusters, data)

// COMMAND ----------

// MAGIC %md
// MAGIC ROC curves in Scala (Preview Feature)
// MAGIC A convenient way to evaluate a binary classifier is through its Receiver Operating Characteristic (ROC) curve. Databricks can build ROCs for LogisticRegressionModels
// MAGIC This display is only available with Spark Clusters version 1.5 or higher
// MAGIC The command for displaying logistic regression models is:
// MAGIC   display(
// MAGIC     model: LogisticRegressionModel,
// MAGIC     data: DataFrame,
// MAGIC     plotType: String
// MAGIC   )
// MAGIC The DataFrame provided must have a features columns that contain dense Vectors. If no plotType is provided, the display defaults to the fittedVsResiduals plot. In order to get the ROC plot, the option plotType="ROC" must be passed in.

// COMMAND ----------

import org.apache.spark.ml.classification.LogisticRegression
val numPoints = 1000
val data = sqlContext.createDataFrame(sc.parallelize((0 until numPoints).map { i =>
  if (i < numPoints / 2) LabeledPoint(0.0, Vectors.dense(Random.nextDouble(), Random.nextDouble()))
  else LabeledPoint(1.0, Vectors.dense(Random.nextDouble(), 0.5 + Random.nextDouble()))
}))

val model = new LogisticRegression()
  .fit(data)
display(model, data, plotType="ROC")