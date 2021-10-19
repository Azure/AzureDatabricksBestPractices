# Databricks notebook source
# MAGIC %md # Using scikit-learn with Spark on Databricks
# MAGIC 
# MAGIC This notebook demonstrates how to take advantage of Spark and Databricks to use [scikit-learn](http://scikit-learn.org/), the popular Python library for doing Machine Learning on a single compute node.
# MAGIC 
# MAGIC The simplest way to use scikit-learn with Spark and Databricks is to run scikit-learn jobs as usual.  However, this will run scikit-learn jobs on the driver, so **be careful** not to run large jobs, especially if other users are working on the same cluster as you.  Nevertheless, a reasonable way to port existing scikit-learn workflows to Spark and start benefiting from distributed computing is to: (a) copy the workflow into Databricks and (b) start parallelizing the workflow piece-by-piece. 

# COMMAND ----------

# DBTITLE 1,Step1: Ingest Data to Notebook
# MAGIC %md 
# MAGIC 
# MAGIC - We will use the R "diamonds" dataset from the "ggplot2" package.  This is a dataset hosted on [**Databricks**](http://ggplot2.tidyverse.org/reference/diamonds.html)
# MAGIC - Our task will be to predict the price of a diamond from its properties.

# COMMAND ----------

# Load data into a Pandas dataframe
import pandas
import cStringIO
from pyspark.sql import *
localData = sc.wholeTextFiles("/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv").collect()[0][1]
output = cStringIO.StringIO(localData)
pandasData = pandas.read_csv(output)
pandasData = pandasData.iloc[:,1:] # remove line number

# COMMAND ----------

# MAGIC %md ###Step2: Explore Data 
# MAGIC 
# MAGIC We quickly demonstrate how to start exploring the data.  For a longer tutorial, see the [Visualizations](https://docs.databricks.com/user-guide/visualizations/index.html).

# COMMAND ----------

# MAGIC %md We can make plots using Python tools like matplotlib.

# COMMAND ----------

import matplotlib.pyplot as plt
plt.clf()
plt.plot(pandasData['carat'], pandasData['price'], '.')
plt.xlabel('carat')
plt.ylabel('price')
display()

# COMMAND ----------

# MAGIC %md We can also convert the Pandas dataframe into a Spark DataFrame and use DBC's display methods.

# COMMAND ----------

# Create this plot by calling display on the Spark DataFrame, clicking the plot icon, selecting Plot Options, and creating a Histogram of 'carat' values.
sparkDataframe = sqlContext.createDataFrame(pandasData)
display(sparkDataframe)

# COMMAND ----------

# MAGIC %md #### Step 3: Model creation

# COMMAND ----------

# MAGIC %md
# MAGIC Some of our features are text, and we want them to be numerical so we can train a linear model.  We use the Pandas and scikit-learn APIs for these transformations.
# MAGIC 
# MAGIC First, we convert the features to numerical values, in the correct order based on the feature meanings.  Higher indices are "better."  This ordering will help us interpret model weights later on.

# COMMAND ----------

pandasData['cut'] = pandasData['cut'].replace({'Fair':0, 'Good':1, 'Very Good':2, 'Premium':3, 'Ideal':4})
pandasData['color'] = pandasData['color'].replace({'J':0, 'I':1, 'H':2, 'G':3, 'F':4, 'E':5, 'D':6})
pandasData['clarity'] = pandasData['clarity'].replace({'I1':0, 'SI1':1, 'SI2':2, 'VS1':3, 'VS2':4, 'VVS1':5, 'VVS2':6, 'IF':7})
pandasData

# COMMAND ----------

# MAGIC %md Now, we normalize each feature (column) to have unit variance.  (This normalization or standardization often improves performance. See [Wikipedia](http://en.wikipedia.org/wiki/Feature_scaling#Standardization) for more info.)

# COMMAND ----------

# Split data into a labels dataframe and a features dataframe
labels = pandasData['price'].values
featureNames = ['carat', 'cut', 'color', 'clarity', 'depth', 'table', 'x', 'y', 'z']
features = pandasData[featureNames].values

# Normalize features (columns) to have unit variance
from sklearn.preprocessing import normalize
features = normalize(features, axis=0)
features

# COMMAND ----------

# MAGIC %md Hold out a random test set
# MAGIC 
# MAGIC Note that this randomness can cause this notebook to produce different results each time it is run.

# COMMAND ----------

# Hold out 30% of the data for testing.  We will use the rest for training.
from sklearn.cross_validation import train_test_split
trainingLabels, testLabels, trainingFeatures, testFeatures = train_test_split(labels, features, test_size=0.3)
ntrain, ntest = len(trainingLabels), len(testLabels)
print 'Split data randomly into 2 sets: %d training and %d test instances.' % (ntrain, ntest)

# COMMAND ----------

# MAGIC %md we train a single model using fixed hyperparameters on the driver. 

# COMMAND ----------

# Train a model with fixed hyperparameters, and print out the intercept and coefficients.
from sklearn import linear_model
origAlpha = 0.5 # "alpha" is the regularization hyperparameter
origClf = linear_model.Ridge(alpha=origAlpha)
origClf.fit(features, labels)
print 'Trained model with fixed alpha = %g' % origAlpha
print '  Model intercept: %g' % origClf.intercept_
print '  Model coefficients:'
for i in range(len(featureNames)):
  print '    %g\t%s' % (origClf.coef_[i], featureNames[i])

# COMMAND ----------

# MAGIC %md One can draw conclusions about the model coefficients and the affect of features.  However, be wary of several issues:
# MAGIC * Feature meaning: Especially if you index or transform features, be careful about how those transformations can change the meaning.  E.g., reversing an index order or negating a numerical feature can "flip" the meaning.
# MAGIC * Model assumptions: The model may not fit the data, in which case interpreting coefficients may be difficult.  E.g., if the data do not correspond to a linear model, the model may learn non-intuitive weights for some features (in its attempt to fit the data as well as possible).

# COMMAND ----------

# MAGIC %md ### Evaluate the initial model
# MAGIC 
# MAGIC We will evaluate this and other models using [scikit-learn's score function](http://scikit-learn.org/stable/modules/generated/sklearn.linear_model.Ridge.html#sklearn.linear_model.Ridge.score), which computes a value indicating the quality of the model's predictions on data.  A value closer to `1` is better.

# COMMAND ----------

# Score the initial model.  It does not do that well.
origScore = origClf.score(trainingFeatures, trainingLabels)
origScore