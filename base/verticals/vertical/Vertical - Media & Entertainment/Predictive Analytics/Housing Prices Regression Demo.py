# Databricks notebook source
# MAGIC %md #Housing Prediction Application
# MAGIC This is an end-to-end example of using a number of different machine learning algorithms to solve a supervised regression problem.
# MAGIC 
# MAGIC 
# MAGIC *Last Edited: July 6th, 2017*
# MAGIC 
# MAGIC 
# MAGIC ###Table of Contents
# MAGIC 
# MAGIC - *Step 1: Business Understanding*
# MAGIC - *Step 2: Load Your Data*
# MAGIC - *Step 3: Explore Your Data*
# MAGIC - *Step 4: Visualize Your Data*
# MAGIC - *Step 5: Data Preparation*
# MAGIC - *Step 6: Data Modeling*
# MAGIC - *Step 7: Tuning and Evaluation*
# MAGIC - *Step 8: Deployment*
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC *We are trying to predict housing prices given a set of prices from various zipcodes.  The dataset is rich in features and includes number of bedrooms, bathrooms, sq footage area etc..*
# MAGIC 
# MAGIC 
# MAGIC Given this business problem, we need to translate it to a Machine Learning task.  The ML task is regression since the label (or target) we are trying to predict is numeric.
# MAGIC 
# MAGIC 
# MAGIC The example data is available at https://raw.githubusercontent.com/grzegorzgajda/spark-examples/master/spark-examples/data/house-data.csv
# MAGIC 
# MAGIC 
# MAGIC More information about Machine Learning with Spark can be found in the [Spark ML Programming Guide](https://spark.apache.org/docs/latest/ml-guide.html)
# MAGIC 
# MAGIC 
# MAGIC *Please note this example has only been tested with Spark version 2.0 or higher*

# COMMAND ----------

# MAGIC %sh wget https://raw.githubusercontent.com/grzegorzgajda/spark-examples/master/spark-examples/data/house-data.csv .

# COMMAND ----------

# MAGIC %fs
# MAGIC mv file:///databricks/driver/house-data.csv /mnt/wesley/dataset/media/housing/house-data.csv
# MAGIC   

# COMMAND ----------

# MAGIC %md ##Step 1: Business Understanding
# MAGIC The first step in any machine learning task is to understand the business need. 
# MAGIC 
# MAGIC As described in the overview we are trying to predict housing price given a set of prices from various house types in the Seattle Metro area.
# MAGIC 
# MAGIC The problem is a regression problem since the label (or target) we are trying to predict is numeric

# COMMAND ----------

# MAGIC %md ##Step 2: Load Your Data
# MAGIC Now that we understand what we are trying to do, we need to load our data and describe it, explore it and verify it.

# COMMAND ----------

# Use the Spark CSV datasource with options specifying:
#  - First line of file is a header
#  - Automatically infer the schema of the data
data = spark.read.format("com.databricks.spark.csv")\
  .option("header", "true")\
  .option("inferSchema", "true")\
  .load("/mnt/wesley/dataset/media/housing/house-data.csv")

display(data)

# COMMAND ----------

#### Convert date column to type date using Pandas 

import datetime
import pandas as pd

df = data.toPandas()
df['date'] = pd.to_datetime(df['date'], infer_datetime_format=True)
df['date'] = df.date.apply(lambda x: datetime.datetime.strptime(str(x),'%Y-%m-%d %H:%M:%S').date() )
df = spark.createDataFrame(df)
df.write.format("parquet").saveAsTable("default.housing")

# COMMAND ----------

# MAGIC %md ##Step 3: Explore Your Data
# MAGIC Now that we understand what we are trying to do, we need to load our data and describe it, explore it and verify it.

# COMMAND ----------

# MAGIC %md We can use the SQL desc command to describe the schema

# COMMAND ----------

# MAGIC %sql desc default.housing

# COMMAND ----------

# MAGIC %sql select zipcode, date, price from default.housing order by date

# COMMAND ----------

# MAGIC %md ##Step 4: Visualize Your Data
# MAGIC 
# MAGIC To understand our data, we will look for correlations between features and the label.  This can be important when choosing a model.  E.g., if features and a label are linearly correlated, a linear model like Linear Regression can do well; if the relationship is very non-linear, more complex models such as Decision Trees can be better. We use the Databricks built in visualization to view each of our predictors in relation to the label column as a scatter plot to see the correlation between the predictors and the label.

# COMMAND ----------

# MAGIC %sql select * from default.housing

# COMMAND ----------

# MAGIC %sql select * from default.housing

# COMMAND ----------

# MAGIC %md ##Step 5: Data Preparation
# MAGIC 
# MAGIC The next step is to prepare the data. Since all of this data is numeric and consistent, this is a simple task for us today.
# MAGIC 
# MAGIC We will need to convert the predictor features from columns to Feature Vectors using the org.apache.spark.ml.feature.VectorAssembler
# MAGIC 
# MAGIC The VectorAssembler will be the first step in building our ML pipeline.

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import VectorAssembler

data = data.withColumn("zipcode", data["zipcode"].cast("string"))
stringIndexer = StringIndexer(inputCol="zipcode", outputCol="zipcodeIndex")
data = stringIndexer.fit(data).transform(data)

df_model = data.select(data.bedrooms, data.bathrooms, data.sqft_living, data.sqft_lot, data.zipcode, data.zipcodeIndex, data.condition, data.price.alias("label"))

assembler = VectorAssembler(
    inputCols = ['bedrooms', 'bathrooms', 'sqft_living', 'sqft_lot', 'zipcodeIndex', 'condition'],
    outputCol = "features")

output = assembler.transform(df_model.na.drop())
display(output)

# COMMAND ----------

# MAGIC %md ##Step 6: Data Modeling
# MAGIC Now let's model our data to predict what the power output will be given a set of sensor readings
# MAGIC 
# MAGIC Our first model will be based on simple linear regression since we saw some linear patterns in our data based on the scatter plots during the exploration stage.

# COMMAND ----------

(trainDF,testDF) = output.randomSplit([0.7,0.3], seed = 123)

# COMMAND ----------

# MAGIC %md The cell below is based on the Spark ML pipeline API. More information can be found in the Spark ML Programming Guide at https://spark.apache.org/docs/latest/ml-guide.html

# COMMAND ----------

# MAGIC %md 
# MAGIC Since Linear Regression is simply a line of best fit over the data that minimizes the square of the error, given multiple input dimensions we can express each predictor as a line function of the form:
# MAGIC 
# MAGIC \\(y = a + b x_1 + b x_2 + b x_i ...  \\)
# MAGIC 
# MAGIC where a is the intercept and b are coefficients.
# MAGIC 
# MAGIC To express the coefficients of that line we can retrieve the Estimator stage from the PipelineModel and express the weights and the intercept for the function.

# COMMAND ----------

from pyspark.ml.regression import LinearRegression

lr = LinearRegression(maxIter=10, regParam=0.0005, elasticNetParam=0.8)

# Fit the model
lrModel = lr.fit(trainDF)

# Print the coefficients and intercept for linear regression
print("Coefficients: %s" % str(lrModel.coefficients))
#print("Intercept: %s" % str(lrModel.intercept))

# Summarize the model over the training set and print out some metrics
trainingSummary = lrModel.summary
#print("numIterations: %d" % trainingSummary.totalIterations)
#print("objectiveHistory: %s" % str(trainingSummary.objectiveHistory))
#trainingSummary.residuals.show()
print("RMSE: %f" % trainingSummary.rootMeanSquaredError)
print("r2: %f" % trainingSummary.r2)

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC Now let's see what our predictions look like given this model.

# COMMAND ----------

#get original zipcodes and create a sql table which we can use to run queries
predictions = lrModel.transform(testDF)
predictions = predictions.join(df_model, df_model["zipcodeIndex"] == predictions["zipcodeIndex"], "left").select(predictions["bedrooms"], predictions["bathrooms"], predictions["sqft_living"], predictions["sqft_lot"], df_model["zipcode"], df_model["zipcodeIndex"], predictions["condition"], predictions["label"], predictions["features"], predictions["prediction"])
predictions.write.format("parquet").mode("append").saveAsTable("default.HousingPredictions")


# COMMAND ----------

# MAGIC %sql select * from default.HousingPredictions

# COMMAND ----------

# MAGIC %md #Step 7: Tuning and Evaluation
# MAGIC 
# MAGIC Now that we have our model, we need to see if we can better the model.

# COMMAND ----------

query = "select label as price, prediction as predictedPrice, (label-prediction) as Residual_Error, ((label-prediction)/%s) as Within_RMSE from default.HousingPredictions" % lrModel.summary.rootMeanSquaredError
rmseEvaluator = spark.sql(query)
rmseEvaluator.createOrReplaceTempView("RMSE_Evaluation")

# COMMAND ----------

# MAGIC %sql select * from RMSE_Evaluation

# COMMAND ----------

# MAGIC %sql -- Now we can display the RMSE as a Histogram. Clearly this shows that the RMSE is centered around 0 with the vast majority of the error within 2 RMSEs.
# MAGIC SELECT Within_RMSE from RMSE_Evaluation

# COMMAND ----------

# MAGIC %md Let's see within what price range do we have an accurate prediction.

# COMMAND ----------

# MAGIC %md We can see this definitively if we count the number of predictions within + or - 1.0 and + or - 2.0 and display this as a pie chart:

# COMMAND ----------

# MAGIC %sql SELECT case when Within_RMSE <=1.0 and Within_RMSE >= -1.0 then 1 when Within_RMSE <=2.0 and Within_RMSE <= -2.0 then 2 else 3 end RMSE_Multiple, count(*) count from RMSE_Evaluation group by case when Within_RMSE <= 1.0 and Within_RMSE >= -1.0 then 1 when  Within_RMSE <= 2.0 and Within_RMSE <= -2.0 then 2 else 3 end

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC We have
# MAGIC ####81% of our training data within RMSE 1 and 99% within RMSE 2.#### 
# MAGIC 
# MAGIC The model is pretty good. I'm satisfied with the results, then I'll save the model and get it ready for deployment.

# COMMAND ----------

# MAGIC %md #Step 8: Deployment
# MAGIC 
# MAGIC Now that we have a predictive model it is time to deploy the model into an operational environment. 
# MAGIC 
# MAGIC *Using the save feature within Spark ML, we persist our ML instance to a user input path.* 
# MAGIC 
# MAGIC Read more: http://spark.apache.org/docs/2.1.0/api/python/pyspark.ml.html
# MAGIC 
# MAGIC **The saved model can then be imported back into the sme or another notebook or workflow. **
# MAGIC 
# MAGIC Read more: https://databricks.com/blog/2016/05/31/apache-spark-2-0-preview-machine-learning-model-persistence.html

# COMMAND ----------

path = "/mnt/wesley/model/housing/MLmodel"
lrModel.save(path)

# COMMAND ----------

from pyspark.ml.regression import LinearRegressionModel

finalModel = LinearRegressionModel.load(path)

finalModel

# COMMAND ----------

from pandas import DataFrame
mansion = DataFrame({'bedrooms':[8], 
              'bathrooms':[25], 
              'sqft_living':[50000], 
              'sqft_lot':[225000],
              'floors':[4], 
              'zipcode':['98039'], 
              'condition':[10], 
              'grade':[10],
              'waterfront':[1],
              'view':[4],
              'sqft_above':[37500],
              'sqft_basement':[12500],
              'yr_built':[1994],
              'yr_renovated':[2010],
              'lat':[47.627606],
              'long':[-122.242054],
              'sqft_living15':[5000],
              'sqft_lot15':[40000]})

data = spark.createDataFrame(mansion)
display(data)

# COMMAND ----------

df_test = data.select(data.bedrooms, data.bathrooms, data.sqft_living, data.sqft_lot, data.zipcode, data.condition)

df_test = df_test.withColumn("zipcode", data["zipcode"].cast("string"))
stringIndexer = StringIndexer(inputCol="zipcode", outputCol="zipcodeIndex")
df_test = stringIndexer.fit(df_test).transform(df_test)

assembler = VectorAssembler(
    inputCols = ['bedrooms', 'bathrooms', 'sqft_living', 'sqft_lot', 'condition', 'zipcodeIndex'],
    outputCol = "features")

output = assembler.transform(df_test.na.drop())
prediction = finalModel.transform(output)
pred = prediction.rdd.map(lambda x: x["prediction"]).collect()

print "Predicted price of the house is %s" % pred

# COMMAND ----------

