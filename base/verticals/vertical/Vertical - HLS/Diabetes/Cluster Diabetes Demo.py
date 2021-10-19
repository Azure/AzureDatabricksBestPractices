# Databricks notebook source
# MAGIC %md #Daibetes Analytics
# MAGIC 
# MAGIC Getting diabetes in middle age can reduce life span by about 10 years, claims a new research.
# MAGIC 
# MAGIC A University of Oxford study of more than half a million people found those diagnosed with the condition before 50 lived an average of nine years less than those without the condition. That figure rose to ten years for patients in rural areas.
# MAGIC 
# MAGIC Demonstrate the capabilities of Databricks as a Business Exploration, Advanced Protyping, Machine Learning and Production Refinement platform. 
# MAGIC 
# MAGIC 
# MAGIC ###1. Problem Staement
# MAGIC 
# MAGIC Given a diabetes dataset availble from a publicly avialble dataset, we try to infer the different patterns that influence the outcome of diabetes. 
# MAGIC 
# MAGIC ### 2. Experiment
# MAGIC   Can we extract imortant features that influnce the outcome of diabetes?
# MAGIC   
# MAGIC   Hypothesis: We can use data to predict which individual has a higher likelyhood of being suseptible to diabetes.
# MAGIC   
# MAGIC   For example, let's say we are able to extract features of individual demographic that can influence the outcome of the individual getting Diabetes. Using this information to help prevent other indivuals getting this by taking precaustionary measures.
# MAGIC   
# MAGIC   Can we create a predictive model that will predict the likelyhood whether an individual getting Diabetes?
# MAGIC 
# MAGIC ### 3. Technical Solution
# MAGIC  
# MAGIC 
# MAGIC - The datasets are as follows:
# MAGIC    - The contain the following user demograpics data like "pregnancies", "plasma glucose", "blood pressure", "triceps skin thickness", "insulin", "bmi", "diabetes pedigree", "age" 
# MAGIC - This data was extracted from a [Publicly available dataset](https://raw.githubusercontent.com/AvisekSukul/Regression_Diabetes/master/Custom%20Diabetes%20Dataset.csv)
# MAGIC  

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC **Create** a cluster by...  
# MAGIC   - Click the `Clusters` icon on the left sidebar and then `Create Cluster.` 
# MAGIC   - Enter any text, i.e `demo` into the cluster name text box
# MAGIC   - Select the `Apache Spark Version` value `Spark 2.2 (auto-updating scala 2.11)`  
# MAGIC   - Click the `create cluster` button and wait for your cluster to be provisioned
# MAGIC   
# MAGIC **Attach** this notebook to your cluster by...   
# MAGIC   - Click on your cluster name in menu `Detached` at the top left of this workbook to attach it to this workbook 
# MAGIC   - Add the spark-xml library to the cluster created above. The library is present in libs folder under current user.

# COMMAND ----------

# DBTITLE 1,Step1: Ingest Data to Notebook
# MAGIC %md 
# MAGIC - Ingest data from dbfs sources 
# MAGIC - Create Sources as Tables
# MAGIC - If your running this notebook the first time, you would need to run the Setup_km notebook in the same folder.

# COMMAND ----------

df = spark.read.csv('/mnt/wesley/dataset/medicare/diabetes/custom_diabetes_dataset.csv', header=True, sep=',', inferSchema=True)

# COMMAND ----------

# MAGIC %md display the data ingested to dataframe

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md Describe the schema of the dataset

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md The total number of records ingested

# COMMAND ----------

df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ###Step2: Explore the data
# MAGIC 
# MAGIC - Review the Schema
# MAGIC - Profile the Data 
# MAGIC - Visualize the data

# COMMAND ----------

df.createOrReplaceTempView("diabetes")

# COMMAND ----------

# MAGIC %md Age-wise visualization of indiviuals in diabetes dataset

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from diabetes sort by age

# COMMAND ----------

# MAGIC %md Age-wise visualization of indiviuals with respect to Insulin level in diabetes dataset

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from diabetes sort by age

# COMMAND ----------

# MAGIC %md Visualization of the Top 10 affected age groups with Diabetes

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from diabetes sort by age

# COMMAND ----------

# MAGIC %md ###Step3: Create the Features
# MAGIC 
# MAGIC - Select features using SQL
# MAGIC - User Defined Functions to build custom features

# COMMAND ----------

# MAGIC %md
# MAGIC - Create an assemble `features` vector

# COMMAND ----------

from pyspark.ml.feature import VectorAssembler

train = VectorAssembler(inputCols = ["pregnancies", "plasma glucose", "blood pressure", "triceps skin thickness", "insulin", "bmi", "diabetes pedigree", "age", "diabetes"], outputCol = "features").transform(df)

# COMMAND ----------

# MAGIC %md
# MAGIC - Display DataFrame: `AFTER PREPROCESSING`

# COMMAND ----------

display(train)

# COMMAND ----------

train1=train.withColumnRenamed("diabetes", "label")

# COMMAND ----------

# MAGIC %md  Schema of that feature vector dataframe

# COMMAND ----------

train1.printSchema()

# COMMAND ----------

# MAGIC %md ###Step3: Create the Model from the above feature Set
# MAGIC 
# MAGIC - Select features using SQL
# MAGIC - User Defined Functions to build custom features

# COMMAND ----------

# MAGIC %md
# MAGIC Descision Tree Model

# COMMAND ----------

from pyspark.ml import *
from pyspark.ml.feature import *
from pyspark.ml.classification import *
from pyspark.ml.tuning import *
from pyspark.ml.evaluation import *

# COMMAND ----------

dt = DecisionTreeClassifier()
eval = BinaryClassificationEvaluator(metricName ="areaUnderROC")
grid = ParamGridBuilder().baseOn(
  {
    dt.seed : 102993L,
    dt.maxBins : 64
  }
).addGrid(
  dt.maxDepth, [4,6,8]
).build()

# COMMAND ----------

tvs = TrainValidationSplit(seed = 3923772, estimator=dt, trainRatio=0.7, evaluator = eval, estimatorParamMaps = grid)

# COMMAND ----------

model = tvs.fit(train1)

# COMMAND ----------

# MAGIC %md
# MAGIC Evaluate model

# COMMAND ----------

model.validationMetrics

# COMMAND ----------

model.bestModel.write().overwrite().save("/models/dt")

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.ml.classification.DecisionTreeClassificationModel
# MAGIC 
# MAGIC val dtModel = DecisionTreeClassificationModel.load("/models/dt")
# MAGIC display(dtModel)

# COMMAND ----------

# MAGIC %md
# MAGIC K- Means Model

# COMMAND ----------

from pyspark.ml.clustering import KMeans

kmeans = KMeans().setK(2).setSeed(1)
model = kmeans.fit(train)

# COMMAND ----------

model

# COMMAND ----------

# MAGIC %md
# MAGIC Evaluate clustering

# COMMAND ----------

wssse = model.computeCost(train)
print("Within Set Sum of Squared Errors = " + str(wssse))

# COMMAND ----------

# MAGIC %md
# MAGIC Clustering Results

# COMMAND ----------

centers = model.clusterCenters()
print("Cluster Centers: ")
for center in centers:
    print(center)

# COMMAND ----------

transformed = model.transform(train)

# COMMAND ----------

display(transformed)

# COMMAND ----------

transformed.printSchema()

# COMMAND ----------

display(transformed.sample(False, fraction = 0.5))

# COMMAND ----------

display(
  transformed.groupBy("prediction").count()
)

# COMMAND ----------

display(
  transformed.groupBy("prediction").avg("insulin")
)

# COMMAND ----------

# DBTITLE 1,Results interpretation
# MAGIC 
# MAGIC %md
# MAGIC 
# MAGIC ### 1) Average `insulin` level for cluster with `prediction` = `0` is `32.21`. 
# MAGIC ### 2) Average `insulin` level for cluster with `prediction` = `1` is `253.71`.
# MAGIC 
# MAGIC ![Diabetes-Analysis](https://img.huffingtonpost.com/asset/571e772b1900002e0056c26f.jpeg?cache=uhmcnbcaq7&ops=scalefit_720_noupscale)
# MAGIC 
# MAGIC People with higher `insulin` level can be clubbed to people in cluster `#2` above. This increases the efficacy of predicting a diabetic patient.