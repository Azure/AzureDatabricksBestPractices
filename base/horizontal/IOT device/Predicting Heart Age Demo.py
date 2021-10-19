# Databricks notebook source
# MAGIC %md #Predicting Heart Age
# MAGIC 
# MAGIC ![Fitness ML](/files/img/QBR_ML_Banner.png)

# COMMAND ----------

# DBTITLE 1,0. Setup Databricks Spark cluster:
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

# DBTITLE 1,Step1: Ingest IoT Data to Notebook
# MAGIC %md 
# MAGIC 
# MAGIC - We use internal generated dataset for heartbeat measuring device

# COMMAND ----------

# MAGIC %run ./data_Setup

# COMMAND ----------

# DBTITLE 1,Ingest User & Device Data

from pyspark.sql.types import StringType

userData = sqlContext.read.format('csv').options(header='true', inferSchema='true').load('wasbs://dataset@wesdias.blob.core.windows.net/Azure/dataset/iot/userData.csv')
deviceData = sqlContext.read.format('csv').options(header='true', inferSchema='true').load('wasbs://dataset@wesdias.blob.core.windows.net/Azure/dataset/iot/device.csv')
data = userData.join(deviceData, userData.userid == deviceData.user_id).drop(deviceData.user_id)
data = data.withColumn("riskLabel", data["risk"].cast(StringType()))
data.cache()

# COMMAND ----------

# DBTITLE 1,Step 2: Data Exploration
#display(userData)
display(deviceData)

# COMMAND ----------

# DBTITLE 1,Data Prep & Feature Engineering
from pyspark.ml.feature import StringIndexer
from pyspark.ml import Pipeline

# Select relevant columns
df = data.drop("device_id").drop("time_stamp").drop("risk")

stringColList = [i[0] for i in df.dtypes if i[1] == 'string']
transformedCols = [categoricalCol + "Index" for categoricalCol in stringColList]
stages = [StringIndexer(inputCol = categoricalCol, outputCol = categoricalCol + "Index") for categoricalCol in stringColList]
indexer = Pipeline(stages=stages)
indexed = indexer.fit(df).transform(df)

from pyspark.ml.feature import VectorAssembler

#Use the VectorAssembler to combine all the feature columns into a single vector column. 
#This will include both the numeric columns and the indexed categorical columns.

# In this dataset, numericColList will contain columns of type Int and Double
numericColList = [i[0] for i in df.dtypes if i[1] != 'string']
numericColList.remove('userid')
assemblerInputs = map(lambda c: c + "Index", stringColList) + numericColList

# Remove label from list of features
label = "riskLabelIndex"
assemblerInputs.remove(label)
assemblerInputs
assembler = VectorAssembler(inputCols=assemblerInputs, outputCol="features")

# Append assembler to stages, which currently contains the StringIndexer transformers
stages += [assembler]

#Generate transformed dataset. 
#This will be the dataset that we will use to create our machine learning models.

pipeline = Pipeline(stages=stages)
pipelineModel = pipeline.fit(df)
transformed_df = pipelineModel.transform(df)

# Rename label column
transformed_df = transformed_df.withColumnRenamed('riskLabelIndex', 'label')

# Keep relevant columns (original columns, features, labels)
originalCols = df.columns
selectedcols = ["label", "riskLabel", "features"] + originalCols
dataset = transformed_df.select(selectedcols)

### Randomly split data into training and test sets. set seed for reproducibility
(trainingData, testData) = dataset.randomSplit([0.7, 0.3], seed = 100)

# Register data as temp table to jump to Scala
trainingData.registerTempTable("trainingData")
testData.registerTempTable("testData")

display(dataset)

# COMMAND ----------

# DBTITLE 1,Step 3: Create Decision Tree Model (using Scala)
# MAGIC %scala
# MAGIC 
# MAGIC import org.apache.spark.ml.classification.{DecisionTreeClassifier, DecisionTreeClassificationModel}
# MAGIC 
# MAGIC // Create DataFrames using our earlier registered temporary tables
# MAGIC val trainingData = sqlContext.table("trainingData")
# MAGIC val testData = sqlContext.table("testData")
# MAGIC 
# MAGIC // Create initial Decision Tree Model
# MAGIC val dt = new DecisionTreeClassifier()
# MAGIC   .setLabelCol("label")
# MAGIC   .setFeaturesCol("features")
# MAGIC   .setMaxDepth(10)
# MAGIC   .setMaxBins(32)
# MAGIC 
# MAGIC // Train model with Training Data
# MAGIC val dtModel = dt.fit(trainingData)
# MAGIC 
# MAGIC // Make predictions on test data using the transform() method.
# MAGIC // .transform() will only use the 'features' column as input.
# MAGIC val predictions = dtModel.transform(testData)
# MAGIC 
# MAGIC // View model's predictions and probabilities of each prediction class
# MAGIC //val selected = predictions.select("label", "prediction", "riskLabel", "probability", "userid")
# MAGIC //display(selected)
# MAGIC 
# MAGIC val avgHeartAge = (predictions.groupBy("userid").avg("prediction", "num_steps", "miles_walked", "calories_burnt")
# MAGIC                        .withColumnRenamed("avg(prediction)", "avgPrediction")
# MAGIC                        .withColumnRenamed("avg(num_steps)", "avgSteps")
# MAGIC                        .withColumnRenamed("avg(miles_walked)", "avgMiles")
# MAGIC                        .withColumnRenamed("avg(calories_burnt)", "avgCalories"))
# MAGIC avgHeartAge.createOrReplaceTempView("avgHeartAge")

# COMMAND ----------

# MAGIC %sql select avgPrediction, count(*) as count from avgHeartAge group by avgPrediction order by avgPrediction

# COMMAND ----------

# DBTITLE 1,Join Prediction Data to User Data (back to Python)
from pyspark.sql.types import IntegerType

labelMappings = dataset.select("label", "riskLabel").distinct()

avgHeartAge = table("avgHeartAge")
predictionResults = (avgHeartAge.join(userData, userData.userid == avgHeartAge.userid)
                                .drop(avgHeartAge.userid)
                                .drop(userData.risk)
                                .join(labelMappings, avgHeartAge.avgPrediction == labelMappings.label))
predResults = predictionResults.withColumn("numericPrediction", predictionResults["riskLabel"].cast(IntegerType()))
predResults2 = predResults.withColumn("heartAge", predResults["numericPrediction"] + predResults["age"])
display(predResults2)

# COMMAND ----------

# MAGIC %md ## Step 4: Analyze Prediction Data
# MAGIC - We make use of adult table which is an external table created i.e. CREATE TABLE IF NOT EXISTS adult (age double,workclass string,fnlwgt double,education string,education_num double,marital_status string,occupation string,relationship string,race string,sex string,capital_gain double,capital_loss double,hours_per_week double,native_country string,income string) USING CSV OPTIONS(path='/mnt/wesley/dataset/iot/adult.data')

# COMMAND ----------

# MAGIC %scala 
# MAGIC val df = spark.table("adult")

# COMMAND ----------

df = spark.sql("select income, count(*) from adult group by income")



# COMMAND ----------

# MAGIC %sql select income, count(*) from adult group by income

# COMMAND ----------

# MAGIC %sql 
# MAGIC select income, count(*) as count from adult group by income

# COMMAND ----------

display(predResults2.orderBy("weight"))

# COMMAND ----------

# Sum of avgSteps breakdown by heart age bucket and blood pressure
display(predResults2.orderBy("numericPrediction"))

# COMMAND ----------

# Breakdown of heart age buckets by cholesterol levels
display(predResults.orderBy("numericPrediction"))

# COMMAND ----------

# Breakdown of heart age buckets by smoker/non-smoker
display(predResults.orderBy("numericPrediction"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Results interpretation
# MAGIC 
# MAGIC ![Recomendation-Index](http://trouvus.com/wp-content/uploads/2016/03/2.1.png)