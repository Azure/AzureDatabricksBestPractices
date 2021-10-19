# Databricks notebook source
# MAGIC %md
# MAGIC ### Medicare FPS [Fraud Prevention System] 
# MAGIC #### Medicare doles out nearly $600 billion each year
# MAGIC Its estimated that 10% ($98 billion of Medicare and Medicaid spending) and up to $272 billion across the entire health system.  
# MAGIC Ref: [Economist 2014: The $272 billion swindle](http://www.economist.com/news/united-states/21603078-why-thieves-love-americas-health-care-system-272-billion-swindle).
# MAGIC 
# MAGIC If we could use Apache Spark to save .1% of $100 billion, we could save the government $100M.  
# MAGIC 
# MAGIC Let's looking into the public Medicare Claims data to do exploratory data analysis. We can look at other public data sets to link link these datasets together to find trends across the US. There can be a lot of different fraud claims but the most common appears to be pharmaceutical drug claims. Let's focus on this dataset to find trends. 
# MAGIC 
# MAGIC * [**Databricks Medicare FPS Analytics**](http://www.fiercehealthcare.com/it/paris-hospitals-use-predictive-modeling-to-control-admissions) is the use of data analytics and machine learning to identify the medicare Fraud and is...  
# MAGIC   * Built on top of Databricks Platform
# MAGIC   * Uses a machine learning **ALS recommendation Algorithm** implementation to generate recomendation on healthcare plans   
# MAGIC * This demo...  
# MAGIC   * demonstrates a healthcare recommendation analysis workflow.  We use Patient dataset from the [Health Plan Finder API](https://finder.healthcare.gov/#services/version_3_0) and internally mocked up data.

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

# DBTITLE 1,Step1: Ingest medicare Data to Notebook
# MAGIC %md 
# MAGIC 
# MAGIC - We will extracted the medicare dataset hosted at  [Centers for Medicare & Medicaid Services](https://catalog.data.gov/dataset/medicare-hospital-spending-by-claim-61b57)

# COMMAND ----------

# MAGIC %run ./data_Setup

# COMMAND ----------

from pyspark.sql.functions import * 

df = spark.read.options(header="true", inferSchema="true").csv("wasbs://dataset@wesdias.blob.core.windows.net/Azure/dataset/medicare/fraud/cms/2015-fraud").\
    withColumn("id", monotonically_increasing_id()).withColumn("claims_date", col("claim date").cast("string")).drop("claim date").\
    withColumnRenamed("existing condition", "existing_condition").withColumnRenamed("drug id", "drug_id").\
    withColumnRenamed("fraud reported", "fraud_reported")
display(df)

# COMMAND ----------

# dbutils.fs.rm("/mnt/mwc/cms/2015-fraud-p", True)
# df.coalesce(24).write.parquet("/mnt/mwc/cms/2015-fraud-p")

# COMMAND ----------

# MAGIC %md ###Step2: Explore the fraudulent claims costs

# COMMAND ----------

spark.read.parquet("wasbs://dataset@wesdias.blob.core.windows.net/Azure/dataset/medicare/fraud/cms_p/2015").createOrReplaceTempView("cms_2015")
spark.read.parquet("wasbs://dataset@wesdias.blob.core.windows.net/Azure/dataset/medicare/fraud/cms_2015_unit_price").createOrReplaceTempView("cms_unit_price")
df = spark.read.parquet("wasbs://dataset@wesdias.blob.core.windows.net/Azure/dataset/medicare/fraud/cms/2015-fraud-p")
df.createOrReplaceTempView("cms_fraud")

# COMMAND ----------

# MAGIC %sql
# MAGIC desc cms_unit_price

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from cms_fraud;

# COMMAND ----------

# MAGIC %sql
# MAGIC select a.drug_id, name_of_covered_drug_brand, name_of_covered_drug, avg_unit_price, avg_unit_price_raw, age, state, sex, education, fraud_reported
# MAGIC   from cms_fraud a
# MAGIC   join cms_unit_price b 
# MAGIC   on a.drug_id = b.drug_id 
# MAGIC   where fraud_reported = 'Y'
# MAGIC   order by avg_unit_price_raw desc
# MAGIC   

# COMMAND ----------

# MAGIC %md Count number of categories for every categorical column (Count Distinct).

# COMMAND ----------

# Create a List of Column Names with data type = string
stringColList = [i[0] for i in df.dtypes if i[1] == 'string']
[c for c in stringColList]

# COMMAND ----------

from pyspark.sql.functions import *
  
# Create a function that performs a countDistinct(colName) for string categories
distinctList = []
def countDistinctCats(colName):
  count = df.agg(countDistinct(colName)).collect()[0][0]
  distinctList.append((colName, count))

# Apply function on every column in stringColList
map(countDistinctCats, stringColList)
distinctList

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 3: Visualization
# MAGIC * Show the distribution of the incurred claim loss.

# COMMAND ----------

# Count number of frauds vs non-frauds
display(df.groupBy("fraud_reported").count())

# COMMAND ----------

# MAGIC %md #### Step 5: Model creation

# COMMAND ----------

from pyspark.ml.feature import StringIndexer

transformedCols = [categoricalCol + "Index" for categoricalCol in stringColList]
stages = [StringIndexer(inputCol = categoricalCol, outputCol = categoricalCol + "Index") for categoricalCol in stringColList]
stages

# COMMAND ----------

from pyspark.ml import Pipeline

indexer = Pipeline(stages=stages)
indexed = indexer.fit(df).transform(df)
display(indexed.select(transformedCols))

# COMMAND ----------

# MAGIC %md Use the VectorAssembler to combine all the feature columns into a single vector column. This will include both the numeric columns and the indexed categorical columns.

# COMMAND ----------

from pyspark.ml.feature import VectorAssembler

# In this dataset, numericColList will contain columns of type Int and Double
numericColList = [i[0] for i in df.dtypes if (i[1] != 'string' and i[1] != 'timestamp')]
assemblerInputs = map(lambda c: c + "Index", stringColList) + numericColList

# Remove label from list of features
label = "fraud_reportedIndex"
assemblerInputs.remove(label)
assemblerInputs

# COMMAND ----------

assembler = VectorAssembler(inputCols=assemblerInputs, outputCol="features")

# Append assembler to stages, which currently contains the StringIndexer transformers
stages += [assembler]

# COMMAND ----------

# MAGIC %md Generate transformed dataset. This will be the dataset that we will use to create our machine learning models.

# COMMAND ----------

pipeline = Pipeline(stages=stages)
pipelineModel = pipeline.fit(df)
transformed_df = pipelineModel.transform(df)

# Rename label column
transformed_df = transformed_df.withColumnRenamed('fraud_reportedIndex', 'label')

# Keep relevant columns (original columns, features, labels)
originalCols = df.columns
selectedcols = ["label", "fraud_reported", "features"] + originalCols
dataset = transformed_df.select(selectedcols)
display(dataset)

# COMMAND ----------

# MAGIC %md 
# MAGIC By selecting "label" and "fraud_reported", we can infer that 0 corresponds to **No Fraud Reported** and 1 corresponds to **Fraud Reported**.  
# MAGIC Next, split data into training and test sets.

# COMMAND ----------

### Randomly split data into training and test sets. set seed for reproducibility
(trainingData, testData) = dataset.randomSplit([0.7, 0.3], seed = 100)
print trainingData.count()
print testData.count()

# COMMAND ----------

# Register data as temp table to jump to Scala
trainingData.createOrReplaceTempView("trainingData")
testData.createOrReplaceTempView("testData")

# COMMAND ----------

# MAGIC %md ## Create Decision Tree Model
# MAGIC 
# MAGIC We will create a decision tree model in Scala using the trainingData. This will be our initial model.

# COMMAND ----------

from pyspark.ml import Pipeline
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.feature import StringIndexer, VectorIndexer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# Create DataFrames using our earlier registered temporary tables
trainingData = table("trainingData")
testData = table("testData")

# Create initial Decision Tree Model
dt = DecisionTreeClassifier(labelCol="label", featuresCol="features").setMaxDepth(5).setMaxBins(400)  

# Train model with Training Data
dtModel = dt.fit(trainingData)
predictions = dtModel.transform(testData)

# COMMAND ----------

selected = predictions.select("label", "prediction", "probability")
display(selected)

# COMMAND ----------

# MAGIC %md ## Measuring Error Rate
# MAGIC 
# MAGIC Evaluate our initial model using the BinaryClassificationEvaluator.

# COMMAND ----------

from pyspark.ml.evaluation import BinaryClassificationEvaluator

evaluator = BinaryClassificationEvaluator()
evaluator.evaluate(predictions)

# COMMAND ----------

# MAGIC %md The BinaryClassificationEvaluator uses **areaUnderROC** as a measure of error rate.

# COMMAND ----------

evaluator.getMetricName()

# COMMAND ----------

# MAGIC %md We can change the selected error metric to areaUnderPR - which is the area under the Precision Recall curve.

# COMMAND ----------

evaluator.setMetricName("areaUnderPR")
evaluator.evaluate(predictions)

# COMMAND ----------

# MAGIC %md #### Step 6: Model Tuning
# MAGIC 
# MAGIC We can tune our models using built-in libraries like `ParamGridBuilder` for Grid Search, and `CrossValidator` for Cross Validation. In this example, we will test out a combination of Grid Search with 5-fold Cross Validation.  
# MAGIC 
# MAGIC Here, we will see if we can improve accuracy rates from our initial model.

# COMMAND ----------

from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

paramGrid = ParamGridBuilder()\
  .addGrid(dt.maxDepth, [3, 10, 15])\
  .addGrid(dt.maxBins, [365, 400, 450])\
  .build()

# COMMAND ----------

cv = CrossValidator()\
  .setEstimator(dt)\
  .setEvaluator(evaluator)\
  .setEstimatorParamMaps(paramGrid)\
  .setNumFolds(5)

# Run cross validations
cvModel = cv.fit(trainingData)

# COMMAND ----------

bestTreeModel = cvModel.bestModel
print cvModel.bestModel.toDebugString

# COMMAND ----------

cvPredictions = cvModel.transform(testData)
evaluator.setMetricName("areaUnderROC")
evaluator.evaluate(cvPredictions)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Results interpretation
# MAGIC The plot above shows Index used to measure each of the patent admission days. 
# MAGIC 
# MAGIC ![Churn-Index](http://www.ngdata.com/wp-content/uploads/2016/05/churn.jpg)
# MAGIC 
# MAGIC 1. The loss reserve evaluation index using logistic regression algorithm gives and accuracy of ~99%
# MAGIC 2. Using this method we can predict the loss reserve for the lower half of the loss triangle using data features like Posted reserve and premium