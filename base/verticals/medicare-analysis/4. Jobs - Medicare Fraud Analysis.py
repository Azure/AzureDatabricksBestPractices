# Databricks notebook source
# MAGIC %md
# MAGIC ### Medicare FPS [Fraud Prevention System] 
# MAGIC 
# MAGIC Although analyzing claims data to tailor program integrity efforts is not new, matching Medicare and Medicaid claims data is improving the outcome of these efforts. Sharing and comparing billings from both programs has proven successful in identifying patterns of fraud, previously undetectable, to the individual programs. [AHIMA](http://library.ahima.org/doc?oid=71891)
# MAGIC 
# MAGIC There can be a lot of different fraud claims but the most common appears to be pharmaceutical drug claims. Let's focus on this dataset to find trends. 
# MAGIC 
# MAGIC Benefits of Spark:
# MAGIC * Single data lake for processing across users and use cases
# MAGIC * Optimized for repeated analytics and machine learning workloads
# MAGIC * Optimal file sizes and types for reduced cost
# MAGIC * Elastic infrastructure to reduce TCO and improve efficiency by decoupling storage and compute

# COMMAND ----------

# Load the data
spark.read.parquet("/mnt/mwc/cms_p/2015").createOrReplaceTempView("cms_2015")
spark.read.parquet("/mnt/mwc/cms_2015_unit_price").createOrReplaceTempView("cms_unit_price")
df = spark.read.parquet("/mnt/mwc/cms/2015-fraud-p")
df.createOrReplaceTempView("cms_fraud")

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC   Name_of_Associated_Covered_Drug_or_Biological1, 
# MAGIC   count(1) as counts 
# MAGIC   from cms_2015 
# MAGIC   group by Name_of_Associated_Covered_Drug_or_Biological1 
# MAGIC   order by counts desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC   NDC_of_Associated_Covered_Drug_or_Biological1, 
# MAGIC   count(1) as counts 
# MAGIC   from cms_2015 
# MAGIC   group by NDC_of_Associated_Covered_Drug_or_Biological1 
# MAGIC   order by counts desc

# COMMAND ----------

# MAGIC %md
# MAGIC ### Analyze the fraudulent claims costs

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

# MAGIC %md ## Data Exploration
# MAGIC 
# MAGIC We have several string (categorical) columns in our dataset, along with some ints a timestamp.

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

# Count number of frauds vs non-frauds
display(df.groupBy("fraud_reported").count())

# COMMAND ----------

# Fraud Count by Incident State
display(df.select("state", "fraud_reported"))

# COMMAND ----------

from pyspark.ml.feature import StringIndexer

transformedCols = [categoricalCol + "Index" for categoricalCol in stringColList]
stages = [StringIndexer(inputCol = categoricalCol, outputCol = categoricalCol + "Index") for categoricalCol in stringColList]
stages

# COMMAND ----------

transformedCols

# COMMAND ----------

# MAGIC %md As an example, this is what the transformed dataset will look like after applying the StringIndexer on all categorical columns.

# COMMAND ----------

display(df.select(stringColList))

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

# COMMAND ----------

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

# MAGIC %md ## Model Tuning
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
# MAGIC ### Re-test model

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.ml import Pipeline
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.feature import StringIndexer, VectorIndexer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.feature import VectorAssembler

stringColList = [i[0] for i in df.dtypes if i[1] == 'string']

transformedCols = [categoricalCol + "Index" for categoricalCol in stringColList if categoricalCol != "claims_date"]
stages = [StringIndexer(inputCol = categoricalCol, outputCol = categoricalCol + "Index") for categoricalCol in stringColList if categoricalCol != "claims_date"]

# In this dataset, numericColList will contain columns of type Int and Double
numericColList = [i[0] for i in df.dtypes if (i[1] != 'string' and i[1] != 'timestamp')]
assemblerInputs = map(lambda c: c + "Index", stringColList) + numericColList

# Remove label from list of features
label = "fraud_reportedIndex"
assemblerInputs.remove(label)
assemblerInputs.remove("claims_dateIndex")
assemblerInputs

# COMMAND ----------

assembler = VectorAssembler(inputCols=assemblerInputs, outputCol="features")

# Append assembler to stages, which currently contains the StringIndexer transformers
stages += [assembler]

# COMMAND ----------

pipeline = Pipeline(stages=stages)
pipelineModel = pipeline.fit(df)
transformed_df = pipelineModel.transform(df)

# Rename label column
transformed_df = transformed_df.withColumnRenamed('fraud_reportedIndex', 'label')

# Keep relevant columns (original columns, features, labels)
originalCols = [x for x in df.columns if x != "claims_date"]
selectedcols = ["label", "fraud_reported", "features"] + originalCols
dataset = transformed_df.select(selectedcols)
display(dataset)

# COMMAND ----------

### Randomly split data into training and test sets. set seed for reproducibility
(trainingData, testData) = dataset.randomSplit([0.7, 0.3], seed = 100)
trainingData.createOrReplaceTempView("trainingData")
testData.createOrReplaceTempView("testData")

# Create initial Decision Tree Model
dt2 = DecisionTreeClassifier(labelCol="label", featuresCol="features").setMaxDepth(5).setMaxBins(275)  

# Train model with Training Data
dtModel2 = dt2.fit(trainingData.drop("claims_data"))

# COMMAND ----------

predictions = dtModel2.transform(testData)
evaluator = BinaryClassificationEvaluator()
evaluator.evaluate(predictions)

# COMMAND ----------

paramGrid = ParamGridBuilder()\
  .addGrid(dt2.maxDepth, [10, 15, 20])\
  .addGrid(dt2.maxBins, [180, 200, 250])\
  .build()
  
cv = CrossValidator()\
  .setEstimator(dt2)\
  .setEvaluator(evaluator)\
  .setEstimatorParamMaps(paramGrid)\
  .setNumFolds(5)

# Run cross validations
cvModel = cv.fit(trainingData)

# COMMAND ----------

cvPredictions = cvModel.transform(testData)
evaluator.evaluate(cvPredictions)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Calculate total fraud $$ amount

# COMMAND ----------

def convert_human_readable(total):
  import locale
  locale.setlocale( locale.LC_ALL, '' )
  try:
    return locale.currency( total, grouping=True )
  except:
    return 0

spark.udf.register("convert_human_readable", convert_human_readable)

# COMMAND ----------

# MAGIC %sql
# MAGIC select convert_human_readable(sum(avg_unit_price_raw)) as total_claimed, fraud_reported, count(1) as num_of_claims
# MAGIC   from cms_fraud a
# MAGIC   join cms_unit_price b 
# MAGIC   on a.drug_id = b.drug_id 
# MAGIC   group by fraud_reported 
# MAGIC   