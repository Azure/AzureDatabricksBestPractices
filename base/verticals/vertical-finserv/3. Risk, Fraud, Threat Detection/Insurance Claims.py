# Databricks notebook source
# MAGIC %md # Insurance Claims - Fraud Detection
# MAGIC **Business case:**  
# MAGIC Insurance fraud is a huge problem in the industry. It's difficult to identify fraud claims.   
# MAGIC 
# MAGIC **Problem Statement:**  
# MAGIC Data is stored in different systems and its difficult to build analytics using multiple data sources. Copying data into a single platform is time consuming.  
# MAGIC 
# MAGIC **Business solution:**  
# MAGIC Use S3 as a data lake to store different sources of data in a single platform. This allows data scientists / analysis to quickly analyze the data and generate reports to predict market trends and/or make financial decisions.  
# MAGIC 
# MAGIC **Technical Solution:**  
# MAGIC Use Databricks as a single platform to pull various sources of data from API endpoints, or batch dumps into S3 for further processing. ETL the CSV datasets into efficient Parquet formats for performant processing.  
# MAGIC 
# MAGIC Owner: Jason Pohl  
# MAGIC Runnable: Yes  
# MAGIC Last Tested Spark Version: Spark 2.0  

# COMMAND ----------

# MAGIC %md
# MAGIC In this example, we will be working with some auto insurance data to demonstrate how we can create a predictive model that predicts if an insurance claim is fraudulent or not. This will be a Binary Classification task, and we will be creating a Decision Tree model.
# MAGIC 
# MAGIC With the prediction data, we are able to estimate what our total predicted fradulent claim amount is like, and zoom into various features such as a breakdown of predicted fraud count by insured hobbies - our model's best predictor.
# MAGIC       
# MAGIC We will cover the following steps to illustrate how we build a Machine Learning Pipeline:
# MAGIC * Data Import
# MAGIC * Data Exploration
# MAGIC * Data Processing
# MAGIC * Create Decision Tree Model
# MAGIC * Measuring Error Rate
# MAGIC * Model Tuning
# MAGIC * Zooming in on Prediction Data

# COMMAND ----------

# MAGIC %md
# MAGIC ###0. SETUP -- Databricks Spark cluster:  
# MAGIC 
# MAGIC 1. **Create** a cluster by...  
# MAGIC   - Click the `Clusters` icon on the left sidebar and then `Create Cluster.` OR Click [Here](https://demo.cloud.databricks.com/#clusters/create) 
# MAGIC   - Enter any text, i.e `demo` into the cluster name text box
# MAGIC   - Select the `Apache Spark Version` value `Spark 2.2 (auto-updating scala 2.11)`  
# MAGIC   - Click the `create cluster` button and wait for your cluster to be provisioned
# MAGIC 3. **Attach** this notebook to your cluster by...   
# MAGIC   - Click on your cluster name in menu `Detached` at the top left of this workbook to attach it to this workbook 

# COMMAND ----------

# DBTITLE 1,Step1: Ingest Insurance plan Data to Notebook
# MAGIC %md 
# MAGIC 
# MAGIC - We use internal generated dataset of user insurance plan
# MAGIC - The data used in this example was from a CSV file that was imported using the Tables UI.
# MAGIC - After uploading the data using the UI, we can run SparkSQL queries against the table, or create a DataFrame from the table.  
# MAGIC In this example, we will create a Spark DataFrame.

# COMMAND ----------

data = spark.read.format("csv")\
          .options(inferSchema="true", header="true")\
          .load("/mnt/raela/insurance_claims3.csv")\
          .drop("_c39")\

df = data.withColumn("policy_bind_date", data.policy_bind_date.cast("string"))\
         .withColumn("incident_date", data.incident_date.cast("string"))

# COMMAND ----------

# Preview data
display(df)

# COMMAND ----------

# DBTITLE 1,Step2: Explore insurance plan Data 
# MAGIC %md ## We have several string (categorical) columns in our dataset, along with some ints and doubles.

# COMMAND ----------

display(df.dtypes)

# COMMAND ----------

# MAGIC %md Count number of categories for every categorical column (Count Distinct).

# COMMAND ----------

# Create a List of Column Names with data type = string
stringColList = [i[0] for i in df.dtypes if i[1] == 'string']
print stringColList

# COMMAND ----------

from pyspark.sql.functions import *

# Create a function that performs a countDistinct(colName)
distinctList = []
def countDistinctCats(colName):
  count = df.agg(countDistinct(colName)).collect()
  distinctList.append(count)

# COMMAND ----------

# Apply function on every column in stringColList
map(countDistinctCats, stringColList)
print distinctList

# COMMAND ----------

# MAGIC %md 
# MAGIC We have identified that some string columns have many distinct values (900+). We will remove these columns from our dataset in the Data Processing step to improve model accuracy.
# MAGIC * policy number (1000 distinct)
# MAGIC * policy bind date (935 distinct. Possible to narrow down to year/month to test model accuracy)
# MAGIC * insured zip (995 distinct)
# MAGIC * insured location (1000 distinct)
# MAGIC * incident date (60 distinct. Excluding, but possible to narrow down to year/month to test model accuracy)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 3: Visualization
# MAGIC * Show the distribution of the account length.

# COMMAND ----------

# MAGIC %md Like most fraud datasets, our label distribution is skewed.

# COMMAND ----------

display(df)

# COMMAND ----------

# Count number of frauds vs non-frauds
display(df.groupBy("fraud_reported").count())

# COMMAND ----------

# MAGIC %md We can quickly create one-click plots using Databricks built-in visualizations to understand our data better.
# MAGIC 
# MAGIC Click 'Plot Options' to try out different chart types.

# COMMAND ----------

# Fraud Count by Incident State
display(df)

# COMMAND ----------

# Breakdown of Average Vehicle claim by insured's education level, grouped by fraud reported
display(df)

# COMMAND ----------

# MAGIC %md ## Data Processing
# MAGIC 
# MAGIC Next, we will clean up the data a little and prepare it for our machine learning model.
# MAGIC 
# MAGIC We will first remove the columns that we have identified earlier that have too many distinct categories and cannot be converted to numeric.

# COMMAND ----------

colsToDelete = ["policy_number", "policy_bind_date", "insured_zip", "incident_location", "incident_date"]
filteredStringColList = [i for i in stringColList if i not in colsToDelete]

# COMMAND ----------

# MAGIC %md %md We will convert categorical columns to numeric to pass them into various algorithms. This can be done using the StringIndexer.
# MAGIC 
# MAGIC Here, we are generating a StringIndexer for each categorical column and appending it as a stage of our ML Pipeline.

# COMMAND ----------

from pyspark.ml.feature import StringIndexer

transformedCols = [categoricalCol + "Index" for categoricalCol in filteredStringColList]
stages = [StringIndexer(inputCol = categoricalCol, outputCol = categoricalCol + "Index") for categoricalCol in filteredStringColList]
stages

# COMMAND ----------

# MAGIC %md As an example, this is what the transformed dataset will look like after applying the StringIndexer on all categorical columns.

# COMMAND ----------

from pyspark.ml import Pipeline

indexer = Pipeline(stages=stages)
indexed = indexer.fit(df).transform(df)
display(indexed)

# COMMAND ----------

# MAGIC %md Use the VectorAssembler to combine all the feature columns into a single vector column. This will include both the numeric columns and the indexed categorical columns.

# COMMAND ----------

from pyspark.ml.feature import VectorAssembler

# In this dataset, numericColList will contain columns of type Int and Double
numericColList = [i[0] for i in df.dtypes if i[1] != 'string']
assemblerInputs = map(lambda c: c + "Index", filteredStringColList) + numericColList

# Remove label from list of features
label = "fraud_reportedIndex"
assemblerInputs.remove(label)
assemblerInputs
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

# MAGIC %md By selecting "label" and "fraud reported", we can infer that 0 corresponds to **No Fraud Reported** and 1 corresponds to **Fraud Reported**.

# COMMAND ----------

# MAGIC %md Next, split data into training and test sets.

# COMMAND ----------

### Randomly split data into training and test sets. set seed for reproducibility
(trainingData, testData) = dataset.randomSplit([0.7, 0.3], seed = 100)
print trainingData.count()
print testData.count()

# COMMAND ----------

# MAGIC %md Databricks makes it easy to use multiple languages in the same notebook for your analyses. Just register your dataset as a temporary table and you can access it using a different language!

# COMMAND ----------

# Register data as temp table to jump to Scala
trainingData.createOrReplaceTempView("trainingData")
testData.createOrReplaceTempView("testData")

# COMMAND ----------

# MAGIC %md #### Step 4: Model creation
# MAGIC - Create Decision Tree Model. We will create a decision tree model in Scala using the trainingData. This will be our initial model.

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC import org.apache.spark.ml.classification.{DecisionTreeClassifier, DecisionTreeClassificationModel}
# MAGIC 
# MAGIC // Create DataFrames using our earlier registered temporary tables
# MAGIC val trainingData = spark.table("trainingData")
# MAGIC val testData = spark.table("testData")
# MAGIC 
# MAGIC // Create initial Decision Tree Model
# MAGIC val dt = new DecisionTreeClassifier()
# MAGIC   .setLabelCol("label")
# MAGIC   .setFeaturesCol("features")
# MAGIC   .setMaxDepth(5)
# MAGIC   .setMaxBins(40)
# MAGIC 
# MAGIC // Train model with Training Data
# MAGIC val dtModel = dt.fit(trainingData)

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC // Make predictions on test data using the transform() method.
# MAGIC // .transform() will only use the 'features' column as input.
# MAGIC val predictions = dtModel.transform(testData)

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC // View model's predictions and probabilities of each prediction class
# MAGIC val selected = predictions.select("label", "prediction", "probability")
# MAGIC display(selected)

# COMMAND ----------

# MAGIC %md ## Measuring Error Rate
# MAGIC 
# MAGIC Evaluate our initial model using the BinaryClassificationEvaluator.

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
# MAGIC 
# MAGIC // Evaluate model
# MAGIC 
# MAGIC val evaluator = new BinaryClassificationEvaluator()
# MAGIC evaluator.evaluate(predictions)

# COMMAND ----------

# MAGIC %md The BinaryClassificationEvaluator uses **areaUnderROC** as a measure of error rate.

# COMMAND ----------

# MAGIC %scala
# MAGIC evaluator.getMetricName

# COMMAND ----------

# MAGIC %md We can change the selected error metric to areaUnderPR - which is the area under the Precision Recall curve.

# COMMAND ----------

# MAGIC %scala
# MAGIC evaluator.setMetricName("areaUnderPR")

# COMMAND ----------

# MAGIC %md ## Model Tuning
# MAGIC 
# MAGIC We can tune our models using built-in libraries like `ParamGridBuilder` for Grid Search, and `CrossValidator` for Cross Validation. In this example, we will test out a combination of Grid Search with 5-fold Cross Validation.  
# MAGIC 
# MAGIC Here, we will see if we can improve accuracy rates from our initial model.

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC // View tunable parameters for Decision Trees
# MAGIC dt.explainParams

# COMMAND ----------

# MAGIC %md Create a ParamGrid to perform Grid Search. We will be adding various values of maxDepth and maxBins.

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC import org.apache.spark.ml.tuning.{ParamGridBuilder, CrossValidator}
# MAGIC 
# MAGIC val paramGrid = new ParamGridBuilder()
# MAGIC   .addGrid(dt.maxDepth, Array(3, 10, 15))
# MAGIC   .addGrid(dt.maxBins, Array(40, 50))
# MAGIC   .build()

# COMMAND ----------

# MAGIC %md Perform 5-fold Cross Validation.

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC // Create 5-fold CrossValidator
# MAGIC val cv = new CrossValidator()
# MAGIC   .setEstimator(dt)
# MAGIC   .setEvaluator(evaluator)
# MAGIC   .setEstimatorParamMaps(paramGrid)
# MAGIC   .setNumFolds(5)
# MAGIC 
# MAGIC // Run cross validations
# MAGIC val cvModel = cv.fit(trainingData)

# COMMAND ----------

# MAGIC %md We can print out what our Tree Model looks like using `toDebugString`.

# COMMAND ----------

# MAGIC %scala 
# MAGIC 
# MAGIC val bestTreeModel = cvModel.bestModel.asInstanceOf[DecisionTreeClassificationModel]
# MAGIC println("Learned classification tree model:\n" + bestTreeModel.toDebugString)

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC // Use test set here so we can measure the accuracy of our model on new data
# MAGIC val cvPredictions = cvModel.transform(testData)

# COMMAND ----------

# MAGIC %md Using the same evaluator as before, we can see that Cross Validation improved our model's accuracy from 0.779 to 0.837!

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC // cvModel uses the best model found from the Cross Validation
# MAGIC // Evaluate best model
# MAGIC evaluator.evaluate(cvPredictions)

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC display(bestTreeModel)

# COMMAND ----------

# MAGIC %md We know that feature 5 is our Decision Tree model's root node. Let's see what that corresponds to.

# COMMAND ----------

assemblerInputs[5]

# COMMAND ----------

# MAGIC %md Turns out that `insured hobbies` is the best predictor for whether an insurance claim is fraudulent or not!

# COMMAND ----------

# MAGIC %md ## Step6: Zooming in on Prediction Data
# MAGIC 
# MAGIC We can further analyze the resulting prediction data. As an example, we can view an estimate of what our total predicted fradulent claim amount is like, and zoom into a breakdown of predicted fraud count by insured hobbies since that's our model's best predictor.
# MAGIC 
# MAGIC Lets hop back to Python for this.

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC cvPredictions.createOrReplaceTempView("cvPredictions")

# COMMAND ----------

# Select columns to zoom into (In this example: Total Claim Amount and Auto Make)
# Filter for data points that were predicted to be Fraud cases
cvPredictions = sqlContext.sql("SELECT * FROM cvPredictions")
targetDF = cvPredictions.select("prediction", "total_claim_amount", "insured_hobbies").filter("prediction = 1")

# COMMAND ----------

# View Count of Predicted Fraudulent Claims by Insured Hobbies
display(targetDF)

# COMMAND ----------

# MAGIC %md Looks like people who play chess or are into cross-fit are more prone to committing fraud.

# COMMAND ----------

# View Predicted Total Fraudulent Claims by Insured Hobbies
display(targetDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Results interpretation
# MAGIC 
# MAGIC The table shown above gives the top ten recomended healthcare plans for the user based on the predicted outcomes using the healthcare plans demographics and the ratings provided by the user
# MAGIC 
# MAGIC ![Recomendation-Index](http://trouvus.com/wp-content/uploads/2016/03/2.1.png)