# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC #Evaluating Risk for Loan Approvals
# MAGIC 
# MAGIC ## Business Value
# MAGIC 
# MAGIC Being able to accurately assess the risk of a loan application can save a lender the cost of holding too many risky assets. Rather than a credit score or credit history which tracks how reliable borrowers are, we will generate a score of how profitable a loan will be compared to other loans in the past. The combination of credit scores, credit history, and profitability score will help increase the bottom line for financial institution.
# MAGIC 
# MAGIC Having a interporable model that a loan officer can use before performing a full underwriting can provide immediate estimate and response for the borrower and a informative view for the lender.
# MAGIC 
# MAGIC <a href="https://ibb.co/cuQYr6"><img src="https://preview.ibb.co/jNxPym/Image.png" alt="Image" border="0"></a>
# MAGIC 
# MAGIC This notebook has been tested with *DBR 5.4 ML Beta, Python 3*

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## The Data
# MAGIC 
# MAGIC The data used is public data from Lending Club. It includes all funded loans from 2012 to 2017. Each loan includes applicant information provided by the applicant as well as the current loan status (Current, Late, Fully Paid, etc.) and latest payment information. For a full view of the data please view the data dictionary available [here](https://resources.lendingclub.com/LCDataDictionary.xlsx).
# MAGIC 
# MAGIC 
# MAGIC ![Loan_Data](https://preview.ibb.co/d3tQ4R/Screen_Shot_2018_02_02_at_11_21_51_PM.png)
# MAGIC 
# MAGIC https://www.kaggle.com/wendykan/lending-club-loan-data

# COMMAND ----------

# MAGIC %md ### Databricks MLflow Integration
# MAGIC Uncomment the next cell to showcase Databricks MLflow Integration.  Note, this currently does not work in Databricks Community Edition.

# COMMAND ----------

import mlflow
print(mlflow.__version__)

spark.conf.set("spark.databricks.mlflow.trackMLlib.enabled", "true")

# COMMAND ----------

# DBTITLE 1,Import Data
# Configure location of loanstats_2012_2017.parquet
lspq_path = "/databricks-datasets/samples/lending_club/parquet/"

# Read loanstats_2012_2017.parquet
data = spark.read.parquet(lspq_path)

# Reduce the amount of data (to run on DBCE)
(loan_stats_ce, loan_stats_rest) = data.randomSplit([0.025, 0.975], seed=123)

# Select only the columns needed
loan_stats_ce = loan_stats_ce.select("loan_status", "int_rate", "revol_util", "issue_d", "earliest_cr_line", "emp_length", "verification_status", "total_pymnt", "loan_amnt", "grade", "annual_inc", "dti", "addr_state", "term", "home_ownership", "purpose", "application_type", "delinq_2yrs", "total_acc")

# Print out number of loans
print(str(loan_stats_ce.count()) + " loans opened by Lending Club...")

# COMMAND ----------

# DBTITLE 1,Filter Data and Fix Schema
from pyspark.sql.functions import *

print("------------------------------------------------------------------------------------------------")
print("Create bad loan label, this will include charged off, defaulted, and late repayments on loans...")
loan_stats_ce = loan_stats_ce.filter(loan_stats_ce.loan_status.isin(["Default", "Charged Off", "Fully Paid"]))\
                       .withColumn("bad_loan", (~(loan_stats_ce.loan_status == "Fully Paid")).cast("string"))


print("------------------------------------------------------------------------------------------------")
print("Turning string interest rate and revoling util columns into numeric columns...")
loan_stats_ce = loan_stats_ce.withColumn('int_rate', regexp_replace('int_rate', '%', '').cast('float')) \
                       .withColumn('revol_util', regexp_replace('revol_util', '%', '').cast('float')) \
                       .withColumn('issue_year',  substring(loan_stats_ce.issue_d, 5, 4).cast('double') ) \
                       .withColumn('earliest_year', substring(loan_stats_ce.earliest_cr_line, 5, 4).cast('double'))
loan_stats_ce = loan_stats_ce.withColumn('credit_length_in_years', (loan_stats_ce.issue_year - loan_stats_ce.earliest_year))


print("------------------------------------------------------------------------------------------------")
print("Converting emp_length column into numeric...")
loan_stats_ce = loan_stats_ce.withColumn('emp_length', trim(regexp_replace(loan_stats_ce.emp_length, "([ ]*+[a-zA-Z].*)|(n/a)", "") ))
loan_stats_ce = loan_stats_ce.withColumn('emp_length', trim(regexp_replace(loan_stats_ce.emp_length, "< 1", "0") ))
loan_stats_ce = loan_stats_ce.withColumn('emp_length', trim(regexp_replace(loan_stats_ce.emp_length, "10\\+", "10") ).cast('float'))

# COMMAND ----------

# MAGIC %md ## ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Easily Convert Parquet to Delta Lake format
# MAGIC With Delta Lake, you can easily transform your Parquet data into Delta Lake format. 

# COMMAND ----------

# Configure Path
DELTALAKE_GOLD_PATH = "/ml/loan_stats.delta"

# Remove table if it exists
dbutils.fs.rm(DELTALAKE_GOLD_PATH, recurse=True)

# Save table as Delta Lake
loan_stats_ce.write.format("delta").mode("overwrite").save(DELTALAKE_GOLD_PATH)

# Re-read as Delta Lake
loan_stats = spark.read.format("delta").load(DELTALAKE_GOLD_PATH)

# Review data
display(loan_stats)

# COMMAND ----------

# DBTITLE 1,Assert Allocation
display(loan_stats)

# COMMAND ----------

# DBTITLE 1,Munge Data
print("------------------------------------------------------------------------------------------------")
print("Map multiple levels into one factor level for verification_status...")
loan_stats = loan_stats.withColumn('verification_status', trim(regexp_replace(loan_stats.verification_status, 'Source Verified', 'Verified')))

print("------------------------------------------------------------------------------------------------")
print("Calculate the total amount of money earned or lost per loan...")
loan_stats = loan_stats.withColumn('net', round( loan_stats.total_pymnt - loan_stats.loan_amnt, 2))

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Delta Lake Logo Tiny](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Schema Evolution
# MAGIC With the `mergeSchema` option, you can evolve your Delta Lake table schema

# COMMAND ----------

# Add the mergeSchema option
loan_stats.write.option("mergeSchema","true").format("delta").mode("overwrite").save(DELTALAKE_GOLD_PATH)

# COMMAND ----------

# Original Schema
loan_stats_ce.printSchema()

# COMMAND ----------

# New Schema
loan_stats.printSchema()

# COMMAND ----------

# MAGIC %md ### ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Review Delta Lake Table History
# MAGIC All the transactions for this table are stored within this table including the initial set of insertions, update, delete, merge, and inserts with schema modification

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS loan_stats")
spark.sql("CREATE TABLE loan_stats USING DELTA LOCATION '" + DELTALAKE_GOLD_PATH + "'")

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY loan_stats

# COMMAND ----------

# DBTITLE 1,Feature Distribution and Correlation
display(loan_stats)

# COMMAND ----------

# DBTITLE 1,Loans Per State
display(loan_stats.groupBy("addr_state").agg((count(col("annual_inc"))).alias("ratio")))

# COMMAND ----------

# DBTITLE 1,Asset Allocation
display(loan_stats)
# display(loan_stats.groupBy("bad_loan", "grade").agg((sum(col("net"))).alias("sum_net")))

# COMMAND ----------

# DBTITLE 1,Display munged columns
display(loan_stats.select("net","verification_status","int_rate", "revol_util", "issue_year", "earliest_year", "bad_loan", "credit_length_in_years", "emp_length"))

# COMMAND ----------

# DBTITLE 1,Set Response and Predictor Variables

print("------------------------------------------------------------------------------------------------")
print("Setting variables to predict bad loans")
myY = "bad_loan"
categoricals = ["term", "home_ownership", "purpose", "addr_state",
                "verification_status","application_type"]
numerics = ["loan_amnt","emp_length", "annual_inc","dti",
            "delinq_2yrs","revol_util","total_acc",
            "credit_length_in_years"]
myX = categoricals + numerics

loan_stats2 = loan_stats.select(myX + [myY, "int_rate", "net", "issue_year"])
train = loan_stats2.filter(loan_stats2.issue_year <= 2015).cache()
valid = loan_stats2.filter(loan_stats2.issue_year > 2015).cache()

# train.count()
# valid.count()

# COMMAND ----------

# MAGIC %sql
# MAGIC USE default;
# MAGIC DROP TABLE IF EXISTS loanstats_train;
# MAGIC DROP TABLE IF EXISTS loanstats_valid;

# COMMAND ----------

# Save training and validation tables for future use
train.write.saveAsTable("loanstats_train")
valid.write.saveAsTable("loanstats_valid")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Logistic Regression Notes
# MAGIC * We will be using the Apache Spark pre-installed GLM and GBTClassifier models in this noteboook
# MAGIC * **GLM** is in reference to *generalized linear models*; the Apache Spark *logistic regression* model is a special case of a [generalized linear model](https://spark.apache.org/docs/2.2.0/ml-classification-regression.html#logistic-regression)
# MAGIC * We will also use BinaryClassificationEvaluator, CrossValidator, and ParamGridBuilder to tune our models.
# MAGIC * References to max F1 threshold (i.e. F_1 score or F-score or F-measure) is the measure of our logistic regression model's accuracy; more information can be found at [F1 score](https://en.wikipedia.org/wiki/F1_score).
# MAGIC * **GBTClassifier** is in reference to *gradient boosted tree classifier* which is a popular classification and regression method using ensembles of decision trees; more information can be found at [Gradiant Boosted Tree Classifier](https://spark.apache.org/docs/2.2.0/ml-classification-regression.html#gradient-boosted-tree-classifier)
# MAGIC * In a subsequent notebook, we will be using the XGBoost, an optimized distributed gradient boosting library.  
# MAGIC   * Underneath the covers, we will be using *XGBoost4J-Spark* - a project aiming to seamlessly integrate XGBoost and Apache Spark by fitting XGBoost to Apache Spark’s MLLIB framework.  More inforamtion can be found at [XGBoost4J-Spark Tutorial](https://xgboost.readthedocs.io/en/latest/jvm/xgboost4j_spark_tutorial.html).

# COMMAND ----------

# DBTITLE 1,Build Grid of GLM Models w/ Standardization+CrossValidation
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler, OneHotEncoder
from pyspark.ml.feature import StandardScaler, Imputer
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

## Current possible ways to handle categoricals in string indexer is 'error', 'keep', and 'skip'
indexers = map(lambda c: StringIndexer(inputCol=c, outputCol=c+"_idx", handleInvalid = 'keep'), categoricals)
ohes = map(lambda c: OneHotEncoder(inputCol=c + "_idx", outputCol=c+"_class"),categoricals)
imputers = Imputer(inputCols = numerics, outputCols = numerics)

# Establish features columns
featureCols = list(map(lambda c: c+"_class", categoricals)) + numerics

# Build the stage for the ML pipeline
model_matrix_stages = list(indexers) + list(ohes) + [imputers] + \
                     [VectorAssembler(inputCols=featureCols, outputCol="features"), StringIndexer(inputCol="bad_loan", outputCol="label")]

# Apply StandardScaler to create scaledFeatures
scaler = StandardScaler(inputCol="features",
                        outputCol="scaledFeatures",
                        withStd=True,
                        withMean=True)

# Use logistic regression 
lr = LogisticRegression(maxIter=10, elasticNetParam=0.5, featuresCol = "scaledFeatures")

# Build our ML pipeline
pipeline = Pipeline(stages=model_matrix_stages+[scaler]+[lr])

# Build the parameter grid for model tuning
paramGrid = ParamGridBuilder() \
              .addGrid(lr.regParam, [0.1, 0.01]) \
              .build()

# Execute CrossValidator for model tuning
crossval = CrossValidator(estimator=pipeline,
                          estimatorParamMaps=paramGrid,
                          evaluator=BinaryClassificationEvaluator(),
                          numFolds=5)

# Train the tuned model and establish our best model
cvModel = crossval.fit(train)
glm_model = cvModel.bestModel

# Return ROC
lr_summary = glm_model.stages[len(glm_model.stages)-1].summary
display(lr_summary.roc)

# COMMAND ----------

# DBTITLE 1,Set Max F1 Threshold
fMeasure = lr_summary.fMeasureByThreshold
maxFMeasure = fMeasure.groupBy().max('F-Measure').select('max(F-Measure)').head()
maxFMeasure = maxFMeasure['max(F-Measure)']
fMeasure = fMeasure.toPandas()
bestThreshold = float ( fMeasure[ fMeasure['F-Measure'] == maxFMeasure] ["threshold"])
lr.setThreshold(bestThreshold)

# COMMAND ----------

# DBTITLE 1,Build GBT Model
from pyspark.ml.classification import GBTClassifier

# Establish stages for our GBT model
indexers = map(lambda c: StringIndexer(inputCol=c, outputCol=c+"_idx", handleInvalid = 'keep'), categoricals)
imputers = Imputer(inputCols = numerics, outputCols = numerics)
featureCols = list(map(lambda c: c+"_idx", categoricals)) + numerics

# Define vector assemblers
model_matrix_stages = list(indexers) + [imputers] + \
                     [VectorAssembler(inputCols=featureCols, outputCol="features"), StringIndexer(inputCol="bad_loan", outputCol="label")]

# Define a GBT model.
gbt = GBTClassifier(featuresCol="features",
                    labelCol="label",
                    lossType = "logistic",
                    maxBins = 52,
                    maxIter=20,
                    maxDepth=5)

# Chain indexer and GBT in a Pipeline
pipeline = Pipeline(stages=model_matrix_stages+[gbt])

# Train model.  This also runs the indexer.
gbt_model = pipeline.fit(train)

# COMMAND ----------

# DBTITLE 1,Grab Model Metrics
from pyspark.mllib.evaluation import BinaryClassificationMetrics
from pyspark.ml.linalg import Vectors

def extract(row):
  return (row.net,) + tuple(row.probability.toArray().tolist()) +  (row.label,) + (row.prediction,)

def score(model,data):
  pred = model.transform(data).select("net", "probability", "label", "prediction")
  pred = pred.rdd.map(extract).toDF(["net", "p0", "p1", "label", "prediction"])
  return pred 

def auc(pred):
  metric = BinaryClassificationMetrics(pred.select("p1", "label").rdd)
  return metric.areaUnderROC

glm_train = score(glm_model, train)
glm_valid = score(glm_model, valid)
gbt_train = score(gbt_model, train)
gbt_valid = score(gbt_model, valid)

glm_train.createOrReplaceTempView("glm_train")
glm_valid.createOrReplaceTempView("glm_valid")
gbt_train.createOrReplaceTempView("gbt_train")
gbt_valid.createOrReplaceTempView("gbt_valid")


print ("GLM Training AUC:" + str(auc(glm_train)))
print ("GLM Validation AUC :" + str(auc(glm_valid)))
print ("GBT Training AUC :" + str(auc(gbt_train)))
print ("GBT Validation AUC :" + str(auc(gbt_valid)))

# COMMAND ----------

# DBTITLE 1,Stacked ROC Curves
# MAGIC %scala
# MAGIC import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
# MAGIC // import org.apache.spark.sql.functions.typedLit
# MAGIC import org.apache.spark.sql.functions.{array, lit, map, struct}
# MAGIC 
# MAGIC def roc(pred:org.apache.spark.sql.DataFrame, model_id:String): org.apache.spark.sql.DataFrame = {
# MAGIC   var testScoreAndLabel = pred.select("p1", "label").map{ case Row(p:Double,l:Double) => (p,l)}
# MAGIC   val metrics = new BinaryClassificationMetrics(testScoreAndLabel.rdd, 100)
# MAGIC   val roc = metrics.roc().toDF().withColumn("model", lit(model_id))
# MAGIC   return roc
# MAGIC }
# MAGIC 
# MAGIC val glm_train = roc( spark.table("glm_train"), "glm_train")
# MAGIC val glm_valid = roc( spark.table("glm_valid"), "glm_valid")
# MAGIC val gbt_train = roc( spark.table("gbt_train"), "gbt_train")
# MAGIC val gbt_valid = roc( spark.table("gbt_valid"), "gbt_valid")
# MAGIC 
# MAGIC val roc_curves = glm_train.union(glm_valid).union(gbt_train).union(gbt_valid)
# MAGIC 
# MAGIC display(roc_curves)

# COMMAND ----------

gbt_valid_table = spark.table("gbt_valid")
gbt_valid_table.createOrReplaceTempView("gbt_valid_table")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gbt_valid_table

# COMMAND ----------

# DBTITLE 1,Business Value
display(glm_valid.groupBy("label", "prediction").agg((sum(col("net"))).alias("sum_net")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Using the MLflow Runs Sidebar
# MAGIC 
# MAGIC Because of the code snippet added in cell 5, you can view your MLflow runs using the [MLflow Runs Sidebar](https://databricks.com/blog/2019/04/30/introducing-mlflow-run-sidebar-in-databricks-notebooks.html).  *Note, this feature is currently not available in Databricks Community Edition.*
# MAGIC 
# MAGIC ![](https://pages.databricks.com/rs/094-YMS-629/images/db-mlflow-integration.gif)