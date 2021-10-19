# Databricks notebook source
# MAGIC %md
# MAGIC ![Databricks for Customer Churn](https://www.superoffice.com/blog/wp-content/uploads/2015/05/reduce-customer-churn.png "Databricks for Customer Churn")  
# MAGIC * [**Customer Churn**](https://en.wikipedia.org/wiki/Customer_attrition) also known as Customer attrition, customer turnover, or customer defection, is the loss of clients or customers and is...  
# MAGIC   * Built on top of Databricks Platform
# MAGIC   * Uses a machine learning **Gradient boosting algorithm** implementation to analysis a Customer Churn dataset   
# MAGIC * This demo...  
# MAGIC   * demonstrates a simple churn analysis workflow.  We use Customer Churn dataset from the [UCI Machine Learning Repository](http://archive.ics.uci.edu/ml/index.html).
# MAGIC   * note that the above public repo was taken offline so we use a version of the public data set hosted in our public blob store account mounted at dbfs at /mnt/webinar

# COMMAND ----------

# MAGIC %md
# MAGIC ###0. SETUP -- Databricks Spark cluster:  
# MAGIC 
# MAGIC 1. **Create** a cluster by...  
# MAGIC   - Click the `Clusters` icon on the left sidebar and then `Create Cluster.` 
# MAGIC   - Enter any text, i.e `demo` into the cluster name text box
# MAGIC   - Select the `Apache Spark Version` value `Spark 2.2 (auto-updating scala 2.11)`  
# MAGIC   - Click the `create cluster` button and wait for your cluster to be provisioned
# MAGIC 2. **Attach** this notebook to your cluster by...   
# MAGIC   - Click on your cluster name in menu `Detached` at the top left of this workbook to attach it to this workbook 
# MAGIC   
# MAGIC 3. **Ensure** sample data has been mounted to DBFS by running the <a href="$../02 Mounting Storage"> Mounting Storage </a> notebook 

# COMMAND ----------

# MAGIC %md 
# MAGIC ###Step1: Ingest Churn Data to Notebook
# MAGIC 
# MAGIC From the churn.names metadata file, we can see the meaning of the data columns:
# MAGIC * state: discrete.
# MAGIC * account length: continuous.
# MAGIC * area code: continuous.
# MAGIC * phone number: discrete.
# MAGIC * international plan: discrete.
# MAGIC * voice mail plan: discrete.
# MAGIC * number vmail messages: continuous.
# MAGIC * total day minutes: continuous.
# MAGIC * total day calls: continuous.
# MAGIC * total day charge: continuous.
# MAGIC * total eve minutes: continuous.
# MAGIC * total eve calls: continuous.
# MAGIC * total eve charge: continuous.
# MAGIC * total night minutes: continuous.
# MAGIC * total night calls: continuous.
# MAGIC * total night charge: continuous.
# MAGIC * total intl minutes: continuous.
# MAGIC * total intl calls: continuous.
# MAGIC * total intl charge: continuous.
# MAGIC * number customer service calls: continuous.
# MAGIC * churned: discrete <- This is the label we wish to predict, indicating whether or not the customer churned.

# COMMAND ----------

# MAGIC %py
# MAGIC dbutils.fs.mkdirs("/mnt/churn")
# MAGIC dbutils.fs.cp("/mnt/webinardata/churn.csv", "/mnt/churn/churn.csv")

# COMMAND ----------

# MAGIC %fs ls /mnt/churn

# COMMAND ----------

from pyspark.sql.types import *

# The second step is to create the schema
schema =StructType([
	StructField("state",StringType(), False),
	StructField("account_length",DoubleType(), False),
	StructField("area_code",DoubleType(), False),
	StructField("phone_number",StringType(), False),
	StructField("international_plan",StringType(), False),
	StructField("voice_mail_plan",StringType(), False),
	StructField("number_vmail_messages",DoubleType(), False),
	StructField("total_day_minutes",DoubleType(), False),
	StructField("total_day_calls",DoubleType(), False),
	StructField("total_day_charge",DoubleType(), False),
	StructField("total_eve_minutes",DoubleType(), False),
	StructField("total_eve_calls",DoubleType(), False),
	StructField("total_eve_charge",DoubleType(), False),
	StructField("total_night_minutes",DoubleType(), False),
	StructField("total_night_calls",DoubleType(), False),
	StructField("total_night_charge",DoubleType(), False),
	StructField("total_intl_minutes",DoubleType(), False),
	StructField("total_intl_calls",DoubleType(), False),
	StructField("total_intl_charge",DoubleType(), False),
    StructField("number_customer_service_calls",DoubleType(), False), 
    StructField("churned",StringType(), False)
])

df = (spark.read.option("delimiter", ",")
  .option("inferSchema", "true")
  .option("header", "true")
  .schema(schema)
  .csv("/mnt/churn/churn.csv"))

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md ### Step2: Enrich the data to get additional insights to churn dataset
# MAGIC - We count the number of data points and separate the churned from the unchurned.

# COMMAND ----------

# Because we will need it later...
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md We do a filter and count operation to find the number customers who churned

# COMMAND ----------

numCases = df.count()
numChurned = df.filter(col("churned") == 'True.').count()

# COMMAND ----------

numCases = numCases
numChurned = numChurned
numUnchurned = numCases - numChurned
print("Total Number of cases: {0:,}".format( numCases ))
print("Total Number of cases churned: {0:,}".format( numChurned ))
print("Total Number of cases unchurned: {0:,}".format( numUnchurned ))

# COMMAND ----------

# MAGIC %md The data is converted to a parquet file, which is a data format that is well suited to analytics on large data sets 

# COMMAND ----------

df.repartition(1).write.parquet('/mnt/churn/churn_parquet2')

# COMMAND ----------

# MAGIC %fs ls /mnt/churn/churn_parquet

# COMMAND ----------

# MAGIC %md ###Step3: Explore Churn Data 

# COMMAND ----------

# MAGIC %md We create a table on the parquet data to so we can analyze it at scale with Spark SQL 

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC     DROP TABLE IF EXISTS temp_churndata;
# MAGIC     
# MAGIC     CREATE TEMPORARY TABLE temp_churndata
# MAGIC     USING parquet
# MAGIC     OPTIONS (
# MAGIC       path "/mnt/churn/churn_parquet"
# MAGIC     )

# COMMAND ----------

# DBTITLE 1,Churn by statewide breakup using databricks graph 
# MAGIC %sql
# MAGIC SELECT state, count(*) as statewise_churn FROM temp_churndata where churned= "True." group by state

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 4: Visualization
# MAGIC * Show the distribution of the account length.

# COMMAND ----------

display(df.select("account_length","state").orderBy("state"))

# COMMAND ----------

# MAGIC %md #### Step 5: Model creation

# COMMAND ----------

# DBTITLE 1,Model Fitting and Summarization
from  pyspark.ml.feature import StringIndexer

indexer1 = (StringIndexer()
                   .setInputCol("churned")
                   .setOutputCol("churnedIndex")
                   .fit(df))

indexed = indexer1.transform(df)

display(indexed)

# COMMAND ----------

from pyspark.ml.feature import VectorAssembler

vecAssembler = VectorAssembler()
vecAssembler.setInputCols(["account_length", "total_day_calls",  "total_eve_calls", "total_night_calls", "total_intl_calls",  "number_customer_service_calls"])
vecAssembler.setOutputCol("features")

display(vecAssembler.transform(indexed))


# COMMAND ----------

training, test = indexed.randomSplit([0.9, 0.1], seed=12345)

# COMMAND ----------

from pyspark.ml.classification import GBTClassifier

aft = GBTClassifier()
aft.setLabelCol("churnedIndex")

# COMMAND ----------

# DBTITLE 1,Building model on train data
from pyspark.ml import Pipeline

# We will use the new spark.ml pipeline API. If you have worked with scikit-learn this will be very familiar.
lrPipeline = Pipeline()

# Now we'll tell the pipeline to first create the feature vector, and then do the classification
lrPipeline.setStages([vecAssembler, aft])

# Pipelines are themselves Estimators -- so to use them we call fit:
lrPipelineModel = lrPipeline.fit(training)

# COMMAND ----------

# DBTITLE 1,Using Model for data predicition
predictionsAndLabelsDF = lrPipelineModel.transform(test)
display(predictionsAndLabelsDF)


# COMMAND ----------

# DBTITLE 1,Confusion Matrix for the churn model
from pyspark.mllib.evaluation import MulticlassMetrics

confusionMatrix = predictionsAndLabelsDF.select('churnedIndex', 'prediction')
metrics = MulticlassMetrics(confusionMatrix.rdd)
cm = metrics.confusionMatrix().toArray()

# COMMAND ----------

# DBTITLE 1,Performance Metrics of the model
print metrics.falsePositiveRate(0.0)
print metrics.accuracy

# COMMAND ----------

# DBTITLE 1,confusion matrix in matplotlib
# MAGIC %python
# MAGIC import matplotlib.pyplot as plt
# MAGIC import numpy as np
# MAGIC import itertools
# MAGIC plt.figure()
# MAGIC classes=list([0,1])
# MAGIC plt.imshow(cm, interpolation='nearest', cmap=plt.cm.Blues)
# MAGIC plt.title('Confusion matrix')
# MAGIC plt.colorbar()
# MAGIC tick_marks = np.arange(len(classes))
# MAGIC plt.xticks(tick_marks, classes, rotation=0)
# MAGIC plt.yticks(tick_marks, classes)
# MAGIC 
# MAGIC fmt =  '.2f'
# MAGIC thresh = cm.max() / 2.
# MAGIC for i, j in itertools.product(range(cm.shape[0]), range(cm.shape[1])):
# MAGIC     plt.text(j, i, format(cm[i, j], fmt),
# MAGIC              horizontalalignment="center",
# MAGIC              color="white" if cm[i, j] > thresh else "black")
# MAGIC plt.tight_layout()
# MAGIC plt.ylabel('True label')
# MAGIC plt.xlabel('Predicted label')
# MAGIC plt.show()
# MAGIC display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Results interpretation
# MAGIC The plot above shows Index used to measure each of the churn type. 
# MAGIC 
# MAGIC ![Churn-Index](http://www.ngdata.com/wp-content/uploads/2016/05/churn.jpg)
# MAGIC 
# MAGIC The customer churn index using gradient boosting algorithm gives and accuracy of close to 89%