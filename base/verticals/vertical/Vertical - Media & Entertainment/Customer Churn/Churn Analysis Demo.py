# Databricks notebook source
# MAGIC %md
# MAGIC * [**Customer Churn**](https://en.wikipedia.org/wiki/Customer_attrition) also known as Customer attrition, customer turnover, or customer defection, is the loss of clients or customers and is...  
# MAGIC   * Built on top of Databricks Platform
# MAGIC   * Uses a machine learning **Gradient boosting algorithm** implementation to analysis a Customer Churn dataset   
# MAGIC * This demo...  
# MAGIC   * demonstrates a simple churn analysis workflow.  We use Customer Churn dataset from the [UCI Machine Learning Repository](http://archive.ics.uci.edu/ml/index.html).

# COMMAND ----------

# MAGIC %md
# MAGIC ###0. SETUP -- Databricks Spark cluster:  
# MAGIC 
# MAGIC 1. **Create** a cluster by...  
# MAGIC   - Click the `Clusters` icon on the left sidebar and then `Create Cluster.` 
# MAGIC   - Enter any text, i.e `demo` into the cluster name text box
# MAGIC   - Select the `Apache Spark Version` value `Spark 2.2 (auto-updating scala 2.11)`  
# MAGIC   - Click the `create cluster` button and wait for your cluster to be provisioned
# MAGIC 3. **Attach** this notebook to your cluster by...   
# MAGIC   - Click on your cluster name in menu `Detached` at the top left of this workbook to attach it to this workbook 

# COMMAND ----------

# MAGIC %md 
# MAGIC ###Step1: Ingest IDS Data to Notebook
# MAGIC 
# MAGIC We will download the UCI dataset hosted at  [**UCI site**](http://www.sgi.com/tech/mlc/db/)
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

# MAGIC %sh
# MAGIC mkdir /tmp/churn
# MAGIC wget http://www.sgi.com/tech/mlc/db/churn.data -O /tmp/churn/churn.data
# MAGIC wget http://www.sgi.com/tech/mlc/db/churn.test -O /tmp/churn/churn.test

# COMMAND ----------

# MAGIC %py
# MAGIC dbutils.fs.mkdirs("/mnt/wesley/dataset/Adtech/churn")
# MAGIC dbutils.fs.mv("file:///tmp/churn/churn.data", "/mnt/wesley/dataset/Adtech/churn/churn.data")
# MAGIC dbutils.fs.mv("file:///tmp/churn/churn.test", "/mnt/wesley/dataset/Adtech/churn/churn.test")

# COMMAND ----------

# MAGIC %fs ls /mnt/wesley/dataset/Adtech/churn

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
  .schema(schema)
  .csv("dbfs:/mnt/wesley/dataset/Adtech/churn/churn.data"))

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md ### Step2: Enrich the data to get additional insigts to churn dataset
# MAGIC - We count the number of data points and separate the churned from the unchurned.

# COMMAND ----------

# Because we will need it later...
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

numCases = df.count()
numChurned = df.filter(col("churned") == ' True.').count()

# COMMAND ----------

numCases = numCases
numChurned = numChurned
numUnchurned = numCases - numChurned
print("Total Number of cases: {0:,}".format( numCases ))
print("Total Number of cases churned: {0:,}".format( numChurned ))
print("Total Number of cases unchurned: {0:,}".format( numUnchurned ))

# COMMAND ----------

df.repartition(1).write.parquet('/mnt/wesley/parquet/adtech/churndata')

# COMMAND ----------

# MAGIC %md ###Step3: Explore Churn Data 

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC     Drop table if exists temp_idsdata;
# MAGIC     
# MAGIC     CREATE TEMPORARY TABLE temp_idsdata
# MAGIC     USING parquet
# MAGIC     OPTIONS (
# MAGIC       path "/mnt/wesley/parquet/adtech/churndata"
# MAGIC     )

# COMMAND ----------

# DBTITLE 1,Churn by statewide breakup using databricks graph 
# MAGIC %sql
# MAGIC SELECT state, count(*) as statewise_churn FROM temp_idsdata where churned= " True." group by state

# COMMAND ----------

# DBTITLE 1,Churn by statewide breakup using python matplotlib
import matplotlib.pyplot as plt
importance = sqlContext.sql("SELECT state, count(*) as statewise_churn FROM temp_idsdata where churned= ' True.' group by state")
importanceDF = importance.toPandas()
ax = importanceDF.plot(x="state", y="statewise_churn",lw=3,colormap='Reds_r',title='Importance in Descending Order', fontsize=9)
ax.set_xlabel("protocol")
ax.set_ylabel("num_hits")
plt.xticks(rotation=12)
plt.grid(True)
plt.show()
display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 4: Visualization
# MAGIC * Show the distribution of the account length.

# COMMAND ----------

display(df.select("account_length"))

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md #### Step 5: Model creation

# COMMAND ----------

# DBTITLE 1,Model Fitting and Summarization
from  pyspark.ml.feature import StringIndexer

indexer1 = (StringIndexer()
                   .setInputCol("churned")
                   .setOutputCol("churnedIndex")
                   .fit(df))

indexed1 = indexer1.transform(df)
finaldf = indexed1.withColumn("censor", lit(1))

from pyspark.ml.feature import VectorAssembler
vecAssembler = VectorAssembler()
vecAssembler.setInputCols(["account_length", "total_day_calls",  "total_eve_calls", "total_night_calls", "total_intl_calls",  "number_customer_service_calls"])
vecAssembler.setOutputCol("features")
print vecAssembler.explainParams()

from pyspark.ml.classification import GBTClassifier

aft = GBTClassifier()
aft.setLabelCol("churnedIndex")

print aft.explainParams()

# COMMAND ----------

# DBTITLE 1,Building model on train data
from pyspark.ml import Pipeline

# We will use the new spark.ml pipeline API. If you have worked with scikit-learn this will be very familiar.
lrPipeline = Pipeline()

# Now we'll tell the pipeline to first create the feature vector, and then do the linear regression
lrPipeline.setStages([vecAssembler, aft])

# Pipelines are themselves Estimators -- so to use them we call fit:
lrPipelineModel = lrPipeline.fit(finaldf)

# COMMAND ----------

# DBTITLE 1,Using Model for data predicition
predictionsAndLabelsDF = lrPipelineModel.transform(finaldf)
confusionMatrix = predictionsAndLabelsDF.select('churnedIndex', 'prediction')

# COMMAND ----------

# DBTITLE 1,Confusion Matrix for the churn model
from pyspark.mllib.evaluation import MulticlassMetrics
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
# MAGIC 1. The cutomer churn index using gradient boosting algorithm gives and accuracy of close to 89%