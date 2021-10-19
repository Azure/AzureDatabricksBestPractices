# Databricks notebook source
# DBTITLE 1,Overview
# MAGIC %md
# MAGIC ***Machine learning helps predict admissions, readmissions. Admission and the number of days in hospital are important for managing the schedule of doctors and as well as managing the hospital resources and infrastructure. When you’re looking at a patient population, you’re not going to get 100 percent accuracy. But when it comes to a root cause, there are certain specific factors that helps predict the patient duration at the hospital.***
# MAGIC 
# MAGIC Some of the Data points that can help predict admission risks: 
# MAGIC 
# MAGIC - Patients who take 5 or more medications
# MAGIC - Patient age and gender
# MAGIC - Patients with specific conditions including heart failure, sepsis or congestive obstructive pulmonary disorder
# MAGIC 
# MAGIC 
# MAGIC * [**Patient Admission Analytics**](http://www.fiercehealthcare.com/it/paris-hospitals-use-predictive-modeling-to-control-admissions) is the use of data analytics and machine learning to predict patient admissions down to the hour and is...  
# MAGIC   * Built on top of Databricks Platform
# MAGIC   * Uses a machine learning **decision tree classifier** implementation to analysis a Patient Admission dataset   
# MAGIC * This demo...  
# MAGIC   * demonstrates a simple patient admission analysis workflow.  We use Patient dataset from the [Kaggle Repository](https://github.com/jiunjiunma/heritage-health-prize/blob/master/modeling_set1.csv).

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

# COMMAND ----------

# DBTITLE 1,Step1: Ingest patient Data to Notebook
# MAGIC %md 
# MAGIC 
# MAGIC - We will download the UCI dataset hosted at  [**Kaggle Dataset on Github**](https://github.com/jiunjiunma/heritage-health-prize/blob/master/modeling_set1.csv)
# MAGIC - After downloading we will import this dataset into databricks notebook

# COMMAND ----------

# DBTITLE 1,Load Dataset
# MAGIC %sh
# MAGIC mkdir /tmp/patient
# MAGIC wget https://raw.githubusercontent.com/jiunjiunma/heritage-health-prize/master/modeling_set1.csv -O /tmp/patient/modeling_set1.csv

# COMMAND ----------

# DBTITLE 1,Mount Data
# MAGIC %py
# MAGIC dbutils.fs.mkdirs("/mnt/patient")
# MAGIC dbutils.fs.mv("file:///tmp/patient/modeling_set1.csv", "/mnt/wesley/dataset/medicare/readmission/modeling_set1.csv")

# COMMAND ----------

# MAGIC %fs ls /mnt/wesley/dataset/medicare/readmission/modeling_set1.csv

# COMMAND ----------

# DBTITLE 1,Define schema with 142 columns for data ingestion
## Schema with various data attributes, type and indicators ex: StructField("MemberID_t",IntegerType(),False),

from pyspark.sql.types import *
from pyspark.sql import functions as F

schema =StructType([
	StructField("MemberID_t",IntegerType(),False),
	StructField("YEAR_t",StringType(),False),
	StructField("ClaimsTruncated",IntegerType(),False),
	StructField("DaysInHospital",DoubleType(),False),
	StructField("trainset",IntegerType(),False),
	StructField("age_05",IntegerType(),False),
	StructField("age_15",IntegerType(),False),
	StructField("age_25",IntegerType(),False),
	StructField("age_35",IntegerType(),False),
	StructField("age_45",IntegerType(),False),
	StructField("age_55",IntegerType(),False),
	StructField("age_65",IntegerType(),False),
	StructField("age_75",IntegerType(),False),
	StructField("age_85",IntegerType(),False),
	StructField("age_MISS",IntegerType(),False),
	StructField("sexMALE",IntegerType(),False),
	StructField("sexFEMALE",IntegerType(),False),
	StructField("sexMISS",IntegerType(),False),
	StructField("no_Claims",IntegerType(),False),
	StructField("no_Providers",IntegerType(),False),
	StructField("no_Vendors",IntegerType(),False),
	StructField("no_PCPs",IntegerType(),False),
	StructField("no_PlaceSvcs",IntegerType(),False),
	StructField("no_Specialities",IntegerType(),False),
	StructField("no_PrimaryConditionGroups",IntegerType(),False),
	StructField("no_ProcedureGroups",IntegerType(),False),
	StructField("PayDelay_max",IntegerType(),False),
	StructField("PayDelay_min",IntegerType(),False),
	StructField("PayDelay_ave",IntegerType(),False),
	StructField("PayDelay_stdev",DoubleType(),False),
	StructField("LOS_max",IntegerType(),False),
	StructField("LOS_min",IntegerType(),False),
	StructField("LOS_ave",IntegerType(),False),
	StructField("LOS_stdev",DoubleType(),False),
	StructField("LOS_TOT_UNKNOWN",IntegerType(),False),
	StructField("LOS_TOT_SUPRESSED",IntegerType(),False),
	StructField("LOS_TOT_KNOWN",IntegerType(),False),
	StructField("dsfs_max",IntegerType(),False),
	StructField("dsfs_min",IntegerType(),False),
	StructField("dsfs_range",IntegerType(),False),
	StructField("dsfs_ave",IntegerType(),False),
	StructField("dsfs_stdev",DoubleType(),False),
	StructField("CharlsonIndexI_max",IntegerType(),False),
	StructField("CharlsonIndexI_min",IntegerType(),False),
	StructField("CharlsonIndexI_ave",IntegerType(),False),
	StructField("CharlsonIndexI_range",IntegerType(),False),
	StructField("CharlsonIndexI_stdev",DoubleType(),False),
	StructField("pcg1",IntegerType(),False),
	StructField("pcg2",IntegerType(),False),
	StructField("pcg3",IntegerType(),False),
	StructField("pcg4",IntegerType(),False),
	StructField("pcg5",IntegerType(),False),
	StructField("pcg6",IntegerType(),False),
	StructField("pcg7",IntegerType(),False),
	StructField("pcg8",IntegerType(),False),
	StructField("pcg9",IntegerType(),False),
	StructField("pcg10",IntegerType(),False),
	StructField("pcg11",IntegerType(),False),
	StructField("pcg12",IntegerType(),False),
	StructField("pcg13",IntegerType(),False),
	StructField("pcg14",IntegerType(),False),
	StructField("pcg15",IntegerType(),False),
	StructField("pcg16",IntegerType(),False),
	StructField("pcg17",IntegerType(),False),
	StructField("pcg18",IntegerType(),False),
	StructField("pcg19",IntegerType(),False),
	StructField("pcg20",IntegerType(),False),
	StructField("pcg21",IntegerType(),False),
	StructField("pcg22",IntegerType(),False),
	StructField("pcg23",IntegerType(),False),
	StructField("pcg24",IntegerType(),False),
	StructField("pcg25",IntegerType(),False),
	StructField("pcg26",IntegerType(),False),
	StructField("pcg27",IntegerType(),False),
	StructField("pcg28",IntegerType(),False),
	StructField("pcg29",IntegerType(),False),
	StructField("pcg30",IntegerType(),False),
	StructField("pcg31",IntegerType(),False),
	StructField("pcg32",IntegerType(),False),
	StructField("pcg33",IntegerType(),False),
	StructField("pcg34",IntegerType(),False),
	StructField("pcg35",IntegerType(),False),
	StructField("pcg36",IntegerType(),False),
	StructField("pcg37",IntegerType(),False),
	StructField("pcg38",IntegerType(),False),
	StructField("pcg39",IntegerType(),False),
	StructField("pcg40",IntegerType(),False),
	StructField("pcg41",IntegerType(),False),
	StructField("pcg42",IntegerType(),False),
	StructField("pcg43",IntegerType(),False),
	StructField("pcg44",IntegerType(),False),
	StructField("pcg45",IntegerType(),False),
	StructField("pcg46",IntegerType(),False),
	StructField("sp1",IntegerType(),False),
	StructField("sp2",IntegerType(),False),
	StructField("sp3",IntegerType(),False),
	StructField("sp4",IntegerType(),False),
	StructField("sp5",IntegerType(),False),
	StructField("sp6",IntegerType(),False),
	StructField("sp7",IntegerType(),False),
	StructField("sp8",IntegerType(),False),
	StructField("sp9",IntegerType(),False),
	StructField("sp10",IntegerType(),False),
	StructField("sp11",IntegerType(),False),
	StructField("sp12",IntegerType(),False),
	StructField("sp13",IntegerType(),False),
	StructField("pg1",IntegerType(),False),
	StructField("pg2",IntegerType(),False),
	StructField("pg3",IntegerType(),False),
	StructField("pg4",IntegerType(),False),
	StructField("pg5",IntegerType(),False),
	StructField("pg6",IntegerType(),False),
	StructField("pg7",IntegerType(),False),
	StructField("pg8",IntegerType(),False),
	StructField("pg9",IntegerType(),False),
	StructField("pg10",IntegerType(),False),
	StructField("pg11",IntegerType(),False),
	StructField("pg12",IntegerType(),False),
	StructField("pg13",IntegerType(),False),
	StructField("pg14",IntegerType(),False),
	StructField("pg15",IntegerType(),False),
	StructField("pg16",IntegerType(),False),
	StructField("pg17",IntegerType(),False),
	StructField("pg18",IntegerType(),False),
	StructField("ps1",IntegerType(),False),
	StructField("ps2",IntegerType(),False),
	StructField("ps3",IntegerType(),False),
	StructField("ps4",IntegerType(),False),
	StructField("ps5",IntegerType(),False),
	StructField("ps6",IntegerType(),False),
	StructField("ps7",IntegerType(),False),
	StructField("ps8",IntegerType(),False),
	StructField("ps9",IntegerType(),False),
	StructField("drugCount_max",IntegerType(),False),
	StructField("drugCount_min",IntegerType(),False),
	StructField("drugCount_ave",DoubleType(),False),
	StructField("drugcount_months",IntegerType(),False),
	StructField("labCount_max",IntegerType(),False),
	StructField("labCount_min",IntegerType(),False),
	StructField("labCount_ave",DoubleType(),False),
	StructField("labcount_months",IntegerType(),False),
	StructField("labNull",IntegerType(),False),
	StructField("drugNull",IntegerType(),False)
])

df1 = (spark.read.option("delimiter", ",")
  .option("inferSchema", "true")  # Use tab delimiter (default is comma-separator)
   .option("header", "true")
  .schema(schema)
  .csv("dbfs:/mnt/wesley/dataset/medicare/readmission/modeling_set1.csv"))

df = df1.select("MemberID_t","YEAR_t","ClaimsTruncated","DaysInHospital","trainset","age_05","age_15","age_25","age_35","age_45","age_55","age_65","age_75","age_85","age_MISS","sexMALE","sexFEMALE","sexMISS","no_Claims","no_Providers","no_Vendors","no_PCPs","no_PlaceSvcs","no_Specialities","no_PrimaryConditionGroups","no_ProcedureGroups","PayDelay_max","PayDelay_min","PayDelay_ave","PayDelay_stdev","LOS_max","LOS_min","LOS_ave","LOS_stdev","LOS_TOT_UNKNOWN","LOS_TOT_SUPRESSED","LOS_TOT_KNOWN","dsfs_max","dsfs_min","dsfs_range","dsfs_ave","dsfs_stdev","CharlsonIndexI_max","CharlsonIndexI_min","CharlsonIndexI_ave","CharlsonIndexI_range","CharlsonIndexI_stdev","pcg1","pcg2","pcg3","pcg4","pcg5","pcg6","pcg7","pcg8","pcg9","pcg10","pcg11","pcg12","pcg13","pcg14","pcg15","pcg16","pcg17","pcg18","pcg19","pcg20","pcg21","pcg22","pcg23","pcg24","pcg25","pcg26","pcg27","pcg28","pcg29","pcg30","pcg31","pcg32","pcg33","pcg34","pcg35","pcg36","pcg37","pcg38","pcg39","pcg40","pcg41","pcg42","pcg43","pcg44","pcg45","pcg46","sp1","sp2","sp3","sp4","sp5","sp6","sp7","sp8","sp9","sp10","sp11","sp12","sp13","pg1","pg2","pg3","pg4","pg5","pg6","pg7","pg8","pg9","pg10","pg11","pg12","pg13","pg14","pg15","pg16","pg17","pg18","ps1","ps2","ps3","ps4","ps5","ps6","ps7","ps8","ps9","drugCount_max","drugCount_min","drugCount_ave","drugcount_months","labCount_max","labCount_min","labCount_ave","labcount_months","labNull","drugNull", F.when(df1.DaysInHospital <= 0, 0.0).when(df1.DaysInHospital >= 1, 1.0).alias('Readmitlabel'))

# COMMAND ----------

# DBTITLE 1,Verify Data in a table format
display(df)

# COMMAND ----------

# MAGIC %md ### Step2: Enrich the data to get additional insigts to patient dataset
# MAGIC - We count the number of data points and separate the churned from the unchurned.

# COMMAND ----------

# DBTITLE 1,Import SQL API Functions
# Because we will need it later...
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# DBTITLE 1,Create table
# MAGIC %sql 
# MAGIC 
# MAGIC     Drop table if exists temp_idsdata;
# MAGIC     
# MAGIC     CREATE TEMPORARY TABLE temp_idsdata
# MAGIC     USING parquet
# MAGIC     OPTIONS (
# MAGIC       path "/mnt/wesley/parquet/medicare/readmission/admissiondata"
# MAGIC     )

# COMMAND ----------

# DBTITLE 1,Age wise analysis of patient data
# MAGIC %sql
# MAGIC select DaysInHospital,age_05,age_15,age_25,age_35,age_45,age_55,age_65,age_75,age_85,age_MISS from temp_idsdata where trainset =1

# COMMAND ----------

# MAGIC %md ###Step3: Explore Patient Data 

# COMMAND ----------

# DBTITLE 1,Breakdown of patient days in hospital using databricks graph 
# MAGIC %sql
# MAGIC select Readmitlabel, count(*) as count from temp_idsdata where trainset =1 group by Readmitlabel order by Readmitlabel asc

# COMMAND ----------

# DBTITLE 1,Breakdown of patient days in hospital using matplotlib
import matplotlib.pyplot as plt
importance = sqlContext.sql("select Readmitlabel, count(*) as count from temp_idsdata where trainset =1 group by Readmitlabel order by Readmitlabel asc")
importanceDF = importance.toPandas()
ax = importanceDF.plot(x="Readmitlabel", y="count",lw=3,colormap='Reds_r',title='Importance in Descending Order', fontsize=9)
ax.set_xlabel("Readmitlabel")
ax.set_ylabel("count")
plt.xticks(rotation=12)
plt.grid(True)
plt.show()
display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 4: Visualization
# MAGIC * Show the distribution of the account length.

# COMMAND ----------

display(df.filter(col('trainset') == 1).select("Readmitlabel"))

# COMMAND ----------

# MAGIC %md #### Step 5: Model creation

# COMMAND ----------

from  pyspark.ml.feature import StringIndexer

indexer1 = (StringIndexer()
                   .setInputCol("YEAR_t")
                   .setOutputCol("YearIndex")
                   .fit(df))

indexed1 = indexer1.transform(df)
finaldf = indexed1.filter(col('trainset') == 1)

from pyspark.ml.feature import VectorAssembler
vecAssembler = VectorAssembler()
vecAssembler.setInputCols(["age_05","age_15","age_25","age_35","age_45","age_55","age_65","age_75","age_85","age_MISS","sexMALE","sexFEMALE","sexMISS","no_Claims","no_Providers","no_Vendors","no_PCPs","no_PlaceSvcs","no_Specialities","no_PrimaryConditionGroups","no_ProcedureGroups","PayDelay_max","PayDelay_min","PayDelay_ave","PayDelay_stdev","LOS_max","LOS_min","LOS_ave","LOS_stdev","LOS_TOT_UNKNOWN","LOS_TOT_SUPRESSED","LOS_TOT_KNOWN","dsfs_max","dsfs_min","dsfs_range","dsfs_ave","dsfs_stdev","CharlsonIndexI_max","CharlsonIndexI_min","CharlsonIndexI_ave","CharlsonIndexI_range","CharlsonIndexI_stdev","pcg1","pcg2","pcg3","pcg4","pcg5","pcg6","pcg7","pcg8","pcg9","pcg10","pcg11","pcg12","pcg13","pcg14","pcg15","pcg16","pcg17","pcg18","pcg19","pcg20","pcg21","pcg22","pcg23","pcg24","pcg25","pcg26","pcg27","pcg28","pcg29","pcg30","pcg31","pcg32","pcg33","pcg34","pcg35","pcg36","pcg37","pcg38","pcg39","pcg40","pcg41","pcg42","pcg43","pcg44","pcg45","pcg46","sp1","sp2","sp3","sp4","sp5","sp6","sp7","sp8","sp9","sp10","sp11","sp12","sp13","pg1","pg2","pg3","pg4","pg5","pg6","pg7","pg8","pg9","pg10","pg11","pg12","pg13","pg14","pg15","pg16","pg17","pg18","ps1","ps2","ps3","ps4","ps5","ps6","ps7","ps8","ps9","drugCount_max","drugCount_min","drugCount_ave","drugcount_months","labCount_max","labCount_min","labCount_ave","labcount_months"])
vecAssembler.setOutputCol("features")
print vecAssembler.explainParams()

from pyspark.ml.classification import DecisionTreeClassifier

aft = DecisionTreeClassifier()
aft.setLabelCol("Readmitlabel")
aft.setMaxDepth(30)

print aft.explainParams()

# COMMAND ----------

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
predAnalysis = predictionsAndLabelsDF.select('age_05','age_15','age_25','age_35','age_45','age_55','age_65','age_75','age_85','age_MISS','DaysInHospital', 'prediction')
confusionMatrix = predictionsAndLabelsDF.select('Readmitlabel', 'prediction')

# COMMAND ----------

# DBTITLE 1,Visualizing the for data prediction results
display(predAnalysis)

# COMMAND ----------

# DBTITLE 1,Confusion Matrix for the patient model
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
# MAGIC plt.figure(figsize=(2,2))
# MAGIC classes=list([0,1])
# MAGIC plt.imshow(cm, interpolation='nearest', cmap=plt.cm.Blues)
# MAGIC plt.title('Confusion matrix')
# MAGIC plt.colorbar()
# MAGIC tick_marks = np.arange(len(classes))
# MAGIC plt.xticks(tick_marks, classes, rotation=0)
# MAGIC plt.yticks(tick_marks, classes)
# MAGIC 
# MAGIC 
# MAGIC thresh = cm.max() / 2.
# MAGIC for i, j in itertools.product(range(cm.shape[0]), range(cm.shape[1])):
# MAGIC     plt.text(j, i, format(cm[i, j]),
# MAGIC              horizontalalignment="center",
# MAGIC              verticalalignment="center",
# MAGIC              color="white" if cm[i, j] > thresh else "black")
# MAGIC plt.ylabel('True label')
# MAGIC plt.xlabel('Predicted label')
# MAGIC plt.show()
# MAGIC display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Results interpretation
# MAGIC The plot above shows Index used to measure each of the patent admission days. 
# MAGIC 
# MAGIC ![Churn-Index](http://www.ngdata.com/wp-content/uploads/2016/05/churn.jpg)
# MAGIC 
# MAGIC we have demonstrated prediction of number of days in hospital stay for a given patient based on decision tree classifier on Databricks Notebook.
# MAGIC 
# MAGIC We have achieved a very high accuracy of day in hospital prediction for a huge number of patients by just evaluating the general features like age,gender, types of drug and prescription used by the patient. This would help to better plan the infrastructure available to the patients in the hospital as well as the personnel mangement of doctors and nurses need as well.
# MAGIC 
# MAGIC At the end of the day, all this analysis and research is for one cause: To improve the quality of human lives. I hope this is, and will continue to be, the greatest motivation to overcome any challenge.