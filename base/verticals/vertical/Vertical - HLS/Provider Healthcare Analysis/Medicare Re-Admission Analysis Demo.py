# Databricks notebook source
# DBTITLE 1,Overview
# MAGIC %md
# MAGIC ***Machine learning helps predict admissions, readmissions. Admission and the number of days in hospital are important for managing the schedule of doctors and as well as managing the hospital resources and infrastructure. When you’re looking at a patient population, you’re not going to get 100 percent accuracy. But when it comes to a root cause, there are certain specific factors that helps predict the patient duration at the hospital.***
# MAGIC 
# MAGIC Section 3025 of the 2010 Patient Protection and Affordable Care Act established the Hospital Readmissions Reduction Program (HRRP) as an addition to section 1886(q) of the 1965 Social Security Act. This was partly a result of the 2007 "Promoting Greater Efficiency in Medicare" report which recognized the prevalence and cost of readmissions nationwide. This program established a method for calculating a health system's expected readmission rate and created a system for financially penalizing hospital systems that exceeded their expected readmission rate. The HRRP officially began in 2013 and applied to all acute care hospitals except the following: psychiatric, rehabilitation, pediatric, cancer, and critical access hospitals. Maryland hospitals were excluded, due to the state's unique all-payer model for reimbursement. In the first two years, only readmissions for heart attack, heart failure, and pneumonia were counted; in 2015, chronic obstructive pulmonary disease (COPD) and elective hip replacement and knee replacement were added. CMS plans to add coronary artery bypass graft (CABG) surgery to the list in 2017.
# MAGIC 
# MAGIC A hospital's readmission rate is calculated and then risk adjusted. A ratio of predicted or measured readmissions compared to expected readmissions (based on similar hospitals) is calculated, called the excess readmission ratio. This is calculated for each of the applicable conditions. This ratio is then used to calculate the estimated payments made by CMS to the hospital for excess readmissions as a ratio of the payments by CMS for all discharges. This creates a readmissions adjustment factor, which is then used to calculate a financial penalty to the hospital for excess readmissions. To reach these calculations, up to three previous years of a hospital's data and a minimum of 25 cases for each applicable condition are used.
# MAGIC 
# MAGIC 
# MAGIC * [**Patient Re-Admission Analytics**](http://www.fiercehealthcare.com/it/paris-hospitals-use-predictive-modeling-to-control-admissions) is the use of data analytics and machine learning to predict patient admissions down to the hour and is...  
# MAGIC   * Built on top of Databricks Platform
# MAGIC   * Uses a machine learning **decision tree classifier** implementation to analysis a Patient Admission dataset   
# MAGIC * This demo...  
# MAGIC   * demonstrates a simple patient admission analysis workflow.  We use Patient dataset from the [CMS Repository](https://www.cms.gov).

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
# MAGIC - Dataset is download from the CMS dataset hosted at  [**CMS**](https://www.cms.gov)
# MAGIC - If running the notebook for the first time, please run the data_Setup notebook present in the same folder as this notebook

# COMMAND ----------

# MAGIC %fs ls /mnt/azure/readmission/

# COMMAND ----------

# DBTITLE 1,Define schema with 142 columns for data ingestion
## Schema with various data attributes, type and indicators ex: StructField("MemberID_t",IntegerType(),False),

from pyspark.sql.types import *
from pyspark.sql import functions as F

schema =StructType([
	StructField("Hospital_Name",StringType(),False),
	StructField("Provider_ID",IntegerType(),False),
	StructField("NPI",DoubleType(),False),
	StructField("State",StringType(),False),
	StructField("State_Code",StringType(),False),
	StructField("State_FIPS_Code",IntegerType(),False),
	StructField("Measure_Name",StringType(),False),
	StructField("Measure_Description",StringType(),False),
	StructField("Number_of_Discharges",IntegerType(),False),
	StructField("Footnote",IntegerType(),False),
	StructField("Excess_Readmission_Ratio",DoubleType(),False),
	StructField("Predicted_Readmission_Rate",DoubleType(),False),
	StructField("Actual_Readmission_Rate",DoubleType(),False),
	StructField("Number_of_Readmissions",StringType(),False),
	StructField("Start_Date",TimestampType(),False),
	StructField("End_Date",TimestampType(),False),
	StructField("ICD10_Code",StringType(),False),
	StructField("ICD10_Description",StringType(),False),
	StructField("HCPCS_Code",StringType(),False),
	StructField("HCPCS_Description",StringType(),False)
	])

df1 = (spark.read.option("delimiter", ",")
  .option("inferSchema", "true")  # Use tab delimiter (default is comma-separator)
   .option("header", "true")
  .schema(schema)
  .csv("dbfs:/mnt/azure/readmission/data.csv"))

df = df1.select("Hospital_Name","Provider_ID","NPI","State","State_Code","State_FIPS_Code","Measure_Name","Measure_Description","Number_of_Discharges","Footnote","Excess_Readmission_Ratio","Predicted_Readmission_Rate","Actual_Readmission_Rate","Number_of_Readmissions","Start_Date","End_Date","ICD10_Code","ICD10_Description","HCPCS_Code","HCPCS_Description")

# COMMAND ----------

# DBTITLE 1,Verify Data in a table format
display(df)

# COMMAND ----------

# DBTITLE 1,Repartition and write in parquet format
df1.repartition(1).write.format("parquet").save("/mnt/wesley/readmission-parquet/dataparquet")

# COMMAND ----------

# MAGIC %fs ls /mnt/wesley/readmission-parquet/dataparquet

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
# MAGIC       path "/mnt/wesley/readmission-parquet/dataparquet"
# MAGIC     )

# COMMAND ----------

# DBTITLE 1,Age wise analysis of patient data
# MAGIC %sql
# MAGIC select Hospital_Name,Measure_Name,sum(Number_of_Readmissions) as total_readmission from temp_idsdata group by Hospital_Name,Measure_Name sort by total_readmission desc limit 20

# COMMAND ----------

# MAGIC %md ###Step3: Explore Patient Data 

# COMMAND ----------

# DBTITLE 1,Breakdown of patient days in hospital using databricks graph 
# MAGIC %sql
# MAGIC select State,sum(Number_of_Readmissions) as total_readmission from temp_idsdata group by State order by total_readmission desc limit 20

# COMMAND ----------

# DBTITLE 1,Breakdown of patient days in hospital using matplotlib
import matplotlib.pyplot as plt
importance = sqlContext.sql("select State,sum(Number_of_Readmissions) as total_readmission from temp_idsdata group by State order by total_readmission desc limit 20")
importanceDF = importance.toPandas()
ax = importanceDF.plot(x="State", y="total_readmission",lw=3,colormap='Reds_r',title='Importance in Descending Order', fontsize=9)
ax.set_xlabel("Readmitlabel")
ax.set_ylabel("count")
plt.xticks(rotation=12)
plt.grid(True)
plt.show()
display()

# COMMAND ----------

# MAGIC %md #### Step 4: Model creation

# COMMAND ----------

from  pyspark.ml.feature import StringIndexer

indexer1 = (StringIndexer()
                   .setInputCol("Hospital_Name")
                   .setOutputCol("Hospital_Name_Index")
                   .fit(df))

indexed1 = indexer1.transform(df)

indexer2 = (StringIndexer()
                   .setInputCol("State_Code")
                   .setOutputCol("State_Code_Index")
                   .fit(indexed1))

indexed2 = indexer2.transform(indexed1)

indexer3 = (StringIndexer()
                   .setInputCol("Measure_Name")
                   .setOutputCol("Measure_Name_Index")
                   .fit(indexed2))

indexed3 = indexer3.transform(indexed2)

finaldf = indexed3.filter((col('Actual_Readmission_Rate') > 0) )

from pyspark.ml.feature import VectorAssembler
vecAssembler = VectorAssembler()
vecAssembler.setInputCols(["Hospital_Name_Index","State_Code_Index","Measure_Name_Index"])
vecAssembler.setOutputCol("features")
print vecAssembler.explainParams()

from pyspark.ml.regression import GeneralizedLinearRegression

aft = GeneralizedLinearRegression()
aft.setLabelCol("Actual_Readmission_Rate")

print aft.explainParams()

# COMMAND ----------

from pyspark.ml import Pipeline

# We will use the new spark.ml pipeline API. If you have worked with scikit-learn this will be very familiar.
lrPipeline = Pipeline(stages=[vecAssembler, aft])

# Now we'll tell the pipeline to first create the feature vector, and then do the linear regression
#lrPipeline.setStages([vecAssembler, aft])

# Pipelines are themselves Estimators -- so to use them we call fit:
lrPipelineModel = lrPipeline.fit(finaldf)

# COMMAND ----------

# DBTITLE 1,Using Model for data predicition
predictionsAndLabelsDF = lrPipelineModel.transform(finaldf)
predAnalysis = predictionsAndLabelsDF.select('Hospital_Name_Index','State_Code_Index','Measure_Name_Index','Actual_Readmission_Rate', 'prediction')
confusionMatrix = predictionsAndLabelsDF.select('Actual_Readmission_Rate', 'prediction')

# COMMAND ----------

# DBTITLE 1,Visualizing the for data prediction results
display(predAnalysis)

# COMMAND ----------

# DBTITLE 1,Save the model to DBFS
# Now we can optionally save the fitted pipeline to disk
lrPipelineModel.write().overwrite().save("/tmp/wesley/spark-logistic-regression-model")

# COMMAND ----------

# DBTITLE 1,Ignore....
from pyspark.sql import Row
patient_record = []
patient_record.append(
    Row(Hospital_Name = "THOMAS HOSPITAL",
        State_Code = "AL",
        Measure_Name = "READM-30-COPD-HRRP"))
patientRecDF = sqlContext.createDataFrame(patient_record)

# COMMAND ----------

# DBTITLE 1,Ignore....
indexed1 = indexer1.transform(patientRecDF)
indexed2 = indexer2.transform(indexed1)
indexed3 = indexer3.transform(indexed2)
finaldf = indexed3.filter((col('Hospital_Name_Index') > 0) & (col('State_Code_Index') > 0) & (col('Measure_Name_Index') > 0) )
sampledPatientRec = lrPipelineModel.transform(finaldf)
sampledPatientRec.registerTempTable("sampledPatientRec")

# COMMAND ----------

# DBTITLE 1,Ignore....
display(sqlContext.sql("select * from sampledPatientRec"))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC [**HL7 Parser**](https://demo.cloud.databricks.com/#notebook/738187/command/739138)