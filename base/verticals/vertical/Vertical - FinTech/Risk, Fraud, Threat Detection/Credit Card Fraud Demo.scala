// Databricks notebook source
// MAGIC %md #Credit Card Fraud Demo
// MAGIC 
// MAGIC - According to Forbes, Fraud losses incurred by banks and merchants on all credit, debit, and pre-paid general purpose and private label payment cards issued globally hit $21.84 billion (bn) in 2015, with the United States (US) accounting for almost two-fifths (38.7%) of the total at $8.45bn. But by 2020 it could surpass $12bn, were the global percentage growth rate of 45% to be mirrored.
// MAGIC 
// MAGIC * In this demo we have a series of credit card transactions. 
// MAGIC * Those credit card transactions have been labeled as fraud or not fraud. 
// MAGIC * This data was anonymized and distributed for a fraud detection student competition by for Capital One Financial Corporation. 
// MAGIC * Capital One Financial Corporation has authorized Databricks to use of this data.

// COMMAND ----------

// MAGIC %md ##Fraud Detection and Analysis Process
// MAGIC 1. Ingest the Data
// MAGIC 2. Explore the Data
// MAGIC 3. Featurize Data with SQL
// MAGIC 4. Train a Model
// MAGIC 5. Evaluate the Model
// MAGIC 
// MAGIC ![Credit Card ML](/files/img/fraud_ml_pipeline.png)

// COMMAND ----------

// MAGIC %md
// MAGIC ###0. SETUP -- Databricks Spark cluster:  
// MAGIC 
// MAGIC 1. **Create** a cluster by...  
// MAGIC   - Click the `Clusters` icon on the left sidebar and then `Create Cluster.` OR Click [Here](https://demo.cloud.databricks.com/#clusters/create) 
// MAGIC   - Enter any text, i.e `demo` into the cluster name text box
// MAGIC   - Select the `Apache Spark Version` value `Spark 2.2 (auto-updating scala 2.11)`  
// MAGIC   - Click the `create cluster` button and wait for your cluster to be provisioned
// MAGIC 3. **Attach** this notebook to your cluster by...   
// MAGIC   - Click on your cluster name in menu `Detached` at the top left of this workbook to attach it to this workbook 

// COMMAND ----------

// DBTITLE 1,Step1: Ingest CreditCard Data to Notebook
// MAGIC %md 
// MAGIC 
// MAGIC - We use internal generated dataset of credit card user
// MAGIC - The file is simply text separated by pipe characters. We use the Spark CSV library to import the data and determine the schema

// COMMAND ----------

// MAGIC %run ./data_Setup

// COMMAND ----------

// DBTITLE 1,CSV File is 20.9 GB
val fileSize = dbutils.fs.ls("wasbs://dataset@wesdias.blob.core.windows.net/Azure/dataset/Fintech/CreditCard/credit_card_data.txt").map(_.size).sum / (Math.pow(1024.0,3))
println(f"Size of file in GB: $fileSize%2.2f")

// COMMAND ----------

// DBTITLE 1,Register Table (auth_data) From CSV
val rawAuth = sqlContext.read.format("csv").option("delimiter", "|").option("inferSchema", "true").load("wasbs://dataset@wesdias.blob.core.windows.net/Azure/dataset/Fintech/CreditCard/credit_card_data.txt")

rawAuth.toDF("AUTH_ID",
"ACCT_ID_TOKEN",
"FRD_IND",
"ACCT_ACTVN_DT",
"ACCT_AVL_CASH_BEFORE_AMT",
"ACCT_AVL_MONEY_BEFORE_AMT",
"ACCT_CL_AMT",
"ACCT_CURR_BAL",
"ACCT_MULTICARD_IND",
"ACCT_OPEN_DT",
"ACCT_PROD_CD",
"ACCT_TYPE_CD",
"ADR_VFCN_FRMT_CD",
"ADR_VFCN_RESPNS_CD",
"APPRD_AUTHZN_CNT",
"APPRD_CASH_AUTHZN_CNT",
"ARQC_RSLT_CD",
"AUTHZN_ACCT_STAT_CD",
"AUTHZN_AMT",
"AUTHZN_CATG_CD",
"AUTHZN_CHAR_CD",
"AUTHZN_OPSET_ID",
"AUTHZN_ORIG_SRC_ID",
"AUTHZN_OUTSTD_AMT",
"AUTHZN_OUTSTD_CASH_AMT",
"AUTHZN_RQST_PROC_CD",
"AUTHZN_RQST_PROC_DT",
"AUTHZN_RQST_PROC_TM",
"AUTHZN_RQST_TYPE_CD",
"AUTHZN_TRMNL_PIN_CAPBLT_NUM",
"AVG_DLY_AUTHZN_AMT",
"CARD_VFCN_2_RESPNS_CD",
"CARD_VFCN_2_VLDTN_DUR",
"CARD_VFCN_MSMT_REAS_CD",
"CARD_VFCN_PRESNC_CD",
"CARD_VFCN_RESPNS_CD",
"CARD_VFCN2_VLDTN_CD",
"CDHLDR_PRES_CD",
"CRCY_CNVRSN_RT",
"ELCTR_CMRC_IND_CD",
"HOME_PHN_NUM_CHNG_DUR",
"HOTEL_STAY_CAR_RENTL_DUR",
"LAST_ADR_CHNG_DUR",
"LAST_PLSTC_RQST_REAS_CD",
"MRCH_CATG_CD",
"MRCH_CNTRY_CD",
"NEW_USER_ADDED_DUR",
"PHN_CHNG_SNC_APPN_IND",
"PIN_BLK_CD",
"PIN_VLDTN_IND",
"PLSTC_ACTVN_DT",
"PLSTC_ACTVN_REQD_IND",
"PLSTC_FRST_USE_TS",
"PLSTC_ISU_DUR",
"PLSTC_PREV_CURR_CD",
"PLSTC_RQST_TS",
"POS_COND_CD",
"POS_ENTRY_MTHD_CD",
"RCURG_AUTHZN_IND",
"RVRSL_IND",
"SENDR_RSIDNL_CNTRY_CD",
"SRC_CRCY_CD",
"SRC_CRCY_DCML_PSN_NUM",
"TRMNL_ATTNDNC_CD",
"TRMNL_CAPBLT_CD",
"TRMNL_CLASFN_CD",
"TRMNL_ID",
"TRMNL_PIN_CAPBLT_CD",
"DISTANCE_FROM_HOME").repartition((fileSize/0.25).toInt).write.mode(SaveMode.Overwrite).format("parquet").saveAsTable("auth_data")

// COMMAND ----------

// DBTITLE 1,Step2: Explore Credit card Data 
// MAGIC %md Print the schema, display the first 1,000 rows, use describe to get some statistics

// COMMAND ----------

// DBTITLE 1,Run SQL Query on Table
// MAGIC %sql SELECT * FROM auth_data

// COMMAND ----------

// DBTITLE 1,Describe Statistics of Table
display(table("auth_data").describe())

// COMMAND ----------

// MAGIC %md
// MAGIC ###Step 3: Visualization
// MAGIC * Show the distribution of the account length.

// COMMAND ----------

// DBTITLE 1,Run Query To Show Example
// MAGIC %sql select 
// MAGIC   acct_id_token
// MAGIC , auth_id
// MAGIC , case when FRD_IND = 'Y' then 1 else 0 end fraud_reported 
// MAGIC , cast(AUTHZN_RQST_PROC_DT as bigint) -  cast (ACCT_OPEN_DT as bigint) account_age
// MAGIC , cast(PLSTC_ACTVN_DT as bigint) -  cast (ACCT_OPEN_DT as bigint) activation_age
// MAGIC , cast(PLSTC_FRST_USE_TS as bigint) -  cast (ACCT_OPEN_DT as bigint) time_since_first_use
// MAGIC , int(substring(AUTHZN_RQST_PROC_TM, 0,2)) time_of_day
// MAGIC , AUTHZN_AMT
// MAGIC , ACCT_AVL_CASH_BEFORE_AMT
// MAGIC , ACCT_AVL_MONEY_BEFORE_AMT
// MAGIC , ACCT_CL_AMT
// MAGIC , ACCT_CURR_BAL
// MAGIC , AUTHZN_OUTSTD_AMT	
// MAGIC , AUTHZN_OUTSTD_CASH_AMT
// MAGIC , APPRD_AUTHZN_CNT
// MAGIC , APPRD_CASH_AUTHZN_CNT
// MAGIC , ACCT_PROD_CD
// MAGIC , AUTHZN_CHAR_CD
// MAGIC , AUTHZN_CATG_CD
// MAGIC , CARD_VFCN_2_RESPNS_CD
// MAGIC , CARD_VFCN_2_VLDTN_DUR
// MAGIC , POS_ENTRY_MTHD_CD
// MAGIC , TRMNL_ATTNDNC_CD
// MAGIC , TRMNL_CLASFN_CD
// MAGIC , DISTANCE_FROM_HOME
// MAGIC from auth_data

// COMMAND ----------

// DBTITLE 1,Create DataFrame from Query
val query = sql(""" select 
  acct_id_token
, auth_id
, double(case when FRD_IND = 'Y' then 1.0 else 0.0 end) fraud_reported 
, cast(AUTHZN_RQST_PROC_DT as bigint) -  cast (ACCT_OPEN_DT as bigint) account_age
, cast(PLSTC_ACTVN_DT as bigint) -  cast (ACCT_OPEN_DT as bigint) activation_age
,  cast(PLSTC_FRST_USE_TS as bigint) -  cast (ACCT_OPEN_DT as bigint) time_since_first_use
, int(substring(AUTHZN_RQST_PROC_TM, 0,2)) time_of_day
, AUTHZN_AMT
, ACCT_AVL_CASH_BEFORE_AMT
, ACCT_AVL_MONEY_BEFORE_AMT
, ACCT_CL_AMT
, ACCT_CURR_BAL
, AUTHZN_OUTSTD_AMT	
, AUTHZN_OUTSTD_CASH_AMT
, APPRD_AUTHZN_CNT
, APPRD_CASH_AUTHZN_CNT
, AUTHZN_CHAR_CD
, AUTHZN_CATG_CD
, CARD_VFCN_2_RESPNS_CD
, CARD_VFCN_2_VLDTN_DUR
, POS_ENTRY_MTHD_CD
, TRMNL_ATTNDNC_CD
, TRMNL_CLASFN_CD
, DISTANCE_FROM_HOME
from auth_data""")

// COMMAND ----------

// MAGIC %md #### Step 4: Model creation
// MAGIC * Use MLLib to train a LogisticRegressionModel
// MAGIC * 20 features and 20 million rows
// MAGIC * Split data into Categoricals
// MAGIC * Use StringIndexers to Index the Categoricals
// MAGIC * Use a StandardScaler to scale the numeric features based on a normal distribution

// COMMAND ----------

// DBTITLE 1,Define Feature Columns & Default Null Values
import org.apache.spark.ml._
import org.apache.spark.ml.feature._
import org.apache.spark.ml.classification._
import org.apache.spark.ml.tuning._
import org.apache.spark.ml.evaluation._
import org.apache.spark.sql.functions._
 

val features = query.columns.clone.filterNot(
  x => x.toLowerCase.contains("fraud_reported") ||  
  x.toLowerCase.contains("acct_id_token") ||
  x.toLowerCase.contains("auth_id")
)

val categoricals = features.filter(x => x.contains("_CD"))
val numerics = features.filterNot(x => x.contains("_CD"))

val authDataSet = query.na.fill(0.0).na.fill("N/A")

// COMMAND ----------

// DBTITLE 1,Data Ready To Train Model
display(authDataSet)

// COMMAND ----------

// DBTITLE 1,Define ML Pipeline to Engineer Features
val stringIndexers = categoricals.map(x => new StringIndexer().setInputCol(x).setOutputCol(x+"_IDX"))
val catColumns =  categoricals.map(_ +"_IDX")
val vaNumerics = new VectorAssembler().setInputCols(numerics).setOutputCol("numericFeatures")
val vaCategorical = new VectorAssembler().setInputCols(catColumns).setOutputCol("categoricalFeatures")
val scaler = new StandardScaler().setInputCol("numericFeatures").setOutputCol("scaledNumericFeatures")
val allFeatures = new VectorAssembler().setInputCols(Array("scaledNumericFeatures", "categoricalFeatures")).setOutputCol("features")
val stages = stringIndexers :+ vaNumerics :+ scaler :+ vaCategorical :+ allFeatures
val featurePipeline = new Pipeline().setStages(stages)

// COMMAND ----------

// DBTITLE 1,Engineer Features
val featureModel = featurePipeline.fit(authDataSet)

featureModel.stages.filter(x => x.isInstanceOf[StringIndexerModel]).map(x => (x.asInstanceOf[StringIndexerModel].getInputCol, x.asInstanceOf[StringIndexerModel].labels.size))

val featurizedDataset = featureModel.transform(authDataSet)

// COMMAND ----------

// DBTITLE 1,Display New Features
display(featurizedDataset)

// COMMAND ----------

// DBTITLE 1,Cache Features in Memory
featurizedDataset.cache().rdd.count

// COMMAND ----------

// DBTITLE 1,Train Model Against Features
//val dt = new DecisionTreeClassifier().setFeaturesCol("features").setMaxDepth(10).setLabelCol("fraud_reported").setSeed(10020L)
val lr = new LogisticRegression().setTol(1e-9).setFeaturesCol("features").setLabelCol("fraud_reported").setRegParam(0.01)
val paramGrid = new ParamGridBuilder().addGrid(lr.maxIter, Array(100)).build()
val eval = new BinaryClassificationEvaluator().setLabelCol("fraud_reported").setMetricName("areaUnderROC")
//crossval.setEstimatorParamMaps(paramGrid)
val tvs = new TrainValidationSplit().setEstimator(lr).setSeed(10020L).setEstimatorParamMaps(paramGrid).setEvaluator(eval)
val model = tvs.fit(featurizedDataset)

// COMMAND ----------

// MAGIC %md ###Step5: Evaluate the Model
// MAGIC 
// MAGIC * Check the Area Under the Curve which 0.75 (which is an average score not that great)
// MAGIC * Examine the intercept and the coefficients
// MAGIC * Lastly we examine the absolute value of the coefficients as a general measure of the importance of the various variables we used in the predictive model

// COMMAND ----------

model.validationMetrics

// COMMAND ----------

val weights = sqlContext.createDataFrame((numerics++catColumns).zip(model.bestModel.asInstanceOf[LogisticRegressionModel].coefficients.toArray)).toDF("feature", "coefficient")

// COMMAND ----------

println("Intercept: " + model.bestModel.asInstanceOf[LogisticRegressionModel].intercept)
display(weights)

// COMMAND ----------

display(
  weights.selectExpr("feature", "abs(coefficient) as weight")
  .orderBy(desc("weight")).limit(10)
)

// COMMAND ----------

// MAGIC %md
// MAGIC ##Results interpretation
// MAGIC 
// MAGIC The table shown above gives the top ten features that influence the outcome of a credit card fraud
// MAGIC 
// MAGIC ![Recomendation-Index](http://trouvus.com/wp-content/uploads/2016/03/2.1.png)