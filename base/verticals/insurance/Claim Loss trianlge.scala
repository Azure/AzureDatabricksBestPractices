// Databricks notebook source
// DBTITLE 1,Overview
// MAGIC %md
// MAGIC 
// MAGIC • ** Traditional approach to loss reserving** : A handful of time-tested techniques judgmentally weighted together.
// MAGIC 
// MAGIC • ** The focus of improving loss reserving** : New methods are continually being added to the repertoire of traditional approaches with emphasis on greater accuracy (and some measure of variability). Many of these methods, for example, Bootstrapping, GLMs, and Markov Chain Monte Carlo techniques, are built on advanced statistical methods.
// MAGIC 
// MAGIC • ** The next generation of loss reserving** : Full use of computer power through machine learning approaches including tree-based methods and their enhancements. This demo will focus on and test an ensembling approach to the reserving problem.
// MAGIC 
// MAGIC Some of the Data points that can help predict loss reserve risks: 
// MAGIC 
// MAGIC * Incremantal loss with respect to accidental year and valuation year
// MAGIC 
// MAGIC ![Databricks for loss reserve  Analytics](https://i.stack.imgur.com/hAtlw.png "Databricks for loss reserve Analytics")  
// MAGIC * [**loss reserve Analytics**](http://www.casact.org/education/clrs/2016/presentations/AR-1_1.pdf) is the use of data analytics and machine learning to predict claims loss reserve and is...  
// MAGIC   * Built on top of Databricks Platform
// MAGIC   * Uses a machine learning **linear regression** implementation to analysis a claim loss dataset   
// MAGIC * This demo...  
// MAGIC   * demonstrates a simple claim loss analysis workflow.  We use claim dataset from the [Casualty Acturial Society](http://www.casact.org/research/index.cfm?fa=loss_reserves_data).

// COMMAND ----------

// DBTITLE 1,0. Setup Databricks Spark cluster:
// MAGIC %md
// MAGIC 
// MAGIC **Create** a cluster by...  
// MAGIC   - Click the `Clusters` icon on the left sidebar and then `Create Cluster.` 
// MAGIC   - Enter any text, i.e `demo` into the cluster name text box
// MAGIC   - Select the `Apache Spark Version` value `Spark 2.2 (auto-updating scala 2.11)`  
// MAGIC   - Click the `create cluster` button and wait for your cluster to be provisioned
// MAGIC   
// MAGIC **Attach** this notebook to your cluster by...   
// MAGIC   - Click on your cluster name in menu `Detached` at the top left of this workbook to attach it to this workbook 

// COMMAND ----------

// DBTITLE 1,Step1: Ingest patient Data to Notebook
// MAGIC %md 
// MAGIC 
// MAGIC - We will download the claim dataset hosted at  [** Casualty Acturial Society**](http://www.casact.org/research/index.cfm?fa=loss_reserves_data)
// MAGIC - After downloading we will import this dataset into databricks notebook

// COMMAND ----------

// DBTITLE 1,Load Dataset
// MAGIC %sh
// MAGIC mkdir /tmp/claim
// MAGIC wget http://www.casact.org/research/reserve_data/ppauto_pos.csv -O /tmp/claim/ppauto_pos.csv

// COMMAND ----------

// DBTITLE 1,Mount Data
// MAGIC %py
// MAGIC dbutils.fs.mkdirs("/mnt/claimn")
// MAGIC dbutils.fs.mv("file:///tmp/claim/ppauto_pos.csv", "/mnt/wesley/dataset/Fintech/losstriangle/ppauto_pos.csv")

// COMMAND ----------

// MAGIC %fs ls /mnt/claim

// COMMAND ----------

// DBTITLE 1,Define schema with 13 columns for data ingestion
// MAGIC %python
// MAGIC 
// MAGIC from pyspark.sql.types import *
// MAGIC 
// MAGIC schema =StructType([
// MAGIC 	StructField("GRCODE",IntegerType(), False),
// MAGIC 	StructField("GRNAME",StringType(), False),
// MAGIC 	StructField("AccidentYear",IntegerType(), False),
// MAGIC 	StructField("DevelopmentYear",IntegerType(), False),
// MAGIC 	StructField("DevelopmentLag",IntegerType(), False),
// MAGIC 	StructField("IncurLoss_B",IntegerType(), False),
// MAGIC 	StructField("CumPaidLoss_B",IntegerType(), False),
// MAGIC 	StructField("BulkLoss_B",IntegerType(), False),
// MAGIC 	StructField("EarnedPremDIR_B",IntegerType(), False),
// MAGIC 	StructField("EarnedPremCeded_B",IntegerType(), False),
// MAGIC 	StructField("EarnedPremNet_B",IntegerType(), False),
// MAGIC 	StructField("Single",IntegerType(), False),
// MAGIC 	StructField("PostedReserve97_B",IntegerType(), False)
// MAGIC ])
// MAGIC     
// MAGIC df = (spark.read.option("delimiter", ",")
// MAGIC   .option("inferSchema", "true")  # Use tab delimiter (default is comma-separator)
// MAGIC    .option("header", "true")
// MAGIC   .schema(schema)
// MAGIC   .csv("dbfs:/mnt/wesley/dataset/Fintech/losstriangle/ppauto_pos.csv"))

// COMMAND ----------

// MAGIC %md ### Step2: Enrich the data to get additional insigts to patient dataset
// MAGIC - We count the number of data points and separate the churned from the unchurned.

// COMMAND ----------

// DBTITLE 1,Import SQL API Functions
// MAGIC %python
// MAGIC # Because we will need it later...
// MAGIC from pyspark.sql.functions import *
// MAGIC from pyspark.sql.types import *

// COMMAND ----------

// DBTITLE 1,Select data for Training dataset for our model to built
// MAGIC %python
// MAGIC # For each maxDepth setting, make predictions on the test data, and compute the accuracy metric.
// MAGIC accuracies_list = []
// MAGIC maxDepth = 11
// MAGIC for maxYear in range(1988,1998):
// MAGIC     tempdf = df.select("GRCODE","GRNAME","AccidentYear","DevelopmentYear","DevelopmentLag","IncurLoss_B","CumPaidLoss_B","BulkLoss_B","EarnedPremDIR_B","EarnedPremCeded_B","EarnedPremNet_B","Single","PostedReserve97_B").filter((col('DevelopmentLag') < maxDepth) & (col('AccidentYear') == maxYear) )
// MAGIC     accuracies_list.append(tempdf)
// MAGIC     maxDepth = maxDepth-1
// MAGIC     
// MAGIC def union_all(dfs):
// MAGIC     if len(dfs) > 1:
// MAGIC         return dfs[0].unionAll(union_all(dfs[1:]))
// MAGIC     else:
// MAGIC         return dfs[0]
// MAGIC 
// MAGIC traindf = union_all(accuracies_list)

// COMMAND ----------

// DBTITLE 1,Save as Parquet format
// MAGIC %python
// MAGIC traindf.repartition(1).write.parquet('/mnt/wesley/parquet/insurance/lossdata1')

// COMMAND ----------

// DBTITLE 1,Create table
// MAGIC %sql 
// MAGIC  
// MAGIC     Drop table if exists temp_idsdata1;
// MAGIC     
// MAGIC     CREATE TEMPORARY TABLE temp_idsdata1
// MAGIC     USING parquet
// MAGIC     OPTIONS (
// MAGIC       path "/mnt/wesley/parquet/insurance/lossdata1"
// MAGIC     )

// COMMAND ----------

// DBTITLE 1,Claim Loss Triangle 
// MAGIC %python
// MAGIC import seaborn as sns
// MAGIC import matplotlib.pyplot as plt
// MAGIC 
// MAGIC importance = sqlContext.sql("select AccidentYear,DevelopmentLag,CumPaidLoss_B from temp_idsdata1 where GRCODE=43 order by AccidentYear,DevelopmentLag")
// MAGIC importanceDF = importance.toPandas()
// MAGIC pivotimpdf = importanceDF.pivot("DevelopmentLag", "AccidentYear", "CumPaidLoss_B")
// MAGIC 
// MAGIC sns.set()    
// MAGIC plt.title("Loss Reserve")
// MAGIC ax = sns.heatmap(pivotimpdf, mask=pivotimpdf.isnull(),annot=True, fmt=".0f")
// MAGIC ax.set_xlabel("Accident Year")
// MAGIC ax.set_ylabel("Development Lag")
// MAGIC plt.xticks(rotation=12)
// MAGIC plt.tight_layout()
// MAGIC plt.grid(True)
// MAGIC plt.show()
// MAGIC display()

// COMMAND ----------

// MAGIC %md ###Step3: Explore Churn Data 

// COMMAND ----------

// DBTITLE 1,Max cumulative loss incurred for group ID 43 using databricks graph 
// MAGIC %sql
// MAGIC select AccidentYear,max(CumPaidLoss_B) as totalpaidloss from temp_idsdata1 where GRCODE=43 group by AccidentYear order by AccidentYear

// COMMAND ----------

// DBTITLE 1,Max cumulative loss incurred for group ID 43 using python Matplotlib
// MAGIC %python
// MAGIC import matplotlib.pyplot as plt
// MAGIC importance = sqlContext.sql("select AccidentYear,max(CumPaidLoss_B) as totalpaidloss from temp_idsdata1 where GRCODE=43 group by AccidentYear order by AccidentYear")
// MAGIC importanceDF = importance.toPandas()
// MAGIC ax = importanceDF.plot(x="AccidentYear", y="totalpaidloss",lw=3,colormap='Reds_r',title='Importance in Descending Order', fontsize=9)
// MAGIC ax.set_xlabel("AccidentYear")
// MAGIC ax.set_ylabel("totalpaidloss")
// MAGIC plt.xticks(rotation=12)
// MAGIC plt.grid(True)
// MAGIC plt.show()
// MAGIC display()

// COMMAND ----------

// MAGIC %md
// MAGIC ###Step 4: Visualization
// MAGIC * Show the distribution of the incurred claim loss.

// COMMAND ----------

// MAGIC %python
// MAGIC display(df.filter(col('GRCODE') == 43).select("IncurLoss_B"))

// COMMAND ----------

// MAGIC %md #### Step 5: Model creation

// COMMAND ----------

// MAGIC %python
// MAGIC from  pyspark.ml.feature import StringIndexer
// MAGIC 
// MAGIC from pyspark.ml.feature import VectorAssembler
// MAGIC vecAssembler = VectorAssembler()
// MAGIC vecAssembler.setInputCols(["DevelopmentLag","IncurLoss_B","BulkLoss_B","EarnedPremDIR_B","EarnedPremCeded_B","EarnedPremNet_B","Single","PostedReserve97_B"])
// MAGIC vecAssembler.setOutputCol("features")
// MAGIC print vecAssembler.explainParams()
// MAGIC 
// MAGIC from pyspark.ml.regression import DecisionTreeRegressor
// MAGIC 
// MAGIC aft = DecisionTreeRegressor()
// MAGIC aft.setLabelCol("CumPaidLoss_B")
// MAGIC aft.setMaxDepth(30)
// MAGIC aft.setMaxBins(100000)
// MAGIC 
// MAGIC print aft.explainParams()

// COMMAND ----------

// MAGIC %python
// MAGIC from pyspark.ml import Pipeline
// MAGIC 
// MAGIC # We will use the new spark.ml pipeline API. If you have worked with scikit-learn this will be very familiar.
// MAGIC lrPipeline = Pipeline()
// MAGIC 
// MAGIC # Now we'll tell the pipeline to first create the feature vector, and then do the linear regression
// MAGIC lrPipeline.setStages([vecAssembler, aft])
// MAGIC 
// MAGIC # Pipelines are themselves Estimators -- so to use them we call fit:
// MAGIC lrPipelineModel = lrPipeline.fit(traindf)

// COMMAND ----------

// DBTITLE 1,Using Model for data predicition
// MAGIC %python
// MAGIC predictionsAndLabelsDF = lrPipelineModel.transform(df)
// MAGIC display(predictionsAndLabelsDF.select("GRCODE","GRNAME","AccidentYear","DevelopmentYear","DevelopmentLag",'CumPaidLoss_B', 'prediction'))

// COMMAND ----------

// DBTITLE 1,Performance Metrics of the model
// MAGIC %python
// MAGIC from pyspark.ml.evaluation import RegressionEvaluator
// MAGIC eval = RegressionEvaluator()
// MAGIC eval.setLabelCol("CumPaidLoss_B")
// MAGIC print eval.explainParams()
// MAGIC 
// MAGIC # Default metric is RMSE
// MAGIC eval.evaluate(predictionsAndLabelsDF)
// MAGIC # ANSWER
// MAGIC eval.setMetricName("r2").evaluate(predictionsAndLabelsDF)

// COMMAND ----------

// DBTITLE 1,Visualizing the for data prediction results
// MAGIC %python
// MAGIC # For each maxDepth setting, make predictions on the test data, and compute the accuracy metric.
// MAGIC accuracies_list = []
// MAGIC maxDepth = 10
// MAGIC for maxYear in range(1988,1998):
// MAGIC     tempdf = predictionsAndLabelsDF.select("GRCODE","GRNAME","AccidentYear","DevelopmentYear","DevelopmentLag","IncurLoss_B","CumPaidLoss_B","BulkLoss_B","EarnedPremDIR_B","EarnedPremCeded_B","EarnedPremNet_B","Single","PostedReserve97_B","prediction").filter((col('DevelopmentLag') > maxDepth) & (col('AccidentYear') == maxYear) )
// MAGIC     accuracies_list.append(tempdf)
// MAGIC     maxDepth = maxDepth-1
// MAGIC     
// MAGIC def union_all(dfs):
// MAGIC     if len(dfs) > 1:
// MAGIC         return dfs[0].unionAll(union_all(dfs[1:]))
// MAGIC     else:
// MAGIC         return dfs[0]
// MAGIC 
// MAGIC preddf = union_all(accuracies_list)
// MAGIC 
// MAGIC preddf.repartition(1).write.mode("Overwrite").parquet('/mnt/wesley/parquet/insurance/lossdata2')

// COMMAND ----------

// MAGIC %sql 
// MAGIC  
// MAGIC     DROP TABLE IF EXISTS temp_idsdata2;
// MAGIC     
// MAGIC     CREATE TEMPORARY TABLE temp_idsdata2
// MAGIC     USING parquet
// MAGIC     OPTIONS (
// MAGIC       path "/mnt/wesley/parquet/insurance/lossdata2"
// MAGIC     )

// COMMAND ----------

// MAGIC %python
// MAGIC import seaborn as sns
// MAGIC import matplotlib.pyplot as plt
// MAGIC 
// MAGIC importancepred = sqlContext.sql("select AccidentYear,DevelopmentLag,prediction from temp_idsdata2 where GRCODE=43 order by AccidentYear,DevelopmentLag")
// MAGIC importancepredDF = importancepred.toPandas()
// MAGIC pivotimppreddf = importancepredDF.pivot("DevelopmentLag", "AccidentYear", "prediction")
// MAGIC 
// MAGIC sns.set()    
// MAGIC plt.title("Loss Reserve prediction")
// MAGIC ax = sns.heatmap(pivotimppreddf, mask=pivotimppreddf.isnull(),annot=True, fmt=".0f")
// MAGIC plt.xticks(rotation=12)
// MAGIC plt.tight_layout()
// MAGIC plt.ylabel('Development Lag')
// MAGIC plt.xlabel('Accident Year')
// MAGIC plt.show()
// MAGIC display()

// COMMAND ----------

// MAGIC %md
// MAGIC ##Results interpretation
// MAGIC The plot above shows Index used to measure each of the patent admission days. 
// MAGIC 
// MAGIC ![Churn-Index](http://www.ngdata.com/wp-content/uploads/2016/05/churn.jpg)
// MAGIC 
// MAGIC 1. The loss reserve evaluation index using logistic regression algorithm gives and accuracy of ~99%
// MAGIC 2. Using this method we can predict the loss reserve for the lower half of the loss triangle using data features like Posted reserve and premium