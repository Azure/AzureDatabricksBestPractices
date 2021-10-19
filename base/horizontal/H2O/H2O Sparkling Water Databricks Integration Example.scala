// Databricks notebook source
// MAGIC %md 
// MAGIC #H2O Sparkling Water Databricks Integration Example
// MAGIC 
// MAGIC [H2O](http://www.h2o.ai/) is an open source machine learning project for distributed machine learning much like Apache Spark(tm).
// MAGIC 
// MAGIC This guide explains how to integrate with H2O using the [Sparkling Water](http://docs.h2o.ai/h2o/latest-stable/h2o-docs/faq.html#sparkling-water) module.

// COMMAND ----------

import org.apache.spark.h2o._
val h2oConf = new H2OConf(sc).set("spark.ui.enabled", "false")
val h2oContext = H2OContext.getOrCreate(sc, h2oConf)

// COMMAND ----------

// DBTITLE 1,Ingest Data to Notebook and Perform Exploration
// MAGIC %md 
// MAGIC 
// MAGIC - We will use the "sms dataset" from the Kaggle site.  This is a dataset hosted on [**Databricks**](https://www.kaggle.com/uciml/sms-spam-collection-dataset/data)
// MAGIC - Our task will be to predict the type of sms(ham or spam) from its properties.

// COMMAND ----------

// MAGIC %sql 
// MAGIC DROP TABLE IF EXISTS smsData;
// MAGIC CREATE TABLE smsData(hamorspam string, msg string)
// MAGIC USING com.databricks.spark.csv
// MAGIC OPTIONS (path "dbfs:/databricks-datasets/sms_spam_collection/data-001/smsData.csv", delimiter "\t", inferSchema "true")

// COMMAND ----------

// MAGIC %sql select  hamorspam `Ham Or Spam`, count(1) cnt from smsdata group by hamorspam

// COMMAND ----------

val smsData = spark.table("smsData")
spark.udf.register("removeSpecialChars", (s : String) => {
    val ignoredChars = Seq(',', ':', ';', '/', '<', '>', '"', '.', '(', ')', '?', '-', '\'','!','0', '1')
    var result : String = s
    for( c <- ignoredChars) {
         result = result.replace(c, ' ')
    }
    result
 })
 
 val smsDataNoSpecialChars = smsData.selectExpr("*", "removeSpecialChars(msg) as msg_no_special_chars")

// COMMAND ----------

// DBTITLE 1,Build a Spam Classifier in MLlib
import org.apache.spark.ml.feature.{Tokenizer, StopWordsRemover, IDF, VectorAssembler, HashingTF, StringIndexer}
import org.apache.spark.ml.Pipeline

val tok = new Tokenizer()
  .setInputCol("msg_no_special_chars")
  .setOutputCol("words")
val sw = new StopWordsRemover()
  .setInputCol("words")
  .setOutputCol("filtered_words")
val tf = new HashingTF()
  .setInputCol("filtered_words")
  .setOutputCol("tf")
  .setNumFeatures(10000)
val idf = new IDF()
  .setInputCol("tf")
  .setOutputCol("tf_idf")
val labeler = new StringIndexer()
  .setInputCol("hamorspam")
  .setOutputCol("label")
val assembler = new VectorAssembler()
  .setInputCols(Array("tf_idf"))
  .setOutputCol("features")

val pipeline = new Pipeline().setStages(Array(tok, sw, tf, idf, labeler, assembler))

val sparkDataPrepModel = pipeline.fit(smsDataNoSpecialChars)
val sparkPreparedData = sparkDataPrepModel.transform(smsDataNoSpecialChars)

display(sparkPreparedData)

// COMMAND ----------

// DBTITLE 1,Build Other Classifiers Using h2o
import h2oContext._
import h2oContext.implicits._
import hex.tree.drf.DRF;
import hex.tree.drf.DRFModel;
import hex.tree.drf.DRFModel.DRFParameters;
import hex.tree.gbm.GBM;
import hex.tree.gbm.GBMModel;
import hex.tree.gbm.GBMModel.GBMParameters;
import hex.ensemble.StackedEnsemble;
import hex.StackedEnsembleModel;
import hex.StackedEnsembleModel.StackedEnsembleParameters
import water.Key

val Array(trainDf, testDf) = sparkPreparedData.randomSplit(Array(0.7, 0.3))

val train = h2oContext.asH2OFrame(trainDf.select("label", "features"))
val valid = h2oContext.asH2OFrame(testDf.select("label", "features"))

// COMMAND ----------

// MAGIC %md You can evaluate using the random forrest model 

// COMMAND ----------

val drfparams = new DRFParameters()
	drfparams._train = train
    drfparams._valid = valid 
	drfparams._response_column = 'target
	drfparams._ntrees = 100
	drfparams._nfolds = 2
	drfparams._keep_cross_validation_predictions = true
	drfparams._max_depth = 10
	drfparams._seed = 10
    drfparams._response_column = "label"
    drfparams._ignored_columns = Array("id")

val drf  = new DRF(drfparams);
val drfModel = drf.trainModel.get

// COMMAND ----------

// MAGIC %md You can evaluate using the gradient boosting(GBM) model 

// COMMAND ----------

val gbmparams = new GBMParameters()
	gbmparams._train = train
    gbmparams._valid = valid 
	gbmparams._nfolds = 2
	gbmparams._keep_cross_validation_predictions = true
	gbmparams._response_column = 'target
    gbmparams._response_column = "label"
	gbmparams._seed = 10
    gbmparams._ignored_columns = Array("id")

val gbm  = new GBM(gbmparams,water.Key.make("gbmModel.hex"));
val gbmModel = gbm.trainModel.get

// COMMAND ----------

// DBTITLE 1,Create an Ensemble Model
val separams = new StackedEnsembleParameters()
	separams._train = train
    separams._response_column = "label"
    separams._ignored_columns = Array("id")
    separams._base_models=Array(water.Key.make("gbmModel.hex"),water.Key.make("DRF_model_1505885569654_17"))

val se  = new StackedEnsemble(separams);
val seModel = se.trainModel