// Databricks notebook source
// MAGIC %md
// MAGIC 
// MAGIC #Evaluating Risk for Loan Approvals (XGBoost 0.81)
// MAGIC 
// MAGIC ## Business Value
// MAGIC 
// MAGIC Being able to accurately assess the risk of a loan application can save a lender the cost of holding too many risky assets. Rather than a credit score or credit history which tracks how reliable borrowers are, we will generate a score of how profitable a loan will be compared to other loans in the past. The combination of credit scores, credit history, and profitability score will help increase the bottom line for financial institution.
// MAGIC 
// MAGIC Having a interporable model that an loan officer can use before performing a full underwriting can provide immediate estimate and response for the borrower and a informative view for the lender.
// MAGIC 
// MAGIC <a href="https://ibb.co/cuQYr6"><img src="https://preview.ibb.co/jNxPym/Image.png" alt="Image" border="0"></a>
// MAGIC 
// MAGIC This notebook is dependent on the original Loan Risk Analysis Python notebook and executes its binary classifier using XGBoost.
// MAGIC 
// MAGIC * Use the Databricks Runtime 5.0 ML Beta where XGBoost ([v.0.81](https://xgboost.readthedocs.io/en/latest/jvm/scaladocs/xgboost4j-spark/index.html#package)) is automatically installed 
// MAGIC * This notebook has been tested with *DBR 5.4 ML Beta, Python 3*
// MAGIC * To install XGBoost on an existing cluster, refer to [Databricks Documentation > XGBoost](https://docs.databricks.com/spark/latest/mllib/third-party-libraries.html#xgboost) 
// MAGIC 
// MAGIC Helpful References:
// MAGIC * [Integrate XGBoost with ML Pipelines](https://docs.databricks.com/spark/latest/mllib/third-party-libraries.html#integrate-xgboost-with-ml-pipelines)
// MAGIC * [MLlib Evaluation Metrics > Binary Classification](https://spark.apache.org/docs/latest/mllib-evaluation-metrics.html#binary-classification)
// MAGIC * [Binary Classification > MLlib Pipelines](https://docs.databricks.com/spark/latest/mllib/binary-classification-mllib-pipelines.html)
// MAGIC * [ML Tuning: model selection and hyperparameter tuning > Cross-Validation](https://spark.apache.org/docs/latest/ml-tuning.html#cross-validation)

// COMMAND ----------

// DBTITLE 1,Define columns
import ml.dmlc.xgboost4j.scala.spark.{XGBoost,XGBoostClassifier}
import org.apache.spark.ml.feature._
import org.apache.spark.ml._
import org.apache.spark.sql.functions._

// Define our categorical and numeric columns
val categoricals = Array("term", "home_ownership", "purpose", "addr_state","verification_status","application_type")
val numerics = Array("loan_amnt","emp_length", "annual_inc","dti","delinq_2yrs","revol_util","total_acc","credit_length_in_years")

// COMMAND ----------

// DBTITLE 1,Create column lists for downstream ML pipeline
// imputer output column list 
val numerics_out = numerics.map(_ + "_out")

// stringindexer output column list
val categoricals_idx = categoricals.map(_ + "_idx")

// One Hot Encoder output column list
val categoricals_class = categoricals.map(_ + "_class")

// COMMAND ----------

// DBTITLE 1,Load Training and Validation Data 
// Load Training data where ETL was executed in previous `Loan Risk Analyis (Python) notebook 
val dataset_train = table("loanstats_train").cache()
val dataset_valid = table("loanstats_valid").cache()

// COMMAND ----------

// DBTITLE 1,Build and Train XGBoost ML Pipeline
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.StringIndexer

// Imputation estimator for completing missing values
val numerics_out = numerics.map(_ + "_out")
val imputers = new Imputer()
  .setInputCols(numerics)
  .setOutputCols(numerics_out)

// Apply StringIndexer for our categorical data
val categoricals_idx = categoricals.map(_ + "_idx")
val indexers = categoricals.map(
  x => new StringIndexer().setInputCol(x).setOutputCol(x + "_idx").setHandleInvalid("keep")
)

// Apply OHE for our StringIndexed categorical data
val categoricals_class = categoricals.map(_ + "_class")
val oneHotEncoders = new OneHotEncoderEstimator()
  .setInputCols(categoricals_idx)
  .setOutputCols(categoricals_class)

// Set feature columns
val featureCols = categoricals_class ++ numerics_out

// Create assembler for our numeric columns (including label)
val assembler =  new VectorAssembler()
  .setInputCols(featureCols)
  .setOutputCol("features")

// Establish label
val labelIndexer = new StringIndexer()
  .setInputCol("bad_loan")
  .setOutputCol("label")

// Apply StandardScaler
val scaler = new StandardScaler()
  .setInputCol("features")
  .setOutputCol("scaledFeatures")
  .setWithMean(true)
  .setWithStd(true)

// Build pipeline array
val pipelineAry = indexers ++ Array(oneHotEncoders, imputers, assembler, labelIndexer, scaler)

// COMMAND ----------

// Create XGBoostClassifier
val xgBoostClassifier = new XGBoostClassifier(
         Map[String, Any](
           "num_round" -> 5, 
           "objective" -> "binary:logistic", 
           "nworkers" -> 16, 
           "nthreads" -> 4
         )
  )
  .setFeaturesCol("scaledFeatures")
  .setLabelCol("label")

// Create XGBoost Pipeline
val xgBoostPipeline = new Pipeline().setStages(pipelineAry :+ xgBoostClassifier)

// Create XGBoost Model based on the training dataset
val xgBoostModel = xgBoostPipeline.fit(dataset_train)

// COMMAND ----------

// DBTITLE 1,Execute XGBoost Model against Test Dataset
// Test our model against the validation dataset
val predictions = xgBoostModel.transform(dataset_valid)
display(predictions.select("probability", "label"))

// COMMAND ----------

// DBTITLE 1,Review Metrics
// Include BinaryClassificationEvaluator
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator

// Evaluate 
val evaluator = new BinaryClassificationEvaluator()
  .setRawPredictionCol("probability")

// Calculate Validation AUC
val auc = evaluator.evaluate(predictions)

// COMMAND ----------

// DBTITLE 1,Tune Model using MLlib Cross Validation
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel, ParamGridBuilder}

// Build parameter grid
val paramGrid = new ParamGridBuilder()
      .addGrid(xgBoostClassifier.maxDepth, Array(4, 7))
      .addGrid(xgBoostClassifier.eta, Array(0.1, 0.6))
      .addGrid(xgBoostClassifier.numRound, Array(5, 10))
      .build()

// Set evaluator as a BinaryClassificationEvaluator
val evaluator = new BinaryClassificationEvaluator()
  .setRawPredictionCol("probability")

// Establish CrossValidator()
val cv = new CrossValidator()
      .setEstimator(xgBoostPipeline)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(4)

// Run cross-validation, and choose the best set of parameters.
val cvModel = cv.fit(dataset_train)

// COMMAND ----------

// // Make predictions on dataset_valid; cvModel uses the best model found.
// import org.apache.spark.ml.linalg.Vector
// cvModel.transform(dataset_valid)
//   .select("home_ownership", "loan_amnt", "purpose", "addr_state", "verification_status", "bad_loan", "probabilities", "prediction")
//   .collect()
//   .foreach { case Row(home_ownership: String, loan_amnt: Float, purpose: String, addr_state: String, verification_status: String, bad_loan: String, prob: Vector, prediction: Double) =>
//     println(s"($home_ownership, $loan_amnt, $purpose, $addr_state, $verification_status, $bad_loan) --> prob=$prob, prediction=$prediction")
//   }

// COMMAND ----------

// Test our model against the cvModel and validation dataset
val predictions_cv = cvModel.transform(dataset_valid)
display(predictions_cv.select("probability", "label"))

// COMMAND ----------

// Calculate cvModel Validation AUC 
val cvAUC = evaluator.evaluate(predictions_cv)

// COMMAND ----------

// Review bestModel parameters
cvModel.bestModel.asInstanceOf[PipelineModel].stages(11).extractParamMap

// COMMAND ----------

// DBTITLE 1,Business Value
// Confusion Matrix
// Prediction = 1, Label = 1 (Blue Bar  ): Correctly found bad loans. sum_net = loss avoided
// Prediction = 1, Label = 0 (Orange Bar): Incorrectly labeled bad loans. sum_net = profit forfeited.
// Prediction = 0, Label = 1 (Green Bar ): Incorrectly labeled good loans. sum_net = loss still incurred.
// Prediction = 0, Label = 0 (Red Bar   ): Correctly found good loans. sum_net = profit retained.

// Value gained from implementing model = -(loss avoided-profit forfeited)
display(predictions_cv.groupBy("label", "prediction").agg((sum(col("net"))/(1E6)).alias("sum_net_mill")))

// COMMAND ----------

