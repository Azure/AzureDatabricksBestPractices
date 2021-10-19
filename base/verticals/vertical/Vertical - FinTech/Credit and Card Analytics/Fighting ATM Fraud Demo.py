# Databricks notebook source
# DBTITLE 1,Overview
# MAGIC %md
# MAGIC *** According to the Secret Service, the crime is responsible for about $350,000 of monetary losses each day in the United States and is considered to be the number one ATM-related crime. Trade group Global ATM Security Alliance estimates that skimming costs the U.S.-banking industry about $60 million a year.***
# MAGIC 
# MAGIC The easiest way that companies identify atm fraud is by recognizing a break in spending patterns.  For example, if you live in Wichita, KS and suddenly your card is used to buy something in Bend, OR – that may tip the scales in favor of possible fraud, and your credit card company might decline the charges and ask you to verify them.
# MAGIC 
# MAGIC * [**ATM Fraud Analytics**](https://www.csoonline.com/article/2124891/fraud-prevention/atm-skimming--how-to-recognize-card-fraud.html) is the use of data analytics and machine learning to detect ATM fraud and is...  
# MAGIC   * Built on top of Databricks Platform
# MAGIC   * Uses a machine learning implementation to detect ATM fraud   
# MAGIC * This demo...  
# MAGIC   * demonstrates a ATM fraud detection workflow.  We use dataset is internally mocked up data.

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
# MAGIC   - Add the spark-xml library to the cluster created above. The library is present in libs folder under current user.

# COMMAND ----------

# DBTITLE 1,Step1: Ingest the Data to Notebook
# MAGIC %md 
# MAGIC 
# MAGIC - If this the first time your running this notebook make sure you run the "Table_Setup" Notebook to populate the table to be used in this notebook. This notebook is kept under the same folder as this notebook.
# MAGIC - We will use the user atm usage dataset that is internally gernerated

# COMMAND ----------

# MAGIC %sql use fraud

# COMMAND ----------

# MAGIC %sql select * from atm_visits 

# COMMAND ----------

# MAGIC %md ###Step2: Explore the fraudulent card usage costs

# COMMAND ----------

# MAGIC %sql select * from atm_customers c 
# MAGIC        inner join  
# MAGIC          atm_visits v using (customer_id) 
# MAGIC        inner join 
# MAGIC          atm_locations l using (atm_id)

# COMMAND ----------

# MAGIC %sql create or replace view atm_dataset as 
# MAGIC   select concat(substr(card_number,0,4), '********',substr(card_number,-4)) masked_card_number, c.checking_savings, c.first_name, c.last_name, c.customer_since_date, c.customer_id, v.*, l.* 
# MAGIC     from atm_customers c 
# MAGIC       inner join  
# MAGIC         atm_visits v using (customer_id) 
# MAGIC       inner join 
# MAGIC         atm_locations l using (atm_id)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 3: Visualization
# MAGIC * Show the distribution of the fraud claim

# COMMAND ----------

# MAGIC %sql select count(1), city_state_zip.state state from atm_dataset where year = 2016 and fraud_report = 'Y' group by city_state_zip.state

# COMMAND ----------

# MAGIC %sql select sum(amount), month, fraud_report from atm_dataset where year = 2016 group by month, fraud_report order by month

# COMMAND ----------

# MAGIC %md #### Step 5: Model creation

# COMMAND ----------

fraud_dataset = table("atm_visits").join(table("atm_customers"), "customer_id").join(table("atm_locations"), "atm_id")
fraud_selected = fraud_dataset.select("withdrawl_or_deposit", "amount", "day", "month", "year", "hour", "min", "checking_savings", "pos_capability", "offsite_or_onsite", "bank", "fraud_report").cache()

# COMMAND ----------

display(fraud_selected)

# COMMAND ----------

from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml import Pipeline

categoricals = ["withdrawl_or_deposit", "checking_savings", "pos_capability", "offsite_or_onsite", "bank"]
indexers = map(lambda c: StringIndexer(inputCol=c, outputCol=c+"_idx"), categoricals)
featureCols = map(lambda c: c+"_idx", categoricals) + ["amount", "day", "month", "year", "hour", "min"]

stages = indexers + [VectorAssembler(inputCols=featureCols, outputCol="features"), StringIndexer(inputCol="fraud_report", outputCol="label")]

# COMMAND ----------

from pyspark.ml.classification import DecisionTreeClassifier 
from pyspark.ml.tuning import TrainValidationSplit, ParamGridBuilder
from pyspark.ml.evaluation import BinaryClassificationEvaluator

eval = BinaryClassificationEvaluator(metricName ="areaUnderROC")
dt = DecisionTreeClassifier(labelCol="label", featuresCol="features", seed=1028)
grid = ParamGridBuilder().addGrid(
  dt.maxDepth, [3, 4, 5]
).build()

pipeline = Pipeline(stages=stages+[dt])
tvs = TrainValidationSplit(seed = 3923772, estimator=pipeline, trainRatio=0.7, evaluator = eval, estimatorParamMaps = grid)

# COMMAND ----------

model = tvs.fit(fraud_selected)

# COMMAND ----------

model.validationMetrics

# COMMAND ----------

#model.bestModel.write().overwrite().save("/mnt/wesley/model/atm_fraud")

# COMMAND ----------

zip(range(0, len(featureCols)+1), featureCols)

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.ml.PipelineModel
# MAGIC import org.apache.spark.ml.classification.DecisionTreeClassificationModel
# MAGIC 
# MAGIC val model = PipelineModel.load("/mnt/wesley/model/atm_fraud")
# MAGIC val dtModel = model.stages.last.asInstanceOf[DecisionTreeClassificationModel]
# MAGIC display(dtModel)

# COMMAND ----------

# MAGIC %md  #### Step 6: Stream incoming data and score fraud in near real-time

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC //Define some user defined helper functions 
# MAGIC 
# MAGIC import org.joda.time.LocalDateTime
# MAGIC import org.joda.time.LocalDate
# MAGIC import org.joda.time.LocalTime
# MAGIC import org.apache.spark.sql.types.TimestampType
# MAGIC 
# MAGIC 
# MAGIC def to_sql_timestamp(year : Integer, month : Integer, date : Integer, hour : Integer, min : Integer, sec : Integer) : java.sql.Timestamp = {
# MAGIC   val ld = new LocalDate(year, month, 1).plusDays(date-1)
# MAGIC   val ts = ld.toDateTime(new LocalTime(hour, min, sec))
# MAGIC   return new java.sql.Timestamp(ts.toDateTime.getMillis())
# MAGIC }
# MAGIC 
# MAGIC spark.udf.register("to_sql_timestamp", to_sql_timestamp _)
# MAGIC spark.udf.register("getFraudProbability", (v : org.apache.spark.ml.linalg.Vector) => v.toArray.apply(1))

# COMMAND ----------

scoringFraudDF = fraud_dataset.where("year = 2016").orderBy("year", "month", "day", "hour", "min").limit(100000).repartition(1000)
#scoringFraudDF.write.mode("overwrite").format("json").save("/mnt/wesley/json/atm_fraud/streaming/")

# COMMAND ----------

streamingInputDF = spark.readStream.schema(scoringFraudDF.schema).option("maxFilesPerTrigger", 1).json("/mnt/wesley/json/atm_fraud/streaming/")

# COMMAND ----------

streamingFraudDataset = streamingInputDF.selectExpr("to_sql_timestamp(year, month, day, hour, min, sec) as timestamp", "withdrawl_or_deposit", "amount", "day", "month", "year", "hour", "min", "checking_savings", "pos_capability", "offsite_or_onsite", "bank", "fraud_report")

# COMMAND ----------

model.transform(streamingFraudDataset).createOrReplaceTempView("predictions")

# COMMAND ----------

# MAGIC %sql select timestamp, amount, bank, withdrawl_or_deposit, prediction, getFraudProbability(probability) fraud_probability from predictions 

# COMMAND ----------

# MAGIC %sql select year, month, day, hour, min, sum(case when prediction = 1.0 then amount else 0 end) fraud_amount, sum(case when prediction = 0.0 then amount else 0 end) approved_amount  from predictions group by year, month, day, hour, min

# COMMAND ----------

# MAGIC %md #### Step 7: Advanced SQL and Point of Compromise

# COMMAND ----------

# MAGIC %sql select sum(amount), month, year, fraud_report from atm_dataset where  group by month, year, fraud_report order by year, month

# COMMAND ----------

# MAGIC %sql select count(1), city_state_zip.state 
# MAGIC   from atm_dataset 
# MAGIC     where year = 2017 and month = 7 and fraud_report = 'Y' 
# MAGIC   group by city_state_zip.state

# COMMAND ----------

sql("select visit_id, city_state_zip.city, city_state_zip.state, city_state_zip.zip, atm_id, amount, customer_id, first_name, last_name, fraud_report, year, month, day, hour, min from atm_dataset where month = 7 and year = 2017 and city_state_zip.state in ('AZ', 'NM', 'CA', 'TX') and fraud_report = 'Y'").createOrReplaceTempView("fraud_drilldown")

# COMMAND ----------

sql("select sum(amount) as fraud_amount,  state, city from fraud_drilldown  group by state, city").createOrReplaceTempView("fraud_by_city_state_tmp")

# COMMAND ----------

# MAGIC %sql SELECT
# MAGIC   state,
# MAGIC   city,
# MAGIC   fraud_amount
# MAGIC FROM (
# MAGIC   SELECT
# MAGIC     state,
# MAGIC     city,
# MAGIC     fraud_amount,
# MAGIC     dense_rank() OVER (PARTITION BY state ORDER BY fraud_amount DESC) as rank
# MAGIC   FROM fraud_by_city_state_tmp) tmp
# MAGIC WHERE
# MAGIC   rank <= 3
# MAGIC order by state, fraud_amount

# COMMAND ----------

# MAGIC %sql select sum(amount) fraud_amount, count(1) number_of_trx, customer_id, first_name, last_name 
# MAGIC        from fraud_drilldown 
# MAGIC      group by customer_id, first_name, last_name 
# MAGIC        order by 1 desc

# COMMAND ----------

# MAGIC %sql select * from atm_dataset 
# MAGIC   where customer_id = $customer_id
# MAGIC     order by year, month, day, hour, min, sec

# COMMAND ----------

fraud_drilldown = table("fraud_drilldown")
edges = table("atm_dataset").where("(year = 2017 and month <=7) OR  (year <= 2016)").alias("all_trx").join(fraud_drilldown.alias("fraud"), "customer_id").selectExpr("fraud.atm_id src", "all_trx.atm_id dst")

# COMMAND ----------

display(edges)

# COMMAND ----------

from graphframes import *

vertices = table("atm_locations").selectExpr("atm_id as id", "city_state_zip", "offsite_or_onsite", "bank")
graph = GraphFrame(vertices, edges)

# COMMAND ----------

sc.setCheckpointDir("/mnt/wesley/tmp/checkpoints")

# COMMAND ----------

from pyspark.sql.functions import desc
pageRank = graph.pageRank(tol=0.01)
display(pageRank.vertices.orderBy(desc("pagerank")))

# COMMAND ----------

# MAGIC %sql select count(distinct customer_id) customers_reporting_fraud from fraud_drilldown

# COMMAND ----------

# MAGIC %sql select count(distinct customer_id) customers_reporting_fraud from fraud_drilldown inner join atm_dataset all_trx using (customer_id) 
# MAGIC     where ((all_trx.year = 2017 and all_trx.month <=7) OR (all_trx.year <= 2016) and all_trx.atm_id = 1289)

# COMMAND ----------

# MAGIC %md 
# MAGIC ####The authorities visit Haltom City, TX and find this:
# MAGIC 
# MAGIC ![Skimmer](/files/tables/npfg6pit1504559336012/card_skimmer.jpg)
# MAGIC 
# MAGIC *It's a credit card skimmer*

# COMMAND ----------

# MAGIC %md #### Step 8: Deep Learning and Facial Recognition

# COMMAND ----------

# MAGIC %md Import some libraries

# COMMAND ----------

# MAGIC %run ./boa_import

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ####The ATM owner reviews the video and finds this image of the person at the gas station:

# COMMAND ----------

showMugshot()

# COMMAND ----------

# MAGIC %md Can we compare known images and see if we can match this person to known frausters

# COMMAND ----------

criminals = table("criminal_thumbnails").where("gender = 'male'").select("name", "image")

# COMMAND ----------

display(criminals)

# COMMAND ----------

from pyspark.ml.feature import StringIndexer

labeler = StringIndexer(inputCol="name", outputCol="label")
indexer = labeler.fit(table("criminals"))
training_data = indexer.transform(table("criminals"))

# COMMAND ----------

featurizer = DeepImageFeaturizer(inputCol="image", outputCol="features", modelName="InceptionV3")
lr = LogisticRegression(labelCol="label")
p = Pipeline(stages=[featurizer, lr])

# COMMAND ----------

deepLearningModel = p.fit(training_data)

# COMMAND ----------

labels = indexer.labels
def getName(prediction):
  return labels[int(prediction)]
def probabilityMatch(probs):
  result = []
  for r in zip(labels, probs.toArray()):
    result.append(r[0] + " | " + '{:.8%}'.format(r[1]))
  return "\n".join(result)
spark.udf.register("matches", probabilityMatch, StringType())
spark.udf.register("getName", getName, StringType())


# COMMAND ----------

testing_data = table("testing_data")
df = deepLearningModel.transform(testing_data)

# COMMAND ----------

display(df.selectExpr("image", "getName(prediction) label", "matches(probability) probability"))

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ####Josh Brolin starred in *No Country for Old Men*
# MAGIC 
# MAGIC ![No Country](/files/tables/oxxep0ad1504673306444/no_country.jpg)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Results interpretation
# MAGIC 
# MAGIC ![Churn-Index](http://www.ngdata.com/wp-content/uploads/2016/05/churn.jpg)
# MAGIC 
# MAGIC The above created model shows the different techniques used to identify ATM card fraud. 