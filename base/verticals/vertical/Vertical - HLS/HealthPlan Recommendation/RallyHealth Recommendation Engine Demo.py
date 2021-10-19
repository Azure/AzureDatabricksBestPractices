# Databricks notebook source
# MAGIC %md
# MAGIC *** The average consumer is willing to spend 9 minutes choosing a plan so often ends up taking an “educated guess.” And picking up the phone isn’t a great option — the typical health insurance exchange call center wait time is the better part of an hour.
# MAGIC It’s time for a new vocabulary: insurance in the context of the individual.  We set out to ensure consumers can make a logic-driven decision in 9 minutes or less,  without confusion and without resorting to educated guesses.***
# MAGIC 
# MAGIC Recommendation algorithm delivers plans to consumers by providing "apples-to-apples comparison".  An individual can enter as much, or as little, information as she wants about her personal health profile to refine the forecast and plan recommendation. 
# MAGIC 
# MAGIC Think of it as boiling down a health plan to the 3 or 4 data points that truly matter to that individual.
# MAGIC 
# MAGIC 
# MAGIC * [**Healthcare Plan Analytics**](http://www.fiercehealthcare.com/it/paris-hospitals-use-predictive-modeling-to-control-admissions) is the use of data analytics and machine learning to recomend health plan to the end user and is...  
# MAGIC   * Built on top of Databricks Platform
# MAGIC   * Uses a machine learning **ALS recommendation Algorithm** implementation to generate recomendation on healthcare plans   
# MAGIC * This demo...  
# MAGIC   * demonstrates a healthcare recommendation analysis workflow.  We use Patient dataset from the [Health Plan Finder API](https://finder.healthcare.gov/#services/version_3_0) and internally mocked up data.

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

# DBTITLE 1,Step1: Ingest healthcare plan Data to Notebook
# MAGIC %md 
# MAGIC 
# MAGIC - We will extracted the healthcare dataset hosted at  [Health Plan Finder API](https://finder.healthcare.gov/#services/version_3_0)
# MAGIC - We also used used internal generated dataset of user rating against each plan

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC * If your running the notebook the first time please"data_Setup"

# COMMAND ----------

from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

quotedf = (sqlContext.read.format('com.databricks.spark.xml').options(rowTag='Plan').load('/mnt/azure/medicare/recomendation/HealthPlan.xml'))
clientdf = (sqlContext.read.format('com.databricks.spark.xml').options(rowTag='user').load('/mnt/azure/medicare/recomendation/quote2.xml'))

# COMMAND ----------

display(quotedf)

# COMMAND ----------

display(clientdf)

# COMMAND ----------

# DBTITLE 1,Step2: Enrich the data and prep for modeling
from  pyspark.ml.feature import StringIndexer

indexer1 = (StringIndexer()
                   .setInputCol("_id")
                   .setOutputCol("id")
                   .fit(quotedf))

indexed1 = indexer1.transform(quotedf)

indexer2 = (StringIndexer()
                   .setInputCol("PlanId")
                   .setOutputCol("planidindex")
                   .fit(clientdf))

indexed2 = indexer2.transform(clientdf)
indexed1.registerTempTable("healthplan")
indexed2.registerTempTable("userplan")

# COMMAND ----------

# MAGIC %md Select 10 random health palns from the most rated, as those as likely to be commonly recognized health plans. Create Databricks Widgets to allow a user to enter in ratings for those health plans.

# COMMAND ----------

sqlContext.sql("""
  select u.planidindex,h.PlanNameText,count(*) as times_rated from healthplan h
    join userplan u
      on h._id = u.PlanId
    group by 
    u.planidindex,h.PlanNameText
    order by 
    times_rated desc
    limit 200
 """).registerTempTable("most_rated_healthplan")

# COMMAND ----------

if not "most_rated_healthplan" in vars():
  most_rated_healthplan = sqlContext.table("most_rated_healthplan").rdd.takeSample(True, 10)
  for i in range(0, len(most_rated_healthplan)):
    dbutils.widgets.dropdown("plan_%i" % i, "5", ["1", "2", "3", "4", "5"], most_rated_healthplan[i].PlanNameText)

# COMMAND ----------

# MAGIC %md Change the values on top to be your own personal ratings before proceeding.

# COMMAND ----------

from datetime import datetime
from pyspark.sql import Row
ratings = []
for i in range(0, len(most_rated_healthplan)):
  ratings.append(
    Row(_id = 0.0,
        planidindex = most_rated_healthplan[i].planidindex,
        Rating = float(dbutils.widgets.get("plan_%i" %i))
       ))
myRatingsDF = sqlContext.createDataFrame(ratings)

# COMMAND ----------

# DBTITLE 1,Step3: Explore Patient Data 
# MAGIC %sql 
# MAGIC -- Let's see how many healthcare products there are each provider in this set.
# MAGIC select IssuerNameText, count(*) as the_count from healthplan group by IssuerNameText order by IssuerNameText asc

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Let's look at product trends for each issuer in the healthcare plan.

# COMMAND ----------

# MAGIC %sql 
# MAGIC select IssuerNameText,ProductID, count(*) as the_count 
# MAGIC from healthplan 
# MAGIC group by IssuerNameText,ProductID 
# MAGIC order by IssuerNameText,ProductID asc

# COMMAND ----------

# MAGIC %sql 
# MAGIC select u.planidindex,h.PlanNameText,count(*) as times_rated from healthplan h
# MAGIC     join userplan u
# MAGIC       on h._id = u.PlanId
# MAGIC     group by 
# MAGIC     u.planidindex,h.PlanNameText
# MAGIC     order by 
# MAGIC     times_rated desc
# MAGIC     limit 1000

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Let's see the distribution of the ratings.

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 4: Visualization
# MAGIC * Show the distribution of the account length.

# COMMAND ----------

# DBTITLE 1,Distribution of the ratings. using matplotlib
import matplotlib.pyplot as plt
importance = sqlContext.sql("select u.planidindex,h.PlanNameText,count(*) as times_rated from healthplan h join userplan u on h._id = u.PlanId group by u.planidindex,h.PlanNameText order by times_rated desc limit 1000")
importanceDF = importance.toPandas()
ax = importanceDF.plot(x="planidindex", y="times_rated",lw=3,colormap='Reds_r',title='Importance in Descending Order', fontsize=9)
ax.set_xlabel("plan_id")
ax.set_ylabel("count")
plt.xticks(rotation=12)
plt.grid(True)
plt.show()
display()

# COMMAND ----------

# MAGIC %md #### Step 5: Model creation

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Split into training and test sets.

# COMMAND ----------

from pyspark.sql import functions

ratings = sqlContext.table("userplan").select("_id","planidindex","Rating").groupBy("_id", "planidindex").max("Rating").select("_id","planidindex",functions.col("max(Rating)").alias("Rating"))
ratings = ratings.withColumn("planidindex", ratings.planidindex.cast("long"))

# COMMAND ----------

(training, test) = ratings.randomSplit([0.8, 0.2])

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Fit an ALS model on the ratings table.

# COMMAND ----------

from pyspark.ml.recommendation import ALS

als = ALS(maxIter=10, regParam=0.01, userCol="_id", itemCol="planidindex", ratingCol="Rating")
model = als.fit(myRatingsDF.select("_id","planidindex","Rating").union(training))

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC Evaluate the model by computing Root Mean Square error on the test set.

# COMMAND ----------

predictions = model.transform(test).dropna()
predictions.registerTempTable("predictions")

# COMMAND ----------

# MAGIC %sql select _id as user_id,planidindex as plan_id,Rating,prediction from predictions

# COMMAND ----------

from pyspark.ml.evaluation import RegressionEvaluator

evaluator = RegressionEvaluator(metricName="rmse", labelCol="Rating", predictionCol="prediction")

# COMMAND ----------

rmse = evaluator.evaluate(predictions)

# COMMAND ----------

displayHTML("<h4>The Root-mean-square error is %s</h4>" % str(rmse))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Let's see how the model predicts for you.

# COMMAND ----------

mySampledQuotes = model.transform(myRatingsDF.select("_id","planidindex","Rating"))
mySampledQuotes.registerTempTable("mySampledQuotes")

# COMMAND ----------

display(mySampledQuotes)

# COMMAND ----------

my_rmse = evaluator.evaluate(mySampledQuotes)

# COMMAND ----------

displayHTML("<h4>My Root-mean-square error is %s</h4>" % str(my_rmse))

# COMMAND ----------

from pyspark.sql import functions
df = sqlContext.table("userplan")
myGeneratedPredictions = model.transform(df.select(df.planidindex.alias("planidindex")).withColumn("_id", functions.expr("int('0')")))
myGeneratedPredictions.dropna().registerTempTable("myPredictions")

# COMMAND ----------

df.select(df.planidindex.alias("plan_id")).withColumn("_id", functions.expr("int('0')"))

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT 
# MAGIC   distinct PlanNameText, prediction 
# MAGIC from 
# MAGIC   myPredictions 
# MAGIC join 
# MAGIC   most_rated_healthplan on myPredictions.planidindex = most_rated_healthplan.planidindex
# MAGIC order by
# MAGIC   prediction desc
# MAGIC LIMIT
# MAGIC   5

# COMMAND ----------

# MAGIC %md
# MAGIC ##Results interpretation
# MAGIC 
# MAGIC The table shown above gives the top ten recomended healthcare plans for the user based on the predicted outcomes using the healthcare plans demographics and the ratings provided by the user
# MAGIC 
# MAGIC ![Recomendation-Index](http://trouvus.com/wp-content/uploads/2016/03/2.1.png)