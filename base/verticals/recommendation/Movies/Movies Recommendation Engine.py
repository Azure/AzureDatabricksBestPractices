# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Movie Recommender Engine
# MAGIC 
# MAGIC ![Movie Recommend](https://i1.wp.com/dataaspirant.com/wp-content/uploads/2015/05/da.png)
# MAGIC 
# MAGIC - This notebook aims at building a recommendation engine from the content of the movie_metadata.csv dataset. 
# MAGIC 
# MAGIC Recommendation algorithm delivers plans to consumers by providing "apples-to-apples comparison".  An individual can enter as much, or as little, information as she wants about her personal movie choices to refine the forecast and plan recommendation. 
# MAGIC 
# MAGIC Think of it as boiling down a movie choice to the 3 or 4 data points that truly matter to that individual.
# MAGIC 
# MAGIC * [**Movie Reccomendation Engine**](http://www.fiercehealthcare.com/it/paris-hospitals-use-predictive-modeling-to-control-admissions) is the use of data analytics and machine learning to recomend movie to the end user and is...  
# MAGIC   * Built on top of Databricks Platform
# MAGIC   * Uses a machine learning **ALS recommendation Algorithm** implementation to generate recomendation on movie choices   
# MAGIC * This demo...  
# MAGIC   * demonstrates a movie recommendation analysis workflow.  We use movie dataset from the [Kaggle Dataset](https://finder.healthcare.gov/#services/version_3_0).

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

# DBTITLE 1,Step1: Ingest movie Data to Notebook
# MAGIC %md 
# MAGIC 
# MAGIC - We will extracted the movie dataset hosted at  [Kaggle](https://finder.healthcare.gov/#services/version_3_0)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Select 10 random movies from the most rated, as those as likely to be commonly recognized movies.
# MAGIC Create Databricks Widgets to allow a user to enter in ratings for those movies.

# COMMAND ----------

sqlContext.sql("""
    select 
      movie_id, movies.name, count(*) as times_rated 
    from 
      ratings
    join 
      movies on ratings.movie_id = movies.id
    group by 
      movie_id, movies.name, movies.year
    order by 
      times_rated desc
    limit
      200
    """
).registerTempTable("most_rated_movies")

# COMMAND ----------

if not "most_rated_movies" in vars():
  most_rated_movies = sqlContext.table("most_rated_movies").rdd.takeSample(True, 10)
  for i in range(0, len(most_rated_movies)):
    dbutils.widgets.dropdown("movie_%i" % i, "5", ["1", "2", "3", "4", "5"], most_rated_movies[i].name)

# COMMAND ----------

# MAGIC %sql desc ratings

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Change the values on top to be your own personal ratings before proceeding.

# COMMAND ----------

from datetime import datetime
from pyspark.sql import Row
ratings = []
for i in range(0, len(most_rated_movies)):
  ratings.append(
    Row(user_id = 0,
        movie_id = most_rated_movies[i].movie_id,
        rating = float(dbutils.widgets.get("movie_%i" %i)),
        timestamp = datetime.now()))
myRatingsDF = sqlContext.createDataFrame(ratings)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Step2: Enrich the data and prep for modeling

# COMMAND ----------

# MAGIC %sql select min(user_id) from ratings 

# COMMAND ----------

from pyspark.sql import functions

ratings = sqlContext.table("ratings")
ratings = ratings.withColumn("rating", ratings.rating.cast("float"))

# COMMAND ----------

(training, test) = ratings.randomSplit([0.8, 0.2])

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Step 3: Model Creation
# MAGIC - Fit an ALS model on the ratings table.

# COMMAND ----------

from pyspark.ml.recommendation import ALS

als = ALS(maxIter=5, regParam=0.01, userCol="user_id", itemCol="movie_id", ratingCol="rating")
model = als.fit(training.unionAll(myRatingsDF))

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ### Step 4: Model Evaluation
# MAGIC 
# MAGIC - Evaluate the model by computing Root Mean Square error on the test set.

# COMMAND ----------

predictions = model.transform(test).dropna()
predictions.registerTempTable("predictions")

# COMMAND ----------

# MAGIC %sql select user_id, movie_id, rating, prediction from predictions

# COMMAND ----------

from pyspark.ml.evaluation import RegressionEvaluator

evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating", predictionCol="prediction")

# COMMAND ----------

rmse = evaluator.evaluate(predictions)

# COMMAND ----------

displayHTML("<h4>The Root-mean-square error is %s</h4>" % str(rmse))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Step 5: Model Testing
# MAGIC 
# MAGIC - Let's see how the model predicts for you.

# COMMAND ----------

mySampledMovies = model.transform(myRatingsDF)
mySampledMovies.registerTempTable("mySampledMovies")

# COMMAND ----------

display(sqlContext.sql("select user_id, movie_id, rating, prediction from mySampledMovies"))

# COMMAND ----------

my_rmse = evaluator.evaluate(mySampledMovies)

# COMMAND ----------

displayHTML("<h4>My Root-mean-square error is %s</h4>" % str(my_rmse))

# COMMAND ----------

from pyspark.sql import functions
df = sqlContext.table("movies")
myGeneratedPredictions = model.transform(df.select(df.id.alias("movie_id")).withColumn("user_id", functions.expr("int('0')")))
myGeneratedPredictions.dropna().registerTempTable("myPredictions")

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT 
# MAGIC   name, prediction 
# MAGIC from 
# MAGIC   myPredictions 
# MAGIC join 
# MAGIC   most_rated_movies on myPredictions.movie_id = most_rated_movies.movie_id
# MAGIC order by
# MAGIC   prediction desc
# MAGIC LIMIT
# MAGIC   10

# COMMAND ----------

# MAGIC %md
# MAGIC ##Results interpretation
# MAGIC 
# MAGIC The table shown above gives the top ten recomended movie choices for the user based on the predicted outcomes using the movie demographics and the ratings provided by the user
# MAGIC 
# MAGIC ![Recomendation-Index](http://trouvus.com/wp-content/uploads/2016/03/2.1.png)