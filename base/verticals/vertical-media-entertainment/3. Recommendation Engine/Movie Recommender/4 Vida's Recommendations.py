# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Movie Recommender

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Step 1: Add your own movie recommendations.

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
# MAGIC ### Step 1: Load in the data and split into training and test sets.

# COMMAND ----------

from pyspark.sql import functions

ratings = sqlContext.table("ratings")
ratings = ratings.withColumn("rating", ratings.rating.cast("float"))

# COMMAND ----------

(training, test) = ratings.randomSplit([0.8, 0.2])

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Step 2: Fit an ALS model on the ratings table.

# COMMAND ----------

from pyspark.ml.recommendation import ALS

als = ALS(maxIter=5, regParam=0.01, userCol="user_id", itemCol="movie_id", ratingCol="rating")
model = als.fit(training.unionAll(myRatingsDF))

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ### Step 3: Evaluate the model by computing Root Mean Square error on the test set.

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
# MAGIC ### Step 4: Let's see how the model predicts for you.

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