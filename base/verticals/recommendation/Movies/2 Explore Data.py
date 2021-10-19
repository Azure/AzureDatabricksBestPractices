# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Explore the Data

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Let's look at the Movies Data set.

# COMMAND ----------

# MAGIC %sql show create table movies

# COMMAND ----------

# MAGIC %sql select count(*) from movies

# COMMAND ----------

# MAGIC %sql select id, name, year, categories from movies

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Let's see how many movies there are each year in this set.

# COMMAND ----------

# MAGIC %sql select year, count(*) as the_count from movies group by year order by year asc

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Let's try to break out categories - we'll need to use explode the categories column since it's an array.

# COMMAND ----------

from pyspark.sql.functions import explode

sqlContext.sql("select * from movies").select(
  "id", "name", "year", explode("categories").alias("category")
).registerTempTable("exploded_movies")

# COMMAND ----------

# MAGIC %sql select * from exploded_movies

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Let's look at category trends over the years.

# COMMAND ----------

# MAGIC %sql 
# MAGIC select 
# MAGIC   year, category, count(*) as the_count
# MAGIC from
# MAGIC   exploded_movies
# MAGIC where
# MAGIC   year > 1950
# MAGIC group by
# MAGIC   year, category 
# MAGIC order by
# MAGIC   year asc, the_count desc

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Let's explore the ratings data set.

# COMMAND ----------

# MAGIC %sql select * from ratings

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # This is a Title

# COMMAND ----------

# MAGIC %sh pwd

# COMMAND ----------

title = "sdfkdsf"
displayHTML("<h1>This ia a Title: %s</h1>" % title)

# COMMAND ----------

# MAGIC %sql select count(*) from ratings

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Let's cache the ratings table since that was a bit slow to just count.

# COMMAND ----------

# MAGIC %sql cache table ratings

# COMMAND ----------

# MAGIC %sql select count(*) from ratings

# COMMAND ----------

# MAGIC %sql select count(distinct(user_id)), count(distinct(movie_id)) from ratings

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Let's figure out which movies are most commonly rated - we'll need to join on the movies table to get the name and year.

# COMMAND ----------

# MAGIC %sql 
# MAGIC select 
# MAGIC   movie_id, movies.name, movies.year, count(*) as times_rated 
# MAGIC from 
# MAGIC   ratings
# MAGIC join 
# MAGIC   movies on ratings.movie_id = movies.id
# MAGIC group by 
# MAGIC   movie_id, movies.name, movies.year
# MAGIC order by 
# MAGIC   times_rated desc

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Let's see the distribution of the ratings.

# COMMAND ----------

# MAGIC %sql 
# MAGIC select 
# MAGIC   movie_id, movies.name, movies.year, count(*) as times_rated 
# MAGIC from 
# MAGIC   ratings
# MAGIC join 
# MAGIC   movies on ratings.movie_id = movies.id
# MAGIC group by 
# MAGIC   movie_id, movies.name, movies.year
# MAGIC order by 
# MAGIC   times_rated desc
# MAGIC limit 1000