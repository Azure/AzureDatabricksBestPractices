# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Movie Recommender System

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Step 1: Download the dataset and copy to DBFS.
# MAGIC 
# MAGIC Use the Unix tool, wget to download the zip file http://files.grouplens.org/datasets/movielens/ml-1m.zip.
# MAGIC 
# MAGIC You can use the shortcut, **%sh** to run shell commands on the Spark Driver.

# COMMAND ----------

# MAGIC %sh wget http://files.grouplens.org/datasets/movielens/ml-1m.zip -O /tmp/ml-1m.zip

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Unzip the files.

# COMMAND ----------

# MAGIC %sh unzip /tmp/ml-1m.zip -d /tmp

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Copy the file to S3 so that it can be accessed by the Spark workers as well.

# COMMAND ----------

# MAGIC %fs cp -r file:/tmp/ml-1m/ dbfs:/mnt/wesley/movielens1m

# COMMAND ----------

# MAGIC %sh cat /tmp/ml-1m/README

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Step 2: Register the ratings data as a DataFrame.

# COMMAND ----------

# MAGIC %fs head dbfs:/mnt/wesley/movielens1m/users.dat

# COMMAND ----------

# MAGIC %sql 
# MAGIC DROP TABLE IF EXISTS users;
# MAGIC 
# MAGIC CREATE EXTERNAL TABLE users (
# MAGIC   user_id INT,
# MAGIC   gender STRING,
# MAGIC   age INT,
# MAGIC   occupation_id INT,
# MAGIC   zipcode STRING
# MAGIC )
# MAGIC ROW FORMAT
# MAGIC   SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
# MAGIC WITH SERDEPROPERTIES (
# MAGIC   'input.regex' = '([^:]+)::([^:]+)::([^:]+)::([^:]+)::([^:]+)'
# MAGIC )
# MAGIC LOCATION 
# MAGIC   'dbfs:/mnt/wesley/movielens1m/users*.dat'

# COMMAND ----------

# MAGIC %sql select * from users

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Step 3: Register the ratings data as a DataFrame.

# COMMAND ----------

# MAGIC %fs head dbfs:/mnt/wesley/movielens1m/ratings.dat

# COMMAND ----------

from pyspark.sql import Row
import datetime

def create_row_for_rating(line):
  atoms = line.split("::")
  return Row(user_id = int(atoms[0]),
             movie_id = int(atoms[1]),
             rating = int(atoms[2]),
             timestamp = datetime.datetime.fromtimestamp(long(atoms[3])))

ratings = sc.textFile("dbfs:/mnt/wesley/movielens1m/ratings.dat").map(create_row_for_rating)
ratingsDF = sqlContext.createDataFrame(ratings)
ratingsDF.registerTempTable("ratings")

# COMMAND ----------

# MAGIC %sql select * from ratings

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Step 4: Import the movies data as a DataFrame.

# COMMAND ----------

# MAGIC %fs head dbfs:/mnt/wesley/movielens1m/movies.dat

# COMMAND ----------

from pyspark.sql import Row

def create_row(line):
  atoms = line.split("::")
  movie = {"id": int(atoms[0])}
  year_begin = atoms[1].rfind("(")
  movie["name"] = atoms[1][0:year_begin].strip()
  movie["year"] = int(atoms[1][year_begin+1:-1])
  movie["categories"] = atoms[2].split("|")
  return Row(**movie)
  
movies = sc.textFile("dbfs:/mnt/wesley/movielens1m/movies.dat").map(create_row)
moviesDF = sqlContext.createDataFrame(movies)
moviesDF.registerTempTable("movies")

# COMMAND ----------

# MAGIC %sql select id, name, year, categories from movies