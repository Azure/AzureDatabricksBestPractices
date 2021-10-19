// Databricks notebook source
// MAGIC 
// MAGIC %md-sandbox
// MAGIC 
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC # Data Cleansing with Airbnb
// MAGIC 
// MAGIC We're going to start by doing some exploratory data analysis & cleansing. The data we are working with is Airbnb rentals from SF. You can read more at [Inside Airbnb](http://insideairbnb.com/get-the-data.html)
// MAGIC 
// MAGIC <img src="http://insideairbnb.com/images/insideairbnb_graphic_site_1200px.png" style="width:800px"/>
// MAGIC 
// MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lesson you:<br>
// MAGIC  - Work with messy CSV files
// MAGIC  - Impute missing values
// MAGIC  - Identify & remove outliers

// COMMAND ----------

// MAGIC %run "./Includes/Classroom-Setup"

// COMMAND ----------

// MAGIC %fs ls mnt/training/airbnb/sf-listings/sf-listings-2018-12-06.csv

// COMMAND ----------

// MAGIC %md
// MAGIC Let's take a look at the first few records.

// COMMAND ----------

// MAGIC %fs head mnt/training/airbnb/sf-listings/sf-listings-2018-12-06.csv

// COMMAND ----------

// MAGIC %md
// MAGIC How will Spark parse this with the default CSV Reader?

// COMMAND ----------

val filePath = "mnt/training/airbnb/sf-listings/sf-listings-2018-12-06.csv"

val rawDF = spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .csv(filePath)

display(rawDF)

// COMMAND ----------

// MAGIC %md
// MAGIC We have multiline records. Let's fix this by tuning the Spark CSV Reader
// MAGIC [Python](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrameReader.csv)/
// MAGIC [Scala](https://spark.apache.org/docs/latest/api/scala/#org.apache.spark.sql.DataFrameReader).

// COMMAND ----------

val rawDF = spark.read
  .option("header", "true")
  .option("multiLine", "true")
  .option("inferSchema", "true")
  .csv(filePath)

display(rawDF)

// COMMAND ----------

// MAGIC %md
// MAGIC Almost perfect. Let's indicate that quotation marks are escaped with `"`:

// COMMAND ----------

val rawDF = spark.read
  .option("header", "true")
  .option("multiLine", "true")
  .option("inferSchema", "true")
  .option("escape","\"")
  .csv(filePath)

display(rawDF)

// COMMAND ----------

rawDF.columns

// COMMAND ----------

// MAGIC %md
// MAGIC For the sake of simplicity, only keep certain columns from this dataset. We will talk about feature selection later.

// COMMAND ----------

val baseDF = rawDF.select(
  "host_is_superhost",
  "cancellation_policy",
  "instant_bookable",
  "host_total_listings_count",
  "neighbourhood_cleansed",
  "zipcode",
  "latitude",
  "longitude",
  "property_type",
  "room_type",
  "accommodates",
  "bathrooms",
  "bedrooms",
  "beds",
  "bed_type",
  "minimum_nights",
  "number_of_reviews",
  "review_scores_rating",
  "review_scores_accuracy",
  "review_scores_cleanliness",
  "review_scores_checkin",
  "review_scores_communication",
  "review_scores_location",
  "review_scores_value",
  "price")

baseDF.cache().count
display(baseDF)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Fixing Data Types
// MAGIC 
// MAGIC Take a look at the schema above. You'll notice that the `price` field got picked up as string. For our task, we need it to be a numeric (double type) field.
// MAGIC 
// MAGIC Let's fix that.

// COMMAND ----------

import org.apache.spark.sql.functions.regexp_replace

val fixedPriceDF = baseDF.withColumn("price", regexp_replace($"price", "[\\$,]", "").cast("double"))

display(fixedPriceDF)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Summary statistics
// MAGIC 
// MAGIC Two options:
// MAGIC * describe
// MAGIC * summary (describe + IQR)
// MAGIC 
// MAGIC **Question:** When to use IQR/median over mean? Vice versa?

// COMMAND ----------

display(fixedPriceDF.describe())

// COMMAND ----------

display(fixedPriceDF.summary())

// COMMAND ----------

// MAGIC %md
// MAGIC ## Nulls
// MAGIC 
// MAGIC There are a lot of different ways to handle null values. Sometimes, null can actually be a key indicator of the thing you are trying to predict (e.g. if you don't fill in certain portions of a form, probability of it getting approved decreases).
// MAGIC 
// MAGIC Some ways to handle nulls:
// MAGIC * Drop any records that contain nulls
// MAGIC * Numeric:
// MAGIC   * Replace them with mean/median/zero/etc.
// MAGIC * Categorical:
// MAGIC   * Replace them with the mode
// MAGIC   * Create a special category for null
// MAGIC * Use techniques like ALS which are designed to impute missing values
// MAGIC 
// MAGIC **If you do ANY imputation techniques for categorical/numerical features, you MUST include an additional field specifying that field was imputed (think about why this is necessary)**

// COMMAND ----------

// MAGIC %md
// MAGIC There are a few nulls in the categorical feature `zipcode` and `host_is_superhost`. Let's get rid of those rows where any of these columns is null.
// MAGIC 
// MAGIC SparkML's Imputer (will cover below) does not support imputation for categorical features, so this is the simplest approach for the time being.

// COMMAND ----------

val noNullsDF = fixedPriceDF.na.drop(cols = Seq("zipcode", "host_is_superhost"))

// COMMAND ----------

// MAGIC %md
// MAGIC ## Impute: Cast to Double
// MAGIC 
// MAGIC SparkML's `Imputer` requires all fields be of type double
// MAGIC [Python](https://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.feature.Imputer)/
// MAGIC [Scala](https://spark.apache.org/docs/latest/api/scala/#org.apache.spark.ml.feature.Imputer). Let's cast all integer fields to double.

// COMMAND ----------

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.IntegerType

val integerColumns = for (x <- baseDF.schema.fields if (x.dataType == IntegerType)) yield x.name
var doublesDF = noNullsDF

for (c <- integerColumns)
  doublesDF = doublesDF.withColumn(c, col(c).cast("double"))

val columns = integerColumns.mkString("\n - ")
println(s"Columns converted from Integer to Double:\n - $columns \n")
println("*-"*80)

// COMMAND ----------

// MAGIC %md
// MAGIC Add in dummy variable if we will impute any value.

// COMMAND ----------

import org.apache.spark.sql.functions.when

val imputeCols = Array(
  "bedrooms",
  "bathrooms",
  "beds",
  "review_scores_rating",
  "review_scores_accuracy",
  "review_scores_cleanliness",
  "review_scores_checkin",
  "review_scores_communication",
  "review_scores_location",
  "review_scores_value"
)

for (c <- imputeCols)
  doublesDF = doublesDF.withColumn(c + "_na", when(col(c).isNull, 1.0).otherwise(0.0))

// COMMAND ----------

display(doublesDF.describe())

// COMMAND ----------

import org.apache.spark.ml.feature.Imputer

val imputer = new Imputer()
  .setStrategy("median")
  .setInputCols(imputeCols)
  .setOutputCols(imputeCols)

val imputedDF = imputer.fit(doublesDF).transform(doublesDF)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Getting rid of extreme values
// MAGIC 
// MAGIC Let's take a look at the *min* and *max* values of the `price` column:

// COMMAND ----------

display(imputedDF.select("price").describe())

// COMMAND ----------

// MAGIC %md
// MAGIC There are some super-expensive listings. But that's the Data Scientist's job to decide what to do with them. We can certainly filter the "free" AirBNBs though.
// MAGIC 
// MAGIC Let's see first how many listings we can find where the *price* is zero.

// COMMAND ----------

imputedDF.filter($"price" === 0).count

// COMMAND ----------

// MAGIC %md
// MAGIC Now only keep rows with a strictly positive *price*.

// COMMAND ----------

val posPricesDF = imputedDF.filter($"price" > 0)

// COMMAND ----------

// MAGIC %md
// MAGIC Let's take a look at the *min* and *max* values of the *minimum_nights* column:

// COMMAND ----------

display(posPricesDF.select("minimum_nights").describe())

// COMMAND ----------

display(posPricesDF
  .groupBy("minimum_nights").count()
  .orderBy($"count".desc, $"minimum_nights")
)

// COMMAND ----------

// MAGIC %md
// MAGIC A minimum stay of one year seems to be a reasonable limit here. Let's filter out those records where the *minimum_nights* is greater then 365:

// COMMAND ----------

val cleanDF = posPricesDF.filter($"minimum_nights" <= 365)

display(cleanDF)

// COMMAND ----------

// MAGIC %md
// MAGIC OK, our data is cleansed now. Let's save this DataFrame to a file so that we can start building models with it.

// COMMAND ----------

val outputPath = userhome + "/airbnb-cleansed.parquet"

cleanDF.write.mode("overwrite").parquet(outputPath)


// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>