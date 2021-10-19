// Databricks notebook source
// MAGIC %md # Amazon Product Recommendations
// MAGIC 
// MAGIC **Business case:**  
// MAGIC _TEXT_
// MAGIC   
// MAGIC **Problem Statement:**  
// MAGIC _TEXT_
// MAGIC   
// MAGIC **Business solution:**  
// MAGIC _TEXT_
// MAGIC   
// MAGIC **Technical Solution:**  
// MAGIC _TEXT_
// MAGIC 
// MAGIC 
// MAGIC Owner: Richard    
// MAGIC Runnable: True   
// MAGIC Last Spark Version: 2.1  

// COMMAND ----------

val df = table("amazon")
display(df)

// COMMAND ----------

val items = df.select("asin", "title")
val brands = df.select("asin", "brand")
items.createOrReplaceTempView("items")
brands.createOrReplaceTempView("brands")

// COMMAND ----------

// MAGIC %sql select brand, count(1) from items inner join brands on items.asin = brands.asin
// MAGIC group by brand order by count(1) desc limit 10

// COMMAND ----------

// MAGIC %sql select * from items where title like '%iPhone%'

// COMMAND ----------

// MAGIC %sql select * from items where title like '%Blackberry%'

// COMMAND ----------

// MAGIC %md ## ALS - Alternating Least Squares
// MAGIC 
// MAGIC **Matrix Factorization** algorithm -- find out two (or more) matrices such that when you multiply them together you will get back the original matrix.
// MAGIC 
// MAGIC ALS alternately optimizes U and V iteratively solving the two following objectives. 
// MAGIC 
// MAGIC $$
// MAGIC \sum_{j\in R_i}(M_i,_j-U_iV_j^\top)^2+\lambda U_i^\top U_i; \enspace R_i=\\{j|(i, j)\in R\\}
// MAGIC $$
// MAGIC 
// MAGIC $$
// MAGIC \sum_{i\in R_j}(M_i,_j-U_iV_j^\top)^2+\lambda V_i^\top V_i; \enspace R_j=\\{i|(i, j)\in R\\}
// MAGIC $$

// COMMAND ----------

// MAGIC %md ![Alternating Least Squares - Matrix Factorization](https://raw.githubusercontent.com/cfregly/spark-after-dark/master/img/ALS.png)

// COMMAND ----------

import org.apache.spark.sql.functions._

val dataset = table("amazon")
display(dataset.filter("user = 'A3OXHLG6DIBRW8'").select("brand", "title", "rating").orderBy(desc("rating")))

// COMMAND ----------

import org.apache.spark.ml.recommendation._

val hashId = sqlContext.udf.register("generateHashCode", (s : String) => s.hashCode)
val trainingData = dataset.withColumn("itemId", hashId($"asin")).withColumn("userId", hashId($"user"))

// Train ALS model
val als = new ALS().setItemCol("itemId").setUserCol("userId")
val model = als.fit(trainingData)

// COMMAND ----------

dataset.count()

// COMMAND ----------

// MAGIC %md Create recommendations for user A3OXHLG6DIBRW8

// COMMAND ----------

val userItems = dataset.filter("user = 'A3OXHLG6DIBRW8'")
                       .select("asin", "brand", "title", "rating", "user")
                       .withColumn("itemId", hashId($"asin"))
                       .withColumn("userId", hashId($"user"))

// COMMAND ----------

val recommendations = model.transform(userItems)

// COMMAND ----------

display(recommendations.select("prediction", "rating", "asin", "title", "brand").orderBy(desc("prediction")))

// COMMAND ----------

// MAGIC %md You can save your ALS model out to S3.

// COMMAND ----------

model.write.overwrite().save("dbfs:/ALSmodel/")

// COMMAND ----------

// MAGIC %fs ls /ALSmodel/

// COMMAND ----------

// MAGIC %md What does this person want?
// MAGIC Look at item B009EFVSEO 

// COMMAND ----------

val itemBrands = items.join(brands, "asin")
display(itemBrands.join(recommendations, "asin").orderBy(desc("prediction")).limit(10))

// COMMAND ----------

// MAGIC %md #Lesson Learned
// MAGIC 
// MAGIC ![Meme](https://i.imgflip.com/13xyz4.jpg)