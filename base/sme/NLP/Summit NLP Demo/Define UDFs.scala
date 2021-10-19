// Databricks notebook source
// MAGIC %md ## Cluster Library Requirements
// MAGIC 
// MAGIC 0. nltk (pypi)
// MAGIC 0. textblob (pypi)
// MAGIC 0. Stanford Spark CoreNLP
// MAGIC      * databricks:spark-corenlp:0.3.1-s_2.11 (Maven)
// MAGIC      * com.sun.xml.bind:jaxb-impl:2.2.7 (Maven)
// MAGIC 0. John Snow Labs
// MAGIC      * com.johnsnowlabs.nlp:spark-nlp_2.11:1.7.2 (Maven)
// MAGIC 
// MAGIC 0. Optional:
// MAGIC     0. wordcloud (pypi) - Generate word cloud visualiations
// MAGIC     0. spark-nlp==1.7.2 (Pypi) - only necessary for using Python API for SparkNLP

// COMMAND ----------

import org.apache.spark.sql.functions._

// COMMAND ----------

// DBTITLE 1,TextBlob
// MAGIC %python
// MAGIC from textblob import TextBlob
// MAGIC from pyspark.sql.types import *
// MAGIC from pyspark.sql.functions import *
// MAGIC 
// MAGIC def sentimentTextBlob(text):
// MAGIC   sentiment = TextBlob(text).sentiment
// MAGIC   return (sentiment.polarity, sentiment.subjectivity)
// MAGIC 
// MAGIC spark.udf.register("sentimentTextBlobUDF", sentimentTextBlob, StructType([StructField("polarity", DoubleType()), 
// MAGIC                                                                           StructField("subjectivity", DoubleType())]))

// COMMAND ----------

// DBTITLE 1,NLTK
// MAGIC %python
// MAGIC import nltk
// MAGIC from nltk.sentiment.vader import *
// MAGIC from pyspark.sql.types import *
// MAGIC from pyspark.sql.functions import col
// MAGIC 
// MAGIC def sentimentNLTK(text):
// MAGIC   nltk.download('vader_lexicon')
// MAGIC   sia = SentimentIntensityAnalyzer()
// MAGIC   return sia.polarity_scores(text)
// MAGIC spark.udf.register("sentimentNLTKUDF", sentimentNLTK, MapType(StringType(), DoubleType()))

// COMMAND ----------

// DBTITLE 1,John Snow Labs
import com.johnsnowlabs.nlp.pretrained.pipelines.en.SentimentPipeline

// Don't need to pre-define anything

// COMMAND ----------

// DBTITLE 1,Stanford CoreNLP
import com.databricks.spark.corenlp.functions._

val version = "3.9.1"
val baseUrl = s"http://repo1.maven.org/maven2/edu/stanford/nlp/stanford-corenlp"
val model = s"stanford-corenlp-$version-models.jar" // 
val url = s"$baseUrl/$version/$model"
if (!sc.listJars().exists(jar => jar.contains(model))) {
  import scala.sys.process._
  // download model
  s"wget -N $url".!!
  // make model files available to driver
  s"jar xf $model".!!
  // add model to workers
  sc.addJar(model)
}

// COMMAND ----------

