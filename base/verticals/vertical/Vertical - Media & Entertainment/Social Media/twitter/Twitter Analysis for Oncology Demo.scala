// Databricks notebook source
// DBTITLE 1,Use Case: Social Media Analytics - Twitter Analysis for Oncology
// MAGIC %md 
// MAGIC 
// MAGIC Twitter Streaming is a great way to execute Spark Streaming. 
// MAGIC 
// MAGIC In this example, we show how to perform various social media analytics on real-time data focusing mainly on twitter data with a search text. 
// MAGIC 
// MAGIC Pre-requisites: Capture Twitter API credintials for consumer key, consumer secret, access token, and access token secret 

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ![IMG](https://s3-us-west-1.amazonaws.com/databricks-binu-mathew/image/Social_Media_Img.png)

// COMMAND ----------

// DBTITLE 1,Step1: Ingest Twitter Data
// MAGIC %md
// MAGIC * Enter your Twitter API Credentials.
// MAGIC * Go to https://apps.twitter.com and look up your Twitter API Credentials, or create an app to create them.
// MAGIC * Run this cell for the input cells to appear.
// MAGIC * Enter your credentials.
// MAGIC * Run the cell again to pick up your defaults.

// COMMAND ----------

// DBTITLE 1,Setup Streaming Libraries and Create UI Widgets 
/* Importing Spark Streaming APIs */

import org.apache.spark._
import org.apache.spark.storage._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.sql._
import scala.math.Ordering
import java.util.Date
import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder

/* Create UI Widgets for this notebook to capture input data */

dbutils.widgets.text("twitter_consumer_key", "", "1. Consumer Key (API Key)")
dbutils.widgets.text("twitter_consumer_secret", "", "2. Consumer Secret (API Secret)")
dbutils.widgets.text("twitter_access_token", "", "3. Access Token")
dbutils.widgets.text("twitter_access_secret", "", "4. Access Token Secret")
dbutils.widgets.text("slide_interval", "5", "5. Slide Interval - Recompute the top hashtags every N seconds")
dbutils.widgets.text("window_length", "600", "6. Window Length - Compute the top hashtags for the last N seconds")
dbutils.widgets.text("search_text", "", "7. Search Text")

/* Set variables for Spark streaming */
val slideInterval = new Duration(dbutils.widgets.get("slide_interval").toInt * 1000)
val windowLength = new Duration(dbutils.widgets.get("window_length").toInt * 1000)

// COMMAND ----------

// DBTITLE 1,Step2: Enrich Twitter Stream and capture micro batches to calculate hashtags 
/* 
Create SparkStreaming Context, 
Create Twitter Stream for input source,  
Create micro batch table at every interval
Calculate hashtags 
*/
var newContextCreated = false
var num = 0

// This is a helper class used for 
object SecondValueOrdering extends Ordering[(String, Int)] {
  def compare(a: (String, Int), b: (String, Int)) = {
    a._2 compare b._2
  }
}

//case class WordCount(word: String, count: Int)
// This is the function that creates the SteamingContext and sets up the Spark Streaming job.
def creatingFunc(): StreamingContext = {
  
  // 1. Create a Spark Streaming Context.
  val ssc = new StreamingContext(sc, slideInterval)
  
  // 2. Create a Twitter Stream for the input source. 
  val auth = Some(new OAuthAuthorization(new ConfigurationBuilder().setOAuthConsumerKey(dbutils.widgets.get("twitter_consumer_key"))
  .setOAuthConsumerSecret(dbutils.widgets.get("twitter_consumer_secret"))
  .setOAuthAccessToken(dbutils.widgets.get("twitter_access_token"))
  .setOAuthAccessTokenSecret(dbutils.widgets.get("twitter_access_secret")).build()))
  val twitterStream = TwitterUtils.createStream(ssc, auth)
  
  val tweetText = twitterStream.map {
  x =>(x.getId(),x.getUser().getScreenName(),x.getLang(),x.getRetweetCount(),x.getText(),new java.sql.Timestamp(x.getCreatedAt().asInstanceOf[java.util.Date].getTime()))
  }
  // 3. Create temp table at every batch interval
  tweetText.foreachRDD(rdd => { 
    val sqlContext = SQLContext.getOrCreate(SparkContext.getOrCreate())
    sqlContext.createDataFrame(rdd).toDF("id","user","language","retweetcount", "review","createdat").write.mode(SaveMode.Append).saveAsTable("tweets_live8")  
    rdd.take(1)
  }) 
    
  
  //val englishWordStream = tweetText.filter(tweetText(1)=="cancer").map(_.getText).flatMap(_.split(" "))
  val englishWordStream = twitterStream.filter(_.getLang.equals("en")).map(_.getText).flatMap(_.split(" "))
  
  // Compute the counts of the words by window.
  val windowedCountStream = englishWordStream.map((_, 1)).reduceByKeyAndWindow((x: Int, y: Int) => x + y, windowLength, slideInterval)

  // 4. For each window, calculate the top hashtags for that time period.
  windowedCountStream.foreachRDD(countRDD => {    
    countRDD.toDF("word", "count").createOrReplaceTempView("twitter_word_count")    
    
    countRDD.filter(_._1.startsWith("#")).toDF("word", "count").createOrReplaceTempView("twitter_hashtag_count")    
  })
  
  // To make sure data is not deleted by the time we query it interactively
  ssc.remember(Minutes(10)) 
  
  println("Creating function called to create new StreamingContext")
  newContextCreated = true
  ssc
}

/*Create the StreamingContext using getActiveOrCreate, as required when starting a streaming job in Databricks.*/
@transient val ssc = StreamingContext.getActiveOrCreate(creatingFunc)

/*Start the Spark Streaming Context and return when the Streaming job exits or return with the specified timeout.*/
ssc.start()
ssc.awaitTerminationOrTimeout(5 * 1000)

// COMMAND ----------

// DBTITLE 1,Step3: Explore Twitter Data by capturing Tweets into a Table and Analyze Data
// MAGIC %sql drop table if exists tweets_live8;
// MAGIC 
// MAGIC create table tweets_live8 (id long,user string,language string,retweetcount int,review string,createdat timestamp) USING PARQUET;
// MAGIC 
// MAGIC desc tweets_live8;

// COMMAND ----------

// DBTITLE 1,Step 4: Visualization
// MAGIC %md
// MAGIC * Visualizing and find outliers
// MAGIC * Twitter Timeline
// MAGIC * To build a timelineof twitter usage, we can simply plot the number of tweets per minute.

// COMMAND ----------

// DBTITLE 1,Timeline of Twitter usage - Tweets per minute
// MAGIC %sql select to_date(createdat,"yyyy-MM-dd"),count(*) as total_tweet from tweets_live8 group by to_date(createdat,"yyyy-MM-dd") order by to_date(createdat,"yyyy-MM-dd")

// COMMAND ----------

// DBTITLE 1,Top 10 Twitter Users (loudest voices) of the SearchText during this time
// MAGIC %sql select user,count(*) as total_tweet from tweets_live8 where language = "en" and lower(review) rlike "$search_text" group by user order by total_tweet desc limit 10

// COMMAND ----------

// DBTITLE 1,Review Top Tweets in English
// MAGIC %sql select a.user,b.total_tweet,a.review,getArgument("search_text") as topic from tweets_live8 a,(select user,count(*) as total_tweet from tweets_live8 where language= "en" and lower(review) rlike "$search_text" group by user order by total_tweet desc limit 10) b where a.user == b.user

// COMMAND ----------

// DBTITLE 1,Stop any active Streaming Contexts, but don't stop the spark contexts they are attached to.
StreamingContext.getActive.foreach { _.stop(stopSparkContext = false) }

// COMMAND ----------

// DBTITLE 1,Step 5: Model creation
// MAGIC %md

// COMMAND ----------

// DBTITLE 1,Cache training data set to memory before Model training
dbutils.fs.cacheTable("amazon")

// COMMAND ----------

// DBTITLE 1,Model Training and Prediction
import org.apache.spark.sql._
val sqlContext = SQLContext.getOrCreate(SparkContext.getOrCreate())
val surveyData = sqlContext.sql("select * from tweets_live8 where lower(review) rlike 'trump' ")

// COMMAND ----------

// DBTITLE 1,Text Tokenization
import org.apache.spark.ml.feature.RegexTokenizer

// Set params for RegexTokenizer
val tokenizer = new RegexTokenizer()
  .setPattern("[\\W_]+")
  .setMinTokenLength(4) // Filter away tokens with length < 4
  .setInputCol("review")
  .setOutputCol("tokens")

// Tokenize document
val tokenized_df = tokenizer.transform(surveyData)

display(tokenized_df.select("tokens"))

// COMMAND ----------

// DBTITLE 1,Remove Stop-words
// MAGIC %sh wget http://ir.dcs.gla.ac.uk/resources/linguistic_utils/stop_words -O /tmp/stopwords

// COMMAND ----------

// DBTITLE 1,specify a list of stopwords for the StopWordsRemover() to use
// MAGIC %fs cp file:/tmp/stopwords dbfs:/tmp/stopwords

// COMMAND ----------

// DBTITLE 1,Review Topics
val stopwords = sc.textFile("/tmp/stopwords").collect()

import org.apache.spark.ml.feature.StopWordsRemover

// Set params for StopWordsRemover
val remover = new StopWordsRemover()
  .setStopWords(stopwords) // This parameter is optional
  .setInputCol("tokens")
  .setOutputCol("filtered")

// Create new DF with Stopwords removed
val filtered_df = remover.transform(tokenized_df)

val filtered_df_2 = filtered_df.withColumn("id", 'id.cast("Long")).select("id", "filtered")

import org.apache.spark.ml.feature.CountVectorizer

// Set params for CountVectorizer
val vectorizer = new CountVectorizer()
  .setInputCol("filtered")
  .setOutputCol("features")
  .setVocabSize(10000)
  .setMinDF(5)
  .fit(filtered_df)
// Create vector of token counts
val countVectors = vectorizer.transform(filtered_df_2).select("id","features")

display(countVectors)


// Convert DF to RDD
import org.apache.spark.ml.linalg.Vector

val lda_countVector = countVectors.map { case Row(id: scala.Long, countVector: Vector) => (id, countVector) }

lda_countVector.take(1)

val numTopics = 10
import org.apache.spark.ml.clustering.{LDA, DistributedLDAModel}

// Set LDA parameters
val em_lda  = new LDA()
  .setOptimizer("online")
  .setMaxIter(3)

val em_ldaModel = em_lda.fit(countVectors)

val topicIndices = em_ldaModel.describeTopics(maxTermsPerTopic = 5)

for ((row) <- topicIndices) {
        val topicNumber = row.get(0)
        val topicTerms  = row.get(1)
        val topicweights  = row.get(2)
        println ("Topic: "+ topicNumber)
}

// COMMAND ----------

import scala.collection.mutable.WrappedArray

val vocab = vectorizer.vocabulary

for ((row) <- topicIndices) {
    val topicNumber = row.get(0)
    //val terms = row.get(1)
    val terms:WrappedArray[Int] = row.get(1).asInstanceOf[WrappedArray[Int]]
    for ((termIdx) <- 0 until 4) {
        println("Topic:" + topicNumber + " Word:" + vocab(termIdx))
    }
}

topicIndices.printSchema

// COMMAND ----------

import org.apache.spark.sql.Row

topicIndices.collect().foreach { r => 
                r match {
                        case _: Row => ("Topic:" + r)
                        case unknow => println("Something Else")
        }
}

topicIndices.collect().foreach { r => {
                        println("Topic:" + r(0))
                        val terms:WrappedArray[Int] = r(1).asInstanceOf[WrappedArray[Int]]
                        terms.foreach {
                          var i=0
                                t => {
                                        println("Term:" + vocab(t) + " weights:" + r(2).asInstanceOf[WrappedArray[Double]](i))
                                  i=i+1
                                }
                        }
                }
        }

// COMMAND ----------

// DBTITLE 1,Step 6: Visualization 
val topicIndices = em_ldaModel.describeTopics(maxTermsPerTopic = 5)
val vocabList = vectorizer.vocabulary

import scala.collection.mutable.ArrayBuffer


var termArray = List[(String,Double,Int)]()

topicIndices.collect().foreach { r => {
                        println("Topic:" + r(0))
                        val terms:WrappedArray[Int] = r(1).asInstanceOf[WrappedArray[Int]]
                        val weights:WrappedArray[Double] = r(2).asInstanceOf[WrappedArray[Double]]
                        terms.foreach {
                          var i=0
                                t => {
                                        termArray ::= (vocabList(t), weights(i), t)
                                  i+1
                                }
                        }
                
                }
        }

// Zip topic terms with topic IDs
//val termArray = topics.zipWithIndex
// Transform data into the form (term, probability, topicId)
val termRDD = spark.sparkContext.parallelize(termArray).toDF().withColumnRenamed("_1", "term").withColumnRenamed("_2", "probability").withColumnRenamed("_3", "topicId")

display(termRDD)

// Create JSON data
val rawJson = termRDD.toJSON.collect().mkString(",\n")

// COMMAND ----------

// MAGIC %md 
// MAGIC We will try visualizing the results obtained from the EM LDA model with a d3 bubble chart.

// COMMAND ----------

// DBTITLE 1,Topic Visualization
displayHTML(s"""
<!DOCTYPE html>
<meta charset="utf-8">
<style>

circle {
  fill: rgb(31, 119, 180);
  fill-opacity: 0.5;
  stroke: rgb(31, 119, 180);
  stroke-width: 1px;
}

.leaf circle {
  fill: #ff7f0e;
  fill-opacity: 1;
}

text {
  font: 14px sans-serif;
}

</style>
<body>
<script src="https://cdnjs.cloudflare.com/ajax/libs/d3/3.5.5/d3.min.js"></script>
<script>

var json = {
 "name": "data",
 "children": [
  {
     "name": "topics",
     "children": [
      ${rawJson}
     ]
    }
   ]
};

var r = 1500,
    format = d3.format(",d"),
    fill = d3.scale.category20c();

var bubble = d3.layout.pack()
    .sort(null)
    .size([r, r])
    .padding(1.5);

var vis = d3.select("body").append("svg")
    .attr("width", r)
    .attr("height", r)
    .attr("class", "bubble");

  
var node = vis.selectAll("g.node")
    .data(bubble.nodes(classes(json))
    .filter(function(d) { return !d.children; }))
    .enter().append("g")
    .attr("class", "node")
    .attr("transform", function(d) { return "translate(" + d.x + "," + d.y + ")"; })
    color = d3.scale.category20();
  
  node.append("title")
      .text(function(d) { return d.className + ": " + format(d.value); });

  node.append("circle")
      .attr("r", function(d) { return d.r; })
      .style("fill", function(d) {return color(d.topicName);});

var text = node.append("text")
    .attr("text-anchor", "middle")
    .attr("dy", ".3em")
    .text(function(d) { return d.className.substring(0, d.r / 3)});
  
  text.append("tspan")
      .attr("dy", "1.2em")
      .attr("x", 0)
      .text(function(d) {return Math.ceil(d.value * 10000) /10000; });

// Returns a flattened hierarchy containing all leaf nodes under the root.
function classes(root) {
  var classes = [];

  function recurse(term, node) {
    if (node.children) node.children.forEach(function(child) { recurse(node.term, child); });
    else classes.push({topicName: node.topicId, className: node.term, value: node.probability});
  }

  recurse(null, root);
  return {children: classes};
}
</script>
""")