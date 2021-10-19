// Databricks notebook source
// DBTITLE 1,Overview
// MAGIC %md
// MAGIC ### Email Analytics Engine
// MAGIC 
// MAGIC Making sense of your email analytics can be tricky at first.
// MAGIC 
// MAGIC How do you know which numbers to focus on? What do all those numbers really mean anyway? Is this really worth your time?
// MAGIC To approach your email reports with confidence, you have to understand what information you have available and how to use that information to improve your strategy. 
// MAGIC 
// MAGIC * [**Databricks Email Analytics Engine**](http://www.fiercehealthcare.com/it/paris-hospitals-use-predictive-modeling-to-control-admissions) is the use of data analytics and machine learning to identify email pattern and is...  
// MAGIC   * Built on top of Databricks Platform
// MAGIC   * Uses a machine learning implementation to generate patterns among email users   
// MAGIC * This demo...  
// MAGIC   * demonstrates a Email Analytical workflow.  We use Enron Email dataset from the [Carnegie Mellon University](https://www.cs.cmu.edu/%7E./enron/enron_mail_20150507.tgz)

// COMMAND ----------

// DBTITLE 1,0. Setup Databricks Spark cluster:
// MAGIC %md
// MAGIC 
// MAGIC **Create** a cluster by...  
// MAGIC   - Click the `Clusters` icon on the left sidebar and then `Create Cluster.` 
// MAGIC   - Enter any text, i.e `demo` into the cluster name text box
// MAGIC   - Select the `Apache Spark Version` value `Spark 2.2 (auto-updating scala 2.11)`  
// MAGIC   - Click the `create cluster` button and wait for your cluster to be provisioned
// MAGIC   
// MAGIC **Attach** this notebook to your cluster by...   
// MAGIC   - Click on your cluster name in menu `Detached` at the top left of this workbook to attach it to this workbook 
// MAGIC   - Add the spark-xml library to the cluster created above. The library is present in libs folder under current user.

// COMMAND ----------

// DBTITLE 1,Step1: Ingest medicare Data to Notebook
// MAGIC %md 
// MAGIC 
// MAGIC ![Enron Logo](http://uebercomputing.com/img/Logo_de_Enron.svg) We will extracted the Enron Email dataset from the [Carnegie Mellon University](https://www.cs.cmu.edu/%7E./enron/enron_mail_20150507.tgz)
// MAGIC 
// MAGIC * We run script to copy the email dataset file from wasb to dbfs. The dataset is in avro format. This is a one time activity. the script is "data_Setup" which is in the same folder using the command %run ./data_Setup

// COMMAND ----------

// MAGIC %md
// MAGIC Read Enron Emails from Avro

// COMMAND ----------

// MAGIC %run ./data_Setup

// COMMAND ----------

import com.databricks.spark.avro._

val emailsAvroDf = spark.read.avro("/mnt/azure/dataset/security/email/mail-2015.avro")
val allEmailsCount = emailsAvroDf.count()

// COMMAND ----------

// MAGIC %md ###Step2: Explore the Email Dataset

// COMMAND ----------

// MAGIC %md
// MAGIC Show email distribution by year

// COMMAND ----------

import org.apache.spark.sql.functions.from_unixtime
import org.apache.spark.sql.functions.year

//unixtime expects seconds from 01-01-1970 00:00:00 UTC - we have millis
val emailsWithDateDf = emailsAvroDf.withColumn("year", year(from_unixtime($"dateUtcEpoch" / 1000)) )

//count per year
val countByYearsDf = emailsWithDateDf.groupBy("year").count().orderBy("year")

display(countByYearsDf)

// COMMAND ----------

// MAGIC %md
// MAGIC Keep only emails from 1999-2002

// COMMAND ----------

val emailsOfInterestDf = emailsWithDateDf.where($"year" >= 1999 && $"year" <= 2002)
val filteredEmailsCount = emailsOfInterestDf.count()

println(s"We filtered out ${allEmailsCount - filteredEmailsCount} emails.")

// COMMAND ----------

// MAGIC %md
// MAGIC Count emails from 2000 - Avro

// COMMAND ----------

emailsOfInterestDf.where($"year" === 2000).count() //not cached - must read from original Avro

// COMMAND ----------

// MAGIC %md
// MAGIC Save Enron emails in Parquet format, partitioning by year

// COMMAND ----------

emailsOfInterestDf.write.mode(SaveMode.Overwrite).partitionBy("year").parquet("/mnt/wesley/parquet/security/email/enron-years")

// COMMAND ----------

// MAGIC %fs ls /mnt/wesley/parquet/security/email/enron-years

// COMMAND ----------

// MAGIC %fs ls /mnt/wesley/parquet/security/email/enron-years/year=2000

// COMMAND ----------

// MAGIC %md
// MAGIC Count emails from 2000 - Parquet (Predicate pushdown - skip whole directories by partitioning)

// COMMAND ----------

val emailsParquetDf = spark.read.parquet("/mnt/wesley/parquet/security/email/enron-years")
emailsParquetDf.where($"year" === 2000).count()


// COMMAND ----------

// MAGIC %sql describe extended enron2015

// COMMAND ----------

// MAGIC %fs ls dbfs:/user/hive/warehouse/enron2015

// COMMAND ----------

// MAGIC %md
// MAGIC ###Step 3: Visualization
// MAGIC * Show the distribution of the Email Dataset.

// COMMAND ----------

// MAGIC %md 
// MAGIC How many unique senders?

// COMMAND ----------

// MAGIC %sql select count(distinct `from`) as distinctFroms from enron2015

// COMMAND ----------

// MAGIC %md Who sent the most emails? (Pie / Bar Chart)

// COMMAND ----------

// MAGIC %sql SELECT `from`, count(*) as emailCount FROM enron2015 GROUP BY `from` ORDER BY count(*) DESC LIMIT 5

// COMMAND ----------

// MAGIC %md 
// MAGIC 
// MAGIC Create Dataframe from table

// COMMAND ----------

val enronFromTableDf = spark.read.table("enron2015")
enronFromTableDf.count()

// COMMAND ----------

enronFromTableDf.printSchema

// COMMAND ----------

// MAGIC %md Sender emails by state

// COMMAND ----------

// MAGIC %md _Define State Codes (demo only)._

// COMMAND ----------

//in case unable to get to enron-lib library run this package cell
package com.databricks.demo.enron

/**
 * Utils for Enron Demo.
 */
object EnronUtils {

  val States = Array("AK", "AL", "AR", "AZ", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "IA", "ID", "IL", "IN", "KS", "KY", "LA", "MA", "MD", "ME",
    "MI", "MN", "MO", "MS", "MT", "NC", "ND", "NE", "NH", "NJ", "NM", "NV", "NY", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VA", "VT",
    "WA", "WI", "WV", "WY")

  /**
   * Map email to a state based on hashCode (demo only).
   */
  def emailToState(email: String): String = {
    val bucketNumber = if (email == null) {
      0
    } else {
      math.abs(email.hashCode) % States.size
    }
    States(bucketNumber)
  }
  
}

// COMMAND ----------

//previously attached to our cluster
import com.databricks.demo.enron.EnronUtils

spark.udf.register("emailToState", (email: String) => EnronUtils.emailToState(email))

val stateCounts = enronFromTableDf.selectExpr("emailToState(from) as state").groupBy($"state").count().orderBy($"count".desc)
display(stateCounts)

// COMMAND ----------

// MAGIC %md How many emails from Jeff Skilling (via his executive admin, Sherri Sera)?

// COMMAND ----------

import org.apache.spark.sql.functions.{from_unixtime,year}

display(
  enronFromTableDf.filter("from = 'sherri.sera@enron.com' ")
  .withColumn("date", from_unixtime($"dateUtcEpoch"/1000))
  .withColumn("year", year($"date"))  
)

// COMMAND ----------

// MAGIC %md #### Step 5: Model creation
// MAGIC 
// MAGIC Exploring the Enron Email Dataset with Databricks - ML1 Clustering

// COMMAND ----------

// MAGIC %md
// MAGIC Read Enron emails from S3

// COMMAND ----------

import org.apache.spark.sql.functions._

// COMMAND ----------

val emailsParquetDf = sqlContext.read.parquet("/mnt/wesley/parquet/security/email/enron-years")
println(emailsParquetDf.count())

// COMMAND ----------

emailsParquetDf.printSchema

// COMMAND ----------

// MAGIC %md Let's get rid of some spam first

// COMMAND ----------

val ignoreFrom = """nytdirect@nytimes.com
gizicki@bipac.org
issuealert@scientech.com
ireland@bipac.org
noreply@ccomad3.uu.commissioner.com
registered-users@lists.economist.com
countrybriefingseditor-admin@lists.economist.com
feedback_economist@lists.economist.com
mediatrec@emailfactory.net
whitney@thelaw.net
venturewire@venturewire.com
kbtoyoffers@marketing.kbkids.com
wppoliticsdaily@letters.washingtonpost.com
rrga-l@list.rtowest.org
bryant@cheatsheets.net
vireturn@uts.cc.utexas.edu
ada@clickaction.net
inbox@messaging.accuweather.com
no.address@enron.com
40enron@enron.com
enron.announcements@enron.com
mailer-daemon@postmaster.enron.com
mailer-daemon@ect.enron.com
news@maillist.diabetes.org
shockwave@mms.shockwave.com
accountmanager@shockwave.com
stp@sierratradingpost.com
specials@icruise.com
system.administrator@enron.com
news@eyeforenergy.com
mailer@lists.smarterliving.com
news@luxurylink.com
news@real-net.net
sweetdeals@streetmail.com
columbiahouse_music@clickaction.net
support@hotwire.com
partner-news@amazon.com
dailynews-text-request@mailing.smartmoneylist.com
sportslinerewards@mail.0mm.com
readers_advantage@email.bn.com
weekly-admin@smartmoneylist.com""".split("\\n")

// COMMAND ----------

val lowerBodiesDf = emailsParquetDf
  .filter($"from".isin(ignoreFrom:_*) === false)
  .filter($"from".endsWith("@ino.com") === false)
  .filter($"from".endsWith(".m0.net") === false)
  .filter($"from".endsWith("@sportsline.com") === false)
  .filter($"from".endsWith("smartmoney.com") === false)
  .filter($"from".endsWith("@espn.go.com") === false)
  .filter($"from".endsWith("@1.americanexpress.com") === false)
  .filter($"from".endsWith(".cdnow.com") === false)
  .select($"from", lower($"subject").as("lowerSubject"), lower($"body").as("lowerBody"))
println(lowerBodiesDf.count())

// COMMAND ----------

// MAGIC %md
// MAGIC Create all the parts of our ML Pipeline (tokenizer, stop words remover, count vectorizer)

// COMMAND ----------

val moreStopWords = Array(
      "enron", "forward", "attached", "attach", "http", "[image]", "mail", "said", "message", "mailto", "href", "subject", "corp", "email", "font", "thank", "thanks", "sent", "html", "table", "width", "height", "image", "nbsp", "size", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "forwarded"
    )

// COMMAND ----------

import org.apache.spark.ml.feature.{RegexTokenizer, StopWordsRemover, CountVectorizer, HashingTF, IDF, Normalizer}

val tokenizer = new RegexTokenizer()
  .setInputCol("lowerBody")
  .setOutputCol("words")
  .setPattern("[A-z]+")
  .setGaps(false)
  .setMinTokenLength(4)

val remover = new StopWordsRemover()
  .setStopWords(sc.textFile("/mnt/wesley/dataset/others/utilty/stopwords.txt").collect()
    .union(moreStopWords))
  .setInputCol(tokenizer.getOutputCol)
  .setOutputCol("noStopWords")

val countVectorizer = new CountVectorizer()
  .setInputCol(remover.getOutputCol)
  .setOutputCol("features")
  .setVocabSize(10000)
  .setMinDF(5)

// COMMAND ----------

// MAGIC %md Create the LDA ML Pipeline

// COMMAND ----------

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.clustering.LDA
 
val lda = new LDA()
  .setOptimizer("em")
  .setK(8)
  .setMaxIter(20)
 
@transient val ldaModel = new Pipeline()
  .setStages(Array(tokenizer, remover, countVectorizer, lda)).fit(lowerBodiesDf)

// COMMAND ----------

// MAGIC %md
// MAGIC Save the LDA model

// COMMAND ----------

ldaModel.write.overwrite().save("/mnt/wesley/model/enron/lda.model")

// COMMAND ----------

// MAGIC %md Create the Word2Vec ML Pipeline

// COMMAND ----------

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.Word2Vec

val word2vec = new Word2Vec()
  .setInputCol(remover.getOutputCol)
  .setOutputCol("result")
  .setVectorSize(3)
  .setMinCount(0)

@transient val word2vecModel = new Pipeline()
  .setStages(Array(tokenizer, remover, word2vec)).fit(lowerBodiesDf)

// COMMAND ----------

// MAGIC %md Save the Word2Vec Model

// COMMAND ----------

word2vecModel.write.overwrite().save("/mnt/wesley/model/enron/words.model")

// COMMAND ----------

// MAGIC %md Load saved models

// COMMAND ----------

import org.apache.spark.ml.PipelineModel

@transient val ldaModel = PipelineModel.load("/mnt/wesley/model/enron/lda.model")
@transient val word2vecModel = PipelineModel.load("/mnt/wesley/model/enron/words.model")

// COMMAND ----------

val vocab = ldaModel.stages(2).asInstanceOf[org.apache.spark.ml.feature.CountVectorizerModel].vocabulary
@transient val lda = ldaModel.stages(3).asInstanceOf[org.apache.spark.ml.clustering.LDAModel]

// COMMAND ----------

// MAGIC %md Display synonyms based on Word2Vec model

// COMMAND ----------

dbutils.widgets.text("word", "", "term")

val synonyms = {
  try {
    val _word2vec = word2vecModel.stages(2).asInstanceOf[org.apache.spark.ml.feature.Word2VecModel]
    val synonyms = _word2vec.findSynonyms(dbutils.widgets.get("word"), 10).select($"word")

    synonyms
  } catch {
    case _: Throwable => {
      Seq("Word not found in vocabulary!").toDF()
    }
  }
}

display(synonyms)

// COMMAND ----------

synonyms.filter($"word" === "midlands").createOrReplaceTempView("mysyns")

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC display(spark.table("mysyns"))

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC cache table mysyns;

// COMMAND ----------

// MAGIC %md Find emails based on synonyms

// COMMAND ----------

val contains_any_of = udf((a: Seq[Any], b: Seq[Any]) => a.exists(b.contains))

val filter_values = array(synonyms.collect().map(_.getString(0)).map(lit):_*)

display(tokenizer.transform(lowerBodiesDf).filter(contains_any_of($"words", filter_values)).drop("words"))

// COMMAND ----------

// MAGIC %md
// MAGIC Run LDA model against Enron emails

// COMMAND ----------

val predictionsDf = ldaModel.transform(lowerBodiesDf).cache()
predictionsDf.count()

// COMMAND ----------

// MAGIC %md Filter emails based on topic

// COMMAND ----------

val topicsWithTerms = lda
  .describeTopics(10)
  .explode("termIndices", "word") { termIndices: Seq[Int] => termIndices.map(vocab(_)) }
  .groupBy($"topic")
  .agg(collect_list($"word").as("terms"))
  .orderBy($"topic")
  .cache()
display(topicsWithTerms)

// COMMAND ----------

val to_array = udf((v: org.apache.spark.ml.linalg.Vector) => v.toDense.values)

val topic = 0

display(predictionsDf.select($"from", $"lowerSubject", $"lowerBody").filter(to_array($"topicDistribution").getItem(topic) > 0.7))

// COMMAND ----------

// MAGIC %md #### Step 6: Exploring Email as Graph
// MAGIC 
// MAGIC ![Enron Logo](http://uebercomputing.com/img/Logo_de_Enron.svg) Exploring the Enron Email Dataset as Graph

// COMMAND ----------

// MAGIC %md Prepare Enron emails as GraphFrame.Put each entry of To array into its own row.

// COMMAND ----------

val emailsDf = sqlContext.read.parquet("/mnt/wesley/parquet/security/email/enron-years")

val oneToPerRow = emailsDf
  .filter($"from".endsWith("@enron.com"))
  .explode("to", "singleTo"){ to: Seq[String] => to }
  .filter($"singleTo".endsWith("@enron.com"))
  .select("from", "singleTo")

display(oneToPerRow)

// COMMAND ----------

oneToPerRow.cache().count()

// COMMAND ----------

val uniqueFroms = oneToPerRow.groupBy("from").count()

// COMMAND ----------

// MAGIC %md Convert uniqueFroms to vertices DataFrame with id

// COMMAND ----------

val vertices = uniqueFroms.orderBy($"count".desc).toDF("id", "count")

display(vertices)

// COMMAND ----------

// MAGIC %md Prepare graph edges (src,dst + count attribute)

// COMMAND ----------

val emailsSent = oneToPerRow
  .select("from", "singleTo")
  .groupBy("from", "singleTo")
  .count()
  .orderBy($"count".desc)

display(emailsSent)

// COMMAND ----------

val edges = emailsSent.toDF("src", "dst", "count")

// COMMAND ----------

// MAGIC %md ### Create GraphFrame

// COMMAND ----------

import org.graphframes.GraphFrame

val graph = GraphFrame(vertices, edges)

// COMMAND ----------

// MAGIC %md Who received emails from most unique senders?

// COMMAND ----------

import org.apache.spark.sql.functions.{max, min}

val inDegrees = graph.inDegrees

display(inDegrees.orderBy($"inDegree".desc).limit(10))

// COMMAND ----------

// MAGIC %md Who sent emails to most unique receivers?

// COMMAND ----------

val outDegrees = graph.outDegrees

display(outDegrees.orderBy($"outDegree".desc).limit(10))

// COMMAND ----------

// MAGIC %scala
// MAGIC package d3v21
// MAGIC // We use a package object so that we can define top level classes like Edge that need to be used in other cells
// MAGIC 
// MAGIC import org.apache.spark.sql._
// MAGIC import com.databricks.backend.daemon.driver.EnhancedRDDFunctions.displayHTML
// MAGIC 
// MAGIC case class Edge(src: String, dst: String, count: Long)
// MAGIC 
// MAGIC case class Node(name: String)
// MAGIC case class Link(source: Int, target: Int, value: Long)
// MAGIC case class Graph(nodes: Seq[Node], links: Seq[Link])
// MAGIC 
// MAGIC object graphs {
// MAGIC val sqlContext = SQLContext.getOrCreate(org.apache.spark.SparkContext.getOrCreate())  
// MAGIC import sqlContext.implicits._
// MAGIC   
// MAGIC def force(graph: Dataset[Edge], height: Int = 100, width: Int = 960): Unit = {
// MAGIC   val data = graph.collect()
// MAGIC   val nodes = (data.map(_.src) ++ data.map(_.dst)).map(_.replaceAll("_", " ")).toSet.toSeq.map(Node)
// MAGIC   val links = data.map { t =>
// MAGIC     Link(nodes.indexWhere(_.name == t.src.replaceAll("_", " ")), nodes.indexWhere(_.name == t.dst.replaceAll("_", " ")), t.count / 20 + 1)
// MAGIC   }
// MAGIC   showGraph(height, width, Seq(Graph(nodes, links)).toDF().toJSON.first())
// MAGIC }
// MAGIC 
// MAGIC /**
// MAGIC  * Displays a force directed graph using d3
// MAGIC  * input: {"nodes": [{"name": "..."}], "links": [{"source": 1, "target": 2, "value": 0}]}
// MAGIC  */
// MAGIC def showGraph(height: Int, width: Int, graph: String): Unit = {
// MAGIC 
// MAGIC displayHTML(s"""
// MAGIC <!DOCTYPE html>
// MAGIC <html>
// MAGIC <head>
// MAGIC   <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
// MAGIC   <meta charset="utf-8">
// MAGIC <style>
// MAGIC 
// MAGIC .node_circle {
// MAGIC   stroke: #777;
// MAGIC   stroke-width: 1.3px;
// MAGIC }
// MAGIC 
// MAGIC .node_label {
// MAGIC   pointer-events: none;
// MAGIC }
// MAGIC 
// MAGIC path.link {
// MAGIC   fill: none;
// MAGIC   stroke: #666;
// MAGIC   stroke-width: 1.5px;
// MAGIC }
// MAGIC 
// MAGIC // .link {
// MAGIC //   stroke: #777;
// MAGIC //   stroke-opacity: .2;
// MAGIC // }
// MAGIC 
// MAGIC .node_count {
// MAGIC   stroke: #777;
// MAGIC   stroke-width: 1.0px;
// MAGIC   fill: #999;
// MAGIC }
// MAGIC 
// MAGIC text.legend {
// MAGIC   font-family: Verdana;
// MAGIC   font-size: 13px;
// MAGIC   fill: #000;
// MAGIC }
// MAGIC 
// MAGIC .node text {
// MAGIC   display: none;
// MAGIC   font-family: "Helvetica Neue","Helvetica","Arial",sans-serif;
// MAGIC   font-size: 17px;
// MAGIC   font-weight: 200;
// MAGIC   z-index: 1000;
// MAGIC }
// MAGIC 
// MAGIC .node:hover text {
// MAGIC   display: block;
// MAGIC }
// MAGIC 
// MAGIC </style>
// MAGIC </head>
// MAGIC 
// MAGIC <body>
// MAGIC <script src="//d3js.org/d3.v3.min.js"></script>
// MAGIC <script>
// MAGIC 
// MAGIC var graph = $graph;
// MAGIC 
// MAGIC var margin = {top: -5, right: -5, bottom: -5, left: -5},
// MAGIC     width = $width - margin.left - margin.right,
// MAGIC     height = $height - margin.top - margin.bottom;
// MAGIC 
// MAGIC var zoom = d3.behavior.zoom()
// MAGIC     .scaleExtent([-2, 10])
// MAGIC     .on("zoom", zoomed);
// MAGIC 
// MAGIC var drag = d3.behavior.drag()
// MAGIC     .origin(function(d) { return d; })
// MAGIC     .on("dragstart", dragstarted)
// MAGIC     .on("drag", dragged)
// MAGIC     .on("dragend", dragended);
// MAGIC 
// MAGIC var svg = d3.select("body").append("svg")
// MAGIC     .attr("width", width + margin.left + margin.right)
// MAGIC     .attr("height", height + margin.top + margin.bottom);
// MAGIC 
// MAGIC var defs = svg.append("defs");
// MAGIC 
// MAGIC svg = svg.append("g")
// MAGIC     .attr("transform", "translate(" + margin.left + "," + margin.right + ")")
// MAGIC     .call(zoom);
// MAGIC    
// MAGIC var rect = svg.append("rect")
// MAGIC     .attr("width", width)
// MAGIC     .attr("height", height)
// MAGIC     .style("fill", "none")
// MAGIC     .style("pointer-events", "all");
// MAGIC 
// MAGIC var container = svg.append("g");
// MAGIC 
// MAGIC var color = d3.scale.category20();
// MAGIC 
// MAGIC var force = d3.layout.force()
// MAGIC     .charge(-200)
// MAGIC     .linkDistance(80)
// MAGIC     .size([width, height]);
// MAGIC 
// MAGIC force
// MAGIC     .nodes(graph.nodes)
// MAGIC     .links(graph.links)
// MAGIC     .start();
// MAGIC  
// MAGIC // build the arrow.
// MAGIC defs.selectAll("marker")
// MAGIC     .data(["end"])      // Different link/path types can be defined here
// MAGIC     .enter().append("marker")    // This section adds in the arrows
// MAGIC     .attr("id", String)
// MAGIC     .attr("viewBox", "0 -5 10 10")
// MAGIC     .attr("refX", 15)
// MAGIC     .attr("refY", -1.5)
// MAGIC     .attr("markerWidth", 6)
// MAGIC     .attr("markerHeight", 6)
// MAGIC     .attr("orient", "auto")
// MAGIC     .append("path")
// MAGIC     .attr("d", "M0,-5L10,0L0,5");
// MAGIC   
// MAGIC // add the links and the arrows
// MAGIC var path = container.append("g").selectAll("path")
// MAGIC     .data(graph.links)
// MAGIC     .enter().append("path")
// MAGIC     .attr("class", "link")
// MAGIC     .attr("marker-end", "url(#end)");
// MAGIC 
// MAGIC var node = container.append("g")
// MAGIC     .attr("class", "nodes")
// MAGIC     .selectAll(".node")
// MAGIC     .data(graph.nodes)
// MAGIC     .enter().append("g")
// MAGIC     .attr("class", "node")
// MAGIC     .attr("cx", function(d) { return d.x; })
// MAGIC     .attr("cy", function(d) { return d.y; })
// MAGIC     .call(drag);
// MAGIC 
// MAGIC node.append("circle")
// MAGIC     .attr("r", function(d) { return 8; })
// MAGIC     .style("fill", function (d) { return color(d.name); });
// MAGIC 
// MAGIC node.append("text")
// MAGIC       .attr("dx", 10)
// MAGIC       .attr("dy", ".35em")
// MAGIC       .text(function(d) { return d.name });
// MAGIC       
// MAGIC //Now we are giving the SVGs co-ordinates - the force layout is generating the co-ordinates which this code is using to update the attributes of the SVG elements
// MAGIC force.on("tick", function () {
// MAGIC //     link.attr("x1", function (d) {
// MAGIC //         return d.source.x;
// MAGIC //     }).attr("y1", function (d) {
// MAGIC //         return d.source.y;
// MAGIC //     }).attr("x2", function (d) {
// MAGIC //         return d.target.x;
// MAGIC //     }).attr("y2", function (d) {
// MAGIC //         return d.target.y;
// MAGIC //     });
// MAGIC   path.attr("d", function(d) {
// MAGIC         var dx = d.target.x - d.source.x,
// MAGIC             dy = d.target.y - d.source.y,
// MAGIC             dr = Math.sqrt(dx * dx + dy * dy);
// MAGIC         return "M" + 
// MAGIC             d.source.x + "," + 
// MAGIC             d.source.y + "A" + 
// MAGIC             dr + "," + dr + " 0 0,1 " + 
// MAGIC             d.target.x + "," + 
// MAGIC             d.target.y;
// MAGIC   });
// MAGIC   
// MAGIC   node.attr("transform", function(d) { return "translate(" + d.x + "," + d.y + ")"; });
// MAGIC });
// MAGIC 
// MAGIC function zoomed() {
// MAGIC   container.attr("transform", "translate(" + d3.event.translate + ")scale(" + d3.event.scale + ")");
// MAGIC }
// MAGIC 
// MAGIC function dragstarted(d) {
// MAGIC   d3.event.sourceEvent.stopPropagation();
// MAGIC   d3.select(this).classed("dragging", true);
// MAGIC   force.start();
// MAGIC }
// MAGIC 
// MAGIC function dragged(d) {
// MAGIC   d3.select(this).attr("cx", d.x = d3.event.x).attr("cy", d.y = d3.event.y);
// MAGIC }
// MAGIC 
// MAGIC function dragended(d) {
// MAGIC   d3.select(this).classed("dragging", false);
// MAGIC }
// MAGIC     
// MAGIC </script>
// MAGIC </html>
// MAGIC """)
// MAGIC }
// MAGIC   
// MAGIC   def help() = {
// MAGIC displayHTML("""
// MAGIC <p>
// MAGIC Produces a force-directed graph given a collection of edges of the following form:</br>
// MAGIC <tt><font color="#a71d5d">case class</font> <font color="#795da3">Edge</font>(<font color="#ed6a43">src</font>: <font color="#a71d5d">String</font>, <font color="#ed6a43">dst</font>: <font color="#a71d5d">String</font>, <font color="#ed6a43">count</font>: <font color="#a71d5d">Long</font>)</tt>
// MAGIC </p>
// MAGIC <p>Usage:<br/>
// MAGIC <tt>%scala</tt></br>
// MAGIC <tt><font color="#a71d5d">import</font> <font color="#ed6a43">d3._</font></tt><br/>
// MAGIC <tt><font color="#795da3">graphs.force</font>(</br>
// MAGIC &nbsp;&nbsp;<font color="#ed6a43">height</font> = <font color="#795da3">500</font>,<br/>
// MAGIC &nbsp;&nbsp;<font color="#ed6a43">width</font> = <font color="#795da3">500</font>,<br/>
// MAGIC &nbsp;&nbsp;<font color="#ed6a43">graph</font>: <font color="#795da3">Dataset</font>[<font color="#795da3">Edge</font>])</tt>
// MAGIC </p>""")
// MAGIC   }
// MAGIC }

// COMMAND ----------

// MAGIC %md Visualize the network of email traffic

// COMMAND ----------

import d3v21._

val forceGraph = edges.as[Edge].filter(_.count > 200)
graphs.force(height=768, width=1024, graph=forceGraph)

// COMMAND ----------

// MAGIC %md Pagerank as measure of importance?

// COMMAND ----------

val results = graph.pageRank.resetProbability(0.15).maxIter(10).run()

// Display resulting pageranks
display(results.vertices.select("id", "pagerank").orderBy($"pagerank".desc).limit(25))

// COMMAND ----------

// MAGIC %md Visualize top ten email senders/receivers to/from Jeff Skilling

// COMMAND ----------

dbutils.widgets.dropdown("email", "jeff.skilling@enron.com", results.vertices.orderBy($"pagerank".desc).select($"id").take(10).map(_.getString(0)), "Person of Interest")

// COMMAND ----------

//method to convert Spark Row into JSON Array string containing src (String), dst (String), count (long)
def rowToJson(row: Row):String = {
  s"""["${row.getString(0)}", "${row.getString(1)}", ${row.getLong(2)}]"""
}

// COMMAND ----------

val poi = dbutils.widgets.get("email")
val from = edges.filter($"src" === poi).orderBy($"count".desc).limit(10).collect().map(row => rowToJson(row)).mkString("[", ",", "]")
val to = edges.filter($"dst" === poi).orderBy($"count".desc).limit(10).collect().map(row => rowToJson(row)).mkString("[", ",", "]")

// COMMAND ----------

dbutils.widgets.get("email")

displayHTML(s"""
<!DOCTYPE html>
<body>
<script type="text/javascript"
           src="https://www.google.com/jsapi?autoload={'modules':[{'name':'visualization','version':'1.1','packages':['sankey']}]}">
</script>

<div id="sankey_multiple" style="width: 1024px; height: 600px;"></div>

<script type="text/javascript">

google.setOnLoadCallback(drawChart);
   function drawChart() {
    var data = new google.visualization.DataTable();
    data.addColumn('string', 'src');
    data.addColumn('string', 'dst');
    data.addColumn('number', 'count');
    data.addRows(${to});
    data.addRows(${from});
    // Set chart options
    var options = {
      width: 1000,
      sankey: {
        link: { color: { fill: '#grey', fillOpacity: 0.3 } },
        node: { color: { fill: '#a61d4c' },
                label: { color: 'black' } },
      }
    };
    // Instantiate and draw our chart, passing in some options.
    var chart = new google.visualization.Sankey(document.getElementById('sankey_multiple'));
    chart.draw(data, options);
   }
</script>
  </body>
</html>""")

// COMMAND ----------

// MAGIC %md
// MAGIC ##Results interpretation
// MAGIC 
// MAGIC ![Churn-Index](http://www.ngdata.com/wp-content/uploads/2016/05/churn.jpg)
// MAGIC 
// MAGIC The above created model shows the different techniques used to identify Email analytics pattern