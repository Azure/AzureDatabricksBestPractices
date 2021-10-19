// Databricks notebook source
// MAGIC %md
// MAGIC * [**Intrusion Detection System(IDS)**](https://en.wikipedia.org/wiki/Intrusion_detection_system) is a device or software application that monitors a network or systems for malicious activity or policy violations and is...  
// MAGIC   * Built on top of Apache Spark in and written in Scala inside Databricks Platform
// MAGIC   * Uses a machine learning **linear Regression** implementation to find the most *important* attribute to a IDS of interest   
// MAGIC * This demo...  
// MAGIC   * Includes a dataset with a subset of simulated network traffic samples  

// COMMAND ----------

// MAGIC %md
// MAGIC ###0. SETUP -- Databricks Spark cluster:  
// MAGIC 
// MAGIC 1. **Create** a cluster by...  
// MAGIC   - Click the `Clusters` icon on the left sidebar and then `Create Cluster.` 
// MAGIC   - Enter any text, i.e `demo` into the cluster name text box
// MAGIC   - Select the `Apache Spark Version` value `Spark 2.2 (auto-updating scala 2.11)`  
// MAGIC   - Click the `create cluster` button and wait for your cluster to be provisioned
// MAGIC 3. **Attach** this notebook to your cluster by...   
// MAGIC   - Click on your cluster name in menu `Detached` at the top left of this workbook to attach it to this workbook 

// COMMAND ----------

// MAGIC %md 
// MAGIC ###Step1: Ingest IDS Data to Notebook
// MAGIC * The CIDDS-001 data set.zip can be downloaded from [**CIDDS site**](https://www.hs-coburg.de/index.php?id=927)
// MAGIC * Unzip the data and upload the CIDDS-001>traffic>ExternalServer>*.csv from unziped folder to Databricks notebooks

// COMMAND ----------

// MAGIC %run ./data_Setup

// COMMAND ----------

val idsdata = sqlContext.read.format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .load("wasbs://dataset@wesdias.blob.core.windows.net/Azure/dataset/security/ids")

display(idsdata)

// COMMAND ----------

val newNames = Seq("datefirstseen", "duration", "proto", "srcip","srcpt","dstip","dstpt","packets","bytes","flows","flags","tos","transtype","label","attackid","attackdescription")
val dfRenamed = idsdata.toDF(newNames: _*)
val dfReformat = dfRenamed.select("label","datefirstseen", "duration", "proto", "srcip","srcpt","dstip","dstpt","packets","bytes","flows","flags","tos","transtype","attackid","attackdescription")

// COMMAND ----------

// MAGIC %md ### Step2: Enrich the data to get additional insigts to IDS dataset
// MAGIC - we create a temporay table from the file location "/tmp/wesParquet" in paraquet file format e.g. dfReformat.write.mode("overwrite").parquet("/mnt/wesley/tmp/wesParquet/")
// MAGIC - Parquet file format is the prefered file format since it's optimized for the Notebooks in the Databricks platform 

// COMMAND ----------

// MAGIC %sql 
// MAGIC 
// MAGIC     CREATE TEMPORARY TABLE temp_idsdata
// MAGIC     USING parquet
// MAGIC     OPTIONS (
// MAGIC       path "/mnt/wesley/tmp/wesParquet/"
// MAGIC     )

// COMMAND ----------

// MAGIC %md Calculate statistics on the Content Sizes returned.

// COMMAND ----------

// MAGIC %sql
// MAGIC select min(trim(bytes)) as min_bytes,max(trim(bytes)) as max_bytes,avg(trim(bytes)) as avg_bytes from temp_idsdata

// COMMAND ----------

// MAGIC %md ###Step3: Explore IDS Data by capturing the type of attacks on the network

// COMMAND ----------

// DBTITLE 1,Analysis of Attack type caught
// MAGIC %sql
// MAGIC 
// MAGIC select label, count(*) as the_count from temp_idsdata where label <> '---' group by label order by the_count desc

// COMMAND ----------

// MAGIC %md
// MAGIC ###Step 4: Visualization
// MAGIC * Visualizing and find outliers
// MAGIC * View a list of IPAddresses that has accessed the server more than N times.

// COMMAND ----------

// DBTITLE 1,Explore the Source IP used for attacks
// MAGIC %sql
// MAGIC -- Use the parameterized query option to allow a viewer to dynamically specify a value for N.
// MAGIC -- Note how it's not necessary to worry about limiting the number of results.
// MAGIC -- The number of values returned are automatically limited to 1000.
// MAGIC -- But there are options to view a plot that would contain all the data to view the trends.
// MAGIC SELECT srcip, COUNT(*) AS total FROM temp_idsdata GROUP BY srcip HAVING total > $N order by total desc

// COMMAND ----------

// DBTITLE 1,Explore Statistics about the protocol used for attack using spark SQL
// MAGIC %sql
// MAGIC 
// MAGIC -- Display a plot of the distribution of the number of hits across the endpoints.
// MAGIC SELECT Proto, count(*) as num_hits FROM temp_idsdata GROUP BY Proto ORDER BY num_hits DESC

// COMMAND ----------

// DBTITLE 1,Explore Statistics about the protocol used for attack using Matplotlib
// MAGIC %python
// MAGIC import matplotlib.pyplot as plt
// MAGIC importance = sqlContext.sql("SELECT Proto as protocol, count(*) as num_hits FROM temp_idsdata GROUP BY Proto ORDER BY num_hits DESC")
// MAGIC importanceDF = importance.toPandas()
// MAGIC ax = importanceDF.plot(x="protocol", y="num_hits",lw=3,colormap='Reds_r',title='Importance in Descending Order', fontsize=9)
// MAGIC ax.set_xlabel("protocol")
// MAGIC ax.set_ylabel("num_hits")
// MAGIC plt.xticks(rotation=12)
// MAGIC plt.grid(True)
// MAGIC plt.show()
// MAGIC display()

// COMMAND ----------

// MAGIC %r
// MAGIC library(SparkR)
// MAGIC library(ggplot2)
// MAGIC importance_df  = collect(sql(sqlContext,'SELECT Proto as protocol, count(*) as num_hits FROM temp_idsdata GROUP BY Proto ORDER BY num_hits DESC'))
// MAGIC ggplot(importance_df, aes(x=protocol, y=num_hits)) + geom_bar(stat='identity') + scale_x_discrete(limits=importance_df[order(importance_df$num_hits), "protocol"]) + coord_flip()

// COMMAND ----------

// MAGIC %md #### Step 5: Model creation

// COMMAND ----------

import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.feature._;
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.StringIndexer

case class Data(label: Double, feature: Seq[Double])

val indexer1 = new StringIndexer()
                   .setInputCol("proto")
                   .setOutputCol("protoIndex")
                   .fit(dfReformat)

val indexed1 = indexer1.transform(dfReformat)

val indexer2 = new StringIndexer()
                   .setInputCol("label")
                   .setOutputCol("labelIndex")
                   .fit(indexed1)

val indexed2 = indexer2.transform(indexed1)

val features = indexed2.rdd.map(row => 
Data(
   row.getAs[Double]("labelIndex"),   
   Seq(row.getAs[Double]("duration"),row.getAs[Double]("protoIndex"))
)).toDF


val assembler = new VectorAssembler()
  .setInputCols(Array("duration", "protoIndex"))
  .setOutputCol("feature")

val output = assembler.transform(indexed2)
println("Assembled columns 'hour', 'mobile', 'userFeatures' to vector column 'features'")
output.select("feature", "labelIndex").show(false)

val labeled = output.rdd.map(row => 
LabeledPoint(
   row.getAs[Double]("labelIndex"),   
   row.getAs[org.apache.spark.ml.linalg.Vector]("feature")
)).toDF



val splits = labeled randomSplit Array(0.8, 0.2)

val training = splits(0) cache
val test = splits(1) cache

val algorithm = new LogisticRegression()
val model = algorithm fit training

val prediction = model.transform(test)

val predictionAndLabel = prediction.rdd.zip(test.rdd.map(x => x.getAs[Double]("label")))

predictionAndLabel.foreach((result) => println(s"predicted label: ${result._1}, actual label: ${result._2}"))

// COMMAND ----------

val loss = predictionAndLabel.map { case (p, l) =>
  val err = p.getAs[Double]("prediction") - l
  err * err
}.reduce(_ + _)

val numTest = test.count()
val rmse = math.sqrt(loss / numTest) 

// COMMAND ----------

// MAGIC %md
// MAGIC ##Results interpretation
// MAGIC The plot above shows Index used to measure each of the attack type. 
// MAGIC 
// MAGIC ![IDS-Index](http://resources.infosecinstitute.com/wp-content/uploads/030817_1737_Top5FreeInt1.jpg)
// MAGIC 
// MAGIC 1. The most comman type of attack were Denial of Service(DoS), followed by port scan.
// MAGIC 2. IP 192.168.220.16 was the origin for most of the attacks, amounting to atleast 14% of all attacks
// MAGIC 3. Most of the Atacks used TCP protocol.
// MAGIC 4. As you can infer from the RMSE on running the model on the test data to predict the type of network attack we got a good accuracy of 0.4919