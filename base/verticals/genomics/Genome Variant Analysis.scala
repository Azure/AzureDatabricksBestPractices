// Databricks notebook source
// MAGIC %md ## Genome Variant Analysis using k-means, ADAM, and Apache Spark
// MAGIC 
// MAGIC Authors: Deborah Siegel and Denny Lee
// MAGIC 
// MAGIC This notebook shows how to perform analysis with public data from the 1000 genomes project using the [Big Data Genomics](http://bdgenomics.org) ADAM Project ([0.19.0 Release](http://bdgenomics.org/blog/2016/02/25/adam-0-dot-19-dot-0-release/)).  We perform k-means clustering to predict which geographic population each person is from and visualize the results.
// MAGIC 
// MAGIC **special thanks and resources:**
// MAGIC * [Big Data Genomics ADAM](http://bdgenomics.org/projects/adam/)
// MAGIC * [ADAM: Genomics Formats and Processing Patterns for Cloud Scale Computing (Berkeley AMPLab)](https://amplab.cs.berkeley.edu/publication/adam-genomics-formats-and-processing-patterns-for-cloud-scale-computing/)
// MAGIC * Andy Petrella.  [Lightning Fast Genomics with Spark and ADAM](http://www.slideshare.net/noootsab/lightning-fast-genomics-with-spark-adam-and-scala) and [GitHub repository](https://github.com/andypetrella).
// MAGIC * Neil Ferguson [Population Stratification Analysis on Genomics Data Using Deep Learning](https://github.com/nfergu/popstrat).
// MAGIC * Matthew Conlen [Lightning-viz](http://lightning-viz.org/).

// COMMAND ----------

// MAGIC %md ### Some potentially useful terminology. 
// MAGIC 
// MAGIC For this notebook, the following terms are defined:
// MAGIC 
// MAGIC * **location**  : a position on a chromosome. People have ~3 billion locations in their genetic code. 
// MAGIC * **variant** : a specific location on a chromosome where different people can have different genotypes or haplotypes. A rough estimate is that each person has 3 million variants. 
// MAGIC * **genotype**  : a person's genetic code at a specific location, represented by 2 potentially different letters because each person generally has 2 instances of each location (i.e. G/A)
// MAGIC * **haplotype** : half of a person's genetic code at a specific location, given with 1 letter (i.e. G)
// MAGIC * **allele**  : a specific haplotype, categorized into "reference allele" (i.e. G) and "alternate allele" (i.e. A)
// MAGIC * **biallelic** : a variant which is only represented by 2 different letters in our data (i.e. only G and A are present for all the people in our data, not T or C). Most variants are biallelic. 

// COMMAND ----------

// MAGIC %md ### Table of Contents
// MAGIC 
// MAGIC 1. Launch special cluster.
// MAGIC 2. Import some Big Data Genomics libraries.
// MAGIC 3. Set File Paths, and setup to load data. 
// MAGIC 4. Load the original data file ([Variant Call Format (VCF) file](http://www.1000genomes.org/wiki/Analysis/variant-call-format)) and convert/save the data into BDG ADAM format.
// MAGIC 5. Prepare to read the ADAM data into RDDs.
// MAGIC 6. Read some of the ADAM data into RDDs.
// MAGIC 7. Explore the data.
// MAGIC 8. Clean the data (filters).
// MAGIC 9. Prepare data for K-means clustering.
// MAGIC 10. Run K-means clustering.
// MAGIC 11. Predict populations and compute the confusion matrix.
// MAGIC 12) Visualize the clusters with a force graph on lightning-viz. 

// COMMAND ----------

// MAGIC %md ## 1) Launch Special Cluster

// COMMAND ----------

// MAGIC %md
// MAGIC * Your cluster needs to be the Scala 2.10 and Spark 1.6.1 (Hadoop 2).
// MAGIC * When launching the cluster, ensure that the following configurations have been set:
// MAGIC  * `spark.serializer org.apache.spark.serializer.KryoSerializer`
// MAGIC  * `spark.kryo.registrator org.bdgenomics.adam.serialization.ADAMKryoRegistrator`
// MAGIC * The library `org.bdgenomics.utils.misc.Hadoop (utils-misc_2.10-0.2.4.jar)` must be installed /attached to the cluster *prior* to 
// MAGIC  * For this exact version, search for `org.bdgenomics.utils:utils-misc_2.10:0.2.4`
// MAGIC * Installing / attaching the `org.bdgenomics.adam.core (adam-core_2.10-0.19.0)` library.
// MAGIC  * For this exact version, search for `org.bdgenomics.adam:adam-core_2.10:0.19.0`
// MAGIC * You will need the lightning-viz python client. Get the lightning-viz client here and install/attach to your cluster before you start: https://pypi.python.org/pypi/lightning-python
// MAGIC 
// MAGIC For more information on the cluster setup, please refer to the [Setup Required for Genome Variant Analysis on DBCE Notebook](http://cdn2.hubspot.net/hubfs/438089/notebooks/help/Setup_Required_for_Genome_Variant_Analysis_on_DBCE.html)

// COMMAND ----------

// MAGIC %md ## 2) Import Big Data Genomics Libraries

// COMMAND ----------

import org.bdgenomics.adam.converters
import org.bdgenomics.formats.avro
import org.bdgenomics.formats.avro._
import org.bdgenomics.adam.models.VariantContext
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.variation._
import org.bdgenomics.adam.rdd.ADAMContext

// COMMAND ----------

// MAGIC %md ## 3) Set file paths
// MAGIC Note, the datasets for this notebook do not need to be downloaded as they are *already available on dbfs at the filepaths below*.  The source of these files can be found in teh bullet points below.
// MAGIC * The original source for the vcf data is the [1000 genomes project](http://www.1000genomes.org/). All of the data for the 1000 genomes project is also publicly available via [aws s3](https://aws.amazon.com/1000genomes/).
// MAGIC * The source for the abbreviated sample of chrosomose 6 data used in this analysis can be [downloaded](http://med-at-scale.s3.amazonaws.com/samples/6-sample.vcf). (full path on [s3](s3://med-at-scale/samples/6-sample.vcf)). You can also create your own subsample using [tabix](http://www.1000genomes.org/faq/how-do-i-get-sub-section-vcf-file/). We are using a subsample vcf for tutorial purposes. Ultimately, one could run the same code on the whole chromosome vcf, and filter the ADAM records by position. 
// MAGIC * The entire chromosome 6 data that was sampled from can be found on [ftp](ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/phase1/analysis_results/integrated_call_sets/ALL.chr6.integrated_phase1_v3.20101123.snps_indels_svs.genotypes.vcf.gz) or [s3](s3://1000genomes/phase1/analysis_results/integrated_call_sets/ALL.chr6.integrated_phase1_v3.20101123.snps_indels_svs.genotypes.vcf.gz). 
// MAGIC * The panel is available via [ftp](ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/release/20130502/integrated_call_samples_v3.20130502.ALL.panel)  and [s3](s3://1000genomes/release/20130502/integrated_call_samples_v3.20130502.ALL.panel).

// COMMAND ----------

// Set file locations
val vcf_path = "/databricks-datasets/samples/adam/samples/6-sample.vcf"
val tmp_path = "/tmp/adam/6-sample.adam"
val panel_path = "/databricks-datasets/samples/adam/panel/integrated_call_samples_v3.20130502.ALL.panel"

// COMMAND ----------

// Remove temporary folder if it was there previously
dbutils.fs.rm(tmp_path, true)

// COMMAND ----------

// MAGIC %fs ls dbfs:/databricks-datasets/samples/adam/samples/

// COMMAND ----------

// MAGIC %md
// MAGIC Our VCF file contains the people's genotypes at locations which are variants on a subset of chromosome 6. This vcf file contains 250 lines of metadata. The data we will need to analyze starts on line 251 of the file. 

// COMMAND ----------

dbutils.fs.head(vcf_path)

// COMMAND ----------

// MAGIC %md ## 4) Load VCF data and convert/save into ADAM parquet format
// MAGIC Parquet files are smaller than vcf files. And importantly, parquet files are designed to be distributable across a cluster (vcf are not).

// COMMAND ----------

val gts:RDD[Genotype] =  sc.loadGenotypes(vcf_path)
gts.adamParquetSave(tmp_path)

// COMMAND ----------

// Review the files in the l_tmp folder
display(dbutils.fs.ls(tmp_path))

// COMMAND ----------

// MAGIC %md  
// MAGIC After conversion to ADAM, our genotypes are now in avro data models, and are stored as JSON within the parquet files. Here is what one record looks like for a variant which was found to have alleles G and C in all of our data, but the person HG00110 has genotype G/G (Ref,Ref):

// COMMAND ----------

// MAGIC %md
// MAGIC ```
// MAGIC {
// MAGIC     "variant": {
// MAGIC         "variantErrorProbability": 100, 
// MAGIC         "contig": {
// MAGIC             "contigName": "6", 
// MAGIC             "contigLength": null, 
// MAGIC             "contigMD5": null, 
// MAGIC             "referenceURL": null, 
// MAGIC             "assembly": null, 
// MAGIC             "species": null, 
// MAGIC             "referenceIndex": null}, 
// MAGIC             "start": 1000012, 
// MAGIC             "end": 1000013, 
// MAGIC             "referenceAllele": "G", 
// MAGIC             "alternateAllele": "C", 
// MAGIC             "svAllele": null, 
// MAGIC             "isSomatic": false}, 
// MAGIC         "variantCallingAnnotations": {
// MAGIC             "variantIsPassing": true, 
// MAGIC             "variantFilters": [], 
// MAGIC             "downsampled": null, 
// MAGIC             "baseQRankSum": null, 
// MAGIC             "fisherStrandBiasPValue": null, 
// MAGIC             "rmsMapQ": null, 
// MAGIC             "mapq0Reads": null, 
// MAGIC             "mqRankSum": null, 
// MAGIC             "readPositionRankSum": null, 
// MAGIC             "genotypePriors": [], 
// MAGIC             "genotypePosteriors": [], 
// MAGIC             "vqslod": null, 
// MAGIC             "culprit": null, 
// MAGIC             "attributes": {}
// MAGIC         }, 
// MAGIC     "sampleId": "HG00110", 
// MAGIC     "sampleDescription": null, 
// MAGIC     "processingDescription": null, 
// MAGIC     "alleles": ["Ref", "Ref"], 
// MAGIC     "expectedAlleleDosage": null, 
// MAGIC     "referenceReadDepth": null, 
// MAGIC     "alternateReadDepth": null, 
// MAGIC     "readDepth": null, 
// MAGIC     "minReadDepth": null, 
// MAGIC     "genotypeQuality": null, 
// MAGIC     "genotypeLikelihoods": [], 
// MAGIC     "nonReferenceLikelihoods": [], 
// MAGIC     "strandBiasComponents": [], 
// MAGIC     "splitFromMultiAllelic": false, 
// MAGIC     "isPhased": true, 
// MAGIC     "phaseSetId": null, 
// MAGIC     "phaseQuality": null
// MAGIC }
// MAGIC ```

// COMMAND ----------

// MAGIC %md ## 5) Prepare to read the ADAM data into RDDs.

// COMMAND ----------

// MAGIC %md
// MAGIC VCF data contains sample IDs, but not population codes. Although we are doing an unsupervised algorithm in this analysis, we still need the response variables in order to filter our samples and to estimate our prediction error.  We can get the population codes for each sample from the panel file. 

// COMMAND ----------

val panel = sqlContext.read
  .format("com.databricks.spark.csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .option("delimiter", "\\t")
  .load(panel_path)
panel.count

// COMMAND ----------

// MAGIC %md
// MAGIC Our panel contains columns for person ID (sample), population code (pop), super population code (super_pop), and gender.  This will be our lookup panel for each person's population membership for our supervised learning model.
// MAGIC 
// MAGIC The population code definitions can be found at http://www.1000genomes.org/cell-lines-and-dna-coriell.
// MAGIC 
// MAGIC ![](http://www.1000genomes.org/sites/1000genomes.org/files/documents/1000-genomes-map_11-6-12-2_750.jpg)
// MAGIC 
// MAGIC Note, this image is from the [1000 genomes project](http://www.1000genomes.org/)

// COMMAND ----------

// MAGIC %md Let's look at how many individuals are in each of these populations in the panel

// COMMAND ----------

display(panel)

// COMMAND ----------

// MAGIC %md
// MAGIC To get a sense of this geographically, let's add an extra column to the panel dataframe for the ISO Alpha-3 Country Codes for a map visualization.  

// COMMAND ----------

val countryMap = Map("FIN" -> "FIN", "CHS" -> "CHN", "GBR" -> "GBR", "PUR" -> "PRI", "CLM" -> "COL", "MXL" -> "MEX", "TSI" -> "ITA", "LWK" -> "KEN", "JPT" -> "JPN", "IBS" -> "ESP", "PEL" -> "PER", "CDX" -> "CHN", "YRI" -> "NGA", "KHV" -> "VNM", "ASW" -> "USA", "ACB" -> "BRB", "CHB" -> "CHN", "GIH" -> "IND", "GWD" -> "GMB", "PJL" -> "PAK", "MSL" -> "SLE", "BEB" -> "BGD", "ESN" -> "NGA", "STU" -> "LKA", "ITU" -> "IND")
def udftoCC = udf((pop: String) => {
 countryMap.get(pop)})
val panelWithCountryDF = panel.withColumn("CCode", udftoCC(panel("pop")))

// COMMAND ----------

display(panelWithCountryDF)

// COMMAND ----------

// MAGIC %md
// MAGIC For our k-means clustering algorithms, we will model for 3 clusters, so we will create a filter for 3 populations:
// MAGIC British from England and Scotland (**GBR**), African Ancestry in Southwest US (**ASW**), and Han Chinese in Bejing, China (**CHB**). 

// COMMAND ----------

val filterPanel = panel.select("sample", "pop").where("pop IN ('GBR', 'ASW', 'CHB')")

// COMMAND ----------

// MAGIC %md
// MAGIC Let's look at our filtered panel to make sure we have a good number for each of the 3 populations in our training set

// COMMAND ----------

display(filterPanel)

// COMMAND ----------

// MAGIC %md
// MAGIC This is a small panel, so broadcasting it to all the executors will result in less data shuffling when we do further operations, thus it will be more efficient. 

// COMMAND ----------

val fpanel = filterPanel
  .rdd
  .map{x => x(0).toString -> x(1).toString}
  .collectAsMap()
val bPanel = sc.broadcast(fpanel)

// COMMAND ----------

// MAGIC %md ## 6) Read some of the ADAM data into RDDs to begin parallel processing of genotypes.
// MAGIC  
// MAGIC Parquet files enable predicate pushdown, so it will be efficient to apply our filter panel when we load the data from ADAM parquet files, and only load data from the people in our 3 populations. 

// COMMAND ----------

val popFilteredGts : RDD[Genotype] = sc.loadGenotypes(tmp_path).filter(genotype => {bPanel.value.contains(genotype.getSampleId)})
popFilteredGts.count

// COMMAND ----------

// MAGIC %md ## 7) Explore the data
// MAGIC We know our data comes from 3 populations, but let's do some exploratory analysis to see what locations our data contains. In this case, we know our data is only from chromosome 6. The entire chromosome 6 is around 170 million bp (base pairs) long. But our data, for now,  is only a subset of that.

// COMMAND ----------

// check our data locations. this is the first time we will run a action on our popFilteredGts RDD, so it will take some time, but will then be cached in memory. 
val startRDD = popFilteredGts.map(genotype => genotype.getVariant.getStart)
val minstart = startRDD.reduce((a, b) => if (a < b) a else b)
val maxstart = startRDD.reduce((a, b) => if (a > b) a else b)

// COMMAND ----------

// MAGIC %md
// MAGIC Our subsample of genotypes only contains variants which start between positions 800,444 and 1,249,939, covering about 0.5 Mb (half a million base pairs). 

// COMMAND ----------

// how many people's data do we have (should be the same as the number in our filter panel, but its good to check)
val peopleCount = popFilteredGts.map(_.getSampleId.toString.hashCode).distinct.count

// COMMAND ----------

// MAGIC %md ## 8) Clean the Data (2 filters)

// COMMAND ----------

// MAGIC %md #### Filter 1 -- If we are missing some data for a variant, or if the variant is triallelic, we want to remove them from our training data. 

// COMMAND ----------

// MAGIC %md 
// MAGIC The variants in the vcf don't come with unique identifiers, so we will make some which we can hash, in order to start filtering the variants efficiently. 

// COMMAND ----------

import scala.collection.JavaConverters._
import org.bdgenomics.formats.avro._

//create a unique ID for each variant which is a combination of chromosome, start, and end position
def variantId(g:Genotype):String = {
  val name = g.getVariant.getContig.getContigName
    val start = g.getVariant.getStart
    val end = g.getVariant.getEnd
    s"$name:$start:$end"
}

// COMMAND ----------

val variantsById = popFilteredGts.keyBy(g => variantId(g).hashCode).groupByKey.cache

// COMMAND ----------

// MAGIC %md 
// MAGIC If a variant is missing data, the number of genotypes per variant will be lower than the number of people. 
// MAGIC If a variant is triallelic, the number of genotypes per variant will be higher than the number of people.
// MAGIC 
// MAGIC (see this [blog post](https://wegetsignal.wordpress.com/2015/09/30/big-data-genomics-avro-schema-representation-of-biallelic-multi-allelic-sites-from-vcf/) for more detail on how genotypes are represented in the bdg avro schema)

// COMMAND ----------

val filterVariants = variantsById.filter { case (k, it) => it.size != peopleCount }.keys.collect

// COMMAND ----------

//and filter those out
val completeGts = popFilteredGts.filter { g => ! (filterVariants contains variantId(g).hashCode) }

// COMMAND ----------

// MAGIC %md #### Filter 2 --  Filter for variants with allele frequency > 30

// COMMAND ----------

// MAGIC %md
// MAGIC We will represent each genotype for each sample as a double: 
// MAGIC <pre>0.0 -> no copies of the alternate allele. 
// MAGIC 1.0 -> 1 copy alternate allele and 1 copy reference allele.
// MAGIC 2.0 -> 2 copies alternate allele</pre>

// COMMAND ----------

//get the alternate alleles to count
def altAsDouble(g:Genotype):Double = g.getAlleles.asScala.count(_ == GenotypeAllele.Alt)

val varToData = completeGts.map { g => ((variantId(g).hashCode), altAsDouble(g)) }

// COMMAND ----------

val variantFrequencies = varToData.reduceByKey((x,y) => x + y)

// COMMAND ----------

//this is a filter of variant ids to keep
val freqFilter = variantFrequencies.filter { case (k, it) => it > 30.0 }.keys

// COMMAND ----------

freqFilter.count()

// COMMAND ----------

val frequencies = freqFilter.collect()

// COMMAND ----------

// now apply the filter
val finalGts = completeGts.filter { g =>  (frequencies contains variantId(g).hashCode) }

// COMMAND ----------

// MAGIC %md
// MAGIC The genotypes for the 805 variants we have left in our data will be our features. Let's create a features vector and DataFrame to run k-means clustering. 

// COMMAND ----------

// MAGIC %md ## 9) Prepare data for K-means clustering

// COMMAND ----------

import org.apache.spark.mllib.linalg.{Vector=>MLVector, Vectors}

val sampleToData: RDD[(String, (Int, Double))] = finalGts.map { g => (g.getSampleId.toString, ((variantId(g).hashCode), altAsDouble(g))) }

// group our data by sample
val groupedSampleToData = sampleToData.groupByKey


// make an MLVector for each sample, which contains the variants in the exact same order
def makeSortedVector(g: Iterable[(Int,Double)]): MLVector = Vectors.dense( g.toArray.sortBy(_._1).map(_._2) )

val dataPerSampleId:RDD[(String, MLVector)] =
    groupedSampleToData.mapValues { it =>
        makeSortedVector(it)
    }

// COMMAND ----------

// Pull out the features vector to run the model on
val features:RDD[MLVector] = dataPerSampleId.values

// COMMAND ----------

// MAGIC %md ## 10) Run K-means clustering to build model

// COMMAND ----------

import org.apache.spark.mllib.clustering.{KMeans,KMeansModel}

// Cluster the data into three classes using KMeans
val numClusters = 3
val numIterations = 20
val clusters:KMeansModel = KMeans.train(features, numClusters, numIterations)

// Evaluate clustering by computing Within Set Sum of Squared Errors
val WSSSE = clusters.computeCost(features)
println(s"Compute Cost: ${WSSSE}")

// COMMAND ----------

// MAGIC %md ## 11) Predict populations, compute the confusion matrix.

// COMMAND ----------

// Create predictionRDD that utilizes clusters.predict method to output the model's predictions
val predictionRDD: RDD[(String, Int)] = dataPerSampleId.map(sd => {
    (sd._1, clusters.predict(sd._2)) 
})

// Convert to DataFrame to more easily query the data
val predictDF = predictionRDD.toDF("sample","prediction")

// COMMAND ----------

// Join back to the filterPanel to get the original label
val resultsDF =  filterPanel.join(predictDF, "sample")
display(resultsDF)

// COMMAND ----------

resultsDF.registerTempTable("results_table")

// COMMAND ----------

// MAGIC %r
// MAGIC resultsRDF <- sql(sqlContext, "SELECT pop, prediction FROM results_table")
// MAGIC confusion_matrix <- crosstab(resultsRDF, "prediction", "pop")
// MAGIC head(confusion_matrix)

// COMMAND ----------

// MAGIC %md
// MAGIC Using majority rule from our model, let's assign coded cluster numbers to our populations

// COMMAND ----------

val popCodeDF = sqlContext.sql("""SELECT pop, max(case when seqnum = 1 then prediction end) as popcode
                        FROM (select results_table.*, ROW_NUMBER() over (partition by pop order by cnt desc) as seqnum
                        FROM (select results_table.*,  count(*) over (partition by pop,prediction) as cnt
                        FROM results_table ) results_table ) results_table group by pop""")

val resultsWithCodeDF = resultsDF.join(popCodeDF, "pop")
resultsWithCodeDF.registerTempTable("final_results_table")
display(resultsWithCodeDF)

// COMMAND ----------

// MAGIC %md ## 12)  Visualize the clusters with a force graph on lightning-viz. 

// COMMAND ----------

// MAGIC %md Create the input data vectors for the lightning-viz:
// MAGIC 
// MAGIC 1. list of the group memberships (the populations)
// MAGIC 2. list of the people (the sample IDs)
// MAGIC 3. Nested list of the graph links, with each person linked to their predicted cluster. 

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC #prepare our data into a suitable format for our viz
// MAGIC 
// MAGIC from pyspark.sql.functions import rowNumber
// MAGIC from pyspark.sql.window import Window
// MAGIC 
// MAGIC #ensure that our data arrays come out in the same order
// MAGIC 
// MAGIC df = sqlContext.sql("select sample, popcode, prediction from final_results_table").coalesce(1)
// MAGIC w = Window().orderBy()
// MAGIC df = df.withColumn("rownumber", rowNumber().over(w))
// MAGIC 
// MAGIC pop = df.select("popcode", "rownumber").collect()
// MAGIC peeps = df.select("sample", "rownumber").collect()
// MAGIC preds= df.select("prediction", "rownumber").collect()
// MAGIC 
// MAGIC pop = [(str(x),str(y)) for (x,y) in pop]
// MAGIC peeps = [(str(x),str(y)) for (x,y) in peeps]
// MAGIC preds = [( x, str(y), 1) for (x,y) in preds]
// MAGIC 
// MAGIC 
// MAGIC def getKey(item):
// MAGIC   return item[1]
// MAGIC 
// MAGIC g = sorted(pop, key=getKey)
// MAGIC l = sorted(peeps, key=getKey)
// MAGIC pr = sorted(preds, key=getKey)
// MAGIC 
// MAGIC # add 3 points for our force graph centers
// MAGIC 
// MAGIC groups = ["0","1","2"] + [x[0] for x in g]
// MAGIC labels = ["0","1","2"] + [x[0] for x in l]
// MAGIC predictions = [[x[0],x[2]] for x in pr]
// MAGIC 
// MAGIC listIndices= list(range(3,len(predictions) + 3))
// MAGIC i = 0
// MAGIC for sublist in predictions:
// MAGIC   sublist.insert(0,listIndices[i])
// MAGIC   i += 1

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC #create the viz
// MAGIC 
// MAGIC from lightning import Lightning
// MAGIC 
// MAGIC lgn = Lightning(host='http://public.lightning-viz.org')
// MAGIC lgn.create_session("new")
// MAGIC viz = lgn.force(predictions, group=groups, labels=labels)
// MAGIC viz.get_public_link()

// COMMAND ----------

// MAGIC %md #### Your visualization is now being served at the above public link to lightning-viz server!
// MAGIC This is a simple viz to show predicted cluster membership (vertices) vs population (colors). 
// MAGIC 
// MAGIC [![](http://cdn2.hubspot.net/hubfs/438089/notebooks/media/lightning-viz.gif?t=1463413708876)](http://public.lightning-viz.org/visualizations/0d3defe5-db48-4d41-9b89-5c6cbd562212/public/)
// MAGIC 
// MAGIC Right Click the image and open the image within the lightning-viz server.  Try moving around the viz, zooming, and clicking on individuals to see their labels.