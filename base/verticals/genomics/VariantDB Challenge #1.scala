// Databricks notebook source
val dataDir = "/data/variant_db/1"
val relatednessFile = "README.sample_cryptic_relations"
val populationsFile = "/databricks-datasets/samples/adam/panel/integrated_call_samples_v3.20130502.ALL.panel"
val vcfFile = "1KG.chr22.anno.infocol.vcf.gz"
val tmpPath = "/data/variant_db/1"

// COMMAND ----------

// MAGIC %sh
// MAGIC cd /dbfs/data/variant_db/1
// MAGIC zcat 1KG.chr22.anno.infocol.vcf.gz | head -n 1000

// COMMAND ----------

// MAGIC %sh
// MAGIC cd /dbfs/
// MAGIC mkdir -p data/variant_db/1
// MAGIC mkdir -p tmp/variant_db/1
// MAGIC cd data/variant_db/1
// MAGIC wget https://s3-us-west-2.amazonaws.com/mayo-bic-tools/variant_miner/vcfs/1KG.chr22.anno.infocol.vcf.gz
// MAGIC wget ftp://ftp-trace.ncbi.nih.gov/1000genomes/ftp/release/20110521/README.sample_cryptic_relations

// COMMAND ----------

case class Sample(sample: String, pop: String)

// COMMAND ----------

val panel = sqlContext.read
  .format("com.databricks.spark.csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .option("delimiter", "\\t")
  .load(populationsFile)
  .select("sample", "pop")
  .as[Sample]

// COMMAND ----------

display(panel)

// COMMAND ----------

val samples = sqlContext.read
  .format("com.databricks.spark.csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .option("delimiter", "\\t")
  .load("%s/%s".format(dataDir, relatednessFile))

val rowsToDrop = samples
  .where((samples("Population") =!= "ASW") || (samples("Relationship") =!= "Sibling"))

val toDrop = rowsToDrop.select("Population", "Sample 1", "Sample 2")
  .flatMap{ case Row(population: String, sample1: String, sample2: String) =>
    Iterable(Sample(population, sample1),
      Sample(population, sample2))
  }.as[Sample]

// COMMAND ----------

val finalSamples = panel.except(toDrop)

display(finalSamples)

// COMMAND ----------

import org.bdgenomics.adam.rdd.ADAMContext._

// COMMAND ----------

// MAGIC %sh
// MAGIC rm -rf /dbfs/tmp/variant_db/1/chr22.annotated.gt.adam

// COMMAND ----------

val vcfGenotypes = sc.loadGenotypes("%s/%s".format(dataDir, vcfFile))
vcfGenotypes.saveAsParquet("%s/chr22.annotated.gt.adam".format(tmpPath))

// COMMAND ----------

// load the rdd from disk
val genotypes = sc.loadParquetGenotypes("%s/chr22.annotated.gt.adam".format(tmpPath))

// COMMAND ----------

val sampleToPopulationMap = panel.collect
  .map(sample => (sample.sample, sample.pop))
  .toArray
  .toMap
val bcastSampleToPopulationMap = sc.broadcast(sampleToPopulationMap)

// COMMAND ----------

import org.bdgenomics.formats.avro.GenotypeAllele
import org.bdgenomics.adam.models.ReferenceRegion

val variantsByPopulation = genotypes.rdd
  .flatMap(gt => {
    bcastSampleToPopulationMap.value
      .get(gt.getSampleId)
      .map(population => (population, gt))
})
println(variantsByPopulation.first)

// COMMAND ----------

val hetGenotypes = variantsByPopulation.filter(kv => {
  val (_, gt) = kv
  (gt.getAlleles.contains(GenotypeAllele.REF) &&
   gt.getAlleles.contains(GenotypeAllele.ALT))
})

// COMMAND ----------

val goodDepthGenotypes = hetGenotypes.filter(kv => {
  val (_, gt) = kv
  (Option(gt.getReadDepth).map(i => i: Int).orElse({
 	 (Option(gt.getReferenceReadDepth), Option(gt.getAlternateReadDepth)) match {
 	    case (Some(refDepth), Some(altDepth)) => Some(refDepth + altDepth)
	    case _ => None
 	  }
 	}).fold(true)(_ > 30)) && // DP or sum(AD) is > 30, if provided
   Option(gt.getAlternateReadDepth).fold(false)(_ > 10) // alt depth > 10
})

// COMMAND ----------

val impactfulGenotypes = goodDepthGenotypes.filter(kv => {
  val (_, gt) = kv
  Option(gt.getVariant.getAnnotation.getAttributes.get("SAVANT_IMPACT")).exists(impact => {
 	impact == "HIGH" || impact == "MODERATE"
  })
})

// COMMAND ----------

val genotypesByExACAF = impactfulGenotypes.filter(kv => {
  val (_, gt) = kv
  Option(gt.getVariant.getAnnotation.getAttributes.get("ExAC.Info.AF")).fold(true)(afString => {
    try {
 	  afString.toFloat < 0.1
 	} catch {
 	  case t: Throwable => {
 	    true
 	}
  }
  })
}).cache
println(genotypesByExACAF.first)

val genotypesAsPopulationVariants = genotypesByExACAF.map(kv => {
  val (population, gt) = kv
  (ReferenceRegion(gt), population, gt.getVariant.getAlternateAllele)
}).distinct
  .map(t => {
    val (_, population, _) = t
}).countByValue

// COMMAND ----------

genotypesByExACAF.map(kv => {
  val (pop, gt) = kv
  (ReferenceRegion(gt), pop, gt.getVariant.getAlternateAllele)
}).distinct
.map(t => t._2).countByValue

// COMMAND ----------

val genotypeDf = sqlContext.read.parquet("%s/chr22.annotated.gt.adam".format(tmpPath))

val hetGenotypesDs = genotypeDf.where(
  (genotypeDf("alleles").getItem(0) === "REF" && genotypeDf("alleles").getItem(1) === "ALT") ||
  (genotypeDf("alleles").getItem(1) === "REF" && genotypeDf("alleles").getItem(0) === "ALT"))

val goodDepthGenotypesDs = hetGenotypesDs.where(
  (hetGenotypesDs("alternateReadDepth").isNotNull && hetGenotypesDs("alternateReadDepth") > 10) &&
  ((hetGenotypesDs("readDepth").isNotNull && hetGenotypesDs("readDepth") > 30) ||
   (hetGenotypesDs("referenceReadDepth").isNotNull && ((hetGenotypesDs("referenceReadDepth") +
                                                       hetGenotypesDs("alternateReadDepth")) > 30))))

val impactfulGenotypesDs = goodDepthGenotypesDs.where(
  (goodDepthGenotypesDs("variant.annotation.attributes").getItem("SAVANT_IMPACT") === "HIGH" ||
   goodDepthGenotypesDs("variant.annotation.attributes").getItem("SAVANT_IMPACT") === "MODERATE"))

import org.apache.spark.sql.functions.udf
def afExceeds(attrMap: Map[String, String]): Boolean = {
  attrMap.get("ExAC.Info.AF").fold(true)(af => {
    af.toFloat < 0.1
  })
}
val afUdf = udf[Boolean, Map[String, String]](afExceeds(_))
val genotypesPassingExACAFDs = impactfulGenotypesDs.withColumn("afExceeds",   afUdf(impactfulGenotypesDs("variant.annotation.attributes")))
val genotypesByExACAFDs = genotypesPassingExACAFDs.where(genotypesPassingExACAFDs("afExceeds"))

val genotypesJoinedToPopulationDs = genotypesByExACAFDs
  .join(finalSamples, finalSamples("sample") === genotypesByExACAFDs("sampleId"))

val genotypesByPopulationDs = genotypesJoinedToPopulationDs.select(
  genotypesJoinedToPopulationDs("pop"),
  genotypesJoinedToPopulationDs("variant.alternateAllele"),
  genotypesJoinedToPopulationDs("start"),
  genotypesJoinedToPopulationDs("contigName"))

val variantsByPopulation = genotypesByPopulationDs.dropDuplicates()
  .groupBy("pop").count

display(variantsByPopulation)

// COMMAND ----------

// MAGIC %py
// MAGIC 
// MAGIC panelDf = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", "\\t").load("/databricks-datasets/samples/adam/panel/integrated_call_samples_v3.20130502.ALL.panel").select("sample", "pop")
// MAGIC 
// MAGIC samplesDf = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", "\\t").load("/data/variant_db/1/README.sample_cryptic_relations").select("Population", "Sample 1", "Sample 2", "Relationship").withColumnRenamed("Sample 1", "sample1").withColumnRenamed("Sample 2", "sample2")
// MAGIC 
// MAGIC rowsToDropDf = samplesDf.filter((samplesDf.Population != "ASW") | (samplesDf.Relationship != "Sibling"))
// MAGIC 
// MAGIC from pyspark.sql import Row
// MAGIC 
// MAGIC def flattenRow(row):
// MAGIC   
// MAGIC   return [Row(sampleId=row.sample1, pop=row.Population),
// MAGIC    Row(sampleId=row.sample1, pop=row.Population)]
// MAGIC 
// MAGIC toDropDf = sqlContext.createDataFrame(rowsToDropDf.rdd.flatMap(flattenRow))
// MAGIC 
// MAGIC finalSamples = panelDf.withColumnRenamed("sample", "sampleId").subtract(toDropDf)
// MAGIC 
// MAGIC display(finalSamples)

// COMMAND ----------

dbutils.fs.ls("/data/variant_db/1")

// COMMAND ----------

// MAGIC %py
// MAGIC 
// MAGIC genotypes = spark.read.load("/data/variant_db/1/chr22.annotated.gt.adam", format="parquet")
// MAGIC 
// MAGIC hetGenotypes = genotypes.filter((genotypes.alleles.getItem(0) == "REF") & (genotypes.alleles.getItem(1) == "ALT") |
// MAGIC   ((genotypes.alleles.getItem(1) == "REF") & (genotypes.alleles.getItem(0) == "ALT")))
// MAGIC 
// MAGIC goodDepthGenotypes = hetGenotypes.filter(
// MAGIC   (hetGenotypes.alternateReadDepth.isNotNull() & (hetGenotypes.alternateReadDepth > 10)) &
// MAGIC   (hetGenotypes.readDepth.isNotNull() & (hetGenotypes.readDepth > 30)) |
// MAGIC    (hetGenotypes.referenceReadDepth.isNotNull() & ((hetGenotypes.referenceReadDepth +
// MAGIC                                                        hetGenotypes.alternateReadDepth) > 30)))
// MAGIC 
// MAGIC impactfulGenotypes = goodDepthGenotypes.filter(
// MAGIC   (goodDepthGenotypes.variant.annotation.attributes.getItem("SAVANT_IMPACT") == "HIGH") |
// MAGIC    (goodDepthGenotypes.variant.annotation.attributes.getItem("SAVANT_IMPACT") == "MODERATE"))
// MAGIC 
// MAGIC def afExceeds(attrMap):
// MAGIC   
// MAGIC   if ("ExAC.Info.AF" in attrMap):
// MAGIC     return float(attrMap["ExAC.Info.AF"]) < 0.1
// MAGIC   else:
// MAGIC     return False
// MAGIC 
// MAGIC from pyspark.sql.types import BooleanType
// MAGIC from pyspark.sql.functions import udf
// MAGIC afExceedsUdf = udf(afExceeds, BooleanType())
// MAGIC genotypesByExACAFDf = impactfulGenotypes.filter(afExceedsUdf(impactfulGenotypes.variant.annotation.attributes))
// MAGIC 
// MAGIC print genotypesByExACAFDf
// MAGIC print finalSamples
// MAGIC 
// MAGIC genotypesWithPopulations = genotypesByExACAFDf.join(finalSamples, genotypesByExACAFDf.sampleId == finalSamples.sampleId)
// MAGIC variantsByPopulation = genotypesWithPopulations.select(genotypesWithPopulations.pop,
// MAGIC     genotypesWithPopulations.variant.alternateAllele,
// MAGIC     genotypesWithPopulations.start,
// MAGIC     genotypesWithPopulations.contigName).dropDuplicates().groupBy("pop").count()
// MAGIC 
// MAGIC display(variantsByPopulation)