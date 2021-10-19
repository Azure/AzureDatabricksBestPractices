// Databricks notebook source
import org.apache.commons.math3.stat.regression.SimpleRegression
import org.apache.spark.SparkContext._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.formats.avro._

// COMMAND ----------

// MAGIC %sh
// MAGIC cd /dbfs/fnothaft
// MAGIC rm -rf eqtl-files
// MAGIC mkdir eqtl-files
// MAGIC cd eqtl-files
// MAGIC wget https://github.com/jpdna/eQTL-analysis-using-ADAM-and-Spark/blob/master/data/chr22vcf.vcf.gz?raw=true
// MAGIC mv chr22vcf.vcf.gz?raw=true chr22vcf.vcf.gz
// MAGIC wget https://github.com/jpdna/eQTL-analysis-using-ADAM-and-Spark/blob/master/data/RNASEQ60_array_rep_expr.txt?raw=true
// MAGIC mv RNASEQ60_array_rep_expr.txt?raw=true RNASEQ60_array_rep_expr.txt
// MAGIC wget https://github.com/jpdna/eQTL-analysis-using-ADAM-and-Spark/raw/master/data/list_chr22_probes

// COMMAND ----------

val probeList = "/fnothaft/eqtl/list_chr22_probes"
val vcfFile = "/fnothaft/eqtl-files/chr22vcf.vcf.gz"
val outputFile = "/fnothaft/eqtl-files/eqtl_results"
val expression = "/fnothaft/eqtl-files/RNASEQ60_array_rep_expr.txt"

// COMMAND ----------

val expressionByTarget = sqlContext.read
  .format("com.databricks.spark.csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .option("delimiter", "\\t")
  .load(expression)

val samples = expressionByTarget.schema
  .drop(4)
  .map(_.name)
val numSamples = samples.size

case class Expression(sampleId: String,
  target: String,
  expression: Double)

val expressionByTargetBySample = expressionByTarget.flatMap(target => {
  val targetName = target.getString(0)
  
  (0 until numSamples).map(sIdx => {
    Expression(samples(sIdx), targetName, target.getDouble(sIdx + 4))
  })
})

// COMMAND ----------

val genotypes = sc.loadGenotypes(vcfFile).toDataset

val biallelicOnly = genotypes.filter(!genotypes("splitFromMultiAllelic"))

case class Locus(contigName: String,
  start: Long,
  ref: String,
  alt: String) {
  
  override def toString(): String = {
    "%s_%d_%s_%s".format(contigName, start, ref, alt)
  }
}

case class GenotypeState(sampleId: String,
  contig: String,
  start: Long,
  ref: String,
  alt: String,
  altState: Int) {
  
  def toLocus: Locus = {
    Locus(contig, start, ref, alt)
  }
}

val states = biallelicOnly.select(biallelicOnly("sampleId"),
  biallelicOnly("contigName"),
  biallelicOnly("start"),
  biallelicOnly("variant.referenceAllele"),
  biallelicOnly("variant.alternateAllele"),
  (biallelicOnly("alleles").getItem(0) === "ALT") + (biallelicOnly("alleles").getItem(1) === "ALT"))
.as[GenotypeState]

// COMMAND ----------

val expressionAndStates = states.joinWith(expressionByTargetBySample,
  states("sampleId") === expressionAndStates("sampleId"))

case class ExpressionState(state: Int,
  expression: Double)

val phenotypesBySites = expressionAndStates.map(pair => {
  val (state, targetExpression) = pair
  
  (state.toLocus, ExpressionState())
}).groupByKey()

// COMMAND ----------

case class RegressionResult(
  key: String,
  target: String,
  significance: Double,
  r2: Double)
    
def runVariantPhenoRegression(kv: (String, Iterable[GenotypeState]),
   bcastTargetExpression: Broadcast[Map[String, Double]],
   bcastPhenotypes: Broadcast[Iterable[String]]): Iterable[RegressionResult] = {
      val (key, samples) = kv

      // Helper function performing the linear regression using the apache commons
      // linear regression object SimpleRegression
      def setupRegressionPerTarget(target: String) = {
        var regression = new SimpleRegression()
        samples.foreach(gs => {
          val phenotypeKey = "%s-%s".format(gs.sampleId, target)
          regression.addData(gs.altState, bcastTargetExpression.value(phenotypeKey) )
        })

        (key, target, regression)
      }
      bcastPhenotypes.value
        .map(setupRegressionPerTarget).map(t => {
          val (key, target, regressionModel) = t
          RegressionResult(key,
            target,
            regressionModel.getSignificance(),
            regressionModel.getRSquare())
        }).filter(_.significance < 0.001)
  }

// COMMAND ----------

val bcastTargetExpression: Broadcast[Map[String, Double]] = sc.broadcast(expressionByTargetBySample.collect
  .toMap)
val bcastTargets: Broadcast[Iterable[String]] = sc.broadcast(targets.collect.map(_.getString(0)).toIterable)

// COMMAND ----------

val genotypeStatesBySite = simplifiedData.map(countAltsPerSamplePerSite)
  .groupByKey()

val associations = genotypeStatesBySite.flatMap(site => {
  runVariantPhenoRegression(site,
    bcastTargetExpression,
    bcastTargets)
})

display(sqlContext.createDataset(associations))