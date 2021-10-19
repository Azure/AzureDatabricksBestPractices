// Databricks notebook source
// MAGIC %md
// MAGIC 
// MAGIC Import Libraries
// MAGIC ===

// COMMAND ----------

import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression
import org.apache.commons.math3.distribution.TDistribution
import org.apache.spark.SparkContext._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.formats.avro._

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Set Paths and Input Arguments
// MAGIC ===

// COMMAND ----------

// the user directory to run in
val user = "fnothaft"

// input and output file paths
val probeList = s"/${user}/eqtl/list_chr22_probes"
val vcfFile = s"/${user}/eqtl-files/chr22vcf.vcf.gz"
val outputFile = s"/${user}/eqtl-files/eqtl_results"
val expression = s"/${user}/eqtl-files/RNASEQ60_array_rep_expr.txt"

// the number of probes from the file to take
// set to `None` to take all the probes
// set to `Some(number)` to take `number` probes
val optNumProbes = Some(100)

// the number of partitions to use
// set to `None` to skip repartitioning
// set to `Some(number)` to repartition to `number` partitions
val optNumPartitions = Some(16)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Import Expression Data
// MAGIC ===

// COMMAND ----------

dbutils.fs.head(expression)

// COMMAND ----------

// the expression data is stored in a tsv where each row is a quantified gene (transcript?)
// there are four metadata columns, followed by one column per sample
val expressionByTarget = sqlContext.read
  .format("com.databricks.spark.csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .option("delimiter", "\\t")
  .load(expression)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC #### Compare gene expression correlation between two samples

// COMMAND ----------

display(expressionByTarget)

// COMMAND ----------

display(expressionByTarget)

// COMMAND ----------

// we can extract the sample names from the schema
// specifically, we drop the first four columns, which are metadata columns
val samples = expressionByTarget.schema
  .drop(4)
  .map(_.name)
val numSamples = samples.size

// loop over the expression/target file and extract per-sample data
val expressionByTargetBySample = expressionByTarget.flatMap(target => {
  val targetName = target.getString(0)
  
  (0 until numSamples).map(sIdx => {
    ("%s-%s".format(samples(sIdx), targetName), target.getDouble(sIdx + 4))
  })
})

// the targets we are looking at are the first column of the target dataset
val targets = expressionByTarget.select("TargetID")

// COMMAND ----------

// MAGIC %md
// MAGIC #### Look at gene expression across samples in a single gene

// COMMAND ----------

import org.apache.spark.sql.functions._
val expressionByTarget = expressionByTargetBySample.withColumnRenamed("_1", "gene")
  .withColumnRenamed("_2", "expression")
  .select(split($"gene", "-").getItem(1).as("target"), $"expression")

// COMMAND ----------

display(expressionByTarget.groupBy("target").agg(min("expression"), max("expression"))
  .select($"target", ($"max(expression)" / $"min(expression)").as("fc"))
  .orderBy(-$"fc"))

// COMMAND ----------

display(expressionByTarget.where($"target" === "ILMN_45594_650411"))

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Load in genotypes
// MAGIC ===

// COMMAND ----------

val genotypes = sc.loadGenotypes(vcfFile)

// if the user requested repartitioning, repartition now
val maybeRepartitionedGenotypes = optNumPartitions.fold(genotypes.rdd)(numPartitions => {
  genotypes.rdd.repartition(numPartitions)
})

case class SimpleGenotype(contig: String,
  start: Long,
  ref: String,
  alt: String,
  a0: GenotypeAllele,
  a1: GenotypeAllele,
  sampleId: String) {
  
  def toSiteKey: String = {
    "%s_%d_%s_%s".format(contig, start, ref, alt)
  }
}

// we only need the genotype state (num of alt/ref alleles), so let's discard all annotations
val simplifiedData = maybeRepartitionedGenotypes.filter(!_.getSplitFromMultiAllelic)
  .map(gt => {
    SimpleGenotype(
      gt.getVariant.getContigName,
      gt.getVariant.getStart,
      gt.getVariant.getReferenceAllele,
      gt.getVariant.getAlternateAllele,
      gt.getAlleles.get(0),
      gt.getAlleles.get(1),
      gt.getSampleId)
  })

// COMMAND ----------

case class GenotypeState(sampleId: String, altState: Int)

/**
 * @param gt A minimal genotype to turn from a genotype record into a genotype state.
 * @return Returns a genotype state count, as defined by the number of called alternate alleles.
 */
def countAltsPerSamplePerSite(gt: SimpleGenotype): (String, GenotypeState) = {
  val count = (gt.a0, gt.a1) match {
    case (GenotypeAllele.ALT, GenotypeAllele.ALT) => 2
    case (GenotypeAllele.ALT, _) | (_, GenotypeAllele.ALT) => 1
    case _ => 0
  }
  
  (gt.toSiteKey, GenotypeState(gt.sampleId, count))
}

// COMMAND ----------

case class RegressionResult(
  key: String,
  target: String,
  pValue: Double,
  r2: Double)
    
/**
 * Runs a linear regression between genotype state and targets.
 * 
 * Performs a cartesian join between the samples called at this site and all
 * of the targets that were quantified. Assumes that all samples were called
 * at all sites, and that all samples are quantified for all targets.
 *
 * @param kv A tuple describing the site we are processing ($chrom_$pos_$ref_$alt)
 *    and the genotype state called per each sample at this site.
 * @param bcastTargetExpression A broadcast value containing the expression of all
 *    genes for all samples.
 * @param bcastPhenotypes A broadcast value containing all quantified targets.
 * @return Returns an iterable collection containing a linear regression per target
 *    against the genotypes called at this site.
 */
def runVariantPhenoRegression(kv: (String, Iterable[GenotypeState]),
   bcastTargetExpression: Broadcast[Map[String, Double]],
   bcastPhenotypes: Broadcast[Iterable[String]]): Iterable[RegressionResult] = {
      val (key, samples) = kv
      val targetExpressionMap: Map[String, Double] = bcastTargetExpression.value
    
      // Helper function performing the linear regression using the apache commons
      // linear regression object SimpleRegression
      def setupRegressionPerTarget(target: String) = {
        val regression = new OLSMultipleLinearRegression()
        
        samples.foreach(gs => {
          val phenotypeKey = "%s-%s".format(gs.sampleId, target)
          regression.addData(gs.altState, targetExpressionMap(phenotypeKey) )
        })

        (key, target, regression)
      }
  
  bcastPhenotypes.value
        .map(setupRegressionPerTarget).map(t => {
          val (key, target, regressionModel) = t
          
          // compute the regression parameters standard errors
          val standardErrors = ols.estimateRegressionParametersStandardErrors()

          // get standard error for genotype parameter (for p value calculation)
          val genoSE = standardErrors(1)

          // test statistic t for jth parameter is equal to bj/SEbj, the parameter estimate divided by its standard error
          val t = beta(1) / genoSE

          /* calculate p-value and report: 
             Under null hypothesis (i.e. the j'th element of weight vector is 0) the relevant distribution is 
             a t-distribution with N-p-1 degrees of freedom.
             */
          val tDist = new TDistribution(numObservations - observationLength - 1)
          val pValue = 1.0 - tDist.cumulativeProbability(t)
          
          RegressionResult(key,
            target,
            pValue,
            regressionModel.getRSquare())
        }).filter(_.pValue < 0.001)
  }

// COMMAND ----------

// collect and broadcast the expression dataset
val bcastTargetExpression: Broadcast[Map[String, Double]] = sc.broadcast(expressionByTargetBySample.collect
  .toMap)

// collect the number of targets we have requested
val collectedTargets = optNumProbes.fold(targets.collect)(numProbes => targets.take(numProbes))

// broadcast these targets out
val bcastTargets: Broadcast[Iterable[String]] = sc.broadcast(collectedTargets.map(_.getString(0)).toIterable)

// COMMAND ----------

// transform the genotypes into genotype sites
val genotypeStates = simplifiedData.map(countAltsPerSamplePerSite)

// group together all the called genotype states for a single variants
val genotypeStatesBySite = genotypeStates.groupByKey()

// run the linear regression against all targets for all sites
val associations = genotypeStatesBySite.flatMap(site => {
  runVariantPhenoRegression(site,
    bcastTargetExpression,
    bcastTargets)
}).cache()

// plot our results
display(sqlContext.createDataset(associations))