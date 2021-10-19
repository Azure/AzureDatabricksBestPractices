// Databricks notebook source
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.variant.GenotypeRDD
import org.bdgenomics.formats.avro.Genotype

// COMMAND ----------

val vcfInputPath = "/fnothaft/vcf-merge/HG000*.vcf"
val targetPath = "/fnothaft/vcf-merge/targets.bed"
val vcfOutputPath = "/fnothaft/vcf-merge/merged.vcf"

// COMMAND ----------

dbutils.fs.rm(vcfOutputPath)
dbutils.fs.rm(vcfOutputPath + "_head")
dbutils.fs.rm(vcfOutputPath + "_tail", recurse = true)

// COMMAND ----------

import htsjdk.samtools.ValidationStringency
import org.bdgenomics.adam.rdd.ADAMContext._
val genotypesToMerge = sc.loadGenotypes(vcfInputPath)
genotypesToMerge.toVariantContextRDD
  .sort()
  .saveAsVcf(vcfOutputPath, asSingleFile = true, stringency = ValidationStringency.LENIENT)

// COMMAND ----------

val genotypeDataset = genotypesToMerge.toDataset
genotypeDataset.explain()

// COMMAND ----------

import htsjdk.samtools.ValidationStringency
import org.bdgenomics.adam.rdd.ADAMContext._
val genotypesToMerge = sc.loadGenotypes(vcfInputPath)
val targetsOfInterest = sc.loadFeatures(targetPath)
val targetedGenotypeRdd = targetsOfInterest.shuffleRegionJoin(genotypesToMerge)
  .rdd
  .map(_._2)
val targetedGenotypeCalls = genotypesToMerge.copy(rdd = targetedGenotypeRdd)
targetedGenotypeCalls.toVariantContextRDD
.sort()
.saveAsVcf(vcfOutputPath, asSingleFile = true, stringency = ValidationStringency.LENIENT)

// COMMAND ----------

// MAGIC %sh
// MAGIC wget https://github.com/vcftools/vcftools/tarball/master
// MAGIC tar xzvf master
// MAGIC cd vcftools-vcftools-7d1b99a
// MAGIC ls
// MAGIC ./autogen.sh
// MAGIC ./configure
// MAGIC make
// MAGIC make install
// MAGIC cd ..
// MAGIC 
// MAGIC wget https://github.com/samtools/htslib/releases/download/1.3.2/htslib-1.3.2.tar.bz2
// MAGIC tar xjvf htslib-1.3.2.tar.bz2
// MAGIC cd htslib-1.3.2
// MAGIC make
// MAGIC make install

// COMMAND ----------

// MAGIC %sh -e
// MAGIC 
// MAGIC rm -rf /dbfs/fnothaft/vcf-tools
// MAGIC mkdir /dbfs/fnothaft/vcf-tools
// MAGIC 
// MAGIC cp /dbfs/fnothaft/vcf-merge/HG00096.vcf /dbfs/fnothaft/vcf-tools
// MAGIC bgzip /dbfs/fnothaft/vcf-tools/HG00096.vcf
// MAGIC tabix /dbfs/fnothaft/vcf-tools/HG00096.vcf.gz
// MAGIC 
// MAGIC cp /dbfs/fnothaft/vcf-merge/HG00097.vcf /dbfs/fnothaft/vcf-tools
// MAGIC bgzip /dbfs/fnothaft/vcf-tools/HG00097.vcf
// MAGIC tabix /dbfs/fnothaft/vcf-tools/HG00097.vcf.gz
// MAGIC 
// MAGIC cp /dbfs/fnothaft/vcf-merge/HG00099.vcf /dbfs/fnothaft/vcf-tools
// MAGIC bgzip /dbfs/fnothaft/vcf-tools/HG00099.vcf
// MAGIC tabix /dbfs/fnothaft/vcf-tools/HG00099.vcf.gz

// COMMAND ----------

// MAGIC %sh
// MAGIC vcf-merge /dbfs/fnothaft/vcf-tools/HG00096.vcf.gz /dbfs/fnothaft/vcf-tools/HG00097.vcf.gz /dbfs/fnothaft/vcf-tools/HG00099.vcf.gz | vcf-sort -c > /dbfs/fnothaft/vcf-tools/chr22.vcf

// COMMAND ----------

println("hello")

// COMMAND ----------

// MAGIC %py
// MAGIC print "hello world"

// COMMAND ----------

// MAGIC %sh
// MAGIC ls /dbfs/fnothaft/vcf-tools/
// MAGIC head /dbfs/fnothaft/vcf-tools/chr22.vcf

// COMMAND ----------

// MAGIC %sh
// MAGIC 
// MAGIC grep -v "^##" /dbfs/fnothaft/vcf-merge/merged.vcf | awk '{ print $1"\t"$2"\t"$4"\t"$5"\t"$10 }' > adam_HG00096
// MAGIC grep -v "^##" /dbfs/fnothaft/vcf-merge/merged.vcf | awk '{ print $1"\t"$2"\t"$4"\t"$5"\t"$11 }' > adam_HG00097
// MAGIC grep -v "^##" /dbfs/fnothaft/vcf-merge/merged.vcf | awk '{ print $1"\t"$2"\t"$4"\t"$5"\t"$12 }' > adam_HG00099
// MAGIC 
// MAGIC grep -v "^##" /dbfs/fnothaft/vcf-tools/chr22.vcf | awk '{ print $1"\t"$2"\t"$4"\t"$5"\t"$10 }' > vcftools_HG00096
// MAGIC grep -v "^##" /dbfs/fnothaft/vcf-tools/chr22.vcf | awk '{ print $1"\t"$2"\t"$4"\t"$5"\t"$11 }' > vcftools_HG00097
// MAGIC grep -v "^##" /dbfs/fnothaft/vcf-tools/chr22.vcf | awk '{ print $1"\t"$2"\t"$4"\t"$5"\t"$12 }' > vcftools_HG00099
// MAGIC 
// MAGIC echo "Diff-96"
// MAGIC diff adam_HG00096 vcftools_HG00096
// MAGIC 
// MAGIC echo "Diff-97"
// MAGIC diff adam_HG00097 vcftools_HG00097
// MAGIC 
// MAGIC echo "Diff-99"
// MAGIC diff adam_HG00099 vcftools_HG00099