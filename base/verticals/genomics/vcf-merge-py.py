# Databricks notebook source
from bdgenomics.adam.adamContext import ADAMContext
ac = ADAMContext(sc)
genotypes = ac.loadGenotypes("/fnothaft/vcf-merge/HG000*.vcf")
df = genotypes.toDataFrame()

# COMMAND ----------

print df

# COMMAND ----------

print df.count()