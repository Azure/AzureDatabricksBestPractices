# Databricks notebook source
sc = sparkR.callJMethod(spark, "sparkContext")
c = sparkR.newJObject("org.bdgenomics.adam.rdd.ADAMContext", sc)
ac = sparkR.newJObject("org.bdgenomics.adam.apis.java.JavaADAMContext", c)
vcf = sparkR.callJMethod(ac, "loadGenotypes", "/fnothaft/vcf-merge/merged.vcf")

sparkR.callJMethod(vcf,
                   "saveAsVcf",
                   "/fnothaft/vcf-merge/merged_in_r_sorted.vcf",
                   TRUE,
                   sparkR.callJStatic("htsjdk.samtools.ValidationStringency",
                                      "valueOf",
                                      "LENIENT"))