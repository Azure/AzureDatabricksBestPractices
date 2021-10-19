// Databricks notebook source
// MAGIC %md
// MAGIC ![VariantSpark](https://s3.us-east-2.amazonaws.com/csiro-graphics/variant-spark.png)  
// MAGIC * [**VariantSpark**](http://bioinformatics.csiro.au/variantspark) is a machine learning library for real-time genomic data analysis (for thousands of samples and millions of variants) and is...  
// MAGIC   * Built on top of Apache Spark and written in Scala
// MAGIC   * Authored by the team at [CSIRO Bioinformatics](http://bioinformatics.csiro.au/) in Australia
// MAGIC   * Uses a custom machine learning **random forest** implementation to find the most *important* variants attributing to a phenotype of interest   
// MAGIC * This demo...  
// MAGIC   * Includes a dataset with a subset of the samples and variants (in VCF format) from the 1000 Genomes Project  
// MAGIC   * Uses a synthetic phenotype called *HipsterIndex* (in CSV format) factoring various real phenotypes (monobrow, beard, etc.)

// COMMAND ----------

// MAGIC %md
// MAGIC ## HipsterIndex
// MAGIC The synthetic HipsterIndex was created using the following genotypes and formular:
// MAGIC 
// MAGIC | ID |SNP ID     | chromosome | position | phenotype | reference |
// MAGIC |---:|----------:|----:|-------:|-----:|----------:|------:|
// MAGIC | B6 |[rs2218065](https://www.ncbi.nlm.nih.gov/projects/SNP/snp_ref.cgi?rs=2218065) | chr2 | 223034082 | monobrow | [Adhikari K, et al. (2016) Nat Commun.](https://www.ncbi.nlm.nih.gov/pubmed/?term=26926045) |
// MAGIC | R1 |[rs1363387](https://www.ncbi.nlm.nih.gov/projects/SNP/snp_ref.cgi?rs=1363387) | chr5 | 126626044 | Retina horizontal cells (checks) | [Kay, JN et al. (2012) Nature](https://www.ncbi.nlm.nih.gov/pubmed/?term=22407321)
// MAGIC | B2 |[rs4864809](https://www.ncbi.nlm.nih.gov/projects/SNP/snp_ref.cgi?rs=4864809) | chr4 |  54511913 | beard | [Adhikari K, et al. (2016) Nat Commun.](https://www.ncbi.nlm.nih.gov/pubmed/?term=26926045)
// MAGIC | C2 |[rs4410790](https://www.ncbi.nlm.nih.gov/projects/SNP/snp_ref.cgi?rs=4410790)  |chr7 |17284577| coffee consumption        | [Cornelis MC et al. (2011) PLoS Genet.](https://www.ncbi.nlm.nih.gov/pubmed/?term=21490707) |
// MAGIC 
// MAGIC `HipsterIndex = ((2 + GT[B6]) * (1.5 + GT[R1])) + ((0.5 + GT[C2]) * (1 + GT[B2]))`
// MAGIC 
// MAGIC   GT stands for the genotype at this location with *homozygote* reference encoded as 0, *heterozygote* as 1 and *homozygote alternative* as 2. We then label individuals with a HipsterIndex score above 10 as hipsters, and the rest non-hipsters. By doing so, we created a binary annotation for the individuals in the 1000 Genome Project.
// MAGIC 
// MAGIC In the rest of this notebook, we will demonstrate the usage of VariantSpark to reverse-engineer the association of the selected SNPs to the phenotype of insterest (i.e. being a hipster).

// COMMAND ----------

// MAGIC %md
// MAGIC ###0. SETUP -- Databricks Spark cluster and VariantSpark:  
// MAGIC 
// MAGIC 1. **Import** the variant-spark libary into your Databricks instance by...  
// MAGIC   - Navigate to `Workspace > Users > Username` and select `Create > Library`    
// MAGIC   - Click on `maven coordinates` in the source list in the 'Create Library' page
// MAGIC   - Enter `au.csiro.aehrc.variant-spark:variant-spark_2.11:0.0.2-SNAPSHOT` in the coordinate text box
// MAGIC   - Click `advanced` and enter `https://oss.sonatype.org/content/repositories/snapshots` in repository text box
// MAGIC   - Click the `create library` button
// MAGIC   - Verify that the option `automatically attach to all clusters` is checked in the libraries page
// MAGIC 2. **Create** a cluster by...  
// MAGIC   - Click the `Clusters` icon on the left sidebar and then `Create Cluster.` 
// MAGIC   - Enter any text, i.e `demo` into the cluster name text box
// MAGIC   - Select the `Apache Spark Version` value `Spark 2.1 (auto-updating scala 2.11)`  
// MAGIC   - Click the `create cluster` button and wait for your cluster to be provisioned
// MAGIC 3. **Attach** this notebook to your cluster by...   
// MAGIC   - Click on your cluster name in menu `Detached` at the top left of this workbook to attach it to this workbook 

// COMMAND ----------

// MAGIC %md 
// MAGIC ###1. LOAD DATA
// MAGIC 
// MAGIC Use python to import the urllib library to load the demo datasets from the source AWS S3 bucket to the `DBFS`(Databricks file system) destination  

// COMMAND ----------

// MAGIC %python
// MAGIC import urllib
// MAGIC urllib.urlretrieve("https://s3-us-west-1.amazonaws.com/variant-spark-pub/datasets/hipsterIndex/hipster.vcf.bz2", "/tmp/hipster.vcf.bz2")
// MAGIC urllib.urlretrieve("https://s3-us-west-1.amazonaws.com/variant-spark-pub/datasets/hipsterIndex/hipster_labels.txt", "/tmp/hipster_labels.txt")
// MAGIC dbutils.fs.mv("file:/tmp/hipster.vcf.bz2", "dbfs:/vs-datasets/hipsterIndex/hipster.vcf.bz2")
// MAGIC dbutils.fs.mv("file:/tmp/hipster_labels.txt", "dbfs:/vs-datasets/hipsterIndex/hipster_labels.txt")
// MAGIC display(dbutils.fs.ls("dbfs:/vs-datasets/hipsterIndex"))

// COMMAND ----------

// MAGIC %md
// MAGIC ###2. LOAD VARIANTS using VariantSpark     
// MAGIC 
// MAGIC 1. Use Scala to import the VSContext and ImportanceAnalysis objects from the VariantSpark library  
// MAGIC 2. Create an instance of the VSContext object, passing in an instance of the Spark Context object to it  
// MAGIC 3. Call the featureSource method on the instance of the vsContext object and pass in the path the the demo feature file  
// MAGIC      to load the variants from the vcf file
// MAGIC 4. Display the first 10 sample names

// COMMAND ----------

import au.csiro.variantspark.api.VSContext
import au.csiro.variantspark.api.ImportanceAnalysis
implicit val vsContext = VSContext(spark)

val featureSource = vsContext.featureSource("/vs-datasets/hipsterIndex/hipster.vcf.bz2")
println("Names of loaded samples:")
println(featureSource.sampleNames.take(10))

// COMMAND ----------

// MAGIC %md
// MAGIC ###3. LOAD LABELS using VariantSpark   
// MAGIC 
// MAGIC 1. Use Scala to call the labelSource method on the instance of the vsContext object and pass in the path the the demo label file  
// MAGIC 2. Display the first 10 phenotype labels

// COMMAND ----------

val labelSource  = vsContext.labelSource("/vs-datasets/hipsterIndex/hipster_labels.txt", "label")
println("First few labels:")
println(labelSource.getLabels(featureSource.sampleNames).toList.take(10))

// COMMAND ----------

// MAGIC %md
// MAGIC ###4. CONFIGURE ANALYSIS using VariantSpark   
// MAGIC 
// MAGIC 1. Use Scala to create an instance of the ImportanceAnalysis object
// MAGIC 2. Pass the featureSoure (feature file), labelSource (label file), and number of trees to the instance

// COMMAND ----------

val importanceAnalysis = ImportanceAnalysis(featureSource, labelSource, nTrees = 1000)

// COMMAND ----------

// MAGIC %md
// MAGIC ###5. RUN ANALYSIS using VariantSpark   
// MAGIC 
// MAGIC Unlike other statistical approaches, random forests have the advantage of not needing the data to be extensively pre-processed, so the analysis can be triggered on the loaded data directly. The analysis will take around 4 minutes on a Databricks community (one node) cluster.  
// MAGIC 
// MAGIC 1. Use Scala to call the `variableImportance` method on the instance of the `ImportanceAnalysis` object to calcuate the variant importance attributing to the phenotype
// MAGIC 2. Cache the analysis results into a SparkSQL table  
// MAGIC 3. Display the tabular results    

// COMMAND ----------

val variableImportance = importanceAnalysis.variableImportance
variableImportance.cache().registerTempTable("importance")
display(variableImportance)

// COMMAND ----------

// MAGIC %md
// MAGIC ##Variant Importance Score
// MAGIC VariantSpark assigns an "Importance" score to each tested variant reflecting it's association to the phenotype of interest. The Importance score is calculated based on the [Gini Impurity](https://en.wikipedia.org/wiki/Decision_tree_learning#Gini_impurity) scores of the decision trees in the random forest built by VariantSpark. In the decision tree building process, at each tree node, VariantSpark uses variants from the VCF dataset to split the samples and calculate the Gini Impurity scores before and after the split. At a given node _N_, the decrease in the Gini Impurity score by splitting using a feature _p_ is defined as 
// MAGIC $$
// MAGIC \Delta I = I(N) - (I(L) + I(R)),
// MAGIC $$ 
// MAGIC where _I(N)_ is the Gini Impurity at node _N_, and _I(L)_ and _I(R)_ are the Gini Impurity scores of the left and right child node, respectively. Then, the Importance score of variant _p_ is defined as the mean of the decrease in Gini Impurity score by splitting using the variant _p_ across all nodes and trees in the random forest. A variant with __higher Importance score implies it is more strongly associated with the phenotype of interest__.

// COMMAND ----------

// MAGIC %md
// MAGIC ###6a. VISUALIZE ANALYSIS using SparkSQL
// MAGIC 1. Query the SparkSQL table to display the top 25 results in descending order 
// MAGIC 2. Plot the results into a bar chart using the visualization feature in Databricks
// MAGIC 
// MAGIC *Note: the Hipster-Index is constructed from 4 SNPs so we expect the importance to be limited to these SNPs and the ones on [linkage disequilibrium (LD)](https://en.wikipedia.org/wiki/Linkage_disequilibrium) with them.* 

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from importance order by importance desc limit 25

// COMMAND ----------

// MAGIC %md
// MAGIC ###6b. VISUALIZE ANALYSIS using Python  
// MAGIC 
// MAGIC 1. Query the SparkSQL table to display the top 25 results in descending order 
// MAGIC 2. Plot the results into a line chart using the visualization feature in the python libraries

// COMMAND ----------

// MAGIC %python
// MAGIC import matplotlib.pyplot as plt
// MAGIC importance = sqlContext.sql("select * from importance order by importance desc limit 25")
// MAGIC importanceDF = importance.toPandas()
// MAGIC ax = importanceDF.plot(x="variable", y="importance",lw=3,colormap='Reds_r',title='Importance in Descending Order', fontsize=9)
// MAGIC ax.set_xlabel("variable")
// MAGIC ax.set_ylabel("importance")
// MAGIC plt.xticks(rotation=12)
// MAGIC plt.grid(True)
// MAGIC plt.show()
// MAGIC display()

// COMMAND ----------

// MAGIC %md
// MAGIC ###7. LOAD ANALYSIS into an R session 
// MAGIC 
// MAGIC 1. Using the R collect method to load the SparkSQL table into a local R session  
// MAGIC 2. List the results using the head method in R  

// COMMAND ----------

// MAGIC %r
// MAGIC importance_df_full  = collect(sql(sqlContext,'SELECT * FROM importance ORDER BY importance DESC'))
// MAGIC head(importance_df_full)

// COMMAND ----------

// MAGIC %md
// MAGIC ###8. VISUALIZE ANALYSIS using R   
// MAGIC 
// MAGIC 1. Load the ggplot2 library   
// MAGIC 2. Use the R collect method to load the SparkSQL table into a local R session  
// MAGIC 3. Plot the results on a bar chart

// COMMAND ----------

// MAGIC %r
// MAGIC library(ggplot2)
// MAGIC importance_df  = collect(sql(sqlContext,'SELECT * FROM importance ORDER BY importance DESC limit 25'))
// MAGIC ggplot(importance_df, aes(x=variable, y=importance)) + geom_bar(stat='identity') + scale_x_discrete(limits=importance_df[order(importance_df$importance), "variable"]) + coord_flip()

// COMMAND ----------

// MAGIC %md
// MAGIC ##Results interpretation
// MAGIC The plot above shows that VariantSpark has recovered the correct genotypes of this multivariate phenotype with interacting features (multiplicative and additive effects). 
// MAGIC 
// MAGIC ![Hipster-Index](https://s3.us-east-2.amazonaws.com/csiro-graphics/HistperSignatureGraphic-01.png)
// MAGIC 
// MAGIC 1. __chr2_223034082__ (rs2218065) encoding for monobrow is the most important feature 
// MAGIC 2. a group of SNPs encoding for the MEGF10 gene (__chr5_126626044__), which is involved in Retina horizontal cell formation   
// MAGIC    as the second most important marker, explaining why hipsters prefer checked shirts
// MAGIC 3. __chr7_17284577__ (rs4410790) the marker for increased coffee consuption is ranked third  
// MAGIC 4. __chr4_54511913__ (rs4864809) the marker for beards is fourth
// MAGIC 
// MAGIC The last two are in swapped order compared to the formular of the HipsterIndex, however with 0.5 and 1 as weight they may be difficult to differentiate. 

// COMMAND ----------

// MAGIC %md
// MAGIC ### ALTERNATIVE ANALYSIS    
// MAGIC 
// MAGIC Compare the results from other tools, such as [Hail](https://hail.is).  
// MAGIC The Hail P-values for this example were computed in a [different notebook](https://docs.databricks.com/spark/latest/training/1000-genomes.html)  
// MAGIC *NOTE: To use Hail's logistic regression a different Spark version is required.* 
// MAGIC 
// MAGIC 1. Use an alternate tool (Hail) for analysis
// MAGIC 2. Load the pre-computed values using Python

// COMMAND ----------

// MAGIC %python
// MAGIC import urllib
// MAGIC urllib.urlretrieve("https://s3-us-west-1.amazonaws.com/variant-spark-pub/datasets/hipsterIndex/hail_pvals.csv", "/tmp/hail_pvals.csv")
// MAGIC dbutils.fs.cp("file:/tmp/hail_pvals.csv", "dbfs:/vs-datasets/hipsterIndex/hail_pvals.csv")
// MAGIC display(dbutils.fs.ls("dbfs:/vs-datasets/hipsterIndex/hail_pvals.csv"))

// COMMAND ----------

// MAGIC %md
// MAGIC ###1. REVIEW RESULTS FROM ALTERNATIVE ANALYSIS  
// MAGIC 
// MAGIC The list shows the result returned by Hail's logistic regression method, listing important variables in a **different** order,  
// MAGIC suggesting that complex interacting variables are better recoved using random forest.   
// MAGIC *NOTE: logistic regression results show __chr5_126626044__ came up first, instead of __chr2_223034082__.*

// COMMAND ----------

val hailDF = sqlContext.read.format("csv").option("header", "true").load("/vs-datasets/hipsterIndex/hail_pvals.csv")
display(hailDF)

// COMMAND ----------

// MAGIC %md
// MAGIC ###2. REVIEW AND PLOT COMPARATIVE ANALYSIS using R 
// MAGIC 
// MAGIC 1. Use R to setup the list of Hail p-values  
// MAGIC 2. Load the variant-spark results into a local R session  
// MAGIC 3. Prepare both sets of results for a plot  
// MAGIC 4. Plot both results using ggplot

// COMMAND ----------

// MAGIC %r
// MAGIC hail_pvals <- read.df(sqlContext, "/vs-datasets/hipsterIndex/hail_pvals.csv", source="csv", header="true", inferSchema="true")
// MAGIC hail_pvals <- as.data.frame(hail_pvals)
// MAGIC 
// MAGIC hail_df <- aggregate(hail_pvals$pvals, by=list(hail_pvals$snp), min)
// MAGIC rownames(hail_df) <- hail_df$Group.1

// COMMAND ----------

// MAGIC %r
// MAGIC importance_df_full  = collect(sql(sqlContext,'SELECT * FROM importance ORDER BY importance DESC'))
// MAGIC importance_df_agg <- aggregate(importance_df_full$importance, by=list(importance_df_full$variable), max)
// MAGIC rownames(importance_df_agg) <- as.vector(importance_df_agg$Group.1)
// MAGIC importance_df_agg$hail_pv <- hail_df[as.vector(importance_df_agg$Group.1), "x"]
// MAGIC 
// MAGIC importance_df_agg$color <- "b"
// MAGIC importance_df_agg[c("7_17284577", "4_54511913", "2_223034082", "5_126626044"), "color"] <- "a"
// MAGIC 
// MAGIC head(importance_df_agg)

// COMMAND ----------

// MAGIC %r
// MAGIC library(ggplot2)
// MAGIC ggplot(importance_df_agg, aes(-log10(hail_pv), sqrt(x), color=color)) + geom_point() + xlab("-log10(Hail P-value)") + ylab("sqrt(VariantSpark Importance)") + annotate("text", label=c("7_17284577", "4_54511913", "2_223034082", "5_126626044"), x=c(47,20,100,110), y=c(0.0082,0.0063,0.0124,0.0103)) + theme_bw() + theme(legend.position="none") 

// COMMAND ----------

// MAGIC %md
// MAGIC While HAIL identified the correct variables their order is not consistent with their weight in the formular. More generally, HAIL has identified a large number of variables as associated with the label that VariantSpark scores with a low Importance score. Utilizing VariantSpark random forests allows us to reduce the noise and extract the signal with the correct ordering. 

// COMMAND ----------

// MAGIC %md
// MAGIC ###Credit
// MAGIC 
// MAGIC Transformational Bioinformatics team has developed VariantSpark and put together this illustrative example. Thank you to Lynn Langit for input on the presentation of this notebook. 