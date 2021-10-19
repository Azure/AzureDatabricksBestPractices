# Databricks notebook source
# MAGIC %md
# MAGIC # Real-World Data (RWD) Analysis Demo
# MAGIC 
# MAGIC Using three healthcare RWD datasets, this notebook demonstrates the power and ease of Databricks cloud platform.  These datasets are released as Public Use File (PUF) by the Center for Medicare and Medicaid (CMS).   The focus of this demo is to illustrate, under one **unified notebook paradigm**, the simplicity and performance of <br>
# MAGIC 
# MAGIC **1. integrative computing environment** <br>
# MAGIC **2. interactive + collaborative + reproducible**<br>
# MAGIC **3. rich selection of visualization**<br>
# MAGIC **4. dashboard building**<br>
# MAGIC **5. workflow automation and job scheduling**<br>
# MAGIC **6. distributed advanced analytics that can't be done on a single machine**<br>
# MAGIC 
# MAGIC It is worth nothing that the context-dependent interpretation of the results and the choice of statistical modeling techniques is **out of scope** for this notebook demo. 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC **Create** a cluster by...  
# MAGIC   - Click the `Clusters` icon on the left sidebar and then `Create Cluster.` 
# MAGIC   - Enter any text, i.e `demo` into the cluster name text box
# MAGIC   - Select the `Apache Spark Version` value `Spark 2.2 (auto-updating scala 2.11)`  
# MAGIC   - Click the `create cluster` button and wait for your cluster to be provisioned
# MAGIC   
# MAGIC **Attach** this notebook to your cluster by...   
# MAGIC   - Click on your cluster name in menu `Detached` at the top left of this workbook to attach it to this workbook 
# MAGIC   - Add the spark-xml library to the cluster created above. The library is present in libs folder under current user.

# COMMAND ----------

# DBTITLE 1,Step1: Ingest Data to Notebook
# MAGIC %md
# MAGIC - Ingest data from dbfs sources 
# MAGIC - Create Sources as Tables
# MAGIC 
# MAGIC ## Datasets Details
# MAGIC 
# MAGIC 
# MAGIC **CMS 2015 Open Payment Dataset**        <br /> 
# MAGIC CMS is required by law to collect and share information reported by applicable manufacturers and group purchasing organizations (GPOs) about the payments and other transfers of value to physicians and teaching hospitals. The GPOs include manufacturers or distributors in the U.S. that engages in the production, preparation, propagation, compounding, or conversion of a covered drug, device, biological, or medical supply.   Open Payments information release provides transparency about the financial relationships between physicians and teaching hospitals and applicable manufacturers.
# MAGIC <br />
# MAGIC Ref: https://www.cms.gov/OpenPayments/About/How-Open-Payments-Works.html 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC **Hospital Spending by Claim Dataset**     <br />
# MAGIC Also known as **Medicare Spending per Beneficiary (MSPB) Spending Breakdowns by Claim Type** file. The data show average spending levels during hospitals MSPB episodes. An MSPB episode includes all Medicare Part A and Part B claims paid during the period from 3 days prior to a hospital admission through 30 days after discharge.  These payment amounts have been *price-standardized* to remove the effect of geographic payment differences and add-on payments for indirect medical education (IME) and disproportionate share hospitals (DSH). The data only includes pre-risk-adjusted values (without accounting for beneficiary age and severity of illness). <br />
# MAGIC Ref: https://www.medicare.gov/hospitalcompare/Data/spending-per-hospital-patient.html  
# MAGIC      https://catalog.data.gov/dataset/medicare-hospital-spending-by-claim-61b57
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC **Provider Utilization and Payment Dataset:** <br /> 
# MAGIC Medicare made payments to >3,000 hospitals for services and procedures that were provided to Medicare *fee-for service* beneficiaries.  These provider-specific charges and payments are categorized into 296 clinical conditions and procedures as defined by the Medicare Severity-Diagnosis Related Group (MS-DRG).   <br />
# MAGIC Ref: https://www.cms.gov/Research-Statistics-Data-and-Systems/Statistics-Trends-and-Reports/Medicare-Provider-Charge-Data/index.html

# COMMAND ----------

# MAGIC %md
# MAGIC Ingest data using Python (note that we are in R environment)

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC spark.read.parquet("/mnt/wesley/dataset/medicare/rwe/medicare_claims").createOrReplaceTempView("medicare_claims")
# MAGIC spark.read.parquet("/mnt/wesley/dataset/medicare/rwe/drg_payments").createOrReplaceTempView("drg_payments")
# MAGIC spark.read.parquet("/mnt/wesley/dataset/medicare/rwe/cms_p/2015").createOrReplaceTempView("cms_2015")
# MAGIC 
# MAGIC table("medicare_claims").printSchema()
# MAGIC table("drg_payments").printSchema()
# MAGIC table("cms_2015").printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Create SparkR data frames for all three datasets

# COMMAND ----------

library(SparkR)

# COMMAND ----------

cms_2015 = sql("select *, month(TO_DATE(CAST(UNIX_TIMESTAMP(Date_of_Payment, 'MM/dd/yyyy') AS TIMESTAMP))) mon from cms_2015")
medicare_claims = sql('select * from medicare_claims')
drg_payment = sql('select * from drg_payments')
registerTempTable(cms_2015,"cms_2015")
cat('Medicare/Medicaid Open Payment:', prettyNum(count(cms_2015),big.mark=',',scientific=FALSE), '\n')
cat('Medicare Hospital Spending:', prettyNum(count(medicare_claims),big.mark=',',scientific=FALSE), '\n')
cat('Provider Utilization and Payment:', prettyNum(count(drg_payment),big.mark=',',scientific=FALSE), '\n')

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ###Step2: Explore the data
# MAGIC 
# MAGIC - Review the Schema
# MAGIC - Profile the Data 
# MAGIC - Visualize the data

# COMMAND ----------

# MAGIC %md
# MAGIC A super easy and intuitive descriptive analysis on the data of medicare payment to hospitals

# COMMAND ----------

display(medicare_claims)

# COMMAND ----------

# MAGIC %md
# MAGIC Average hospital inpatient spending per episode across states

# COMMAND ----------

medicareClaimsInpatient = sql("  
    select 
      state, claim_type, period
      , avg(avg_spending_per_episode_hospital) as avg_spending_per_episode_hospital 
    from  medicare_claims 
    where claim_type=='Inpatient'
    group by state, claim_type, period")
display(medicareClaimsInpatient)

# COMMAND ----------

# MAGIC %md
# MAGIC Interactive analysis - (at-scale with enabling widgets)

# COMMAND ----------

display(cms_2015)

# COMMAND ----------

dat = sql("select round(sum(Total_Amount_of_Payment_USDollars),2) as total_payment, Submitting_Applicable_Manufacturer_or_Applicable_GPO_Name, Name_of_Associated_Covered_Drug_or_Biological1, mon, Physician_License_State_code1 from cms_2015 group by Submitting_Applicable_Manufacturer_or_Applicable_GPO_Name, Name_of_Associated_Covered_Drug_or_Biological1, mon, Physician_License_State_code1 order by mon")
registerTempTable(dat,'dat')
display(dat)

# COMMAND ----------

display(filter(dat,dat$Submitting_Applicable_Manufacturer_or_Applicable_GPO_Name == 'Eli Lilly and Company'))

# COMMAND ----------

# MAGIC %md
# MAGIC Making it more interactive by enabling run-time query with widget

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from dat where Name_of_Associated_Covered_Drug_or_Biological1 like '%$Name_of_Associated_Covered_Drug_or_Biological%' AND Submitting_Applicable_Manufacturer_or_Applicable_GPO_Name like '%$Submitting_Applicable_Manufacturer_or_Applicable_GPO_Name%'  order by mon

# COMMAND ----------

# MAGIC %md
# MAGIC Distributed advanced analysis  <br />
# MAGIC We show the way to carry out simple statistical analysis that are difficult to do on a large-size data that could be distributed across storages.

# COMMAND ----------

drg_payment = sql("select * from drg_payments")
createOrReplaceTempView(drg_payment,'drg_payment')
display(drg_payment)

# COMMAND ----------

count(unique(drg_payment[drg_payment$provider_state=="CA","provider_name"]))

# COMMAND ----------

dat = sql("select provider_state, drg_definition, round(avg(average_medicare_payments),0) as avg_medicare_payments from drg_payments group by provider_state, drg_definition ")
display(dat)

# COMMAND ----------

# MAGIC %md
# MAGIC Hierarchical bi-clustering analysis
# MAGIC 
# MAGIC Now the large-size data has been aggregated and reduced to smaller summary statistics, let's bring them in and see how the states and clinical procedures are similar to or different from each other.

# COMMAND ----------

tmp = collect(dat, stringsAsFactors=TRUE)
dat_crosstab = reshape(tmp, idvar="provider_state",timevar="drg_definition", direction="wide");  rm(tmp)
rownames(dat_crosstab) = dat_crosstab[,"provider_state"]
dat_crosstab = dat_crosstab[,-1]
colnames(dat_crosstab) = substring(colnames(dat_crosstab),28,100)
display(dat_crosstab)

# COMMAND ----------

install.packages("gplots", repos='http://cran.r-project.org')

# COMMAND ----------

library(gplots)
ixSelectColumn = base::sample(1:ncol(dat_crosstab), size=10)
df <- (scale(t(scale(dat_crosstab[,ixSelectColumn]))))
options(repr.plot.width=1200)
heatmap.2(df, density.info="none", trace="none", margins=c(8,16), col=colorRampPalette(c("navy", "white", "firebrick3")),breaks=seq(from=-1,to=1,by=0.05), keysize=0.8,cexRow=0.8,cexCol=0.6, sepwidth=c(0.05,0.05),sepcolor="white",colsep=1:ncol(df),rowsep=1:nrow(df))

# COMMAND ----------

# MAGIC %md
# MAGIC ### From distributed large-size data, compute the quartiles of average medicare payments to providers

# COMMAND ----------

dat = sql("
      select provider_state
            , cast(average_covered_charges as double) as avg_covered_charges
            , average_medicare_payments*1 as avg_medicare_payments
      from drg_payments")
#cat('Total records:', prettyNum(count(dat),big.mark=',',scientific=FALSE), '\n')
createOrReplaceTempView(dat, "dat")

# COMMAND ----------

qrt = sql("select provider_state \
        , percentile_approx(avg_medicare_payments,0) as q0 \
        , percentile_approx(avg_medicare_payments,0.25) as q1 \
        , percentile_approx(avg_medicare_payments,0.5) as q2 \
        , percentile_approx(avg_medicare_payments,0.75) as q3 \
        , percentile_approx(avg_medicare_payments,1) as q4 \
        from dat group by provider_state")
display(qrt)
qrt = arrange(qrt, desc(qrt$q2))

# COMMAND ----------

# MAGIC %md
# MAGIC Now we have successfully compressed signals in the distributed data into simple statistics.  Let's bring in the aggregated data local to visualize.

# COMMAND ----------

qrt = collect(qrt, stringsAsFactors=TRUE)
str(qrt)

# COMMAND ----------

# MAGIC %md
# MAGIC Let's look at the distribution of median payments made to states by CMS

# COMMAND ----------

options(repr.plot.height=1024, repr.plot.width=1200, repr.plot.res=300)
options(scipen=5)
par(size=1.1)
hist(qrt$q2,col='dodgerblue2',xlab='',ylab='',las=1,main='Distribution of median \n medicare payment',font=2,prob=TRUE)
lines(density(qrt$q2, adjust=2,kernel='gaussian'),lty='dotted',col='red',lwd=2)

# COMMAND ----------

library(ggplot2)
options(repr.plot.height=3600)
gp = ggplot(qrt, aes(x=provider_state, ymin=q0, ymax=q4, lower=q1, middle=q2,upper=q3)) + ggtitle("Avg Medicare Payment")
gp = gp + geom_boxplot(stat='identity',size=1, color='dodgerblue2') + theme_bw() + coord_flip()
gp = gp+ theme(text = element_text(size=12,face='bold'), panel.border=element_rect(colour = "black",size=1), axis.ticks = element_line(size = 1,color='black'), axis.text.x = element_text(angle=360, hjust=0.5, size=12), axis.text.y = element_text(size=8), panel.grid.major=element_line(colour='black', linetype='solid'), panel.grid.minor=element_line(colour='black', linetype = "dotted")) 
gp