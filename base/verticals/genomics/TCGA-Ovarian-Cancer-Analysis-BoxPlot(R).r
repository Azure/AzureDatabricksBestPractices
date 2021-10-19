# Databricks notebook source
# MAGIC %md
# MAGIC # TCGA Ovarian Cancer Clinical Data 
# MAGIC ## Sample Analysis in R notebook

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Ovarian Cancer is the fifth-leading cause of cancer death in women in the United States (2015 stats: 21,290 new cases, 14,180 deaths).  
# MAGIC #### The standard treatment for advanced-stage ovarian cancer is aggressive surgery and chemotherapy, but prognosis still remains poor. 
# MAGIC #### Importantly, however, while approximately 13% of ovarian cancers are associated with BRCA 1/2 mutations, the genomic contribution to the majority of ovarian cancer cases is not well understood. Thus, there is a large potential for characterizing molecular signatures that could aid in the diagnosis, prognosis and treatment of ovarian cancer.

# COMMAND ----------

# MAGIC %md
# MAGIC Load required R libraries

# COMMAND ----------

library("plyr")
install.packages("gplots")
library("gplots")

# COMMAND ----------

# MAGIC %md
# MAGIC Load the TCGA Clinical Data into the Databricks cluster R memory

# COMMAND ----------

datClinical <- read.csv("/dbfs//FileStore//tables//4j2ceagn1483424639084/TCGA_OvCa_Clinical.csv", na.strings=c(""," "))

# COMMAND ----------

# MAGIC %md
# MAGIC Load the TCGA Additional Data into Databricks cluster R memory

# COMMAND ----------

datAdditional <- read.csv("/dbfs//FileStore//tables//4j2ceagn1483424639084/TCGA_OvCa_Additional.csv", na.strings=c("", " " ))

# COMMAND ----------

sum(is.na(datClinical))

# COMMAND ----------

# MAGIC %md
# MAGIC View the column/field names of the loaded data

# COMMAND ----------

colnames(datClinical)

# COMMAND ----------

# MAGIC %md
# MAGIC Change the column names to a more user friendly format

# COMMAND ----------

colnames(datClinical) = c("PATIENT_ID", "AGE_AT_DX","VITAL_STATUS", "TUMOR_STAGE","TUMOR_GRADE","TUMOR_RESIDUAL_DISEASE","PRIMARY_RX_OUTCOME_SUCCESS","PERSON_NEOPLASM_CANCER_STATUS",
                  "OS_MONTHS","PFS_STATUS","PFS_MONTHS","PLATINUM_FREE_INTERVAL_MONTHS","PLATINUM_STATUS")

# COMMAND ----------

# MAGIC %md
# MAGIC Assign legit datatypes

# COMMAND ----------

datClinical$PLATINUM_STATUS = as.factor(as.character(datClinical$PLATINUM_STATUS))
datClinical$PFS_MONTHS = as.numeric(as.character(datClinical$PFS_MONTHS))
datClinical$OS_MONTHS = as.numeric(as.character(datClinical$OS_MONTHS))

# COMMAND ----------

# MAGIC %md
# MAGIC Select only the observations where platinum status is not missing

# COMMAND ----------

platinum = datClinical[datClinical$PLATINUM_STATUS != "Missing", ]
nrow(platinum)

# COMMAND ----------

# MAGIC %md
# MAGIC Just to show how to convert R DataFrame to a Spark DataFrame

# COMMAND ----------

platinum_SparkDF = createDataFrame(sqlContext, data = platinum)

# COMMAND ----------

# MAGIC %md
# MAGIC Visualise the data. Note that display function can work with either R DataFrame or a Spark DataFrame. But is always better to use display function with a Spark DataFrame so that the visualisation process does not convert categorical/nominal variables to numerical values

# COMMAND ----------

display(platinum)  #visualising R DataFrame using display function. Note that the categorical/nominal variables are not in typical "character" format, but rather in a converted numerical format.

# COMMAND ----------

display(platinum_SparkDF) #data looks nice and tidy

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Boxplot is used to visualise the distribution of a numerical variable across multiple categorical variables. In this example, we are interested to see the distribution of Progression free survival in months across the 3 different platinum status groups of patients. These groups are whether the patient is sensitive to platinum containing anticancer drugs, whether they are resistant to them or if it is 'too early'.

# COMMAND ----------

summary(platinum$PFS_MONTHS)

# COMMAND ----------

display(platinum_SparkDF)

# COMMAND ----------

# MAGIC %md
# MAGIC #### The boxplot above shows that the Progression Free Survival in months is greater for subjects who are sensitive to platinum containing anticancer drugs than those who are resistant to it.

# COMMAND ----------

boxplot(PFS_MONTHS~PLATINUM_STATUS, data = platinum, col ="maroon", horizontal=TRUE, na.omit= TRUE,
        main = "Progression Free Survival in Months vs Platinum Status")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 
# MAGIC Overall survival is also greater for subjects who are sensitive to platinum containing anticancer drugs.

# COMMAND ----------

boxplot(OS_MONTHS~PLATINUM_STATUS, data = platinum, col ="maroon", horizontal=TRUE, na.omit= TRUE,
        main = "Overall Survival in Months vs Platinum Status")

# COMMAND ----------

# MAGIC %md
# MAGIC Both boxplots together

# COMMAND ----------

par(mfrow=c(1,2))
platinum$PLATINUM_STATUS = as.factor(as.character(platinum$PLATINUM_STATUS))
boxplot(PFS_MONTHS~PLATINUM_STATUS, data = platinum, col ="maroon", horizontal=TRUE, na.omit= TRUE,
        main = "Progression Free Survival")
boxplot(OS_MONTHS~PLATINUM_STATUS, data = platinum, col ="maroon", horizontal=TRUE, na.omit= TRUE,
        main = "Overall Survival")

# COMMAND ----------

# MAGIC %md
# MAGIC Change the column names to a more user friendly format

# COMMAND ----------

colnames(datAdditional)

# COMMAND ----------

colnames(datAdditional) = c("PATIENT_ID", "SAMPLE_BARCODE", "CENTER", "YEAR_DX", "ETHNICITY", "JEWISH_ORIGIN", "OS", "OS_DAYS", "RECURRENCE_STATUS", "DISTANT_RECURRENCE_STATUS",
                            "SURGICAL_OUTCOME", "PHARMACEUTICAL_THERAPY_TYPE", "DRUG_NAME")

# COMMAND ----------

# MAGIC %md
# MAGIC Assign legit datatypes

# COMMAND ----------

datAdditional$OS_DAYS = as.numeric(as.character(datAdditional$OS_DAYS))
datAdditional$ETHNICITY = as.factor(as.character(datAdditional$ETHNICITY))

# COMMAND ----------

# MAGIC %md
# MAGIC Remove duplicate rows

# COMMAND ----------

datAdditional = datAdditional[!duplicated(datAdditional$PATIENT_ID),]

# COMMAND ----------

# MAGIC %md
# MAGIC Do an inner join of TCGA Clinical and Additional datasets

# COMMAND ----------

dat = plyr::join(datAdditional, datClinical, type="inner")

# COMMAND ----------

dim(dat)

# COMMAND ----------

boxplot(OS_DAYS~TUMOR_STAGE, data = dat, col ="maroon", na.omit= TRUE,
        main = "Overall Survival in Days vs Platinum Status")

# COMMAND ----------

# MAGIC %md
# MAGIC #### The boxplot above shows that as the Stage of Ovarian Cancer gets into more advanced stages, the overall survival generally decreases as the cancer advances.
# MAGIC #### Plot the means of Overall survival in days across the platinum status and tumor stage groups

# COMMAND ----------

gplots::plotmeans(dat$OS_DAYS~dat$TUMOR_STAGE, digits=2, ccol="red", mean.labels=T, 
                  main="Plot of overall survival means by tumor stage",
                  xlab = "Tumor Stages",
                  ylab = "Overall Survival in Days")

# COMMAND ----------

gplots::plotmeans(dat$OS_DAYS~dat$PLATINUM_STATUS, digits=2, ccol="red", mean.labels=T, 
                  main="Plot of overall survival means by platinum status",
                  xlab="Platinum Status",
                  ylab = "Overall Survival in Days")

# COMMAND ----------

fit <- aov(OS_MONTHS ~ PLATINUM_STATUS, data=platinum)

# COMMAND ----------

summary(fit)

# COMMAND ----------

# MAGIC %md
# MAGIC #### The ANOVA result above shows there is a statistically significant difference for overall survival depending on the the platinum status (sensitive, resistant or too early) of the subjects (p-value is much lesser than 0.05).
# MAGIC 
# MAGIC #### Tukey Honest Significance Difference test is used to determine statistically significant differences between individual groups based on platinum status.

# COMMAND ----------

postHoc_TukeyHSD = TukeyHSD(fit, ordered=TRUE, conf.level = 0.95)

# COMMAND ----------

postHoc_TukeyHSD

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Tukey HSD test shows that there is statistically significant difference in overall survival between resistant and too early (platinum status), sensitive and too early and sensitive and resistant.

# COMMAND ----------

plot(postHoc_TukeyHSD, col="blue")

# COMMAND ----------

fit2 = aov(OS_MONTHS~TUMOR_STAGE, data=dat)

# COMMAND ----------

summary(fit2)

# COMMAND ----------

postHoc_TukeyHSD2 = TukeyHSD(fit2, ordered=TRUE, conf.level = 0.95)

# COMMAND ----------

postHoc_TukeyHSD2

# COMMAND ----------

# MAGIC %md
# MAGIC #### Even though the box plot of overall survival across staging of ovarian cancer showed a decreasing trend of overall survival as cancer progresses, ANOVA and Tukey HSD shows that the difference is not statistically significant (p-value is more than 0.05)

# COMMAND ----------

plot(postHoc_TukeyHSD2, col="blue")