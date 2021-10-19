# Databricks notebook source
# DBTITLE 0,Overview
# MAGIC %md # Opioid Addiction Analysis
# MAGIC <div style="width: 100%;display: inline-block;"></div>
# MAGIC <div style="width: 1100px;">
# MAGIC __Using [US Opiate Prescriptions](https://www.kaggle.com/apryor6/us-opiate-prescriptions/data) data from the Kaggle Repository:__
# MAGIC   <div style="margin: 0 auto; width: 600px; height: 75px;"> <!-- use `background: green;` for testing -->
# MAGIC     <ul>
# MAGIC       <li> Cohort Analysis -- Group the cohort based on the prescribed date of the opiod drug to uncover usage patterns.</li>
# MAGIC       <li> Machine Learning -- Predict whether individual prescribed opiate drugs 10+ times within a year using Spark's [Decision Tree Classifier](http://spark.apache.org/docs/latest/ml-classification-regression.html#decision-tree-classifier) algorithm.</li>
# MAGIC     </ul>
# MAGIC   </div>
# MAGIC </div>
# MAGIC <div style="width: 100%;display: inline-block;"></div>

# COMMAND ----------

# DBTITLE 0,Step1: Ingest Opioid Drug Data to Notebook
# MAGIC %md ## Setup: Ingest Opioid Drug Data
# MAGIC 
# MAGIC **The data consists of the following characteristics for each prescriber**
# MAGIC 
# MAGIC - `NPI` – unique National Provider Identifier number
# MAGIC - `Gender` - (M/F)
# MAGIC - `State` - U.S. State by abbreviation
# MAGIC - `Credentials` - set of initials indicative of medical degree
# MAGIC - `Specialty` - description of type of medicinal practice
# MAGIC - `[Drugs]` - A long list of drugs with numeric values indicating the total number of prescriptions written for the year by that individual
# MAGIC - `Opioid_Prescriber` - a boolean label indicating whether or not that individual prescribed opiate drugs more than 10 times in the year

# COMMAND ----------

# MAGIC %md #### Dataframe 'Opioids'
# MAGIC 
# MAGIC _Provides `Drug_Name` and `Generic_Name`._

# COMMAND ----------

# MAGIC %run ./data_Setup

# COMMAND ----------

opioids = (
  spark.read
    .option("delimiter", ",")
    .option("inferSchema", "true")  # Use tab delimiter (default is comma-separator)
    .option("header", "true")
    .csv("wasbs://dataset@wesdias.blob.core.windows.net/Azure/dataset/medicare/opioidanalysis/opioids.csv")
)

opioids.createOrReplaceTempView("opioids")
display(opioids)

# COMMAND ----------

# MAGIC %md #### Dataframe 'Overdoses'
# MAGIC 
# MAGIC _Provides `Deaths` per `State`; also gives `Population`._

# COMMAND ----------

overdoses = (
  spark.read
    .option("delimiter", ",")
    .option("inferSchema", "true")  # Use tab delimiter (default is comma-separator)
    .option("header", "true")
    .csv("dbfs:/mnt/wesley/dataset/medicare/opioidanalysis/overdoses.csv")
)

overdoses.createOrReplaceTempView("overdoses")
display(overdoses.limit(3))

# COMMAND ----------

# MAGIC %md _Visualize Opioid Deaths by State (Choropleth)_

# COMMAND ----------

display(overdoses)

# COMMAND ----------

# MAGIC %md _Vizualize Opioid Deaths as % Population (Line/Bar)._

# COMMAND ----------

display(
  overdoses
  .selectExpr(
    "State",
    "CAST (Deaths as long) as Deaths",
    "CAST (regexp_replace(Population,'(,)+','') as long) as Population"
  )
  .dropna(how='any', subset=["Deaths","Population"])
  .selectExpr(
    "State",
    "ROUND(((Deaths/Population) * 100),5) as Percentage"
  )
  .orderBy("Percentage", ascending=False)
  .limit(25)
)

# COMMAND ----------

# MAGIC %md #### Dataframe 'Prescriber_Info'
# MAGIC 
# MAGIC _Provides summary of all drugs prescribed by a a given prescriber, identified by their `NPI`._

# COMMAND ----------

prescriber_info = (
  spark.read.option("delimiter", ",")
  .option("inferSchema", "true")  # Use tab delimiter (default is comma-separator)
   .option("header", "true")
  .csv("dbfs:/mnt/wesley/dataset/medicare/opioidanalysis/prescriber_info1.csv")
)

prescriber_info.createOrReplaceTempView("prescriber_info")

df=prescriber_info.na.drop()
display(df.limit(3))

# COMMAND ----------

# MAGIC %md #### Dataframe 'Prescriber_Info_Detail'
# MAGIC 
# MAGIC _Provides per-prescription data, i.e. what/when a drug was provided by a `NPI`._ 
# MAGIC 
# MAGIC __Note: This data did not have headers, so we supplied them (also, showing how to supply a schema instead of inferring).__

# COMMAND ----------

from pyspark.sql.types import *

prescriber_headers = [
  "NPI", # IntegerType
  "Gender", # StringType
  "State", # StringType
  "Credentials", # StringType
  "Specialty", # StringType
  "Drug", # StringType
  "Prescribed_Date" # StringType
]

prescriber_schema =StructType()
for field in prescriber_headers:
  if field in ["NPI"]:
    prescriber_schema.add(field, IntegerType(), False)
  elif field in ["Prescribed_Date"]:
    prescriber_schema.add(field, TimestampType(), False)
  else:
    prescriber_schema.add(field, StringType(), False)

prescriber_info_detail = (
  spark.read
    .option("delimiter", ",")
    .option("header", "false")
    .schema(prescriber_schema) 
    .csv("dbfs:/mnt/wesley/dataset/medicare/opioidanalysis/prescriber_info2.csv")
)

prescriber_info_detail.createOrReplaceTempView("prescriber_info_detail")
display(prescriber_info_detail.limit(3))

# COMMAND ----------

# MAGIC %md ## Cohort Analysis
# MAGIC 
# MAGIC * We count the number of data points and find the cohort group and cohort period.
# MAGIC 
# MAGIC #### What is Cohort Analysis?
# MAGIC 
# MAGIC A cohort is a group of users who share something in common, be it their sign-up date, first purchase month, birth date, acquisition channel, etc. Cohort analysis is the method by which these groups are tracked over time, helping you spot trends, understand repeat behaviors (purchases, engagement, amount spent, etc.), and monitor your customer and revenue retention, more [here](http://www.gregreda.com/2015/08/23/cohort-analysis-with-python/).

# COMMAND ----------

# MAGIC %md _Create table 'cohort'. Note: we are extracing a subset of `Prescribed_Date` in the format `yyyy-mm` (as `Prescribed_Date_Abrv`) which forms the basis of our cohort grouping._

# COMMAND ----------

rep_period = sqlContext.sql("select *, substr(Prescribed_date,0,7) as Prescribed_Date_Abrv from prescriber_info_detail")
rep_period.createOrReplaceTempView("cohort")
display(rep_period.select("Prescribed_Date","Prescribed_Date_Abrv").limit(3))

# COMMAND ----------

# MAGIC %md _Create table 'cohort_group' by joining the min `Prescribed_Date_Abrv` for each prescriber NPI on table 'cohort'._  

# COMMAND ----------

cohort_group = sqlContext.sql("select c.NPI,c.Gender,c.State,c.Credentials,c.Specialty,c.Drug,c.Prescribed_Date_Abrv,cg.cohort_group from cohort c join (select NPI,min(Prescribed_Date_Abrv) as cohort_group from cohort group by NPI order by cohort_group) cg on c.NPI=cg.NPI")
cohort_group.createOrReplaceTempView("cohort_group")

display(cohort_group.limit(3))

# COMMAND ----------

# MAGIC %md _Create table 'total_NPI' by extracting total number of prescribers for a partical cohort group from table 'cohort_group'._

# COMMAND ----------

total_NPI = sqlContext.sql("select cohort_group,Prescribed_Date_Abrv,count(distinct NPI) as total_NPI from cohort_group group by cohort_group,Prescribed_Date_Abrv order by cohort_group,Prescribed_Date_Abrv")
total_NPI.createOrReplaceTempView("total_NPI")

display(total_NPI.limit(3))

# COMMAND ----------

# MAGIC %md _Create table 'cohort_period' Here we extract the cohort period (1-12) used in follow-on analysis._

# COMMAND ----------

cohort_period = sqlContext.sql("select cohort_group,Prescribed_Date_Abrv,total_NPI,rank() over (PARTITION by cohort_group order by Prescribed_date_abrv) as cohort_period  from total_NPI order by cohort_group,Prescribed_Date_Abrv")

cohort_period.createOrReplaceTempView("cohort_period")
display(cohort_period.limit(3))

# COMMAND ----------

# MAGIC %md _For the analysis, convert to a pandas Dataframe and adjust the index the cohort group and prescriber counts._

# COMMAND ----------

# drop into pandas Dataframe
cohort_period_pd = cohort_period.toPandas()

# reindex the Dataframe
cohort_period_pd.reset_index(inplace=True)
cohort_period_pd.set_index(['cohort_group', 'cohort_period'], inplace=True)

# create a Series holding the total size of each CohortGroup
cohort_group_size = cohort_period_pd['total_NPI'].groupby(level=0).first()
cohort_group_size.head()

# COMMAND ----------

# --- display the cohort group with respect to thte cohort period for each of the prescribers ---
# cohort_period_pd['total_NPI'].head()
# cohort_period_pd['total_NPI'].unstack(0).head()

# COMMAND ----------

# MAGIC %md _Normalize pandas Dataframe prescribers to a percentage for comparison to be used in further analysis._

# COMMAND ----------

user_retention = cohort_period_pd['total_NPI'].unstack(0).divide(cohort_group_size, axis=1)
user_retention.head(10)

# COMMAND ----------

# MAGIC %md _Line series plot for Cohort Period (months 1-12) vs % of Cohort Prescribing Opiates._
# MAGIC 
# MAGIC __Notice the consistency of group '2016-01' and '2016-06' at/near 100%.__

# COMMAND ----------

import matplotlib.pyplot as plt
import numpy as np
plt_series = ['2016-01','2016-02','2016-03','2016-04','2016-05','2016-06']
user_retention[plt_series].plot(figsize=(12,5))
plt.title('Cohorts: User Retention')
plt.xlabel('Cohort Period #')
plt.xticks(np.arange(1, 12.1, 1))
plt.xlim(1, 12)
plt.ylabel('% of Cohort Prescribing Opiates');
plt.ylim(0.4,1.01)
display()

# COMMAND ----------

# MAGIC %md _Matrix plot for Cohort Period vs Cohort Group._

# COMMAND ----------

import seaborn as sns
sns.set(style='white')

plt.figure(figsize=(24, 20))
plt.title('Cohorts: User Retention')
ax = sns.heatmap(user_retention.T, mask=user_retention.T.isnull(), annot=True, fmt='.0%');
ax.set_xlabel("Cohort Period")
ax.set_ylabel("Cohort Group")
plt.xticks(rotation=12)
plt.tight_layout()
plt.grid(True)
plt.show()
display()

# COMMAND ----------

# MAGIC %md #### Cohort Analysis Results Interpretation
# MAGIC 
# MAGIC We can see that the 2016-01 cohort is the strongest -- _i.e. remaining at 95%+ opioid prescription rate throughout the 12 periods of data_. We are now informed by the data to ask targeted questions about this cohort compared to others. The answers to the following questions would inform future drug administration efforts:
# MAGIC 
# MAGIC * What other attributes (besides first recall month) do these prescribers share which might be causing them to still prescribe a high number of opiod medications? 
# MAGIC * What were the highest number of opiod doses prescribed by these group? 
# MAGIC * Was there a specific reason to prescribe such high doses of opiod by this cohort group? 
# MAGIC 
# MAGIC Also it can be observed from the time the data is analyzed the number of cohort period decreases after each passing cohort group. We can infer that the administration policies are having some positive effects in reducing over-prescription of opiates.

# COMMAND ----------

# MAGIC %md ## Machine Learning: Decision Tree Classifier

# COMMAND ----------

# MAGIC %md #### String Indexer
# MAGIC 
# MAGIC _Before we create the model we convert all categorial values to numeric using `StringIndexer`._

# COMMAND ----------

from  pyspark.ml.feature import StringIndexer

indexer1 = (StringIndexer()
                   .setInputCol("Gender")
                   .setOutputCol("Gendert")
                   .fit(df))

indexed1 = indexer1.transform(prescriber_info)

indexer2 = (StringIndexer()
                   .setInputCol("State")
                   .setOutputCol("Statet")
                   .fit(indexed1))

indexed2 = indexer2.transform(indexed1)

indexer3 = (StringIndexer()
                   .setInputCol("Credentials")
                   .setOutputCol("Credentialst")
                   .fit(indexed2))

indexed3 = indexer3.transform(indexed2)

indexer4 = (StringIndexer()
                   .setInputCol("Opioid_Prescriber")
                   .setOutputCol("Opioid_Prescribert")
                   .fit(indexed2))

indexed4 = indexer4.transform(indexed3)

# COMMAND ----------

# MAGIC %md #### Vector Assembler
# MAGIC 
# MAGIC _We convert the columns to feature vectors to be consumed by ML pipelines further using `VectorAssembler`._

# COMMAND ----------

from pyspark.ml.feature import VectorAssembler

vecAssembler = VectorAssembler()
vecAssembler.setInputCols(["Gendert","ABILIFY","ACETAMINOPHEN_CODEINE","ACYCLOVIR","ADVAIR_DISKUS","AGGRENOX","ALENDRONATE_SODIUM","ALLOPURINOL","ALPRAZOLAM","AMIODARONE_HCL","AMITRIPTYLINE_HCL","AMLODIPINE_BESYLATE","AMLODIPINE_BESYLATE_BENAZEPRIL","AMOXICILLIN","AMOX_TR_POTASSIUM_CLAVULANATE","AMPHETAMINE_SALT_COMBO","ATENOLOL","ATORVASTATIN_CALCIUM","AVODART","AZITHROMYCIN","BACLOFEN","BD_ULTRA_FINE_PEN_NEEDLE","BENAZEPRIL_HCL","BENICAR","BENICAR_HCT","BENZTROPINE_MESYLATE","BISOPROLOL_HYDROCHLOROTHIAZIDE","BRIMONIDINE_TARTRATE","BUMETANIDE","BUPROPION_HCL_SR","BUPROPION_XL","BUSPIRONE_HCL","BYSTOLIC","CARBAMAZEPINE","CARBIDOPA_LEVODOPA","CARISOPRODOL","CARTIA_XT","CARVEDILOL","CEFUROXIME","CELEBREX","CEPHALEXIN","CHLORHEXIDINE_GLUCONATE","CHLORTHALIDONE","CILOSTAZOL","CIPROFLOXACIN_HCL","CITALOPRAM_HBR","CLINDAMYCIN_HCL","CLOBETASOL_PROPIONATE","CLONAZEPAM","CLONIDINE_HCL","CLOPIDOGREL","CLOTRIMAZOLE_BETAMETHASONE","COLCRYS","COMBIVENT_RESPIMAT","CRESTOR","CYCLOBENZAPRINE_HCL","DEXILANT","DIAZEPAM","DICLOFENAC_SODIUM","DICYCLOMINE_HCL","DIGOX","DIGOXIN","DILTIAZEM_24HR_CD","DILTIAZEM_24HR_ER","DILTIAZEM_ER","DILTIAZEM_HCL","DIOVAN","DIPHENOXYLATE_ATROPINE","DIVALPROEX_SODIUM","DIVALPROEX_SODIUM_ER","DONEPEZIL_HCL","DORZOLAMIDE_TIMOLOL","DOXAZOSIN_MESYLATE","DOXEPIN_HCL","DOXYCYCLINE_HYCLATE","DULOXETINE_HCL","ENALAPRIL_MALEATE","ESCITALOPRAM_OXALATE","ESTRADIOL","EXELON","FAMOTIDINE","FELODIPINE_ER","FENOFIBRATE","FENTANYL","FINASTERIDE","FLOVENT_HFA","FLUCONAZOLE","FLUOXETINE_HCL","FLUTICASONE_PROPIONATE","FUROSEMIDE","GABAPENTIN","GEMFIBROZIL","GLIMEPIRIDE","GLIPIZIDE","GLIPIZIDE_ER","GLIPIZIDE_XL","GLYBURIDE","HALOPERIDOL","HUMALOG","HYDRALAZINE_HCL","HYDROCHLOROTHIAZIDE","HYDROCODONE_ACETAMINOPHEN","HYDROCORTISONE","HYDROMORPHONE_HCL","HYDROXYZINE_HCL","IBANDRONATE_SODIUM","IBUPROFEN","INSULIN_SYRINGE","IPRATROPIUM_BROMIDE","IRBESARTAN","ISOSORBIDE_MONONITRATE_ER","JANTOVEN","JANUMET","JANUVIA","KETOCONAZOLE","KLOR_CON_10","KLOR_CON_M10","KLOR_CON_M20","LABETALOL_HCL","LACTULOSE","LAMOTRIGINE","LANSOPRAZOLE","LANTUS","LANTUS_SOLOSTAR","LATANOPROST","LEVEMIR","LEVEMIR_FLEXPEN","LEVETIRACETAM","LEVOFLOXACIN","LEVOTHYROXINE_SODIUM","LIDOCAINE","LISINOPRIL","LISINOPRIL_HYDROCHLOROTHIAZIDE","LITHIUM_CARBONATE","LORAZEPAM","LOSARTAN_HYDROCHLOROTHIAZIDE","LOSARTAN_POTASSIUM","LOVASTATIN","LOVAZA","LUMIGAN","LYRICA","MECLIZINE_HCL","MELOXICAM","METFORMIN_HCL","METFORMIN_HCL_ER","METHADONE_HCL","METHOCARBAMOL","METHOTREXATE","METHYLPREDNISOLONE","METOCLOPRAMIDE_HCL","METOLAZONE","METOPROLOL_SUCCINATE","METOPROLOL_TARTRATE","METRONIDAZOLE","MIRTAZAPINE","MONTELUKAST_SODIUM","MORPHINE_SULFATE","MORPHINE_SULFATE_ER","MUPIROCIN","NABUMETONE","NAMENDA","NAMENDA_XR","NAPROXEN","NASONEX","NEXIUM","NIACIN_ER","NIFEDICAL_XL","NIFEDIPINE_ER","NITROFURANTOIN_MONO_MACRO","NITROSTAT","NORTRIPTYLINE_HCL","NOVOLOG","NOVOLOG_FLEXPEN","NYSTATIN","OLANZAPINE","OMEPRAZOLE","ONDANSETRON_HCL","ONDANSETRON_ODT","ONGLYZA","OXCARBAZEPINE","OXYBUTYNIN_CHLORIDE","OXYBUTYNIN_CHLORIDE_ER","OXYCODONE_ACETAMINOPHEN","OXYCODONE_HCL","OXYCONTIN","PANTOPRAZOLE_SODIUM","PAROXETINE_HCL","PHENOBARBITAL","PHENYTOIN_SODIUM_EXTENDED","PIOGLITAZONE_HCL","POLYETHYLENE_GLYCOL_3350","POTASSIUM_CHLORIDE","PRADAXA","PRAMIPEXOLE_DIHYDROCHLORIDE","PRAVASTATIN_SODIUM","PREDNISONE","PREMARIN","PRIMIDONE","PROAIR_HFA","PROMETHAZINE_HCL","PROPRANOLOL_HCL","PROPRANOLOL_HCL_ER","QUETIAPINE_FUMARATE","QUINAPRIL_HCL","RALOXIFENE_HCL","RAMIPRIL","RANEXA","RANITIDINE_HCL","RESTASIS","RISPERIDONE","ROPINIROLE_HCL","SEROQUEL_XR","SERTRALINE_HCL","SIMVASTATIN","SOTALOL","SPIRIVA","SPIRONOLACTONE","SUCRALFATE","SULFAMETHOXAZOLE_TRIMETHOPRIM","SUMATRIPTAN_SUCCINATE","SYMBICORT","SYNTHROID","TAMSULOSIN_HCL","TEMAZEPAM","TERAZOSIN_HCL","TIMOLOL_MALEATE","TIZANIDINE_HCL","TOLTERODINE_TARTRATE_ER","TOPIRAMATE","TOPROL_XL","TORSEMIDE","TRAMADOL_HCL","TRAVATAN_Z","TRAZODONE_HCL","TRIAMCINOLONE_ACETONIDE","TRIAMTERENE_HYDROCHLOROTHIAZID","VALACYCLOVIR","VALSARTAN","VALSARTAN_HYDROCHLOROTHIAZIDE","VENLAFAXINE_HCL","VENLAFAXINE_HCL_ER","VENTOLIN_HFA","VERAPAMIL_ER","VESICARE","VOLTAREN","VYTORIN","WARFARIN_SODIUM","XARELTO","ZETIA","ZIPRASIDONE_HCL","ZOLPIDEM_TARTRATE"])
vecAssembler.setOutputCol("features")

# COMMAND ----------

print vecAssembler.explainParams()

# COMMAND ----------

# MAGIC %md #### Decision Tree Classifier
# MAGIC 
# MAGIC _For our ML model we use `DecisionTreeClassifier` (output we are trying to predict is of type boolean)._

# COMMAND ----------

from pyspark.ml.classification import DecisionTreeClassifier

aft = DecisionTreeClassifier()
aft.setLabelCol("Opioid_Prescribert")

print aft.explainParams()

# COMMAND ----------

# MAGIC %md #### Pipeline
# MAGIC 
# MAGIC _ML Pipeline created using stages `VectorAssembler` and `DecisionTreeClassifier`._

# COMMAND ----------

from pyspark.ml import Pipeline

# We will use the new spark.ml pipeline API. If you have worked with scikit-learn this will be very familiar.
lrPipeline = Pipeline()

# Now we'll tell the pipeline to first create the feature vector, and then do the linear regression
lrPipeline.setStages([vecAssembler, aft])

# COMMAND ----------

# MAGIC %md #### Estimate 
# MAGIC _Fit the Pipeline Model using output from the `StringIndexer` manipulation accomplished previously._

# COMMAND ----------

# Pipelines are themselves Estimators -- so to use them we call fit:
lrPipelineModel = lrPipeline.fit(indexed4)

# COMMAND ----------

# MAGIC %md #### Predict
# MAGIC 
# MAGIC _Transform the Pipeline Model using `StringIndexer` manipulation._

# COMMAND ----------

# DBTITLE 0,Using Model for data prediction
predictionsAndLabelsDF = lrPipelineModel.transform(indexed4)
predAnalysis = predictionsAndLabelsDF.select('Opioid_Prescribert', 'prediction')
confusionMatrix = predictionsAndLabelsDF.select('Opioid_Prescribert', 'prediction')

display(predAnalysis.limit(5))

# COMMAND ----------

# MAGIC %md ####Model Performance

# COMMAND ----------

# MAGIC %md _Performance metrics of the model (16% FP rate and 92% accuracy)._ 
# MAGIC 
# MAGIC __Note: could also use Random Forest or Gradiant Boosting to attempt to improve accuracy.__

# COMMAND ----------

# DBTITLE 0,Performance Metrics of the model
from pyspark.mllib.evaluation import MulticlassMetrics
metrics = MulticlassMetrics(confusionMatrix.rdd)

print "FP Rate? {0}".format(metrics.falsePositiveRate(0.0))
print "Accuracy? {0}".format(metrics.accuracy)

# COMMAND ----------

# MAGIC %md #### Confusion Matrix

# COMMAND ----------

cm = metrics.confusionMatrix().toArray()

# COMMAND ----------

# MAGIC %md 
# MAGIC * True Positives (TP): Model accurately predicted prescriber will hit 10+ threshold.
# MAGIC * True Negatives (TN): Model accurately predicted prescriber will not hit 10+ threshold..
# MAGIC * False Positives (FP): Model predicted yes, but they don't actually hit the 10+ threshold. (Also known as a "Type I error.")
# MAGIC * False Negatives (FN): Model predicted no, but they actually do hit the 10+ threshold. (Also known as a "Type II error.")
# MAGIC 
# MAGIC ![](https://rasbt.github.io/mlxtend/user_guide/evaluate/confusion_matrix_files/confusion_matrix_1.png)

# COMMAND ----------

# MAGIC %md _Plot using matplotlib._

# COMMAND ----------

# DBTITLE 0,Confusion matrix in matplotlib
import matplotlib.pyplot as plt
import numpy as np
import itertools
plt.figure(figsize=(3.3,3))
classes=list([0,1])
plt.imshow(cm, interpolation='nearest', cmap=plt.cm.Blues)
plt.title('Confusion matrix')
plt.colorbar()
tick_marks = np.arange(len(classes))
plt.xticks(tick_marks, classes, rotation=0)
plt.yticks(tick_marks, classes)


thresh = cm.max() / 2.
for i, j in itertools.product(range(cm.shape[0]), range(cm.shape[1])):
    plt.text(j, i, format(cm[i, j]),
             horizontalalignment="center",
             verticalalignment="center",
             color="white" if cm[i, j] > thresh else "black")
plt.ylabel('True label')
plt.xlabel('Predicted label')
plt.show()
display()

# COMMAND ----------

# MAGIC %md #### Machine Learning Results Interpretation
# MAGIC 
# MAGIC We have demonstrated the possibility to predict whether or not an individual prescribed opiate drugs more than 10 times in a year based on decision tree classifier on Databricks Notebook. This would help to better monitor the identifed prescribers and to potentially know how to target attention where over-prescriptions of opioids might have occurred.