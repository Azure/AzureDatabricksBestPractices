# Databricks notebook source
# DBTITLE 1,Overview
# MAGIC %md
# MAGIC 
# MAGIC • ** Cohort Analysis of FDA food dataset** : A cohort is a group of users who share something in common, be it their sign-up date, first purchase month, birth date, acquisition channel, etc. Cohort analysis is the method by which these groups are tracked over time, helping you spot trends, understand repeat behaviors (purchases, engagement, amount spent, etc.), and monitor your customer and revenue retention.
# MAGIC 
# MAGIC Some of the Data points that can help in building the cohort analysis: 
# MAGIC 
# MAGIC * The report date of the incident for which we are building the cohort
# MAGIC 
# MAGIC * [**Cohort Analysis for food Dataset**](http://www.gregreda.com/2015/08/23/cohort-analysis-with-python/) is the use of data analytics to build cohort analysis and is...  
# MAGIC   * Built on top of Databricks Platform 
# MAGIC * This demo...    
# MAGIC   * demonstrates a cohort analysis between the incident filed for the cohort group as per the reported in incident date.  We use food administration dataset from the [Open FDA site](https://open.fda.gov/food/).

# COMMAND ----------

# DBTITLE 1,0. Setup Databricks Spark cluster:
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

# COMMAND ----------

# DBTITLE 1,Step1: Ingest FDA Food Data to Notebook
# MAGIC %md 
# MAGIC 
# MAGIC - We will download the food dataset hosted at  [** FDA **](http://www.casact.org/research/index.cfm?fa=loss_reserves_data)
# MAGIC - After downloading we will import this dataset into databricks notebook
# MAGIC - Run the "data_Setup" notebook which is in the same folder if your running the notebook for the very first time to populate the below dataset to be used in the notebook

# COMMAND ----------

import json

df = sc.wholeTextFiles('/mnt/azure/dataset/medicare/foodcohort/*.json').flatMap(lambda x: json.loads(x[1])).toDF()
df.registerTempTable("food")
display(df)

# COMMAND ----------

# MAGIC %md ### Step2: Enrich the data to get additional insigts to food dataset
# MAGIC - We count the number of data points and find the cohort group and cohort period.

# COMMAND ----------

rep_period = sqlContext.sql("select *, substr(report_date,0,6) as report_period from food")
rep_period.registerTempTable("cohort")

# COMMAND ----------

cohort_group = sqlContext.sql("select c.address_1,c.address_2,c.center_classification_date,c.city,c.classification,c.code_info,c.country,c.distribution_pattern,c.event_id,c.initial_firm_notification,c.postal_code,c.product_description,c.product_quantity,c.product_type,c.reason_for_recall,c.recall_initiation_date,c.recall_number,c.recalling_firm,c.report_date,c.state,c.status,c.voluntary_mandated,c.report_period,c.recalling_firm,cg.cohort_group from cohort c join (select recalling_firm,min(report_period) as cohort_group from cohort group by recalling_firm order by cohort_group) cg on c.recalling_firm=cg.recalling_firm")
cohort_group.registerTempTable("cohort_group")

# COMMAND ----------

total_recall_firm = sqlContext.sql("select cohort_group,report_period,count(distinct recalling_firm) as total_recall_firm from cohort_group group by cohort_group,report_period order by cohort_group,report_period")
total_recall_firm.registerTempTable("total_recall_firm")

# COMMAND ----------

CohortPeriod = sqlContext.sql("select cohort_group,report_period,total_recall_firm,rank() over (PARTITION by cohort_group order by report_period) as CohortPeriod  from total_recall_firm order by cohort_group,report_period")
CohortPeriod.registerTempTable("CohortPeriod")
CohortPeriod_pd = CohortPeriod.toPandas()

# COMMAND ----------

# MAGIC %md ###Step3: Explore FDA food Data 

# COMMAND ----------

# reindex the DataFrame
CohortPeriod_pd.reset_index(inplace=True)
CohortPeriod_pd.set_index(['cohort_group', 'CohortPeriod'], inplace=True)

# create a Series holding the total size of each CohortGroup
cohort_group_size = CohortPeriod_pd['total_recall_firm'].groupby(level=0).first()
cohort_group_size.head()

# COMMAND ----------

CohortPeriod_pd['total_recall_firm'].head()
CohortPeriod_pd['total_recall_firm'].unstack(0).head()

# COMMAND ----------

user_retention = CohortPeriod_pd['total_recall_firm'].unstack(0).divide(cohort_group_size, axis=1)
user_retention.head(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 4: Visualization
# MAGIC * Show the distribution of the cohort analysis.

# COMMAND ----------

import matplotlib.pyplot as plt
import numpy as np
user_retention[['201206', '201207', '201208']].plot(figsize=(10,5))
plt.title('Cohorts: User Retention')
plt.xticks(np.arange(1, 12.1, 1))
plt.xlim(1, 12)
plt.ylabel('% of Cohort Purchasing');
display()

# COMMAND ----------

# Creating heatmaps in matplotlib is more difficult than it should be.
# Thankfully, Seaborn makes them easy for us.
# http://stanford.edu/~mwaskom/software/seaborn/

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

# MAGIC %md
# MAGIC ##Results interpretation
# MAGIC Unsurprisingly, we can see from the above chart that fewer firms tend to recall as time goes on.
# MAGIC 
# MAGIC 
# MAGIC ![Cohort-Analysis](http://www.goldsteinepi.com/_/rsrc/1432061177402/blog/epivignettescohortstudy/group-of-people-cartoon.jpg)
# MAGIC 
# MAGIC However, we can also see that the 2015-01 cohort is the strongest, which enables us to ask targeted questions about this cohort compared to others -- what other attributes (besides first recall month) do these firms share which might be causing them to still recall high number of products? How were the majority of these products recalled by the firms? Was there a specific reason that caused the firm to recall such high number of products? Was there some sort of outbreak that caused such high number of recalls? The answers to these questions would inform future food administration efforts.