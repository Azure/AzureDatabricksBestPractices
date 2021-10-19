// Databricks notebook source
// MAGIC %md
// MAGIC ![Open Payments](https://demo.cloud.databricks.com/files/tables/x71l06d01479501154403/databrick_001-98b15.jpeg)

// COMMAND ----------

// MAGIC %md
// MAGIC The diagram below shows an example Spark cluster, basically there exists a Driver node that communicates with executor nodes. Each of these executor nodes have slots which are logically like execution cores. 
// MAGIC ![spark-architecture](http://training.databricks.com/databricks_guide/gentle_introduction/videoss_logo.png)
// MAGIC 
// MAGIC The Driver sends Tasks to the empty slots on the Executors when work has to be done:
// MAGIC 
// MAGIC ![spark-architecture](http://training.databricks.com/databricks_guide/gentle_introduction/spark_cluster_tasks.png)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Reading in structured data
// MAGIC 
// MAGIC Apache Spark's goal is to unify all different kinds of data and data sources.
// MAGIC 
// MAGIC ![img](http://training.databricks.com/databricks_guide/gentle_introduction/spark_goal.png)

// COMMAND ----------

// MAGIC %md
// MAGIC # **Analyze Open Payment Data**
// MAGIC Goal: Explore the Open Payments Data set for various health care manufacturing companies
// MAGIC #### https://www.cms.gov/OpenPayments/
// MAGIC ###### Open Payments is a federal program, required by the Affordable Care Act, that collects information about the payments drug and device companies make to physicians and teaching hospitals for things like travel, research, gifts, speaking fees, and meals. 
// MAGIC 
// MAGIC ![Open Payments](https://assets.cms.gov/resources/cms/images/logo/cms.gov-footer.png)

// COMMAND ----------

// MAGIC %md 
// MAGIC 
// MAGIC * If your running the notebook the very first time, you would need to run the "data_Setup" notebook which is kept in the same folder. This will populate the tables which are used below.

// COMMAND ----------

// MAGIC %sql select company from open_payments_companies

// COMMAND ----------

// MAGIC %sql describe open_payments_edges

// COMMAND ----------

// MAGIC %sql cache table open_payments_edges

// COMMAND ----------

// MAGIC %sql select count(*) from open_payments_edges;

// COMMAND ----------

// MAGIC %sql select * from open_payments_companies where company like '%Amgen%' order by company;

// COMMAND ----------

// MAGIC %sql select distinct(treatment_name) from open_payments_edges  LATERAL VIEW OUTER explode(treatments.name) treatment AS treatment_name where companyId = 100000000203 order by treatment_name;

// COMMAND ----------

// MAGIC %md ## How much did Alexion pay in various fees to promote Soliris?

// COMMAND ----------

// MAGIC %sql 
// MAGIC SELECT sum(payment) Total_Payment, nature FROM (
// MAGIC select  sum(amount) OVER (PARTITION BY recordId) payment, company, nature, treatment_name from  open_payments_companies  inner join
// MAGIC open_payments_edges 
// MAGIC LATERAL VIEW OUTER explode(treatments.name) treatment AS treatment_name
// MAGIC WHERE companyId = open_payments_companies.id and company like '%$Company%'
// MAGIC ) V
// MAGIC where treatment_name like '%$Treatment%'
// MAGIC group by nature

// COMMAND ----------

// MAGIC %md ## Top Payers

// COMMAND ----------

// MAGIC %sql 
// MAGIC SELECT sum(payment) Total_Payment, company, nature FROM (
// MAGIC select  sum(amount) OVER (PARTITION BY recordId) payment, company, treatment_name, nature from  open_payments_companies  inner join
// MAGIC open_payments_edges on companyId = open_payments_companies.id
// MAGIC   LATERAL VIEW OUTER explode(treatments.name) treatment AS treatment_name
// MAGIC ) V
// MAGIC group by company, nature order by Total_Payment desc LIMIT 30

// COMMAND ----------

// MAGIC %md ## Alexion's Top Physician Speakers

// COMMAND ----------

// MAGIC %sql 
// MAGIC select  round(sum(payment),2) as totalPaid, firstName, lastName FROM (
// MAGIC select sum(amount) OVER (PARTITION BY recordId) payment, firstName, lastName
// MAGIC from  open_payments_companies  inner join open_payments_edges
// MAGIC on companyId = open_payments_companies.id
// MAGIC inner join open_payments_physicians
// MAGIC on physicianId = open_payments_physicians.id
// MAGIC WHERE company like '%$Company%'
// MAGIC   ) v GROUP BY firstName, lastName order by totalPaid desc

// COMMAND ----------

// MAGIC %md ##Promoters of Soliris by Market

// COMMAND ----------

// MAGIC %sql select  state, round(sum(payment),2) as totalPaid FROM (
// MAGIC   select sum(amount) OVER (PARTITION BY recordId) as payment, opp.state, treatment_name
// MAGIC   from  open_payments_companies opc
// MAGIC     inner join open_payments_edges ope
// MAGIC     on companyId = opc.id
// MAGIC     inner join open_payments_physicians  opp
// MAGIC      on physicianId = opp.id
// MAGIC   LATERAL VIEW OUTER explode(treatments.name) treatment AS treatment_name
// MAGIC   WHERE company like '%$Company%'
// MAGIC ) v   WHERE state not like '%PR%' GROUP BY state

// COMMAND ----------

// MAGIC %sql select  state, round(sum(payment),2) as totalPaid FROM (
// MAGIC   select sum(amount) OVER (PARTITION BY recordId) as payment, opp.state, treatment_name
// MAGIC   from  open_payments_companies opc
// MAGIC     inner join open_payments_edges ope
// MAGIC     on companyId = opc.id
// MAGIC     inner join open_payments_physicians  opp
// MAGIC      on physicianId = opp.id
// MAGIC   LATERAL VIEW OUTER explode(treatments.name) treatment AS treatment_name
// MAGIC   WHERE company like '%$Company%'
// MAGIC ) v   WHERE state not like '%PR%' GROUP BY state