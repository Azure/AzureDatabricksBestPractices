# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Production Issues
# MAGIC 
# MAGIC Deploying machine learning models is complex.  While some devops best practices from traditional software development apply, there are a number of additional concerns.  This lesson explores the various production issues seen in deploying and monitoring machine learning models.
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lesson you:<br>
# MAGIC  - Introduce the primary concerns of production environments: reliability, scalability, and maintainability
# MAGIC  - Explore how model deployment differs from conventional software development deployment
# MAGIC  - Compare and contrast deployment architectures
# MAGIC  - Explore the larger technology landscape for deployment tools

# COMMAND ----------

# MAGIC %md
# MAGIC TODO: add from https://docs.google.com/presentation/d/1rwsqGcvCYOFoqAWWZcDR04b-VMHxD_iK2agFQpoArSI/edit#slide=id.g23d10c2d27_0_12

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ###![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) From Data Science to Data Engineering
# MAGIC 
# MAGIC Data science and data engineering are two related but distinct practices.  Data scientists generally concern themselves with deriving buisiness insights from data.  They look to turn business problems into data problems, model those data problems, and optimize model performance.  
# MAGIC 
# MAGIC Data engineers are generally concerned with a host of different issues involved in putting data science solutions into production.  These issues can be summarized as follows:<br><br>
# MAGIC 
# MAGIC - **Reliability:** The system should work correctly, even when faced with hardware and software failures or human error
# MAGIC - **Scalability:** The system should be able to deal with growing and shrinking volume of data
# MAGIC - **Maintainability:** The system should be able to work with current demands and future feature updates
# MAGIC 
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ML-Part-4/ASUMDM.png" style="height: 450px; margin: 20px"/></div>
# MAGIC 
# MAGIC We need the left side of this equation to be "closed loop" (that is, fully automated)
# MAGIC 
# MAGIC [*Source*](ftp://ftp.software.ibm.com/software/data/sw-library/services/ASUM.pdf) and [*source*](https://www.oreilly.com/library/view/designing-data-intensive-applications/9781491903063/)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Reliability: Working Correctly<br><br>
# MAGIC 
# MAGIC  - **Fault tolerance** 
# MAGIC    - Hardware failures
# MAGIC    - Software failures
# MAGIC    - Human errors
# MAGIC  - **Robustness** ad hoc queries
# MAGIC  - **Security**

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scalability: Growing<br><br>
# MAGIC 
# MAGIC  - **Throughput** 
# MAGIC    - Choose the best *load parameters* (e.g. GET requests per second)
# MAGIC  - **Demand vs resources**
# MAGIC    - Vertical vs horizonal scalability
# MAGIC    - Linear scalability
# MAGIC    - Big O notation, % of task that's parallelizable 
# MAGIC  - **Latency and response time**
# MAGIC    - What is the speed at which applications respond to new requests?

# COMMAND ----------

# MAGIC %md
# MAGIC #### Maintainability: Running and Extending<br><br>
# MAGIC 
# MAGIC  - Operability (ease of operation)
# MAGIC    - Maintenance
# MAGIC    - Monitoring
# MAGIC    - Debuggability
# MAGIC  - Generalization and extensibility
# MAGIC    - the reuse of existing codebase assets (e.g. code, test suites, architectures) within a software applicaiton
# MAGIC    - the ability to extend a system to new feature demands and the level of effort required to implement an extension
# MAGIC    - Quantified in ability to extend changes while minimizing impact to existing system functions
# MAGIC  - Automation
# MAGIC    - to what extent is an application a "closed loop" not requiring human oversight and intervention

# COMMAND ----------

# MAGIC %md
# MAGIC ###![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) DevOps vs "ModelOps"
# MAGIC 
# MAGIC Data scientists us a zoo of different frameworks and languages, often prototyping in a scripting language such as Python or R.  These languages often make use of libraries and code paths not available in production environments.  Most production environments today are run in the Java ecosystem, including Scala which runs on the JVM.  C/C++ and legacy environments are also common.
# MAGIC 
# MAGIC DevOps combines software development with operations and quality assurance.  The goal of DevOps is to reduce the friction between development, quality assurance, and production.  DevOps values agile solutions.
# MAGIC 
# MAGIC "ModelOps" applies this same principle to the deployment of machine learning models into production.  Given the wide array of different tools used by data scientists, ModelOps focuses on porting data science solutions into production environments either through model serialization or refactoring data science solutions into production languages.  Beyond ensuring that models perform well in production, ModelOps looks to reduce the time between model development and deploymnet.
# MAGIC 
# MAGIC Problems: 
# MAGIC  - Data scientists prototype in Python/R, data engineers have to re-implement model for production, often in Java
# MAGIC    - Extra work
# MAGIC    - Different code paths, libraries, etc
# MAGIC    - Data science does not translate to production
# MAGIC    - Slow to update models
# MAGIC    - Some experimental methods don't work in production (latency considerations)
# MAGIC    - In brief: NOT AGILE

# COMMAND ----------

# MAGIC %md
# MAGIC #### Training vs Prediction Time
# MAGIC 
# MAGIC Some models have high training but low prediction time, and vice versa.  This needs to be considered in choosing the best ML algorithm to use.

# COMMAND ----------

# MAGIC %md
# MAGIC Other options:
# MAGIC 
# MAGIC Other Technologies and Frameworks:
# MAGIC   - [Kubeflow](https://www.kubeflow.org/#overview)
# MAGIC   - [FBLearner, a good use case](https://code.fb.com/core-data/introducing-fblearner-flow-facebook-s-ai-backbone/)
# MAGIC   - [Spark-sklearn](https://github.com/databricks/spark-sklearn)

# COMMAND ----------

# MAGIC %md
# MAGIC ###![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Three Architectures
# MAGIC 
# MAGIC * Batch processing (Table Query)
# MAGIC   - Example: churn prediction where high predicted churn generates targeted marketing
# MAGIC   - Most deployments are batch
# MAGIC   - Save predictions to database
# MAGIC   - Query database in order to serve the predictions
# MAGIC * Continuous/Real-Time (Streaming)
# MAGIC   - Typically asynchronous
# MAGIC   - Example: 
# MAGIC   - Live predictions on a stream of data (e.g. web activity logs)
# MAGIC   - Predict using a pretrained model
# MAGIC   - Latency in seconds (lower than REST?)
# MAGIC * On demand (REST, Imbedded)
# MAGIC   - Typically synchronous
# MAGIC   - Millisecond latency (higher than streaming?)
# MAGIC   - Normally served by REST or RMI
# MAGIC   - Example: live fraud detection on credit card transaction requests
# MAGIC   - Serialized or containerized model saved to a blob store or HDFS
# MAGIC   - Served using another engine (e.g. Sagemaker or Azure ML) 
# MAGIC   - Latency in milliseconds

# COMMAND ----------

# MAGIC %md
# MAGIC #### Phases of Deployment
# MAGIC 
# MAGIC - Dev/test/staging/production
# MAGIC - Model versioning/model registry
# MAGIC - When you should retrain, what you should retrain on (e.g. a trailing window, all data), "warm starts"

# COMMAND ----------

# MAGIC %md
# MAGIC #### A/B Testing
# MAGIC 
# MAGIC - A/B Testing (e.g. Clipper or Optimizely or Split IO or mention that we're working on it).  We could do this with batch inference in a custom way.  Include when you talk about the lifecycle

# COMMAND ----------

# MAGIC %md
# MAGIC #### CI/CD
# MAGIC 
# MAGIC  - CI/CD integration with Travis CI (less popular) or git lab (git lab appears to be more popular) or airflow or Jenkins [(see blog)](https://databricks.com/blog/2017/10/30/continuous-integration-continuous-delivery-databricks.html)
# MAGIC 
# MAGIC Types:
# MAGIC * On-Prem
# MAGIC  * Bambo (confluence, might be replaced)
# MAGIC  * TeamCity
# MAGIC  * Jenkins
# MAGIC * Clould
# MAGIC  * Git Lab
# MAGIC  * Amazon - Code Piple

# COMMAND ----------

# MAGIC %md
# MAGIC ###![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) The Technology Landscape
# MAGIC 
# MAGIC  - Mleap
# MAGIC  - [Onnx](https://onnx.ai/supported-tools)
# MAGIC  - Sagemaker
# MAGIC  - Azure ML
# MAGIC  - Note using coefficents or lighter weight models when deploying into a different production environment

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC 
# MAGIC Start the next lesson, [Batch Deployment]($./06-Batch-Deployment ).

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>