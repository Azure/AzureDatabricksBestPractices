# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Alerting
# MAGIC 
# MAGIC Alerting allows you to announce the progress of different applications, which becomes increasingly important in automated production systems.  In this lesson, you explore basic alerting strategies using email and REST integration with tools like Slack.
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lesson you:<br>
# MAGIC  - Explore the alerting landscape
# MAGIC  - Walk through basic email alerting using Databricks Jobs
# MAGIC  - Create a basic REST alert integrated with Slack
# MAGIC  - Create a more complex REST alert for Spark jobs using `SparkListener`

# COMMAND ----------

# MAGIC %md
# MAGIC ### The Alerting Landscape
# MAGIC 
# MAGIC There are a number of different alerting tools with various levels of sophistication.  For monitoring for production outages, PagerDuty has risen to be one of the most popular tools.  It allows for the escalation of issues across a team with alerts including text messages and phone calls.
# MAGIC 
# MAGIC Since many organizations are using Slack, integrating alerting into this communication platform avoids having to examine another platform to look at job status.  Twilio offers easy integration of text message alerts.  Finally, email alerts are easily integrated into a number of tools.  While manual email alerting requires the configuration of an SMTP, email alerts are also offered by tools like Databricks Jobs.
# MAGIC 
# MAGIC Most alerting frameworks allows for custom alerting done through REST integration.  This is how both Slack and PagerDuty allow for alerts.  This lesson will walk through an example of this using Slack.
# MAGIC 
# MAGIC One additional helpful tool for Spark workloads is the `SparkListener`, which can perform custom logic on various Cluster actions.

# COMMAND ----------

# MAGIC %md
# MAGIC Run the following cell to set up our environment.

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setting Basic Alerts
# MAGIC 
# MAGIC Create a basic alert using a Slack endpoint.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC Define a Slack webhook.  This has been done for you.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Define your own Slack webhook <a href="https://api.slack.com/incoming-webhooks#getting-started" target="_blank">Using these 4 steps.</a><br>
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> This same approach applies to PagerDuty as well.

# COMMAND ----------

webhook = "https://hooks.slack.com/services/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"

# COMMAND ----------

# MAGIC %md
# MAGIC Send a test message and check Slack.

# COMMAND ----------

def postToSlack(webhook, content):
  import requests
  from string import Template
  t = Template('{"text": "${content}"}')
  
  response = requests.post(webhook, data=t.substitute(content=content), headers={'Content-Type': 'application/json'})
  
postToSlack(webhook, "This is my post from Python")

# COMMAND ----------

# MAGIC %md
# MAGIC Do the same thing using Scala.  This involves a bit more boilerplate and a different library.

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC def postToSlack(webhook:String, content:String):Unit = {
# MAGIC   import org.apache.http.entity._
# MAGIC   import org.apache.http.impl.client.{HttpClients}
# MAGIC   import org.apache.http.client.methods.HttpPost
# MAGIC 
# MAGIC   val client = HttpClients.createDefault()
# MAGIC   val httpPost = new HttpPost(webhook)
# MAGIC   
# MAGIC   val payload = s"""{"text": "${content}"}"""
# MAGIC 
# MAGIC   val entity = new StringEntity(payload)
# MAGIC   httpPost.setEntity(entity)
# MAGIC   httpPost.setHeader("Accept", "application/json")
# MAGIC   httpPost.setHeader("Content-type", "application/json")
# MAGIC 
# MAGIC   val response = client.execute(httpPost)
# MAGIC   client.close()
# MAGIC }
# MAGIC 
# MAGIC val webhook = "https://hooks.slack.com/services/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"
# MAGIC 
# MAGIC postToSlack(webhook, "This is my post from Scala")

# COMMAND ----------

# MAGIC %md
# MAGIC Now you can easily integrate custom logic back to Slack.

# COMMAND ----------

mse = .45

postToSlack(webhook, "The newly trained model MSE is now {}".format(mse))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Using a `SparkListener`
# MAGIC 
# MAGIC A custom `SparkListener` allows for custom actions taken on cluster activity.  This API is only available in Scala.  Take a look at the following code.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> <a href="http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.scheduler.SparkListener" target="_blank">See the `SparkListener` docs here.</a>

# COMMAND ----------

# MAGIC %scala
# MAGIC // Package in a notebook helps to ensure a proper singleton
# MAGIC package com.databricks.academy
# MAGIC 
# MAGIC object SlackNotifyingListener extends org.apache.spark.scheduler.SparkListener {
# MAGIC   import org.apache.spark.scheduler._
# MAGIC 
# MAGIC   val webhook = "https://hooks.slack.com/services/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"
# MAGIC   
# MAGIC   def postToSlack(message:String):Unit = {
# MAGIC     import org.apache.http.entity._
# MAGIC     import org.apache.http.impl.client.{HttpClients}
# MAGIC     import org.apache.http.client.methods.HttpPost
# MAGIC 
# MAGIC     val client = HttpClients.createDefault()
# MAGIC     val httpPost = new HttpPost(webhook)
# MAGIC 
# MAGIC     val content = """{ "text": "%s" }""".format(message)
# MAGIC     
# MAGIC     val entity = new StringEntity(content)
# MAGIC     httpPost.setEntity(entity)
# MAGIC     httpPost.setHeader("Accept", "application/json")
# MAGIC     httpPost.setHeader("Content-type", "application/json")
# MAGIC 
# MAGIC     val response = client.execute(httpPost)
# MAGIC     client.close()
# MAGIC   }
# MAGIC   
# MAGIC   override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
# MAGIC     postToSlack("Called when the application ends")
# MAGIC   }
# MAGIC 
# MAGIC   override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {
# MAGIC     postToSlack("Called when the application starts")
# MAGIC   }
# MAGIC 
# MAGIC   override def onBlockManagerAdded(blockManagerAdded: SparkListenerBlockManagerAdded): Unit = {
# MAGIC     postToSlack("Called when a new block manager has joined")
# MAGIC   }
# MAGIC 
# MAGIC   override def onBlockManagerRemoved(blockManagerRemoved: SparkListenerBlockManagerRemoved): Unit = {
# MAGIC     postToSlack("Called when an existing block manager has been removed")
# MAGIC   }
# MAGIC 
# MAGIC   override def onBlockUpdated(blockUpdated: SparkListenerBlockUpdated): Unit = {
# MAGIC     postToSlack("Called when the driver receives a block update info.")
# MAGIC   }
# MAGIC 
# MAGIC   override def onEnvironmentUpdate(environmentUpdate: SparkListenerEnvironmentUpdate): Unit = {
# MAGIC     postToSlack("Called when environment properties have been updated")
# MAGIC   }
# MAGIC 
# MAGIC   override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {
# MAGIC     postToSlack("Called when the driver registers a new executor.")
# MAGIC   }
# MAGIC 
# MAGIC   override def onExecutorBlacklisted(executorBlacklisted: SparkListenerExecutorBlacklisted): Unit = {
# MAGIC     postToSlack("Called when the driver blacklists an executor for a Spark application.")
# MAGIC   }
# MAGIC 
# MAGIC   override def onExecutorBlacklistedForStage(executorBlacklistedForStage: SparkListenerExecutorBlacklistedForStage): Unit = {
# MAGIC     postToSlack("Called when the driver blacklists an executor for a stage.")
# MAGIC   }
# MAGIC 
# MAGIC   override def onExecutorMetricsUpdate(executorMetricsUpdate: SparkListenerExecutorMetricsUpdate): Unit = {
# MAGIC     // This one is a bit on the noisy side so I'm pre-emptively killing it
# MAGIC     // postToSlack("Called when the driver receives task metrics from an executor in a heartbeat.")
# MAGIC   }
# MAGIC 
# MAGIC   override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = {
# MAGIC     postToSlack("Called when the driver removes an executor.")
# MAGIC   }
# MAGIC 
# MAGIC   override def onExecutorUnblacklisted(executorUnblacklisted: SparkListenerExecutorUnblacklisted): Unit = {
# MAGIC     postToSlack("Called when the driver re-enables a previously blacklisted executor.")
# MAGIC   }
# MAGIC 
# MAGIC   override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
# MAGIC     postToSlack("Called when a job ends")
# MAGIC   }
# MAGIC 
# MAGIC   override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
# MAGIC     postToSlack("Called when a job starts")
# MAGIC   }
# MAGIC 
# MAGIC   override def onNodeBlacklisted(nodeBlacklisted: SparkListenerNodeBlacklisted): Unit = {
# MAGIC     postToSlack("Called when the driver blacklists a node for a Spark application.")
# MAGIC   }
# MAGIC 
# MAGIC   override def onNodeBlacklistedForStage(nodeBlacklistedForStage: SparkListenerNodeBlacklistedForStage): Unit = {
# MAGIC     postToSlack("Called when the driver blacklists a node for a stage.")
# MAGIC   }
# MAGIC 
# MAGIC   override def onNodeUnblacklisted(nodeUnblacklisted: SparkListenerNodeUnblacklisted): Unit = {
# MAGIC     postToSlack("Called when the driver re-enables a previously blacklisted node.")
# MAGIC   }
# MAGIC 
# MAGIC   override def onOtherEvent(event: SparkListenerEvent): Unit = {
# MAGIC     postToSlack("Called when other events like SQL-specific events are posted.")
# MAGIC   }
# MAGIC 
# MAGIC   override def onSpeculativeTaskSubmitted(speculativeTask: SparkListenerSpeculativeTaskSubmitted): Unit = {
# MAGIC     postToSlack("Called when a speculative task is submitted")
# MAGIC   }
# MAGIC 
# MAGIC   override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
# MAGIC     postToSlack("Called when a stage completes successfully or fails, with information on the completed stage.")
# MAGIC   }
# MAGIC 
# MAGIC   override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
# MAGIC     postToSlack("Called when a stage is submitted")
# MAGIC   }
# MAGIC 
# MAGIC   override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
# MAGIC     postToSlack("Called when a task ends")
# MAGIC   }
# MAGIC 
# MAGIC   override def onTaskGettingResult(taskGettingResult: SparkListenerTaskGettingResult): Unit = {
# MAGIC     postToSlack("Called when a task begins remotely fetching its result (will not be called for tasks that do not need to fetch the result remotely).")
# MAGIC   }
# MAGIC 
# MAGIC   override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
# MAGIC     postToSlack("Called when a task starts")
# MAGIC   }
# MAGIC 
# MAGIC   override def onUnpersistRDD(unpersistRDD: SparkListenerUnpersistRDD): Unit = {
# MAGIC     postToSlack("Called when an RDD is manually unpersisted by the application")
# MAGIC   }
# MAGIC }

# COMMAND ----------

# MAGIC %md
# MAGIC Register this Singleton as a `SparkListener`

# COMMAND ----------

# MAGIC %scala
# MAGIC sc.addSparkListener(com.databricks.academy.SlackNotifyingListener)

# COMMAND ----------

# MAGIC %md
# MAGIC Now run a basic DataFrame operation and observe the results in Slack.

# COMMAND ----------

# MAGIC %scala
# MAGIC spark.read
# MAGIC   .option("header", true)
# MAGIC   .option("inferSchema", true)
# MAGIC   .csv("dbfs:/mnt/training/asa/planes/plane-data.csv")
# MAGIC   .count

# COMMAND ----------

# MAGIC %md
# MAGIC This will also work back in Python.

# COMMAND ----------

(spark.read
  .option("header", True)
  .option("inferSchema", True)
  .csv("dbfs:/mnt/training/asa/planes/plane-data.csv")
  .count()
)

# COMMAND ----------

# MAGIC %md
# MAGIC When you're done, remove the listener.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Review
# MAGIC **Question:** What are the most common alerting tools?  
# MAGIC **Answer:** PagerDuty tends to be the tool most used in production environments.  SMTP servers emailing alerts are also popular, as is Twilio for text message alerts.  Slack webhooks and bots can easily be written as well.
# MAGIC 
# MAGIC **Question:** How can I write custom logic to monitor Spark?  
# MAGIC **Answer:** The `SparkListener` API is only exposed in Scala.  This allows you to write custom logic based on your cluster activity.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC 
# MAGIC Start the next lesson, [Delta Time Travel]($./11-Delta-Time-Travel ).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Topics & Resources
# MAGIC 
# MAGIC **Q:** Where can I find the alerting tools mentioned in this lesson?  
# MAGIC **A:** Check out <a href="https://www.twilio.com" target="_blank">Twilio</a> and <a href="https://www.pagerduty.com" target="_blank">PagerDuty</a>.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>