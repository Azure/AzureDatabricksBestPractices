# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC #Presentation Mode
# MAGIC To enable presentation mode, open the JavaScript console and enter the following command:
# MAGIC 
# MAGIC `window.settings.enablePresentationMode = true`
# MAGIC 
# MAGIC Then from the **View** menu select **Present**

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="display:table; height:600px; width:100%"><div style="display:table-cell; vertical-align:middle">
# MAGIC   <div style="margin: 0 auto; text-align:center; font-size:5vw">
# MAGIC     <p><p>A Caching Story<br/>featuring Tom &amp; Mary</p>
# MAGIC 
# MAGIC     <div style="position=absolute">
# MAGIC       <img src="https://files.training.databricks.com/images/working-left-man.png" style="left:0">
# MAGIC       <img src="https://files.training.databricks.com/images/working-right-woman.png" style="right:0">
# MAGIC     </div>
# MAGIC     
# MAGIC   </div>
# MAGIC </div></div>  

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="display:table; height:600px; width:100%"><div style="display:table-cell; vertical-align:middle">
# MAGIC   <div style="margin: 0 auto; text-align:center; font-size:5vw">
# MAGIC     <p>Same company &amp; the same cluster</p>
# MAGIC 
# MAGIC     <div style="position=absolute">
# MAGIC       <img src="https://files.training.databricks.com/images/working-left-man.png" style="left:0">
# MAGIC       <img src="https://files.training.databricks.com/images/cloud.png" style="width:25%"/>
# MAGIC       <img src="https://files.training.databricks.com/images/working-right-woman.png" style="right:0">
# MAGIC     </div>
# MAGIC     
# MAGIC   </div>
# MAGIC </div></div>  

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="display:table; height:600px; width:100%"><div style="display:table-cell; vertical-align:middle">
# MAGIC   <div style="margin: 0 auto; text-align:center; font-size:5vw">
# MAGIC     <p>20 GB of RAM for caching</p>
# MAGIC 
# MAGIC     <div style="position=absolute">
# MAGIC       <img src="https://files.training.databricks.com/images/working-left-man.png">
# MAGIC       <img src="https://files.training.databricks.com/images/cloud.png"     style="width:25%"/>
# MAGIC       <img src="https://files.training.databricks.com/images/storage-0.png" style="width:10%"/>
# MAGIC       <img src="https://files.training.databricks.com/images/working-right-woman.png">
# MAGIC     </div>
# MAGIC     
# MAGIC   </div>
# MAGIC </div></div>  

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="display:table; height:600px; width:100%"><div style="display:table-cell; vertical-align:middle">
# MAGIC   <div style="margin: 0 auto; text-align:center; font-size:5vw">
# MAGIC     <p>Tom uses 15 GB of the cache</p>
# MAGIC 
# MAGIC     <div style="position=absolute">
# MAGIC       <img src="https://files.training.databricks.com/images/working-left-man.png" style="left:0">
# MAGIC       <img src="https://files.training.databricks.com/images/cloud.png" style="width:25%"/>
# MAGIC       <img src="https://files.training.databricks.com/images/storage-75.png" style="width:10%"/>
# MAGIC     </div>
# MAGIC     
# MAGIC   </div>
# MAGIC </div></div>  

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="display:table; height:600px; width:100%"><div style="display:table-cell; vertical-align:middle">
# MAGIC   <div style="margin: 0 auto; text-align:center; font-size:5vw">
# MAGIC     <p>And Tom takes a coffee break...</p>
# MAGIC 
# MAGIC     <div style="position=absolute">
# MAGIC       <img src="https://files.training.databricks.com/images/cloud.png" style="width:25%"/>
# MAGIC       <img src="https://files.training.databricks.com/images/storage-75.png" style="width:10%"/>
# MAGIC     </div>
# MAGIC     
# MAGIC   </div>
# MAGIC </div></div>  

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="display:table; height:600px; width:100%"><div style="display:table-cell; vertical-align:middle">
# MAGIC   <div style="margin: 0 auto; text-align:center; font-size:5vw">
# MAGIC     <p>Mary starts a job requiring 10 GB</p>
# MAGIC 
# MAGIC     <div style="position=absolute">
# MAGIC       <img src="https://files.training.databricks.com/images/cloud.png" style="width:25%"/>
# MAGIC       <img src="https://files.training.databricks.com/images/storage-75.png" style="width:10%"/>
# MAGIC       <span>+</span>
# MAGIC       <img src="https://files.training.databricks.com/images/storage-50-more.png" style="width:10%"/>
# MAGIC       <img src="https://files.training.databricks.com/images/working-right-woman.png" style="left:0">
# MAGIC     </div>
# MAGIC     
# MAGIC   </div>
# MAGIC </div></div>  

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="display:table; height:600px; width:100%"><div style="display:table-cell; vertical-align:middle">
# MAGIC   <div style="margin: 0 auto; text-align:center; font-size:5vw">
# MAGIC     <p>What will happen to Mary's job?</p>
# MAGIC 
# MAGIC     <div style="white-space-nowrap;">
# MAGIC       <img src="https://files.training.databricks.com/images/cloud.png" style="width:25%"/>
# MAGIC       <img src="https://files.training.databricks.com/images/storage-75.png" style="width:10%"/>
# MAGIC       <span>+</span>
# MAGIC       <img src="https://files.training.databricks.com/images/storage-50-more.png" style="width:10%"/>
# MAGIC       <img src="https://files.training.databricks.com/images/working-right-woman.png" style="left:0">
# MAGIC     </div>
# MAGIC 
# MAGIC   </div>
# MAGIC </div></div>  

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="display:table; height:600px; width:100%"><div style="display:table-cell; vertical-align:middle">
# MAGIC   <div style="margin: 0 auto; text-align:center; font-size:5vw">
# MAGIC     <p>What will happen to Tom's data<br/>when caching MEMORY_ONLY?</p>
# MAGIC 
# MAGIC     <div style="white-space-nowrap;">
# MAGIC       <img src="https://files.training.databricks.com/images/working-left-man.png" style="left:0">
# MAGIC       <img src="https://files.training.databricks.com/images/cloud.png" style="width:25%"/>
# MAGIC       <img src="https://files.training.databricks.com/images/storage-75.png" style="width:10%"/>
# MAGIC       <span>+</span>
# MAGIC       <img src="https://files.training.databricks.com/images/storage-50-more.png" style="width:10%"/>
# MAGIC     </div>
# MAGIC 
# MAGIC   </div>
# MAGIC </div></div>  

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="display:table; height:600px; width:100%"><div style="display:table-cell; vertical-align:middle">
# MAGIC   <div style="margin: 0 auto; text-align:center; font-size:5vw">
# MAGIC     <p>What will happen to Tom's data<br/>when caching DISK_ONLY?</p>
# MAGIC 
# MAGIC     <div style="white-space-nowrap;">
# MAGIC       <img src="https://files.training.databricks.com/images/working-left-man.png" style="left:0">
# MAGIC       <img src="https://files.training.databricks.com/images/cloud.png" style="width:25%"/>
# MAGIC       <img src="https://files.training.databricks.com/images/storage-75.png" style="width:10%"/>
# MAGIC       <span>+</span>
# MAGIC       <img src="https://files.training.databricks.com/images/storage-50-more.png" style="width:10%"/>
# MAGIC     </div>
# MAGIC 
# MAGIC   </div>
# MAGIC </div></div>  

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="display:table; height:600px; width:100%"><div style="display:table-cell; vertical-align:middle">
# MAGIC   <div style="margin: 0 auto; text-align:center; font-size:5vw">
# MAGIC     <p>What will happen to Tom's data<br/>when caching MEMORY_AND_DISK?</p>
# MAGIC 
# MAGIC     <div style="white-space-nowrap;">
# MAGIC       <img src="https://files.training.databricks.com/images/working-left-man.png" style="left:0">
# MAGIC       <img src="https://files.training.databricks.com/images/cloud.png" style="width:25%"/>
# MAGIC       <img src="https://files.training.databricks.com/images/storage-75.png" style="width:10%"/>
# MAGIC       <span>+</span>
# MAGIC       <img src="https://files.training.databricks.com/images/storage-50-more.png" style="width:10%"/>
# MAGIC     </div>
# MAGIC 
# MAGIC   </div>
# MAGIC </div></div>  

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="display:table; height:600px; width:100%"><div style="display:table-cell; vertical-align:middle">
# MAGIC   <div style="margin: 0 auto; text-align:center; font-size:5vw">
# MAGIC     <p>If the existing data is expunged,<br/>how much is expunged?</p>
# MAGIC 
# MAGIC     <div style="white-space-nowrap;">
# MAGIC       <img src="https://files.training.databricks.com/images/storage-75.png" style="width:10%"/>
# MAGIC       <span>+</span>
# MAGIC       <img src="https://files.training.databricks.com/images/storage-50-more.png" style="width:10%"/>
# MAGIC     </div>
# MAGIC 
# MAGIC   </div>
# MAGIC </div></div>  

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="display:table; height:600px; width:100%"><div style="display:table-cell; vertical-align:middle">
# MAGIC   <div style="margin: 0 auto; text-align:center; font-size:5vw">
# MAGIC     <p>Will Spark expunge some <br/>fraction of a partition?</p>
# MAGIC 
# MAGIC     <div style="white-space-nowrap;">
# MAGIC       <img src="https://files.training.databricks.com/images/storage-75.png" style="width:10%"/>
# MAGIC       <span>+</span>
# MAGIC       <img src="https://files.training.databricks.com/images/storage-50-more.png" style="width:10%"/>
# MAGIC     </div>
# MAGIC 
# MAGIC   </div>
# MAGIC </div></div>  

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="display:table; height:600px; width:100%"><div style="display:table-cell; vertical-align:middle">
# MAGIC   <div style="margin: 0 auto; text-align:center; font-size:5vw">
# MAGIC     <p>Why will Spark <b>NOT</b> expunge <br>some fraction of a partition?</p>
# MAGIC 
# MAGIC     <div style="white-space-nowrap;">
# MAGIC       <img src="https://files.training.databricks.com/images/storage-75.png" style="width:10%"/>
# MAGIC       <span>+</span>
# MAGIC       <img src="https://files.training.databricks.com/images/storage-50-more.png" style="width:10%"/>
# MAGIC     </div>
# MAGIC 
# MAGIC   </div>
# MAGIC </div></div>  

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="display:table; height:600px; width:100%"><div style="display:table-cell; vertical-align:middle">
# MAGIC   <div style="margin: 0 auto; text-align:center; font-size:5vw">
# MAGIC     <p>Along comes a couple bright ideas...</p>
# MAGIC 
# MAGIC     <div style="white-space-nowrap;">
# MAGIC       <img src="https://files.training.databricks.com/images/light-bulb-idea.png" style="width:25%"/>
# MAGIC     </div>
# MAGIC 
# MAGIC   </div>
# MAGIC </div></div>  

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="display:table; height:600px; width:100%"><div style="display:table-cell; vertical-align:middle">
# MAGIC   <div style="margin: 0 auto; text-align:center; font-size:5vw">
# MAGIC 
# MAGIC     <div style="white-space-nowrap;">
# MAGIC       <img src="https://files.training.databricks.com/images/light-bulb-idea.png" style="width:25%; float:left"/>
# MAGIC       <p style="text-align:left; font-size:smaller">Good Idea? / Bad Idea?</p>
# MAGIC       <p style="text-align:left; margin-top:1em">If you are sharing a cluster, just commit to using <b>DISK_ONLY</b></p>
# MAGIC     </div>
# MAGIC 
# MAGIC   </div>
# MAGIC </div></div>  

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="display:table; height:600px; width:100%"><div style="display:table-cell; vertical-align:middle">
# MAGIC   <div style="margin: 0 auto; text-align:center; font-size:5vw">
# MAGIC 
# MAGIC     <div style="white-space-nowrap;">
# MAGIC       <img src="https://files.training.databricks.com/images/light-bulb-idea.png" style="width:25%; float:left"/>
# MAGIC       <p style="text-align:left; font-size:smaller">Good Idea? / Bad Idea?</p>
# MAGIC       <p style="text-align:left; margin-top:1em">You should never cache data</p>
# MAGIC     </div>
# MAGIC 
# MAGIC   </div>
# MAGIC </div></div>  

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="display:table; height:600px; width:100%"><div style="display:table-cell; vertical-align:middle">
# MAGIC   <div style="margin: 0 auto; text-align:center; font-size:5vw">
# MAGIC 
# MAGIC     <div style="white-space-nowrap;">
# MAGIC       <img src="https://files.training.databricks.com/images/light-bulb-idea.png" style="width:25%; float:left"/>
# MAGIC       <p style="text-align:left; font-size:smaller">Good Idea? / Bad Idea?</p>
# MAGIC       <p style="text-align:left; margin-top:1em">You should cache data immediately after reading it</p>
# MAGIC     </div>
# MAGIC 
# MAGIC   </div>
# MAGIC </div></div>  

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="display:table; height:600px; width:100%"><div style="display:table-cell; vertical-align:middle">
# MAGIC   <div style="margin: 0 auto; text-align:center; font-size:5vw">
# MAGIC 
# MAGIC     <div style="white-space-nowrap;">
# MAGIC       <img src="https://files.training.databricks.com/images/light-bulb-idea.png" style="width:25%; float:left"/>
# MAGIC       <p style="text-align:left; font-size:smaller">Good Idea? / Bad Idea?</p>
# MAGIC       <p style="text-align:left; margin-top:1em">Fix the underlying data before looking to caching as a solution</p>
# MAGIC     </div>
# MAGIC 
# MAGIC   </div>
# MAGIC </div></div>  

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="display:table; height:600px; width:100%"><div style="display:table-cell; vertical-align:middle">
# MAGIC   <div style="margin: 0 auto; text-align:center; font-size:5vw">
# MAGIC 
# MAGIC     <div style="white-space-nowrap;">
# MAGIC       <img src="https://files.training.databricks.com/images/light-bulb-idea.png" style="width:25%; float:left"/>
# MAGIC       <p style="text-align:left; font-size:smaller">Good Idea? / Bad Idea?</p>
# MAGIC       <p style="text-align:left; margin-top:1em">Data Scientist should <b>ALWAYS</b> cache their training data</p>
# MAGIC     </div>
# MAGIC 
# MAGIC   </div>
# MAGIC </div></div>  

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="display:table; height:600px; width:100%"><div style="display:table-cell; vertical-align:middle">
# MAGIC   <div style="margin: 0 auto; text-align:center; font-size:5vw">
# MAGIC 
# MAGIC     <div style="white-space-nowrap;">
# MAGIC       <img src="https://files.training.databricks.com/images/light-bulb-idea.png" style="width:25%; float:left"/>
# MAGIC       <p style="text-align:left; font-size:smaller">Good Idea? / Bad Idea?</p>
# MAGIC       <p style="text-align:left; margin-top:1em">Analyst should only cache the final result (e.g. an aggregate)</p>
# MAGIC     </div>
# MAGIC 
# MAGIC   </div>
# MAGIC </div></div>  

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="display:table; height:600px; width:100%"><div style="display:table-cell; vertical-align:middle">
# MAGIC   <div style="margin: 0 auto; text-align:center; font-size:5vw">
# MAGIC 
# MAGIC     <div style="white-space-nowrap;">
# MAGIC       <img src="https://files.training.databricks.com/images/light-bulb-idea.png" style="width:25%; float:left"/>
# MAGIC       <p style="text-align:left; font-size:smaller">Good Idea? / Bad Idea?</p>
# MAGIC       <p style="text-align:left; margin-top:1em">Engineers should never cache data in a production job</p>
# MAGIC     </div>
# MAGIC 
# MAGIC   </div>
# MAGIC </div></div>  

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="display:table; height:600px; width:100%"><div style="display:table-cell; vertical-align:middle">
# MAGIC   <div style="margin: 0 auto; text-align:center; font-size:5vw">
# MAGIC 
# MAGIC     <div style="white-space-nowrap;">
# MAGIC       <img src="https://files.training.databricks.com/images/light-bulb-idea.png" style="width:25%; float:left"/>
# MAGIC       <p style="text-align:left; font-size:smaller">Good Idea? / Bad Idea?</p>
# MAGIC       <p style="text-align:left; margin-top:1em">Use your own cluster so that you don't have to share the cache</p>
# MAGIC     </div>
# MAGIC 
# MAGIC   </div>
# MAGIC </div></div>  

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="display:table; height:600px; width:100%"><div style="display:table-cell; vertical-align:middle">
# MAGIC   <div style="margin: 0 auto; text-align:center; font-size:5vw">
# MAGIC 
# MAGIC     <div style="white-space-nowrap;">
# MAGIC       <img src="https://files.training.databricks.com/images/light-bulb-idea.png" style="width:25%; float:left"/>
# MAGIC       <p style="text-align:left; font-size:smaller">Good Idea? / Bad Idea?</p>
# MAGIC       <p style="text-align:left; margin-top:1em">Caching should have never been introduced to Apache Spark</p>
# MAGIC     </div>
# MAGIC 
# MAGIC   </div>
# MAGIC </div></div>  

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>