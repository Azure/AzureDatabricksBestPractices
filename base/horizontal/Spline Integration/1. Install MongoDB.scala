// Databricks notebook source
// MAGIC %sh
// MAGIC 
// MAGIC apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 0C49F3730359A14518585931BC711F9BA15703C6

// COMMAND ----------

// MAGIC %sh
// MAGIC 
// MAGIC echo "deb http://repo.mongodb.org/apt/ubuntu xenial/mongodb-org/3.4 multiverse" | tee /etc/apt/sources.list.d/mongodb-org-3.4.list

// COMMAND ----------

// MAGIC %sh
// MAGIC 
// MAGIC apt-get update
// MAGIC apt-get install -y mongodb-org

// COMMAND ----------

// MAGIC %sh
// MAGIC 
// MAGIC /usr/bin/mongod --quiet --config /etc/mongod.conf

// COMMAND ----------

