// Databricks notebook source
// MAGIC %md #### Create an [Init Script](https://docs.databricks.com/user-guide/clusters/init-scripts.html) for the cluster you want to install XGBoost on
// MAGIC In this example, we create an init script for the cluster named 'xgboost'. The cluster name is important here. 

// COMMAND ----------

dbutils.fs.put("/databricks/init/xgboost/install-xgboost.sh", """
#!/bin/bash 
sudo apt-get update
sudo apt-get install -y maven
sudo apt-get -y install git
sudo git clone --recursive https://github.com/dmlc/xgboost

cd xgboost
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/

find * -name tracker.py -type f -print0 | xargs -0 sed -i '' "s/port=9091/port=33000/g"
make -j4

cd jvm-packages

#Change Scala Version
#sed -i -r -e 's/scala.version.([0-9.]+)/scala.version\>2.10.6/g' /xgboost/jvm-packages/pom.xml

sudo apt-get -y install maven
sudo apt-get -y install cmake
mvn -DskipTests=true package
mkdir -p /databricks/jars/
cp xgboost4j-spark/target/xgboost4j-spark-0.7-jar-with-dependencies.jar /databricks/jars/xgboost4j-spark-0.7-with-dependencies.jar
""", true)

// COMMAND ----------

// MAGIC %md ####Restart/create the cluster named 'xgboost'
// MAGIC This executes the script we just created, which will install XGBoost on each cluster node and move the .jar file into the correct location