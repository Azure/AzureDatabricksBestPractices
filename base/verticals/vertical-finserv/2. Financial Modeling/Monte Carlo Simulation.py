# Databricks notebook source
# MAGIC %md #Monte Carlo Simulation (Python)
# MAGIC Example of how to use Monte Carlo simulatations in Databricks to model the frequency of dice rolls.  
# MAGIC 
# MAGIC **Business case:**  
# MAGIC Use Monte Carlo simluation to perform portfolio analysis.  
# MAGIC **Problem Statement:**  
# MAGIC It's difficult to analyze different portfolios to understand the risk and performance characteristics.  
# MAGIC **Business solution:**  
# MAGIC Use Apache Spark to build models and run across multiple portfolios, and compare the results between the different portfolios.  
# MAGIC **Technical Solution:**  
# MAGIC Use Spark ML to build a monte carlo simulation and generate a dashboard.  

# COMMAND ----------

# MAGIC %md We are trying to prove that with two dice the probability of rolling a 7 has the highest probability and rolling a 12 or snake eyes (2) has the lowest probability.
# MAGIC 
# MAGIC The probability table for rolling two dice is as follows:
# MAGIC 
# MAGIC | Roll   |      Combinations      |  Probability |
# MAGIC |----------|:-------------:|------:|
# MAGIC | 2 |  1 | 2.78% |
# MAGIC | 3 |  2   |   5.56% |
# MAGIC | 4 |  3 |    8.33% |
# MAGIC |5|4|11.11%|
# MAGIC |6|5|13.89%|
# MAGIC |7|6|16.67%|
# MAGIC |8|5|13.89%|
# MAGIC |9|4|11.11%|
# MAGIC |10|3|8.33%|
# MAGIC |11|2|5.56%|
# MAGIC |12|1|2.78%|
# MAGIC |Total|36|100.%|
# MAGIC 
# MAGIC Source: http://boardgames.about.com/od/dicegames/a/probabilities.htm
# MAGIC 
# MAGIC Can we use Spark to simulate dice rolls to prove that these probabilities hold true?

# COMMAND ----------

# MAGIC %md 
# MAGIC Note that if you get an import error referencing Tkinter please run the below in a cell
# MAGIC ```sh
# MAGIC %sh
# MAGIC sudo apt-get update
# MAGIC sudo apt-get install -y python-tk python-gdbm
# MAGIC ```

# COMMAND ----------

# MAGIC %md Import Standard Python Random Uniform number generator and math functions

# COMMAND ----------

from numpy.random import uniform
from math import ceil
from operator import add

# COMMAND ----------

# MAGIC %md ##Step 1: Define a Row Type 
# MAGIC * RollValue: the actual roll that occurred between 2 and 12
# MAGIC * RollCount: number of times this roll occured 

# COMMAND ----------

from pyspark.sql import Row
DiceRoll = Row('RollValue', 'RollCount')

# COMMAND ----------

# MAGIC %md ##Step 2: Create a Function that Simulates a Dice Roll

# COMMAND ----------

# diceRoll takes the number of dice as a parameter and then uses the 
# Python uniform random number generator to roll the dice
# the result is the total of value of the dice roll
def diceRoll(numOfDice):
  total = 0
  for i in xrange(0, numOfDice):
    total = total + ceil(uniform(0,6))
  return total

# COMMAND ----------

# MAGIC %md ##Step 3: Run the Simulation Across the Spark Cluster
# MAGIC Create a dummy array using the Python range function. "Roll the dice" using our predefined function and return a tuple with they key as the total of the dice and 1 to count the number of times this roll appeared. Use the reduceByKey function with the built-in add operator to total the count to get the number of rolls per rollvaluue.

# COMMAND ----------

sc.parallelize(range(0,1000)).map(lambda x: [diceRoll(2), 1])

# COMMAND ----------

diceRDD = sc.parallelize(range(0,1000)).map(lambda x: [diceRoll(2), 1]).reduceByKey(add)

# COMMAND ----------

# MAGIC %md Create a DataFrame with the roll value as the key and the count as the value.

# COMMAND ----------

diceDF = spark.createDataFrame(diceRDD.map(lambda x: DiceRoll(x[0], x[1])))

# COMMAND ----------

# MAGIC %md Register this DataFrame as a Temp Table

# COMMAND ----------



# COMMAND ----------

diceDF.createOrReplaceTempView("dice_rolls")

# COMMAND ----------

# MAGIC %md You can start to see the normal distribution in as little as 1,000 rolls by selecting from the table and sorting by the RollValue.

# COMMAND ----------

# MAGIC %sql select * from dice_rolls order by RollValue

# COMMAND ----------

# MAGIC %md But it is more clear after 100,000 rolls that 7 is most common roll value and 2 and 12 are the lowest.

# COMMAND ----------

diceRDD = sc.parallelize(range(0,100000)).map(lambda x: [diceRoll(2),1]).reduceByKey(add)
diceDF = spark.createDataFrame(diceRDD.map(lambda x: DiceRoll(x[0], x[1])))
diceDF.createOrReplaceTempView("dice_rolls")

# COMMAND ----------

# MAGIC %sql select * from dice_rolls order by RollValue

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Step 4: Let's Calculate the Probabilities
# MAGIC Let's calculate the probabilities -- as you can see the probabilities match watch the gambler's table above says

# COMMAND ----------

totalDF = diceDF.groupBy().sum('RollCount').withColumnRenamed("SUM(RollCount)", "Total")
display(totalDF.crossJoin(diceDF).selectExpr(["RollValue", "(RollCount / Total) as Probability"]).sort("RollValue"))

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Step 5: Alternative Simulation Method
# MAGIC Rather than having a large RDD (100,000 interations) that is partitioned automatically by Spark you can also kick off one process per node and have that process iterate within the process itself. 

# COMMAND ----------

# This function takes the number of dice rolled and the number of trials and returns 
def manyDiceRolls(numOfDice, numofTrials):
  trials = {}
  # we need to initialize each key in the dictionary with 0
  for i in xrange(numOfDice, (numOfDice*6)+1):
    trials[i] = 0
  # Inside the function we will execute a number of trials
  for i in xrange(0, numofTrials):
    trialRollResult = diceRoll(numOfDice)
    trials[int(trialRollResult)] = trials[int(trialRollResult)] + 1
  return trials

# COMMAND ----------

# Example of our Python function
manyDiceRolls(2, 1000)

# COMMAND ----------

# MAGIC %md Now we use Spark and run this function once per executor and get the same result.

# COMMAND ----------

diceRDD = sc.parallelize(range(0,2)).map(lambda x: manyDiceRolls(2,25000)).flatMap(lambda x: x.items()).reduceByKey(add)
diceDF = spark.createDataFrame(diceRDD.map(lambda x: DiceRoll(x[0], x[1])))
diceDF.createOrReplaceTempView("dice_rolls")

# COMMAND ----------

# MAGIC %sql select * from dice_rolls order by RollValue