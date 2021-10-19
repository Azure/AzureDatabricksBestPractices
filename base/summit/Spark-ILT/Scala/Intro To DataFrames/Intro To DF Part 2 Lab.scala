// Databricks notebook source
// MAGIC 
// MAGIC %md-sandbox
// MAGIC 
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC # Introduction to DataFrames, Lab #2
// MAGIC ## Washingtons and Marthas

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/wiki-book/general/logo_spark_tiny.png) Instructions
// MAGIC 
// MAGIC This data was captured in the August before the 2016 presidential election.
// MAGIC 
// MAGIC As a result, articles about the candidates were very popular.
// MAGIC 
// MAGIC For this exercise, you will...
// MAGIC 0. Filter the result to the **en** Wikipedia project.
// MAGIC 0. Find all the articles where the name of the article **ends** with **_Washington** (presumably "George Washington", "Martha Washington", etc)
// MAGIC 0. Return all records as an array to the Driver.
// MAGIC 0. Assign your array of Washingtons (the return value of your action) to the variable `washingtons`.
// MAGIC 0. Calculate the sum of requests for the Washingtons and assign it to the variable `totalWashingtons`. <br/>
// MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** We've not yet covered `DataFrame` aggregation techniques, so for this exercise use the array of records you have just obtained.
// MAGIC 
// MAGIC ** Bonus **
// MAGIC 
// MAGIC Repeat the exercise for the Marthas
// MAGIC 0. Filter the result to the **en** Wikipedia project.
// MAGIC 0. Find all the articles where the name of the article **starts** with **Martha_** (presumably "Martha Washington", "Martha Graham", etc)
// MAGIC 0. Return all records as an array to the Driver.
// MAGIC 0. Assign your array of Marthas (the return value of your action) to the variable `marthas`.
// MAGIC 0. Calculate the sum of requests for the Marthas and assign it to the variable `totalMarthas`.<br/>
// MAGIC <img alt="Hint" title="Hint" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.3em" src="https://files.training.databricks.com/static/images/icon-light-bulb.svg"/>&nbsp;**Hint:** We've not yet covered `DataFrame` aggregation techniques, so for this exercise use the array of records you have just obtained.
// MAGIC 0. But you cannot do it the same way twice:
// MAGIC    * In the filter, don't use the same conditional method as the one used for the Washingtons.
// MAGIC    * Don't use the same action as used for the Washingtons.
// MAGIC 
// MAGIC **Testing**
// MAGIC 
// MAGIC Run the last cell to verify that your results are correct.
// MAGIC 
// MAGIC **Hints**
// MAGIC * <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> Make sure to include the underscore in the condition.
// MAGIC * The actions we've explored for extracting data include:
// MAGIC   * `first()`
// MAGIC   * `collect()`
// MAGIC   * `head()`
// MAGIC   * `take(n)`
// MAGIC * The conditional methods used with a `filter(..)` include:
// MAGIC   * equals
// MAGIC   * not-equals
// MAGIC   * starts-with
// MAGIC   * and there are others - remember, the `DataFrames` API is built upon an SQL engine.
// MAGIC * There shouldn't be more than 1000 records for either the Washingtons or the Marthas

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Getting Started
// MAGIC 
// MAGIC Run the following cell to configure our "classroom."

// COMMAND ----------

// MAGIC %run "../Includes/Classroom Setup"

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/wiki-book/general/logo_spark_tiny.png) Show Your Work

// COMMAND ----------

// TODO

// Replace FILL_IN with your code. You will probably need multiple
// lines of code for this problem.

val parquetDir = "/mnt/training/wikipedia/pagecounts/staging_parquet_en_only_clean/"

val washingtons = FILL_IN

var totalWashingtons = 0

for (washington <- washingtons) {
  totalWashingtons += FILL_IN
}

printf("Total Washingtons: %,d%n", washingtons.size )
printf("Total Washingtons Requests: %,d%n", totalWashingtons)
println("-"*80)

// COMMAND ----------

// TODO

// Replace FILL_IN with your code. You will probably need multiple
// lines of code for this problem.

val marthas = FILL_IN

var totalMarthas = 0

for (martha <- marthas) {
  totalMarthas += FILL_IN
}

printf("Total Marthas: %,d%n", marthas.size )
printf("Total Mathas Requests: %,d%n", totalMarthas)
println("-"*80)

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/wiki-book/general/logo_spark_tiny.png) Verify Your Work
// MAGIC Run the following cell to verify that your `DataFrame` was created properly.

// COMMAND ----------

printf("Total Washingtons: %,d%n", washingtons.size )
printf("Total Washington Requests: %,d%n", totalWashingtons)

val expectedCount = 466
assert (washingtons.size == expectedCount, s"Expected ${expectedCount} articles but found ${washingtons.size}")

val expectedTotal = 3266
assert (totalWashingtons == expectedTotal, s"Expected ${expectedTotal} requests but found ${totalWashingtons}")

println("-"*80)

// COMMAND ----------

printf("Total Marthas: %,d%n", marthas.size )
printf("Total Marthas Requests: %,d%n", totalMarthas)

val expectedCount = 146
assert (marthas.size == expectedCount, s"Expected ${expectedCount} articles but found ${marthas.size}")

val expectedTotal = 708
assert (totalMarthas == expectedTotal, s"Expected ${expectedTotal} requests but found ${totalMarthas}")

println("-"*80)


// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>