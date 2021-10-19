# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC #Introduction to DataFrames, Part #1
# MAGIC 
# MAGIC ** Data Source **
# MAGIC * One hour of Pagecounts from the English Wikimedia projects captured August 5, 2016, at 12:00 PM UTC.
# MAGIC * Size on Disk: ~23 MB
# MAGIC * Type: Compressed Parquet File
# MAGIC * More Info: <a href="https://dumps.wikimedia.org/other/pagecounts-raw" target="_blank">Page view statistics for Wikimedia projects</a>
# MAGIC 
# MAGIC **Technical Accomplishments:**
# MAGIC * Develop familiarity with the `DataFrame` APIs
# MAGIC * Introduce the classes...
# MAGIC   * `SparkSession`
# MAGIC   * `DataFrame` (aka `Dataset[Row]`)
# MAGIC * Introduce the transformations...
# MAGIC   * `limit(..)`
# MAGIC   * `select(..)`
# MAGIC   * `drop(..)`
# MAGIC   * `distinct()`
# MAGIC   * `dropDuplicates(..)`
# MAGIC * Introduce the actions...
# MAGIC   * `show(..)`
# MAGIC   * `display(..)`
# MAGIC   * `count()`

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Getting Started
# MAGIC 
# MAGIC Run the following cell to configure our "classroom."

# COMMAND ----------

# MAGIC %run "../Includes/Classroom Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) **The Data Source**
# MAGIC 
# MAGIC * In this notebook, we will be using a compressed parquet "file" called **pagecounts** (~23 MB file from Wikipedia)
# MAGIC * We will explore the data and develop an understanding of it as we progress.
# MAGIC * You can read more about this dataset here: <a href="https://dumps.wikimedia.org/other/pagecounts-raw/" target="_blank">Page view statistics for Wikimedia projects</a>.
# MAGIC 
# MAGIC We can use **&percnt;fs ls ..** to view our data on the DBFS.

# COMMAND ----------

# MAGIC %fs ls /mnt/training/wikipedia/pagecounts/staging_parquet_en_only_clean/

# COMMAND ----------

# MAGIC %md
# MAGIC As we can see from the files listed above, this data is stored in <a href="https://parquet.apache.org" target="_blank">Parquet</a> files which can be read in a single command, the result of which will be a `DataFrame`.

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Create a DataFrame
# MAGIC * We can read the Parquet files into a `DataFrame`.
# MAGIC * We'll start with the object **spark**, an instance of `SparkSession` and the entry point to Spark 2.0 applications.
# MAGIC * From there we can access the `read` object which gives us an instance of `DataFrameReader`.

# COMMAND ----------

parquetDir = "/mnt/training/wikipedia/pagecounts/staging_parquet_en_only_clean/"

pagecountsEnAllDF = (spark  # Our SparkSession & Entry Point
  .read                     # Our DataFrameReader
  .parquet(parquetDir)      # Returns an instance of DataFrame
)
print(pagecountsEnAllDF)    # Python hack to see the data type

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) count()
# MAGIC 
# MAGIC First, we are going to introduce the `count()` action.
# MAGIC 
# MAGIC If you look at the API docs, `count()` is described like this:
# MAGIC > Returns the number of rows in the Dataset.
# MAGIC 
# MAGIC `count()` will trigger a job to process the request and return a value.
# MAGIC 
# MAGIC We can now count all records in our `DataFrame` like this:

# COMMAND ----------

total = pagecountsEnAllDF.count()

print("Record Count: {0:,}".format( total ))

# COMMAND ----------

# MAGIC %md
# MAGIC That tells us that there are around 2 million rows in the `DataFrame`. 
# MAGIC 
# MAGIC Before we take a closer look at the contents of the `DataFrame`, let us introduce a technique that speeds up processing.  

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) cache() & persist()
# MAGIC 
# MAGIC The ability to cache data is one technique for achieving better performance with Apache Spark. 
# MAGIC 
# MAGIC This is because every action requires Spark to read the data from its source (Azure Blob, Amazon S3, HDFS, etc.) but caching moves that data into the memory of the local executor for "instant" access.
# MAGIC 
# MAGIC `cache()` is just an alias for `persist()`. 

# COMMAND ----------

(pagecountsEnAllDF
  .cache()         # Mark the DataFrame as cached
  .count()         # Materialize the cache
) 

# COMMAND ----------

# MAGIC %md
# MAGIC If you re-run that command, it should take significantly less time.

# COMMAND ----------

pagecountsEnAllDF.count()

# COMMAND ----------

# MAGIC %md
# MAGIC And as a quick side note, you can remove a cache by calling the `DataFrame`'s `unpersist()` method but, it is not necessary.

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Our Data
# MAGIC 
# MAGIC Let's continue by taking a look at the type of data we have. 
# MAGIC 
# MAGIC We can do this with the `printSchema()` command:

# COMMAND ----------

pagecountsEnAllDF.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC We should now be able to see that we have four columns of data:
# MAGIC * **project** (*string*): The name of the Wikipedia project. This will include values such as:
# MAGIC   * **en**: The English version of Wikipedia.
# MAGIC   * **fr**: The French version of Wikipedia.
# MAGIC   * **en.d**: The English version of Wiktionary.
# MAGIC   * **fr.b**: The French version of Wikibooks.
# MAGIC   * **de.n**: The German version of Wikinews.
# MAGIC * **article** (*string*): The name of the article in the corresponding project. This will include values such as:
# MAGIC   * <a href="https://en.wikipedia.org/wiki/Apache_Spark" target="_blank">Apache_Spark</a>
# MAGIC   * <a href="https://en.wikipedia.org/wiki/Matei_Zaharia" target="_blank">Matei_Zaharia</a>
# MAGIC   * <a href="https://en.wikipedia.org/wiki/Kevin_Bacon" target="_blank">Kevin_Bacon</a>
# MAGIC * **requests** (*integer*): The number of requests (clicks) the article has received in the hour this data represents.
# MAGIC * **bytes_served** (*long*): The total number of bytes delivered for the requested article.
# MAGIC   * **Note:** In our copy of the data, this value is zero for all records and consequently is of no value to us.

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Spark API
# MAGIC 
# MAGIC You have already seen one command available to the `DataFrame` class, namely `DataFrame.printSchema()`
# MAGIC   
# MAGIC Let's take a look at the API to see what other operations we have available.

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Spark API Home Page**
# MAGIC 0. Open a new browser tab
# MAGIC 0. Google for **Spark API Latest** or **Spark API _x.x.x_** for a specific version.
# MAGIC 0. Select **Spark API Documentation - Spark _x.x.x_ Documentation - Apache Spark** 
# MAGIC 
# MAGIC Other Documentation:
# MAGIC * Programming Guides for DataFrames, SQL, Graphs, Machine Learning, Streaming...
# MAGIC * Deployment Guides for Spark Standalone, Mesos, Yarn...
# MAGIC * Configuration, Monitoring, Tuning, Security...
# MAGIC 
# MAGIC Here are some shortcuts
# MAGIC   * <a href="https://spark.apache.org/docs/latest/api.html" target="_blank">Spark API Documentation - Latest</a>
# MAGIC   * <a href="https://spark.apache.org/docs/2.1.1/api.html" target="_blank">Spark API Documentation - 2.1.1</a>
# MAGIC   * <a href="https://spark.apache.org/docs/2.1.0/api.html" target="_blank">Spark API Documentation - 2.1.0</a>
# MAGIC   * <a href="https://spark.apache.org/docs/2.0.2/api.html" target="_blank">Spark API Documentation - 2.0.2</a>
# MAGIC   * <a href="https://spark.apache.org/docs/1.6.3/api.html" target="_blank">Spark API Documentation - 1.6.3</a>

# COMMAND ----------

# MAGIC %md
# MAGIC Naturally, which set of documentation you will use depends on which language you will use.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Spark API (Python)
# MAGIC 
# MAGIC 0. Select **Spark Python API (Sphinx)**.
# MAGIC 0. Look up the documentation for `pyspark.sql.DataFrame`.
# MAGIC   0. In the lower-left-hand-corner type **DataFrame** into the search field.
# MAGIC   0. Hit **[Enter]**.
# MAGIC   0. The search results should appear in the right-hand pane.
# MAGIC   0. Click on **pyspark.sql.DataFrame (Python class, in pyspark.sql module)**
# MAGIC   0. The documentation should open in the right-hand pane.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Spark API (Scala)
# MAGIC 
# MAGIC 0. Select **Spark Scala API (Scaladoc)**.
# MAGIC 0. Look up the documentation for `org.apache.spark.sql.DataFrame`.
# MAGIC   0. In the upper-left-hand-corner type **DataFrame** into the search field.
# MAGIC   0. The search will execute automatically.
# MAGIC   0. In the class/package list, click on **DataFrame**.
# MAGIC   0. The documentation should open in the right-hand pane.
# MAGIC   
# MAGIC This isn't going to work, but why?

# COMMAND ----------

# MAGIC %md
# MAGIC ### Spark API (Scala), Try #2
# MAGIC 
# MAGIC Look up the documentation for `org.apache.spark.sql.Dataset`.
# MAGIC   0. In the upper-left-hand-corner type **Dataset** into the search field.
# MAGIC   0. The search will execute automatically.
# MAGIC   0. In the class/package list, click on **Dataset**.
# MAGIC   0. The documentation should open in the right-hand pane.

# COMMAND ----------

# MAGIC %md
# MAGIC Now that we have found the proper documentation, we can take a quick peek at the function `printSchema()`.
# MAGIC 
# MAGIC Nothing special here.
# MAGIC 
# MAGIC If you look at the API docs, `printSchema(..)` is described like this:
# MAGIC > Prints the schema to the console in a nice tree format.

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) show(..)
# MAGIC 
# MAGIC What we want to look for next is a function that will allow us to print the data to the console.
# MAGIC 
# MAGIC In the API docs for `DataFrame`/`Dataset` find the docs for the `show(..)` command(s).
# MAGIC 
# MAGIC In the case of Python, we have one method with two optional parameters.<br/>
# MAGIC In the case of Scala, we have several overloaded methods.<br/>
# MAGIC 
# MAGIC In either case, the `show(..)` method effectively has two optional parameters:
# MAGIC * **n**: The number of records to print to the console, the default being 20.
# MAGIC * **truncate**: If true, columns wider than 20 characters will be truncated, where the default is true.
# MAGIC 
# MAGIC Let's take a look at the data in our `DataFrame` with the `show()` command:

# COMMAND ----------

pagecountsEnAllDF.show()

# COMMAND ----------

# MAGIC %md
# MAGIC In the cell above, change the parameters of the show command to:
# MAGIC * print only the first five records
# MAGIC * disable truncation
# MAGIC * print only the first ten records and disable truncation
# MAGIC 
# MAGIC **Note:** The function `show(..)` is an **action** which triggers a job.

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) display(..)
# MAGIC 
# MAGIC The `show(..)` command is part of the core Spark API and simply prints the results to the console.
# MAGIC 
# MAGIC Our notebooks have a slightly more elegant alternative.
# MAGIC 
# MAGIC Instead of calling `show(..)` on an existing `DataFrame` we can instead pass our `DataFrame` to the `display(..)` command:

# COMMAND ----------

display(pagecountsEnAllDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ### show(..) vs display(..)
# MAGIC * `show(..)` is part of core spark - `display(..)` is specific to our notebooks.
# MAGIC * `show(..)` is ugly - `display(..)` is pretty.
# MAGIC * `show(..)` has parameters for truncating both columns and rows - `display(..)` does not.
# MAGIC * `show(..)` is a function of the `DataFrame`/`Dataset` class - `display(..)` works with a number of different objects.
# MAGIC * `display(..)` is more powerful - with it, you can...
# MAGIC   * Download the results as CSV
# MAGIC   * Render line charts, bar chart & other graphs, maps and more.
# MAGIC   * See up to 1000 records at a time.
# MAGIC   
# MAGIC For the most part, the difference between the two is going to come down to preference.
# MAGIC 
# MAGIC Like `DataFrame.show(..)`, `display(..)` is an **action** which triggers a job.

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) limit(..)
# MAGIC 
# MAGIC Both `show(..)` and `display(..)` are **actions** that trigger jobs (though in slightly different ways).
# MAGIC 
# MAGIC If you recall, `show(..)` has a parameter to control how many records are printed but, `display(..)` does not.
# MAGIC 
# MAGIC We can address that difference with our first transformation, `limit(..)`.
# MAGIC 
# MAGIC If you look at the API docs, `limit(..)` is described like this:
# MAGIC > Returns a new Dataset by taking the first n rows...
# MAGIC 
# MAGIC `show(..)`, like many actions, does not return anything. 
# MAGIC 
# MAGIC On the other hand, transformations like `limit(..)` return a **new** `DataFrame`:

# COMMAND ----------

limitedDF = pagecountsEnAllDF.limit(5) # "limit" the number of records to the first 5

limitedDF # Python hack to force printing of the data type

# COMMAND ----------

# MAGIC %md
# MAGIC ### Nothing Happened
# MAGIC * Notice how "nothing" happened - that is no job was triggered.
# MAGIC * This is because we are simply defining the second step in our transformations.
# MAGIC   0. Read in the parquet file (represented by **pagecountsEnAllDF**).
# MAGIC   0. Limit those records to just the first 5 (represented by **limitedDF**).
# MAGIC * It's not until we induce an action that a job is triggered and the data is processed
# MAGIC 
# MAGIC We can induce a job by calling either the `show(..)` or the `display(..)` actions:

# COMMAND ----------

limitedDF.show(100, False) #show up to 100 records and don't truncate the columns

# COMMAND ----------

display(limitedDF) # defaults to the first 1000 records

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) select(..)
# MAGIC 
# MAGIC Let's say, for the sake of argument, that we don't want to look at all the data:

# COMMAND ----------

pagecountsEnAllDF.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC For example, it was asserted above that **bytes_served** had nothing but zeros in it and consequently is of no value to us.
# MAGIC 
# MAGIC If that is the case, we can disregard it by selecting only the three columns that we want:

# COMMAND ----------

# Transform the data by selecting only three columns
onlyThreeDF = (pagecountsEnAllDF
  .select("project", "article", "requests") # Our 2nd transformation (4 >> 3 columns)
)
# Now let's take a look at what the schema looks like
onlyThreeDF.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Again, notice how the call to `select(..)` does not trigger a job.
# MAGIC 
# MAGIC That's because `select(..)` is a transformation. It's just one more step in a long list of transformations.
# MAGIC 
# MAGIC Let's go ahead and invoke the action `show(..)` and take a look at the result.

# COMMAND ----------

# And lastly, show the first five records which should exclude the bytes_served column.
onlyThreeDF.show(5, False)

# COMMAND ----------

# MAGIC %md
# MAGIC The `select(..)` command is one of the most powerful and most commonly used transformations. 
# MAGIC 
# MAGIC We will see plenty of other examples of its usage as we progress.
# MAGIC 
# MAGIC If you look at the API docs, `select(..)` is described like this:
# MAGIC > Returns a new Dataset by computing the given Column expression for each element.
# MAGIC 
# MAGIC The "Column expression" referred to there is where the true power of this operation shows up. Again, we will go deeper on these later.
# MAGIC 
# MAGIC Just like `limit(..)`, `select(..)` 
# MAGIC * does not trigger a job
# MAGIC * returns a new `DataFrame`
# MAGIC * simply defines the next transformation in a sequence of transformations.

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) drop(..)
# MAGIC 
# MAGIC As a quick side note, you will quickly discover there are a lot of ways to accomplish the same task.
# MAGIC 
# MAGIC Take the transformation `drop(..)` for example - instead of selecting everything we wanted, `drop(..)` allows us to specify the columns we don't want.
# MAGIC 
# MAGIC If you look at the API docs, `drop(..)` is described like this:
# MAGIC > Returns a new Dataset with a column dropped.
# MAGIC 
# MAGIC And we can see that we can produce the same result as the last exercise this way:

# COMMAND ----------

# Transform the data by selecting only three columns
droppedDF = (pagecountsEnAllDF
  .drop("bytes_served") # Our second transformation after the initial read (4 columns down to 3)
)
# Now let's take a look at what the schema looks like
droppedDF.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Again, `drop(..)` is just one more transformation - that is no job is triggered.

# COMMAND ----------

# And lastly, show the first five records which should exclude the bytes_served column.
droppedDF.show(5, False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) distinct() & dropDuplicates()
# MAGIC 
# MAGIC These two transformations do the same thing. In fact, they are aliases for one another.
# MAGIC * You can see this by looking at the source code for these two methods
# MAGIC * ```def distinct(): Dataset[T] = dropDuplicates()```
# MAGIC * See <a href="https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/Dataset.scala" target="_blank">Dataset.scala</a>
# MAGIC 
# MAGIC The difference between them has everything to do with the programmer and their perspective.
# MAGIC * The name **distinct** will resonate with developers, analyst and DB admins with a background in SQL.
# MAGIC * The name **dropDuplicates** will resonate with developers that have a background or experience in functional programming.
# MAGIC 
# MAGIC As you become more familiar with the various APIs, you will see this pattern reassert itself.
# MAGIC 
# MAGIC The designers of the API are trying to make the API as approachable as possible for multiple target audiences.
# MAGIC 
# MAGIC If you look at the API docs, both `distinct(..)` and `dropDuplicates(..)` are described like this:
# MAGIC > Returns a new Dataset that contains only the unique rows from this Dataset....
# MAGIC 
# MAGIC With this transformation, we can now tackle our first business question:

# COMMAND ----------

# MAGIC %md
# MAGIC ### How many different English Wikimedia projects saw traffic during that hour?

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC If you recall, our original `DataFrame` has this schema:

# COMMAND ----------

pagecountsEnAllDF.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC The transformation `distinct()` is applied to the row as a whole - data in the **project**, **article** and **requests** column will effect this evaluation.
# MAGIC 
# MAGIC To get the distinct list of projects, and only projects, we need to reduce the number of columns to just the one column, **project**. 
# MAGIC 
# MAGIC We can do this with the `select(..)` transformation and then we can introduce the `distinct()` transformation.

# COMMAND ----------

distinctDF = (pagecountsEnAllDF     # Our original DataFrame from spark.read.parquet(..)
  .select("project")                # Drop all columns except the "project" column
  .distinct()                       # Reduce the set of all records to just the distinct column.
)

# COMMAND ----------

# MAGIC %md
# MAGIC Just to reinforce, we have three transformations:
# MAGIC 0. Read the data (now represented by `pagecountsEnAllDF`)
# MAGIC 0. Select just the one column
# MAGIC 0. Reduce the records to a distinct set
# MAGIC 
# MAGIC No job is triggered until we perform an action like `show(..)`:

# COMMAND ----------

# There will not be more than 100 projects
distinctDF.show(100, False)               

# COMMAND ----------

# MAGIC %md
# MAGIC You can count those if you like.
# MAGIC 
# MAGIC But, it would be easier to ask the `DataFrame` for the `count()`:

# COMMAND ----------

total = distinctDF.count()     
print("Distinct Projects: {0:,}".format( total ))

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) dropDuplicates(columns...)
# MAGIC 
# MAGIC The method `dropDuplicates(..)` has a second variant that accepts one or more columns.
# MAGIC * The distinction is not performed across the entire record unlike `distinct()` or even `dropDuplicates()`.
# MAGIC * The distinction is based only on the specified columns.
# MAGIC * This allows us to keep all the original columns in our `DataFrame`.

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Recap
# MAGIC 
# MAGIC Our code is spread out over many cells which can make this a little hard to follow.
# MAGIC 
# MAGIC Let's take a look at the same code in a single cell.

# COMMAND ----------

parquetDir = "/mnt/training/wikipedia/pagecounts/staging_parquet_en_only_clean/"

pagecountsEnAllDF = (spark       # Our SparkSession & Entry Point
  .read                          # Our DataFrameReader
  .parquet(parquetDir)           # Returns an instance of DataFrame
)
(pagecountsEnAllDF               # Only if we are running multiple queries
  .cache()                       # mark the DataFrame as cachable
  .count()                       # materialize the cache
)
distinctDF = (pagecountsEnAllDF  # Our original DataFrame from spark.read.parquet(..)
  .select("project")             # Drop all columns except the "project" column
  .distinct()                    # Reduce the set of all records to just the distinct column.
)
total = distinctDF.count()     
print("Distinct Projects: {0:,}".format( total ))

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) DataFrames vs SQL & Temporary Views
# MAGIC 
# MAGIC The `DataFrame`s API is built upon an SQL engine.
# MAGIC 
# MAGIC As such we can "convert" a `DataFrame` into a temporary view (or table) and then use it in "standard" SQL.
# MAGIC 
# MAGIC Let's start by creating a temporary view from a previous `DataFrame`.

# COMMAND ----------

pagecountsEnAllDF.createOrReplaceTempView("pagecounts")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Now that we have a temporary view (or table) we can start expressing our queries and transformations in SQL:

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM pagecounts

# COMMAND ----------

# MAGIC %md
# MAGIC And we can just as easily express in SQL the distinct list of projects, and just because we can, we'll sort that list:

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT DISTINCT project FROM pagecounts ORDER BY project

# COMMAND ----------

# MAGIC %md
# MAGIC And converting from SQL back to a `DataFrame` is just as easy:

# COMMAND ----------

tableDF = spark.sql("SELECT DISTINCT project FROM pagecounts ORDER BY project")
display(tableDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/labs.png) Introduction to DataFrames, Lab #1
# MAGIC It's time to put what we learned to practice.
# MAGIC 
# MAGIC Go ahead and open the notebook [Introduction to DataFrames, Lab #1]($./Intro To DF Part 1 Lab) and complete the exercises.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>