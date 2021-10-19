# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC #Introduction to DataFrames, Part #2
# MAGIC 
# MAGIC ** Data Source **
# MAGIC * One hour of Pagecounts from the English Wikimedia projects captured August 5, 2016, at 12:00 PM UTC.
# MAGIC * Size on Disk: ~23 MB
# MAGIC * Type: Compressed Parquet File
# MAGIC * More Info: <a href="https://dumps.wikimedia.org/other/pagecounts-raw" target="_blank">Page view statistics for Wikimedia projects</a>
# MAGIC 
# MAGIC **Technical Accomplishments:**
# MAGIC * Continue exploring the `DataFrame` set of APIs.
# MAGIC * Introduce the classes...
# MAGIC   * `Column`
# MAGIC   * `Row`
# MAGIC * Introduce the transformations...
# MAGIC   * `orderBy(..)`
# MAGIC   * `sort(..)`
# MAGIC   * `filter(..)`
# MAGIC   * `where(..)`
# MAGIC * Introduce the actions...
# MAGIC   * `collect()`
# MAGIC   * `take(n)`
# MAGIC   * `first()`
# MAGIC   * `head()`

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
# MAGIC We will be using the same data source as our previous notebook.
# MAGIC 
# MAGIC As such, we can go ahead and start by creating our initial `DataFrame`.

# COMMAND ----------

parquetFile = "/mnt/training/wikipedia/pagecounts/staging_parquet_en_only_clean/"

pagecountsEnAllDF = (spark  # Our SparkSession & Entry Point
  .read                     # Our DataFrameReader
  .parquet(parquetFile)     # Returns an instance of DataFrame
  .cache()                  # cache the data
)
print(pagecountsEnAllDF)

# COMMAND ----------

# MAGIC %md
# MAGIC Let's take another look at the number of records in our `DataFrame`

# COMMAND ----------

total = pagecountsEnAllDF.count()

print("Record Count: {0:,}".format( total ))

# COMMAND ----------

# MAGIC %md
# MAGIC Now let's take another peek at our data...

# COMMAND ----------

display(pagecountsEnAllDF)

# COMMAND ----------

# MAGIC %md
# MAGIC As we view the data, we can see that there is no real rhyme or reason as to how the data is sorted.
# MAGIC * We cannot even tell if the column **project** is sorted - we are seeing only the first 1,000 of some 2.3 million records.
# MAGIC * The column **article** is not sorted as evident by the article **A_Little_Boy_Lost** appearing between a bunch of articles starting with numbers and symbols.
# MAGIC * The column **requests** is clearly not sorted.
# MAGIC * And our **bytes_served** contains nothing but zeros.
# MAGIC 
# MAGIC So let's start by sorting our data. In doing this, we can answer the following question:
# MAGIC 
# MAGIC What are the top 10 most requested articles?

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) orderBy(..) & sort(..)
# MAGIC 
# MAGIC If you look at the API docs, `orderBy(..)` is described like this:
# MAGIC > Returns a new Dataset sorted by the given expressions.
# MAGIC 
# MAGIC Both `orderBy(..)` and `sort(..)` arrange all the records in the `DataFrame` as specified.
# MAGIC * Like `distinct()` and `dropDuplicates()`, `sort(..)` and `orderBy(..)` are aliases for each other.
# MAGIC   * `sort(..)` appealing to functional programmers.
# MAGIC   * `orderBy(..)` appealing to developers with an SQL background.
# MAGIC * Like `orderBy(..)` there are two variants of these two methods:
# MAGIC   * `orderBy(Column)`
# MAGIC   * `orderBy(String)`
# MAGIC   * `sort(Column)`
# MAGIC   * `sort(String)`
# MAGIC 
# MAGIC All we need to do now is sort our previous `DataFrame`.

# COMMAND ----------

sortedDF = (pagecountsEnAllDF
  .orderBy("requests")
)
sortedDF.show(10, False)

# COMMAND ----------

# MAGIC %md
# MAGIC As you can see, we are not sorting correctly.
# MAGIC 
# MAGIC We need to reverse the sort.
# MAGIC 
# MAGIC One might conclude that we could make a call like this:
# MAGIC 
# MAGIC `pagecountsEnAllDF.orderBy("requests desc")`
# MAGIC 
# MAGIC Try it in the cell below:

# COMMAND ----------

# Uncomment and try this:
# pagecountsEnAllDF.orderBy("requests desc")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Why does this not work?
# MAGIC * The `DataFrames` API is built upon an SQL engine.
# MAGIC * There is a lot of familiarity with this API and SQL syntax in general.
# MAGIC * The problem is that `orderBy(..)` expects the name of the column.
# MAGIC * What we specified was an SQL expression in the form of **requests desc**.
# MAGIC * What we need is a way to programmatically express such an expression.
# MAGIC * This leads us to the second variant, `orderBy(Column)` and more specifically, the class `Column`.
# MAGIC 
# MAGIC ** *Note:* ** *Some of the calls in the `DataFrames` API actually accept SQL expressions.*<br/>
# MAGIC *While these functions will appear in the docs as `someFunc(String)` it's very*<br>
# MAGIC *important to thoroughly read and understand what the parameter actually represents.*

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) The Column Class
# MAGIC 
# MAGIC The `Column` class is an object that encompasses more than just the name of the column, but also column-level-transformations, such as sorting in a descending order.
# MAGIC 
# MAGIC The first question to ask is how do I create a `Column` object?
# MAGIC 
# MAGIC In Scala we have these options:

# COMMAND ----------

# MAGIC %md
# MAGIC ** *Note:* ** *We are showing both the Scala and Python versions below for comparison.*<br/>
# MAGIC *Make sure to run only the one cell for your notebook's default language (Scala or Python)*

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC // Scala & Python both support accessing a column from a known DataFrame
# MAGIC // Uncomment this if you are using the Scala version of this notebook
# MAGIC // val columnA = pagecountsEnAllDF("requests")    
# MAGIC 
# MAGIC // This option is Scala specific, but is arugably the cleanest and easy to read.
# MAGIC val columnB = $"requests"          
# MAGIC 
# MAGIC // If we import ...sql.functions, we get a couple of more options:
# MAGIC import org.apache.spark.sql.functions._
# MAGIC 
# MAGIC // This uses the col(..) function
# MAGIC val columnC = col("requests")
# MAGIC 
# MAGIC // This uses the expr(..) function which parses an SQL Expression
# MAGIC val columnD = expr("a + 1")
# MAGIC 
# MAGIC // This uses the lit(..) to create a literal (constant) value.
# MAGIC val columnE = lit("abc")

# COMMAND ----------

# MAGIC %md
# MAGIC In Python we have these options:

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC # Scala & Python both support accessing a column from a known DataFrame
# MAGIC # Uncomment this if you are using the Python version of this notebook
# MAGIC # columnA = pagecountsEnAllDF["requests"]
# MAGIC 
# MAGIC # The $"column-name" version that works for Scala does not work in Python
# MAGIC # columnB = $"requests"      
# MAGIC 
# MAGIC # If we import ...sql.functions, we get a couple of more options:
# MAGIC from pyspark.sql.functions import *
# MAGIC 
# MAGIC # This uses the col(..) function
# MAGIC columnC = col("requests")
# MAGIC 
# MAGIC # This uses the expr(..) function which parses an SQL Expression
# MAGIC columnD = expr("a + 1")
# MAGIC 
# MAGIC # This uses the lit(..) to create a literal (constant) value.
# MAGIC columnE = lit("abc")
# MAGIC 
# MAGIC # Print the type of each attribute
# MAGIC print("columnC: {}".format(columnC))
# MAGIC print("columnD: {}".format(columnD))
# MAGIC print("columnE: {}".format(columnE))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC In the case of Scala, the cleanest version is the **$"column-name"** variant.
# MAGIC 
# MAGIC In the case of Python, the cleanest version is the **col("column-name")** variant.
# MAGIC 
# MAGIC So with that, we can now create a `Column` object, and apply the `desc()` operation to it:
# MAGIC 
# MAGIC ** *Note:* ** *We are introducing `...sql.functions` specifically for creating `Column` objects.*<br/>
# MAGIC *We will be reviewing the multitude of other commands available from this part of the API in future notebooks.*

# COMMAND ----------

column = col("requests").desc()

# Print the column type
print("column:", column)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC And now we can piece it all together...

# COMMAND ----------

sortedDescDF = (pagecountsEnAllDF
  .orderBy( col("requests").desc() )
)  
sortedDescDF.show(10, False) # The top 10 is good enough for now

# COMMAND ----------

# MAGIC %md
# MAGIC It should be of no surprise that the **Main_Page** (in both the Wikipedia and Wikimedia projects) is the most requested page.
# MAGIC 
# MAGIC Followed shortly after that is **Special:Search**, Wikipedia's search page.
# MAGIC 
# MAGIC And if you consider that this data was captured in the August before the 2016 presidential election, the Trumps will be one of the most requested pages on Wikipedia.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Review Column Class
# MAGIC 
# MAGIC The `Column` objects provide us a programmatic way to build up SQL-ish expressions.
# MAGIC 
# MAGIC Besides the `Column.desc()` operation we used above, we have a number of other operations that can be performed on a `Column` object.
# MAGIC 
# MAGIC Here is a preview of the various functions - we will cover many of these as we progress through the class:
# MAGIC 
# MAGIC **Column Functions**
# MAGIC * Various mathematical functions such as add, subtract, multiply & divide
# MAGIC * Various bitwise operators such as AND, OR & XOR
# MAGIC * Various null tests such as `isNull()`, `isNotNull()` & `isNaN()`.
# MAGIC * `as(..)`, `alias(..)` & `name(..)` - Returns this column aliased with a new name or names (in the case of expressions that return more than one column, such as explode).
# MAGIC * `between(..)` - A boolean expression that is evaluated to true if the value of this expression is between the given columns.
# MAGIC * `cast(..)` & `astype(..)` - Convert the column into type dataType.
# MAGIC * `asc(..)` - Returns a sort expression based on the ascending order of the given column name.
# MAGIC * `desc(..)` - Returns a sort expression based on the descending order of the given column name.
# MAGIC * `startswith(..)` - String starts with.
# MAGIC * `endswith(..)` - String ends with another string literal.
# MAGIC * `isin(..)` - A boolean expression that is evaluated to true if the value of this expression is contained by the evaluated values of the arguments.
# MAGIC * `like(..)` - SQL like expression
# MAGIC * `rlike(..)` - SQL RLIKE expression (LIKE with Regex).
# MAGIC * `substr(..)` - An expression that returns a substring.
# MAGIC * `when(..)` & `otherwise(..)` - Evaluates a list of conditions and returns one of multiple possible result expressions.
# MAGIC 
# MAGIC The complete list of functions differs from language to language.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC In looking at the data, we can see multiple Wikipedia projects.
# MAGIC 
# MAGIC What if we want to look at only the main Wikipedia project, **en**?
# MAGIC 
# MAGIC For that, we will need to filter out some records.

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) filter(..) & where(..)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC If you look at the API docs, `filter(..)` and `where(..)` are described like this:
# MAGIC > Filters rows using the given condition.
# MAGIC 
# MAGIC Both `filter(..)` and `where(..)` return a new dataset containing only those records for which the specified condition is true.
# MAGIC * Like `distinct()` and `dropDuplicates()`, `filter(..)` and `where(..)` are aliases for each other.
# MAGIC   * `filter(..)` appealing to functional programmers.
# MAGIC   * `where(..)` appealing to developers with an SQL background.
# MAGIC * Like `orderBy(..)` there are two variants of these two methods:
# MAGIC   * `filter(Column)`
# MAGIC   * `filter(String)`
# MAGIC   * `where(Column)`
# MAGIC   * `where(String)`
# MAGIC * Unlike `orderBy(String)` which requires a column name, `filter(String)` and `where(String)` both expect an SQL expression.
# MAGIC 
# MAGIC Let's start by looking at the variant using an SQL expression:

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### filter(..) & where(..) w/SQL Expression

# COMMAND ----------

whereDF = (sortedDescDF
  .where( "project = 'en'" )
)
whereDF.show(10, False)

# COMMAND ----------

# MAGIC %md
# MAGIC Now that we are only looking at the main Wikipedia articles, we get a better picture of the most popular articles on Wikipedia.
# MAGIC 
# MAGIC Next, let's take a look at the second variant that takes a `Column` object as its first parameter:

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### filter(..) & where(..) w/Column

# COMMAND ----------

filteredDF = (sortedDescDF
  .filter( col("project") == "en")
)
filteredDF.show(10, False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### A Scala Issue...

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC With Python, this is pretty straight forward.
# MAGIC 
# MAGIC But in Scala... notice anything unusual in that last command?
# MAGIC 
# MAGIC **Question:** In most every programming language, what is a single equals sign (=) used for?
# MAGIC 
# MAGIC **Question:** What are two equal signs (==) used for?
# MAGIC 
# MAGIC **Question:** 
# MAGIC * Considering that transformations are lazy...
# MAGIC * And the == operator executes now...
# MAGIC * And `filter(..)` and `where(..)` require us to pass a `Column` object...
# MAGIC * What would be wrong with `$"project" == "en"`?
# MAGIC 
# MAGIC Try it...

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC $"project" == "en"

# COMMAND ----------

# MAGIC %md
# MAGIC Compare that to the following call...

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC $"project" === "en"

# COMMAND ----------

# MAGIC %md
# MAGIC Let's take a look at the Scala Doc for the `Column` object. </br>
# MAGIC 
# MAGIC | "Operator" | Function |
# MAGIC |:----------:| -------- |
# MAGIC | === | Equality test |
# MAGIC | !== | Deprecated inequality test |
# MAGIC | =!= | Inequality test |
# MAGIC | <=> | Null safe equality test |

# COMMAND ----------

# MAGIC %md
# MAGIC ### The Solution...

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC With that behind us, we can clearly **see** the top ten most requested articles.
# MAGIC 
# MAGIC But what if we need to **programmatically** extract the value of the most requested article's name and its number of requests?
# MAGIC 
# MAGIC That is to say, how do we get the first record, and from there...
# MAGIC * the value of the second column, **article**, as a string...
# MAGIC * the value of the third column, **requests**, as an integer...
# MAGIC 
# MAGIC Before we proceed, let's apply another filter to get rid of **Main_Page** and anything starting with **Special:** - they're just noise to us.

# COMMAND ----------

articlesDF = (filteredDF
  .drop("bytes_served")
  .filter( col("article") != "Main_Page")
  .filter( col("article") != "-")
  .filter( col("article").startswith("Special:") == False)
)
articlesDF.show(10, False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) first() & head()
# MAGIC 
# MAGIC If you look at the API docs, both `first(..)` and `head(..)` are described like this:
# MAGIC > Returns the first row.
# MAGIC 
# MAGIC Just like `distinct()` & `dropDuplicates()` are aliases for each other, so are `first(..)` and `head(..)`.
# MAGIC 
# MAGIC However, unlike `distinct()` & `dropDuplicates()` which are **transformations** `first(..)` and `head(..)` are **actions**.
# MAGIC 
# MAGIC Once all processing is done, these methods return the object backing the first record.
# MAGIC 
# MAGIC In the case of `DataFrames` (both Scala and Python) that object is a `Row`.
# MAGIC 
# MAGIC In the case of `Datasets` (the strongly typed version of `DataFrames` in Scala and Java), the object may be a `Row`, a `String`, a `Customer`, a `PendingApplication` or any number of custom objects.
# MAGIC 
# MAGIC Focusing strictly on the `DataFrame` API for now, let's take a look at a call with `head()`:

# COMMAND ----------

firstRow = articlesDF.first()

print(firstRow)

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) The Row Class
# MAGIC 
# MAGIC Now that we have a reference to the object backing the first row (or any row), we can use it to extract the data for each column.
# MAGIC 
# MAGIC Before we do, let's take a look at the API docs for the `Row` class.
# MAGIC 
# MAGIC At the heart of it, we are simply going to ask for the value of the object in column N via `Row.get(i)`.
# MAGIC 
# MAGIC Python being a loosely typed language, the return value is of no real consequence.
# MAGIC 
# MAGIC However, Scala is going to return an object of type `Any`. In Java, this would be an object of type `Object`.
# MAGIC 
# MAGIC What we need (at least for Scala), especially if the data type matters in cases of performing mathematical operations on the value, we need to call one of the other methods:
# MAGIC * `getAs[T](i):T`
# MAGIC * `getDate(i):Date`
# MAGIC * `getString(i):String`
# MAGIC * `getInt(i):Int`
# MAGIC * `getLong(i):Long`
# MAGIC 
# MAGIC We can now put it all together to get the number of requests for the most requested project:

# COMMAND ----------

article = firstRow['article']
total = firstRow['requests']

print("Most Requested Article: \"{0}\" with {1:,} requests".format( article, total ))

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) collect()
# MAGIC 
# MAGIC If you look at the API docs, `collect(..)` is described like this:
# MAGIC > Returns an array that contains all of Rows in this Dataset.
# MAGIC 
# MAGIC `collect()` returns a collection of the specific type backing each record of the `DataFrame`.
# MAGIC * In the case of Python, this is always the `Row` object.
# MAGIC * In the case of Scala, this is also a `Row` object.
# MAGIC * If the `DataFrame` was converted to a `Dataset` the backing object would be the user-specified object.
# MAGIC 
# MAGIC Building on our last example, let's take the top 10 records and print them out.

# COMMAND ----------

rows = (articlesDF
  .limit(10)           # We only want the first 10 records.
  .collect()           # The action returning all records in the DataFrame
)

# rows is an Array. Now in the driver, 
# we can just loop over the array and print 'em out.

listItems = ""
for row in rows:
  project = row['article']
  total = row['requests']
  listItems += "    <li><b>{}</b> {:0,d} requests</li>\n".format(project, total)
  
html = """
<body>
  <h1>Top 10 Articles</h1>
  <ol>
    %s
  </ol>
</body>
""" % (listItems.strip())

print(html)

# UNCOMMENT FOR A PRETTIER PRESENTATION
# displayHTML(html)

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) take(n)
# MAGIC 
# MAGIC If you look at the API docs, `take(n)` is described like this:
# MAGIC > Returns the first n rows in the Dataset.
# MAGIC 
# MAGIC `take(n)` returns a collection of the first N records of the specific type backing each record of the `DataFrame`.
# MAGIC * In the case of Python, this is always the `Row` object.
# MAGIC * In the case of Scala, this is also a `Row` object.
# MAGIC * If the `DataFrame` was converted to a `Dataset` the backing object would be the user-specified object.
# MAGIC 
# MAGIC In short, it's the same basic function as `collect()` except you specify as the first parameter the number of records to return.

# COMMAND ----------

rows = articlesDF.take(10)

# rows is an Array. Now in the driver, 
# we can just loop over the array and print 'em out.

listItems = ""
for row in rows:
  project = row['article']
  total = row['requests']
  listItems += "    <li><b>{}</b> {:0,d} requests</li>\n".format(project, total)
  
html = """
<body>
  <h1>Top 10 Articles</h1>
  <ol>
    %s
  </ol>
</body>
""" % (listItems.strip())

print(html)

# UNCOMMENT FOR A PRETTIER PRESENTATION
# displayHTML(html)

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) DataFrame vs Dataset
# MAGIC 
# MAGIC We've been alluding to `Datasets` off and on. 
# MAGIC 
# MAGIC The following example demonstrates how to convert a `DataFrame` to a `Dataset`.
# MAGIC 
# MAGIC And when compared to the previous example, helps to illustrate the difference/relationship between the two.
# MAGIC 
# MAGIC ** *Note:* ** *As a reminder, `Datasets` are a Java and Scala concept and brings to those languages the type safety that *<br/>
# MAGIC *is lost with `DataFrame`, or rather, `Dataset[Row]`. Python and R have no such concept because they are loosely typed.*

# COMMAND ----------

# MAGIC %md
# MAGIC Before we demonstrate this, let's review all our transformations:

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC val parquetFile = "/mnt/training/wikipedia/pagecounts/staging_parquet_en_only_clean/"
# MAGIC 
# MAGIC val articlesDF = spark                          // Our SparkSession & Entry Point
# MAGIC   .read                                         // Our DataFrameReader
# MAGIC   .parquet(parquetFile)                         // Creates a DataFrame from a parquet file
# MAGIC   .filter( $"project" === "en")                 // Include only the "en" project
# MAGIC   .filter($"article" =!= "Main_Page")           // Exclude the Wikipedia Main Page
# MAGIC   .filter($"article" =!= "-")                   // Exclude some "weird" article
# MAGIC   .filter( ! $"article".startsWith("Special:")) // Exclude all the "special" articles
# MAGIC   .drop("bytes_served")                         // We just don't need this column
# MAGIC   .orderBy( $"requests".desc )                  // Sort by requests descending

# COMMAND ----------

# MAGIC %md
# MAGIC Notice above that `articlesDF` is a `Dataset` of type `Row`.
# MAGIC 
# MAGIC Next, create the case class `WikiReq`. 
# MAGIC 
# MAGIC A little later we can convert this `DataFrame` to a `Dataset` of type `WikiReq`:

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC // the name and data type of the case class must match the schema they will be converted from.
# MAGIC case class WikiReq (project:String, article:String, requests:Int)
# MAGIC 
# MAGIC articlesDF.printSchema

# COMMAND ----------

# MAGIC %md
# MAGIC Instead of the `Row` object, we can now back each record with our new `WikiReq` class.
# MAGIC 
# MAGIC And we can see the conversion from `DataFrames` to `Datasets` here:

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC val articlesDS = articlesDF.as[WikiReq]

# COMMAND ----------

# MAGIC %md
# MAGIC Make note of the data type: **org.apache.spark.sql.Dataset[WikiReq]**
# MAGIC 
# MAGIC Compare that to a `DataFrame`: **org.apache.spark.sql.Dataset[Row]**
# MAGIC 
# MAGIC Now when we ask for the first 10, we won't get an array of `Row` objects but instead an array of `WikiReq` objects:

# COMMAND ----------

# MAGIC %scala
# MAGIC val wikiReqs = articlesDS.take(10)
# MAGIC 
# MAGIC // wikiReqs is an Array of WikiReqs. Now in the driver, 
# MAGIC // we can just loop over the array and print 'em out.
# MAGIC 
# MAGIC var listItems = ""
# MAGIC for (wikiReq <- wikiReqs) {
# MAGIC   // Notice how we don't relaly need temp variables?
# MAGIC   // Or more specifically, we don't need to cast.
# MAGIC   listItems += "    <li><b>%s</b> %,d requests</li>%n".format(wikiReq.article, wikiReq.requests)
# MAGIC }
# MAGIC 
# MAGIC var html = s"""
# MAGIC <body>
# MAGIC   <h1>Top 10 Articles</h1>
# MAGIC   <ol>
# MAGIC     ${listItems.trim()}
# MAGIC   </ol>
# MAGIC </body>
# MAGIC """
# MAGIC 
# MAGIC println(html)
# MAGIC println("-"*80)
# MAGIC 
# MAGIC // UNCOMMENT FOR A PRETTIER PRESENTATION
# MAGIC // displayHTML(html)

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/labs.png) Data Frames Lab #2
# MAGIC It's time to put what we learned to practice.
# MAGIC 
# MAGIC Go ahead and open the notebook [Introduction to DataFrames, Lab #2]($./Intro To DF Part 2 Lab) and complete the exercises.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>