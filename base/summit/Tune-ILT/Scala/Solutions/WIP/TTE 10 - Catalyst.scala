// Databricks notebook source
// MAGIC 
// MAGIC %md-sandbox
// MAGIC 
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC # Catalyst
// MAGIC 
// MAGIC ## Catalyst Anti-patterns + Optimizations
// MAGIC 
// MAGIC We will begin by reviewing some anti-patterns that can hurt application performance and prevent Catalyst optimizations (User defined functions and Cartesian products), as well as how to customize Catalyst rules.

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) User defined functions
// MAGIC 
// MAGIC You can write your own function to apply to columns of DataFrames in Spark (known as a UDF).
// MAGIC 
// MAGIC However, UDFs require deserialization of data stored under the Tungsten format. The data needs to be available as an object in an executor so the UDF function can be applied to it. 
// MAGIC 
// MAGIC Below is an example of a UDF which takes a string and converts it to upper/lower case:

// COMMAND ----------

val initDF = spark
            .read
            .option("header", "true")
            .option("inferSchema", "true")
            .option("sep", ":")
            .csv("/mnt/training/dataframes/people-with-header-10m.txt")

// COMMAND ----------

import org.apache.spark.sql.functions.{udf}

val upperUDF = udf((s: String) => s.toUpperCase())


val lowerUDF = udf((s: String) => s.toLowerCase())


val udfDF = initDF.select(upperUDF(initDF("firstName")),
                          lowerUDF(initDF("middleName")), 
                          upperUDF(initDF("lastName")), 
                          lowerUDF(initDF("gender"))).distinct()

display(udfDF)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC But:
// MAGIC 
// MAGIC - Numerous utility functions for DataFrames and Datasets are already available in spark. 
// MAGIC - These functions are located in the <a href="https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#module-pyspark.sql.functions">functions package</a> in spark under `org.apache.spark.sql.functions` or `pyspark.sql.functions`
// MAGIC 
// MAGIC Using built in function usage is preferred over coding UDFs:
// MAGIC - As built-in functions are integrated with Catalyst, they can be optimized in ways in which UDFs cannot. 
// MAGIC - Built-in functions can benefit from code-gen and can also manipulate our dataset even when it's serialized using the Tungsten format without `serialization / deserialization` overhead.
// MAGIC - Python UDFs carry extra overhead as they require additional serialization from the driver VM to the executor's JVM. 
// MAGIC  
// MAGIC Below is an example of using the built in `lower` and `upper` functions. 

// COMMAND ----------

import org.apache.spark.sql.functions.{lower,upper}

val noUDF = initDF.select(upper($"firstName"), lower($"middleName"), upper($"lastName"), lower($"gender")).distinct()

display(noUDF)

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Cartesian Products
// MAGIC 
// MAGIC Put simply, a Cartesian product is a set that contains all possible combinations of elements from two other sets. Related to Spark SQL this can be a table that contains all combinations of rows from two other tables, that were joined together.
// MAGIC 
// MAGIC Cartesian products are problematic as they are a sign of an expensive computation.
// MAGIC 
// MAGIC First, let's force the broadcast join threshold very low, just to ensure no side of the join is broadcast. (We do this for demo purposes.)

// COMMAND ----------

val previous_threshold = spark.conf.get("spark.sql.autoBroadcastJoinThreshold")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "0")

// COMMAND ----------

import spark.implicits._
val numbDF = Seq(
  (1),
  (1),
  (3)
).toDF("n1")

val numbDF2 = Seq(
  (4),
  (5),
  (6)
).toDF("n2")

val cartesianDF = numbDF.crossJoin(numbDF2)
cartesianDF.explain()

// COMMAND ----------

// MAGIC %md
// MAGIC Let's reset the `spark.sql.autoBroadcastJoinThreshold` value to its default.

// COMMAND ----------

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", previous_threshold)
print("Restored broadcast join threshold to {0}".format(spark.conf.get("spark.sql.autoBroadcastJoinThreshold")))

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Let's work through an example use-case. Our dataset contains a JSON file of IP address ranges allocated to various countries (`country_ip_ranges.json`), as well as various IP addresses of interesting transactions (`transaction_ips.json`). We want to work out the country code of a transaction's IP address. This can be accomplished using a ranged query to check if the transaction's address falls between one of the known ranges.
// MAGIC 
// MAGIC First, let's inspect our dataset.

// COMMAND ----------

val ipRangesDF = spark.read.json("/mnt/training/dataframes/country_ip_ranges.json")
val transactionDF = spark.read.json("/mnt/training/dataframes/transaction_ips.json")

ipRangesDF.show(5)
transactionDF.show(5)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Next we want to run the query and check what range contains the transactions' IP addresses, and, thus, the country where the transaction occurred.

// COMMAND ----------

val ipByCountry = transactionDF.join(
  ipRangesDF, 
  (transactionDF("ip_decimal") >= ipRangesDF("start")) && (transactionDF("ip_decimal") <= ipRangesDF("end"))
)

ipByCountry.show(5)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Since we are expecting only the first partition using `show()` the result is rendered almost instantly. We can review what type of join resulted from the operation using the `explain()` function to render the physical plan.

// COMMAND ----------

ipByCountry.explain()

// COMMAND ----------

ipByCountry.count()

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC As expected, the resulting join was a `CartesianProduct`. One easy optimization is to pick the smaller of the two DataFrames and hint at broadcasting it, to instead achieve a `BroadcastNestedLoopJoin` instead.

// COMMAND ----------

import org.apache.spark.sql.functions._

// estimate DF size based on #of elements
print("ipRangesDF: " + (ipRangesDF.columns.size * ipRangesDF.count()))
print("transactionDF: " + (ipRangesDF.columns.size * transactionDF.count()))

// broadcast the smaller df
val ipByCountryBroadcasted = ipRangesDF.join(
  broadcast(transactionDF), 
  (transactionDF("ip_decimal") >= ipRangesDF("start")) && (transactionDF("ip_decimal") <= ipRangesDF("end"))
)
ipByCountryBroadcasted.show(5)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Let's review the type of join resulting from the broadcast hint.

// COMMAND ----------

ipByCountryBroadcasted.explain()

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Datasets: The DataFrame Query Language vs. Lambdas
// MAGIC 
// MAGIC The Dataset API gives you the option to use both the DataFrame query language _and_ RDD-like lambda transformations.
// MAGIC 
// MAGIC **NOTE**: The Dataset API is not available in Python, so this section is in Scala.

// COMMAND ----------

// MAGIC %scala
// MAGIC case class Person(id: Integer, firstName: String, middleName: String, lastName: String, gender: String, birthDate: String, ssn: String, salary: String)
// MAGIC 
// MAGIC val personDS = spark
// MAGIC                 .read
// MAGIC                 .option("header", "true")
// MAGIC                 .option("inferSchema", "true")
// MAGIC                 .option("delimiter", ":")
// MAGIC                 .csv("/mnt/training/dataframes/people-with-header-10m.txt")
// MAGIC                 .as[Person]

// COMMAND ----------

// MAGIC %scala
// MAGIC // DataFrame query DSL
// MAGIC println(personDS.filter($"firstName" === "Nell").distinct().count)

// COMMAND ----------

// MAGIC %scala
// MAGIC // Dataset, with a lambda
// MAGIC println(personDS.filter(x => x.firstName == "Nell").distinct().count)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### Tungsten Encoders' effect on Catalyst Optimization
// MAGIC 
// MAGIC The Domain Specific Language (DSL) used by DataFrames and DataSets allows for data manipulation without having to deserialize that data from the Tungsten format. 
// MAGIC 
// MAGIC <img src="https://files.training.databricks.com/images/tuning/dsl-lambda.png" alt="Lambda serialization overhead"/><br/>
// MAGIC 
// MAGIC The advantage of this is that we avoid any *serialization / deserialization* overhead. <br/>
// MAGIC Datasets give users the ability to carry out data manipulation through lambdas which can be very powerful, especially with semi-structured data. The **downside** of lambda is that they can't directly work with the Tungsten format, thus deserialization is required adding an overhead to the process.
// MAGIC 
// MAGIC Avoiding frequent jumps between DSL and closures would mean that the *serialization / deserialization* to and from Tungsten format would be reduced, leading to a performance gain.

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC 
// MAGIC - Advantages of using lambdas:
// MAGIC     - Good for semi-structured data
// MAGIC     - Very powerful
// MAGIC - Disadvantages:
// MAGIC     - Catalyst can't interpret lambdas until runtime. 
// MAGIC     - Lambdas are opaque to Catalyst. Since it doesn't know what a lambda is doing, it can't move it elsewhere in the processing.
// MAGIC     - Jumping between lambdas and the DataFrame query API can hurt performance.
// MAGIC     - Working with lambdas means that we need to `deserialize` from Tungsten's format to an object and then reserialize back to Tungsten format when the lambda is done.
// MAGIC     
// MAGIC If you _have_ to use lambdas, chaining them together can help.
// MAGIC 
// MAGIC <img src="https://files.training.databricks.com/images/tuning/interleaved-lambdas.png" alt="Interleaved Lambdas" style="border: 1px solid #cccccc; margin: 20px"/>
// MAGIC <img src="https://files.training.databricks.com/images/tuning/chained-lambdas.png" alt="Chained Lambdas" style="border: 1px solid #cccccc; margin: 20px"/><br/>

// COMMAND ----------

// MAGIC %scala
// MAGIC // define the year 40 years ago for the below query
// MAGIC import java.util.Calendar
// MAGIC val earliestYear = Calendar.getInstance.get(Calendar.YEAR) - 40
// MAGIC 
// MAGIC personDS.filter(x => x.birthDate.split("-")(0).toInt > earliestYear) // everyone above 40
// MAGIC         .filter($"salary" > 80000) // everyone earning more than 80K
// MAGIC         .filter(x => x.lastName.startsWith("J")) // last name starts with J
// MAGIC         .filter($"firstName".startsWith("D")) // first name starts with D
// MAGIC         .count()

// COMMAND ----------

// MAGIC %scala
// MAGIC import org.apache.spark.sql.functions._
// MAGIC 
// MAGIC personDS.filter(year($"birthDate") > earliestYear) // everyone above 40
// MAGIC         .filter($"salary" > 80000) // everyone earning more than 80K
// MAGIC         .filter($"lastName".startsWith("J")) // last name starts with J
// MAGIC         .filter($"firstName".startsWith("D")) // first name starts with D
// MAGIC         .count()

// COMMAND ----------

// MAGIC %md
// MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Custom Optimizers
// MAGIC 
// MAGIC As we know Catalyst is the component of spark that optimizes our code. This is achieved through rules that help spark understand that, for example, changing execution order of transformations might lead to better performance overall. The typical example of this scenario is filtering data as early as possible before processing it with spark. 
// MAGIC 
// MAGIC Custom optimizations can be added to Catalyst by extending `Rule`. Additional conditions for operations can be specified. 
// MAGIC 
// MAGIC One example of an optimization could be making **multiplication by 0** directly return 0 and avoiding the arithmetic operation all together. Before we create an optimization, lets review the physical plan of a DataFrame.
// MAGIC 
// MAGIC **NOTE**: Custom optimizers are not currently available in Python, so this section is entirely in Scala.

// COMMAND ----------

// MAGIC %scala
// MAGIC val df = spark.read.parquet("/mnt/training/ssn/names.parquet/")
// MAGIC val dfRegular = df.select($"year" * 0)
// MAGIC dfRegular.explain()
// MAGIC dfRegular.show(5)

// COMMAND ----------

// MAGIC %scala
// MAGIC import org.apache.spark.sql.catalyst.expressions.{Add, Cast, Literal, Multiply}
// MAGIC import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
// MAGIC import org.apache.spark.sql.catalyst.rules.Rule
// MAGIC import org.apache.spark.sql.types.IntegerType

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Next we need to extend Rule and implement our own `apply` function. The `apply` function takes a `LogicalPlan` of operations as the parameter and can operate on various operations. 
// MAGIC 
// MAGIC One example of these operations is `Multiply` which takes two parameters:
// MAGIC  1. `left` is an <a href="https://github.com/apache/spark/blob/master/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/namedExpressions.scala#L212">`AttributeReference`</a> which holds metadata and acts as a unqie way of identifying the data
// MAGIC  2. `right` is the `Literal` that is being applied during the operation, aka the number we're multiplying by.
// MAGIC  
// MAGIC  The logic of the rule is to check if the `Literal == 0` and if so, to just return a 0.

// COMMAND ----------

// MAGIC %scala
// MAGIC object MultiplyBy0Rule extends Rule[LogicalPlan] {
// MAGIC   override def apply(plan: LogicalPlan): LogicalPlan = {
// MAGIC     plan transformAllExpressions {
// MAGIC       case Multiply(left,right) if right.asInstanceOf[Literal].value.asInstanceOf[Int] == 0 => {
// MAGIC         println("Multiplication by 0 detected. Optimization Applied.")
// MAGIC         // return 0, we need to cast our 0 into a literal using an in-built spark function called Cast.
// MAGIC         Cast(Literal(0), IntegerType)
// MAGIC       }
// MAGIC     }
// MAGIC   }
// MAGIC }

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Catalyst needs to be made aware of the new rule. This can be done via the `SparkSession`'s `experimental` functionality.

// COMMAND ----------

// MAGIC %scala
// MAGIC spark.experimental.extraOptimizations = Seq(MultiplyBy0Rule)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Finally, lets review the physical plan that is generated after the optimization is applied when **multiplying by 0**.

// COMMAND ----------

// MAGIC %scala
// MAGIC val df = spark.read.parquet("/mnt/training/ssn/names.parquet/")
// MAGIC val dfZero = df.select($"year" * 0)
// MAGIC dfZero.explain()
// MAGIC dfZero.show(5)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC  - No optimization:   `*Project [(year#399 * 0) AS (year * 0)#405]`
// MAGIC  - With rule applied: `*Project [cast(0 as int) AS (year * 0)#421]`
// MAGIC 
// MAGIC Notice that in the optimized version, 0 is just selected as a literal, without carrying out the arithmetic.

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>