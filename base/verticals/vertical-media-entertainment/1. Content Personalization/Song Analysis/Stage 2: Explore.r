# Databricks notebook source
# MAGIC %md # 2. Exploring songs data
# MAGIC 
# MAGIC ![Explore](http://training.databricks.com/databricks_guide/end-to-end-02.png)
# MAGIC 
# MAGIC 
# MAGIC This is the second notebook in this tutorial. In this notebook we do what any data scientist does with their data right after parsing it: exploring and understanding different aspects of data. 
# MAGIC 
# MAGIC Make sure you understand how we get the `songsTable` by reading and running the ETL notebook. In the ETL notebook we created and cached a temporary table named `songsTable`.

# COMMAND ----------

# MAGIC %md ## A first inspection

# COMMAND ----------

# MAGIC %md A first step to any data exploration is viewing sample data. For this purpose we can use a simple SQL query that returns first 10 rows.

# COMMAND ----------

# MAGIC %sql select * from songsTable limit 10

# COMMAND ----------

# MAGIC %md We can achieve the same thing with SparkR.
# MAGIC 
# MAGIC Let's create a SparkDataFrame from the songsTable table and view the first 10 rows.

# COMMAND ----------

df <- tableToDF("songsTable")
display(head(df, 10))

# COMMAND ----------

# MAGIC %md We can view the schema of our dataset. This will show us what columns and data types we are working with.

# COMMAND ----------

printSchema(df)

# COMMAND ----------

# MAGIC %md Another important first step is know how much data we are dealing with

# COMMAND ----------

nrow(df)

# COMMAND ----------

# MAGIC %md ## Applying a function to every group of your data
# MAGIC 
# MAGIC In this example, we will try to find out what's the average song duration by year using `gapply()`.  
# MAGIC gapply() applies an R function to each group in your dataset.
# MAGIC 
# MAGIC Other UDF functions available:
# MAGIC   - dapply() applies an R function to each partition in your dataset.
# MAGIC   - spark.lapply() runs an R function over a list of elements
# MAGIC   
# MAGIC You can read more about these UDF functions in the [programming guide](http://spark.apache.org/docs/latest/sparkr.html#applying-user-defined-function).

# COMMAND ----------

schema <- structType(structField("year", "integer"), structField("mean_duration", "double"))

result <- gapply(df,
                 "year",
                  function(key, x) {
                    y <- data.frame(key, mean(x$duration))
                  },
                  schema)

# COMMAND ----------

head(result)

# COMMAND ----------

# MAGIC %md ## Exercises
# MAGIC 
# MAGIC 1. Try out dapply(). You could start off with something simple like multiplying duration by 10.  
# MAGIC   *hints*:
# MAGIC     - Repartition your SparkDataFrame with `repartition()`.
# MAGIC     - Output of your function in dapply() should be an R data.frame.
# MAGIC     - dapply() requires a defined schema.

# COMMAND ----------

# Enter your code here!


# COMMAND ----------

# MAGIC %md ## Summarizing and Visualizing data

# COMMAND ----------

# MAGIC %md We have quite a few data points in our table. It is not feasible to visualize every data point, so we do the obvious: summarizing data and graphing the summary.
# MAGIC 
# MAGIC An interesting question is how different parameters of songs change over time. We will try and visualize the data to find out how average song durations changed over time.

# COMMAND ----------

# MAGIC %md To simplify our syntax we can use the `magrittr` package. With `magrittr` we can write code in a pipeline manner (using `%>%`). The package will translate our code into nested function call. For more information about `magritter` visit their documentation [here](https://cran.r-project.org/web/packages/magrittr/vignettes/magrittr.html).  
# MAGIC For our visualization we will be using `ggplot2`. Both these packages are installed by default on Databricks and we just need to import them.

# COMMAND ----------

library(magrittr)
library(ggplot2)
options(repr.plot.height = 400) # Setting default plot height to 400 pixels

# COMMAND ----------

str(df)

# COMMAND ----------

durationByYear <- df %>% group_by("year") %>% avg("duration") %>% withColumnRenamed("avg(duration)", "duration")

# COMMAND ----------

# MAGIC %md We can peak the first six rows of the summary by simply using `head`. Note that this function is being applied to a SparkR DataFrame

# COMMAND ----------

head(durationByYear)

# COMMAND ----------

# MAGIC %md We can plot the results using ggplot now. Note that we are creating a R data frame by running `collect()` before passing it to ggplot. We can call `collect()` when the results of your SparkDataFrame fits on a single machine.
# MAGIC 
# MAGIC ggplot is from an R library and takes in R data frames as input. It does not recognize SparkR DataFrame objects.

# COMMAND ----------

ggplot(collect(durationByYear), aes(year, duration)) + geom_point() + geom_line(color = "blue") + theme_bw()

# COMMAND ----------

# MAGIC %md The resulting graph is somewhat unusual. We do not expect to have a data point for year 0. But it is real data and there are always outliers (as a result of bad measurement or other sources of error). In this case we are going to ignore the outlier point by limiting our query to years greater than 1930.

# COMMAND ----------

durationByYear <- df %>% subset(.$year > 1930) %>% group_by("year") %>% avg("duration") %>% withColumnRenamed("avg(duration)", "duration")
ggplot(collect(durationByYear), aes(year, duration)) + geom_point() + geom_line(color = "blue") + theme_bw()

# COMMAND ----------

# MAGIC %md ## Exercises
# MAGIC 
# MAGIC 1. How did average loudness change over time?
# MAGIC 2. How did tempo change over time?

# COMMAND ----------

# Add your code here!


# COMMAND ----------

# MAGIC %md ## Sampling and Visualizing
# MAGIC 
# MAGIC Another technique for visually exploring large data, which we are going to try, is sampling data. First step is generating a sample.

# COMMAND ----------

sampled <- df %>% subset(.$year > 1930) %>% sample(withReplacement = F, fraction = 0.005)

# COMMAND ----------

# MAGIC %md With sampled data we can produce a scatter plot as follows.

# COMMAND ----------

ggplot(collect(sampled), aes(year, duration)) + geom_point(size = 0.5, alpha = 0.3) + geom_smooth() + theme_bw()

# COMMAND ----------

# MAGIC %md ## Exercises
# MAGIC 
# MAGIC 1. Plot sampled points for other parameters in the data.

# COMMAND ----------

# Add your code here!


# COMMAND ----------

# MAGIC %md Next step is modeling the data. Click on the next notebook (Model) to follow the tutorial.