# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ![Content Personalization](https://marketingland.com/wp-content/ml-loads/2016/12/EZS_1610_Personlztn-dmd-1920.jpg)
# MAGIC 
# MAGIC **Business case:**  
# MAGIC Content personalization of user preference in their choice of songs.
# MAGIC   
# MAGIC **Problem Statement:**  
# MAGIC Datasets are getting larger by day. It is challenging to use single machine tools like R to parse them and it takes time to learn big data technologies.
# MAGIC   
# MAGIC **Business solution:**  
# MAGIC Use the SparkR DataFrames API to process large datasets in R-like syntax. This allows data scientists and analysts to use a language they are familiar with and get up to speed quickly with Spark.
# MAGIC   
# MAGIC **Technical Solution:**  
# MAGIC Use Databricks to pull large datasets from various sources like S3, and use SparkR to ETL data into a usable format.

# COMMAND ----------

# MAGIC %md
# MAGIC ###0. SETUP -- Databricks Spark cluster:  
# MAGIC 
# MAGIC 1. **Create** a cluster by...  
# MAGIC   - Click the `Clusters` icon on the left sidebar and then `Create Cluster.` OR Click [Here](https://demo.cloud.databricks.com/#clusters/create) 
# MAGIC   - Enter any text, i.e `demo` into the cluster name text box
# MAGIC   - Select the `Apache Spark Version` value `Spark 2.2 (auto-updating scala 2.11)`  
# MAGIC   - Click the `create cluster` button and wait for your cluster to be provisioned
# MAGIC 3. **Attach** this notebook to your cluster by...   
# MAGIC   - Click on your cluster name in menu `Detached` at the top left of this workbook to attach it to this workbook 

# COMMAND ----------

# DBTITLE 1,Step1: Ingest Data to Notebook
# MAGIC %md 
# MAGIC 
# MAGIC - We use internal generated dataset of songs database
# MAGIC - Text data files are stored in `dbfs:/databricks-datasets/songs/data-002` 
# MAGIC You can conveniently list files on the distributed file system (DBFS, S3) using `%fs` commands.

# COMMAND ----------

# MAGIC %fs ls /databricks-datasets/songs/data-002/

# COMMAND ----------

# MAGIC %md As you can see in the listing we have data files and a single header file. The header file seems interesting and worth a first inspection of it first. The file is 318 bytes, therefore it is safe to read the entire content of the file in the notebook. 

# COMMAND ----------

readChar("/dbfs/databricks-datasets/songs/data-002/header.txt", 318)

# COMMAND ----------

# MAGIC %md As seen above each line in the header consists of a name and a type separated by colon. We will need to parse the header file as follows:

# COMMAND ----------

header <- read.table("/dbfs/databricks-datasets/songs/data-002/header.txt", sep = ":", header = F, stringsAsFactors = F)
head(header)

# COMMAND ----------

str(header)

# COMMAND ----------

# MAGIC %md Now we turn to data files. First, step is inspecting the first line of data to inspect its format.

# COMMAND ----------

readChar("/dbfs/databricks-datasets/songs/data-002/part-00000", 512)

# COMMAND ----------

# MAGIC %md Each line of data consists of multiple fields separated by `\t`. With that information and what we learned from the header file, we set out to parse our data. To do so, we build a function that takes a line of text and returns an array of parsed fields.
# MAGIC * If header indicates the type is int, we cast the token to integer
# MAGIC * If header indicates the type is double, we cast the token to float
# MAGIC * Otherwise we return the string

# COMMAND ----------

library(SparkR)

splitHeader <- split(header, seq(nrow(header)))
getType <- function(typeStr) {
  if (typeStr == "int") {
    return("integer")
  } else {
    return(typeStr)
  }
}
rawSchema <- lapply(splitHeader, function(l) structField(l$V1, getType(l$V2)))
names(rawSchema) <- "x"
schema <- do.call(structType, rawSchema)
schema

# COMMAND ----------

# Create SparkR DataFrame. This is the distributed DataFrame
df <- read.df( path = "/databricks-datasets/songs/data-002/part-*", source = "csv", schema = schema, delimiter = "\t")

# COMMAND ----------

# DataFrame is a distributed SparkDataFrame
str(df)

# COMMAND ----------

# data.frame is a local R dataframe
str(header)

# COMMAND ----------

# Databricks built-in visualization: display()
display(df)

# COMMAND ----------

# MAGIC %md We can now cache our table. So far all operations have been lazy. This is the first time Spark will attempt to actually read all our data and apply the transformations. 

# COMMAND ----------

registerTempTable(df, "songsTable")

# COMMAND ----------

# MAGIC %sql cache table songsTable

# COMMAND ----------

# MAGIC %md From now on we can easily query our data using the temporary table we just created and cached in memory. Since it is registered as a table we can conveniently use SQL as well as Spark API to access it.

# COMMAND ----------

# MAGIC %sql select * from songsTable limit 10

# COMMAND ----------

# DBTITLE 1,Step2: Explore Songs Data 
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

# MAGIC %md
# MAGIC ###Step 3: Visualization
# MAGIC - We have quite a few data points in our table. It is not feasible to visualize every data point, so we do the obvious: summarizing data and graphing the summary.
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

# MAGIC %md #### Step 4: Model creation
# MAGIC * In this step we attempt to gain deeper understanding of our data. We are going to build a simple model that predicts loudness of songs. 

# COMMAND ----------

songs <- tableToDF("songsTable")

# COMMAND ----------

display(df %>% subset(.$year > 1930))

# COMMAND ----------

# MAGIC %md We will use songs before 2005 as training and then test on more recent data points.

# COMMAND ----------

training <- df %>% subset(.$year < 2008)
testing <- df %>% subset(.$year >= 2008)

# COMMAND ----------

# MAGIC %md First we sample and display the sample to get a table and preview column names

# COMMAND ----------

library(magrittr)

# sample(x, withReplacement, fraction)
training %>% sample(F, 0.001) %>% display

# COMMAND ----------

# MAGIC %md 
# MAGIC We are going to use following variable to predict `loudness`:
# MAGIC * `tempo`
# MAGIC * `year`
# MAGIC * `end_of_fade_in`
# MAGIC * `duration`

# COMMAND ----------

# MAGIC %md Here, we are creating a MLlib model with the SparkR API, which has a really similar syntax to what you're familiar with in R.
# MAGIC 
# MAGIC View the documentation for SparkR glm [here](https://spark.apache.org/docs/latest/api/R/glm.html).

# COMMAND ----------

model <- glm(loudness ~ tempo + year + end_of_fade_in + duration, family = "gaussian", data = training)

# COMMAND ----------

model # spark.ml.PipelineModel

# COMMAND ----------

summary(model)

# COMMAND ----------

# MAGIC %md Before evaluating the model on testing data we can get a sense of its goodness of fit by calculating [\\(R^2\\)](https://en.wikipedia.org/wiki/Coefficient_of_determination) -- a number that indicates the proportion of the variance in the dependent variable that is predictable from the independent variable.

# COMMAND ----------

training.preds <- model %>% predict(training) 
preds <- training.preds %>% subset(select = c("prediction", "loudness")) %>% collect
cor(preds$loudness, preds$prediction) ^ 2

# COMMAND ----------

head(preds)

# COMMAND ----------

# Create a quick plot using the built-in Databricks display() function
training %>% sample(F, 0.001) %>% display

# COMMAND ----------

# MAGIC %md A model with  \\(R^2 = 0.1\\) does not seem to be doing a good job fitting to the data even in the training set. So before moving to the testing phase we are going to try and improve the model. 
# MAGIC 
# MAGIC To do so we can use the build-in scatter-plot matrix in Databricks. To get the plot, first display the sample data table as before. Then from the dropdown menu choose `Scatter` and in the plot options menu pick variables that you are interested in along with our target variable (loudness). Following is an example that we are going to go with.
# MAGIC The scatterplot matrix is very helpful for such diagnosis. Let's look at it again, and this time include the prediction of the model instead of `loudness`

# COMMAND ----------

training.preds %>%  subset(select = c("tempo", "year", "end_of_fade_in", "duration", "prediction"))  %>% sample(F, 0.001) %>% display

# COMMAND ----------

# MAGIC %md The above scatter plot shows significant skew in duration, so we should either normalize it or remove it from the model.

# COMMAND ----------

# MAGIC %md The scatterplot matrix support [brushing and linking](https://en.wikipedia.org/wiki/Brushing_and_linking). This feature helps us identify corresponding values in all panels.
# MAGIC 
# MAGIC * Use the mouse to select the few outliers you see in the panel that represents `prediction` vs. `duration`. You can see these are outlier predictions because duration values can get extremely larger.
# MAGIC * You can see similar outliers in the panel that matches `end_of_fade_in` and `prediction`. Again you can see in the diagonal, that `end_of_fade_in` is skewed
# MAGIC * Similarly you can see skewed distribution for `year` and we know why from the previous section. There are records in data with `year = 0`
# MAGIC 
# MAGIC 
# MAGIC Such skew in parameters is undesirable for a regression model and we need to fix it before building our linear model.

# COMMAND ----------

# MAGIC %md We are going to use our model with testing dataset to evaluate how good/bad it is doing

# COMMAND ----------

model %>% predict(testing) %>% subset(select = c("prediction", "loudness")) %>% collect %>% display

# COMMAND ----------

# MAGIC %md
# MAGIC ##Results interpretation
# MAGIC 
# MAGIC The table shown above gives the top ten recomended healthcare plans for the user based on the predicted outcomes using the healthcare plans demographics and the ratings provided by the user
# MAGIC 
# MAGIC ![Recomendation-Index](http://trouvus.com/wp-content/uploads/2016/03/2.1.png)