# Databricks notebook source
# MAGIC %md # Modeling Songs
# MAGIC 
# MAGIC ![Model](http://training.databricks.com/databricks_guide/end-to-end-03.png)
# MAGIC 
# MAGIC This is the third step into our project. In the first step we parsed raw text files and created a table. Then we explored different aspects of data with visualization. In this step we attempt to gain deeper understanding of our data. We are going to build a simple model that predicts loudness of songs. 

# COMMAND ----------

songs <- tableToDF("songsTable")

# COMMAND ----------

# MAGIC %md We will use songs before 2005 as training and then test on more recent data points.

# COMMAND ----------

training <- subset(songs, songs$year < 2008 & songs$year > 0)
testing <- subset(songs, songs$year >= 2008)

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

# MAGIC %md # Exercise
# MAGIC * Fix the skew in `end_of_fade_in` and `duration`
# MAGIC * Remove all data points that have `year = 0`
# MAGIC * Train the model again and compute \\(R^2\\) 

# COMMAND ----------

# Enter your code here!


# COMMAND ----------

# MAGIC %md We are going to use our model with testing dataset to evaluate how good/bad it is doing

# COMMAND ----------

model %>% predict(testing) %>% subset(select = c("prediction", "loudness")) %>% collect %>% display