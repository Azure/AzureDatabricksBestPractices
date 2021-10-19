# Databricks notebook source
# MAGIC %md #Examples of `rsparkling` extension package, which provides bindings to H2O’s distributed machine learning algorithms via `sparklyr`.
# MAGIC 
# MAGIC 1. Install `rsparkling`
# MAGIC 2. GLM to fit a linear regression model on `mtcars` data set
# MAGIC 3. K-Means clustering on `iris` data set
# MAGIC 4. Principal Components Analysis to perform dimensionality reduction on `iris` data set
# MAGIC 5. Random Forest to perform regression or classification on `iris` data set
# MAGIC 6. Gradient Boosting Machine on `iris` data set
# MAGIC 7. Gradient Boosting Machine on `mtcars` data set

# COMMAND ----------

# MAGIC %md Without `Cmd 3`: `ClassNotFoundException: org.apache.spark.h2o.H2OContext`

# COMMAND ----------

# MAGIC %md #Install rsparkling
# MAGIC 1. Download the binary version of H2O Sparkling Water at http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/0/index.html DO NOT USE MAVEN!
# MAGIC 2. Upload the Assembly jar sparkling-water-assembly_2.11-2.2.0-all library manually to your workspace and attach it to your cluster
# MAGIC 3. Create a new cluster using 3.2 (includes Apache Spark 2.2.0, Scala 2.11) and attach the Sparkling Water library (you must have 3 or more nodes)
# MAGIC 4. Restart the cluster

# COMMAND ----------

install.packages("Rcpp")
install.packages("sparklyr")
install.packages("rsparkling")

options(rsparkling.sparklingwater.version = "2.2.3")

library(rsparkling)
library(sparklyr)
library(dplyr)
library(h2o)

# COMMAND ----------

sc <- spark_connect(method = 'databricks', version = '2.2.3')

# COMMAND ----------

# MAGIC %md Without `Cmd 7`:`NoClassDefFoundError: org/spark_project/jetty/servlet/ServletContextHandler`

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.h2o._
# MAGIC val h2oConf = new H2OConf(sc).set("spark.ui.enabled", "false")
# MAGIC val h2oContext = H2OContext.getOrCreate(sc, h2oConf)

# COMMAND ----------

sessionInfo()

# COMMAND ----------

h2o_release_table()

# COMMAND ----------

# MAGIC %md #Use GLM

# COMMAND ----------

mtcars_tbl <- copy_to(sc, mtcars, overwrite = TRUE)

# COMMAND ----------

partitions <- mtcars_tbl %>%
  filter(hp >= 100) %>%
  mutate(cyl8 = cyl == 8) %>%
  sdf_partition(training = 0.5, test = 0.5, seed = 1099)

# COMMAND ----------

test <- as_h2o_frame(sc, partitions$test, strict_version_check = FALSE)

# COMMAND ----------

test

# COMMAND ----------

training <- as_h2o_frame(sc, partitions$training, strict_version_check = FALSE)

# COMMAND ----------

training

# COMMAND ----------

glm_model <- h2o.glm(x = c("wt", "cyl"), 
                     y = "mpg", 
                     training_frame = training,
                     lambda_search = TRUE)

# COMMAND ----------

print(glm_model)

# COMMAND ----------

library(ggplot2)

# compute predicted values on our test dataset
pred <- h2o.predict(glm_model, newdata = test)
# convert from H2O Frame to Spark DataFrame
predicted <- as_spark_dataframe(sc, pred, strict_version_check = FALSE)

# extract the true 'mpg' values from our test dataset
actual <- partitions$test %>%
  select(mpg) %>%
  collect() %>%
  `[[`("mpg")

# produce a data.frame housing our predicted + actual 'mpg' values
data <- data.frame(
  predicted = predicted,
  actual    = actual
)
# a bug in data.frame does not set colnames properly; reset here 
names(data) <- c("predicted", "actual")

# plot predicted vs. actual values
ggplot(data, aes(x = actual, y = predicted)) +
  geom_abline(lty = "dashed", col = "red") +
  geom_point() +
  theme(plot.title = element_text(hjust = 0.5)) +
  coord_fixed(ratio = 1) +
  labs(
    x = "Actual Fuel Consumption",
    y = "Predicted Fuel Consumption",
    title = "Predicted vs. Actual Fuel Consumption"
  )

# COMMAND ----------

# MAGIC %md #Use K-Means

# COMMAND ----------

iris_tbl <- copy_to(sc, iris, "iris", overwrite = TRUE)
iris_tbl

# COMMAND ----------

iris_hf <- as_h2o_frame(sc, iris_tbl, strict_version_check = FALSE)

# COMMAND ----------

kmeans_model <- h2o.kmeans(training_frame = iris_hf, 
                           x = 3:4,
                           k = 3,
                           seed = 1)

# COMMAND ----------

h2o.centers(kmeans_model)

# COMMAND ----------

h2o.centroid_stats(kmeans_model)

# COMMAND ----------

# MAGIC %md #PCA

# COMMAND ----------

pca_model <- h2o.prcomp(training_frame = iris_hf,
                        x = 1:4,
                        k = 4,
                        seed = 1)
print(pca_model)

# COMMAND ----------

# MAGIC %md #Random Forest

# COMMAND ----------

y <- "Species"
x <- setdiff(names(iris_hf), y)
iris_hf[,y] <- as.factor(iris_hf[,y])

# COMMAND ----------

splits <- h2o.splitFrame(iris_hf, seed = 1)

# COMMAND ----------

rf_model <- h2o.randomForest(x = x, 
                             y = y,
                             training_frame = splits[[1]],
                             validation_frame = splits[[2]],
                             nbins = 32,
                             max_depth = 5,
                             ntrees = 20,
                             seed = 1)

# COMMAND ----------

h2o.confusionMatrix(rf_model, valid = TRUE)

# COMMAND ----------

h2o.varimp_plot(rf_model)

# COMMAND ----------

# MAGIC %md #GBM on `iris`

# COMMAND ----------

gbm_model <- h2o.gbm(x = x, 
                     y = y,
                     training_frame = splits[[1]],
                     validation_frame = splits[[2]],                     
                     ntrees = 20,
                     max_depth = 3,
                     learn_rate = 0.01,
                     col_sample_rate = 0.7,
                     seed = 1)

# COMMAND ----------

h2o.confusionMatrix(gbm_model, valid = TRUE)

# COMMAND ----------

# MAGIC %md #GBM on `mtcars`

# COMMAND ----------

mtcars_hf <- as_h2o_frame(sc, mtcars_tbl, strict_version_check = FALSE)
mtcars_hf

# COMMAND ----------

y <- "mpg"
x <- setdiff(names(mtcars_hf), y)

# COMMAND ----------

splits <- h2o.splitFrame(mtcars_hf, ratios = 0.7, seed = 1)

# COMMAND ----------

fit <- h2o.gbm(x = x, 
               y = y, 
               training_frame = splits[[1]],
               min_rows = 1,
               seed = 1)
print(fit)

# COMMAND ----------

perf <- h2o.performance(fit, newdata = splits[[2]])
print(perf)

# COMMAND ----------

pred_hf <- h2o.predict(fit, newdata = splits[[2]])
head(pred_hf)

# COMMAND ----------

pred_sdf <- as_spark_dataframe(sc, pred_hf, strict_version_check = FALSE)
head(pred_sdf)

# COMMAND ----------

