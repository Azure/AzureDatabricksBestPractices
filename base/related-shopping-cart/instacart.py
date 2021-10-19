# Databricks notebook source
# MAGIC %md
# MAGIC # Analyzing related shopping cart items with Instacart data
# MAGIC 
# MAGIC ![](https://d2guulkeunn7d8.cloudfront.net/assets/beetstrap/brand/instacart-logo-color@2x-9b5e0948b142bc55387c61ae4e7a6628b4b75be1e554321ee5e0a1f0b69727b0.png)
# MAGIC 
# MAGIC The online grocery shopping company Instacart released some [data](https://www.kaggle.com/c/instacart-market-basket-analysis) from their large set of shopping history data. It includes, among other things, lists of items that users added to carts, and in what order. This makes it an interesting data set for basket analysis.
# MAGIC 
# MAGIC Here, we won't apply typical association rule mining, but instead use the baskets of goods to create a meaningful vector embedding of items, ones that _should_ yield item vectors that are similar when the items tend to be added together in a cart. We'll do this with a deep learning model in Keras to learn the embedding, having create "skipgrams" from the ordered items in carts. In a sense this is analogous to how word2vec builds word embeddings. Here we have carts that are ordered lists of items instead of sentences that are ordered lists of words.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read the data
# MAGIC Read the raw data, as `(cart, item, ordering)` and sort according to order within a cart.

# COMMAND ----------

order_df = spark.read.\
  option('header', True).option('inferSchema', True).\
  csv("/mnt/databricks-datasets-private/ML/instacart/").\
  select("order_id", "product_id", "add_to_cart_order").\
  orderBy("order_id", "add_to_cart_order")
display(order_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Collect (ordered) items per cart, ignoring single-item carts. These are 'sentences' of items that are added together.

# COMMAND ----------

from pyspark.sql.functions import *

order_items_df = order_df.\
  groupBy("order_id").\
  agg(collect_list("product_id").alias("item_ids")).\
  select("item_ids").\
  filter(size("item_ids") > 1).\
  cache()
display(order_items_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prepare model input as skipgrams
# MAGIC Produce skipgrams from the carts (see https://keras.io/preprocessing/sequence/). For each item, find another item added in the same 'window' in the cart (+/- 2 items in the ordering). Emit that as a 'positive' example of a match. Also pick another random item and emit that pair as a 'negative' example. Generate a train/test split too.

# COMMAND ----------

from keras.preprocessing.sequence import *
import numpy as np
from sklearn.model_selection import train_test_split

order_sets = [r[0] for r in order_items_df.toPandas().values]
num_items = order_items_df.select(explode("item_ids")).rdd.max()[0] + 1

skipgrams_train, skipgrams_test = train_test_split([skipgrams(order_set, num_items, shuffle=False) for order_set in order_sets], test_size=0.1)

X_train = np.vstack([pairs for (pairs, _) in skipgrams_train])
X_test = np.vstack([pairs for (pairs, _) in skipgrams_test])

y_train = np.vstack([np.array(labels).reshape(-1,1) for (_, labels) in skipgrams_train])
y_test = np.vstack([np.array(labels).reshape(-1,1) for (_, labels) in skipgrams_test])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build and log a model that produces item embeddings
# MAGIC Learn an embedding of the items by learning which pairs of items are actually a match, go together in a cart. This learns a 20-dimensional embedding.

# COMMAND ----------

from keras.layers import Dense, Embedding, Flatten
from keras.models import Model, Sequential

model = Sequential()
model.add(Embedding(num_items, 20, input_length=2))
model.add(Flatten())
model.add(Dense(1, activation='sigmoid'))
model.compile(loss='binary_crossentropy', optimizer='nadam', metrics=['binary_accuracy'])
model.summary()

# COMMAND ----------

# MAGIC %md
# MAGIC Train on a GPU. This is certainly not the only or best model and architecture; just getting some reasonable model here to demonstrate.

# COMMAND ----------

model.fit(X_train, y_train, batch_size=256, epochs=5, validation_data=(X_test, y_test), verbose=2)

# COMMAND ----------

# MAGIC %md
# MAGIC Log the model and evaluation with mlflow.

# COMMAND ----------

(loss, acc) = model.evaluate(X_test, y_test)

# COMMAND ----------

import mlflow
import mlflow.keras

with mlflow.start_run():
  mlflow.log_metric("loss", loss)
  mlflow.log_metric("accuracy", acc)
  mlflow.keras.log_model(model, "model")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load model and score a new prospective pair

# COMMAND ----------

import mlflow
import mlflow.keras

model = mlflow.keras.load_model('model', run_id='9baf3e1974024c79acc4df138027239d')
embedding_layer = model.layers[0]

# COMMAND ----------

# MAGIC %md
# MAGIC Which item is predicted to be the strongest match, most likely to be carted as well, for a given item?

# COMMAND ----------

import numpy as np

embedding_dim = embedding_layer.input_dim
def best_match(input_item):
  to_score = [[input_item, i+1] for i in range(embedding_dim - 1)]
  scores = model.predict([to_score])
  best_idx = np.argmax(scores)
  return (to_score[best_idx][1], scores[best_idx])

print("Best item: %s (score %s)" % best_match(49302))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Try the model as a UDF
# MAGIC Now we can also load the model directly as a Spark UDF, and then apply it to item ID pairs directly in a distribute batch or streaming job.

# COMMAND ----------

import mlflow.pyfunc

model_udf = mlflow.pyfunc.spark_udf(spark, 'model', run_id='9baf3e1974024c79acc4df138027239d')
new_items_df = spark.createDataFrame([(1,i+1) for i in range(100)])

# COMMAND ----------

display(new_items_df.withColumn("score", model_udf("_1", "_2")).orderBy("score"))