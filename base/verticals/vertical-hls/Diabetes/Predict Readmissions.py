# Databricks notebook source
# MAGIC %md
# MAGIC # Training ML model for predicting:
# MAGIC ### patients with high 30-day readmission risk.
# MAGIC 
# MAGIC 
# MAGIC ![Pipeline](https://s3.us-east-2.amazonaws.com/databricks-roy/RFC.jpg)

# COMMAND ----------

# MAGIC %run ./setup_rfc

# COMMAND ----------

# MAGIC %md
# MAGIC # DataFrame: `BEFORE PREPROCESSING`

# COMMAND ----------

display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Handle missing values

# COMMAND ----------

df = df.select([F.when(df[c].cast('string') != "?", F.col(c)).otherwise(None).alias(c) for c in df.columns])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add indicator columns for numeric and categorical missing values. (Retain the id columns for merging)

# COMMAND ----------

id_vars = ['encounter_id', 'patient_nbr', 'discharge_date']
label_var = ['readmitted']
num_vars = ['time_in_hospital', 'num_lab_procedures', 'num_procedures',
            'num_medications', 'number_outpatient', 'number_emergency',
            'number_inpatient', 'diag_1', 'diag_2', 'diag_3', 'number_diagnoses',
            'glucose_min', 'glucose_max', 'glucose_mean', 'glucose_var']
cat_vars = ['race', 'gender', 'age', 'weight', 'admission_type_id',
            'discharge_disposition_id', 'admission_source_id',
            'payer_code', 'medical_specialty',
            'max_glu_serum', 'A1Cresult', 'metformin', 'repaglinide', 'nateglinide',
            'chlorpropamide', 'glimepiride', 'acetohexamide', 'glipizide',
            'glyburide', 'tolbutamide', 'pioglitazone', 'rosiglitazone', 'acarbose',
            'miglitol', 'troglitazone', 'tolazamide', 'examide', 'citoglipton',
            'insulin', 'glyburide-metformin', 'glipizide-metformin',
            'glimepiride-pioglitazone', 'metformin-rosiglitazone',
            'metformin-pioglitazone', 'change', 'diabetesMed']

df_mvi = df.select(id_vars + [F.when(df[c].isNull(), 'y').otherwise('n').alias(c + '_missing') for c in num_vars + cat_vars])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Replace missing numeric values with `mean`

# COMMAND ----------

df_num = df.select(id_vars + [df[c].cast('double') for c in num_vars])
num_var_means = dict(zip(num_vars,
                         df_num.select([F.mean(df_num[c]).alias(c + '_mean') \
                                        for c in num_vars]).rdd.flatMap(lambda x: x).collect()))
df_num = df_num.select(id_vars + [F.when(df_num[c].isNull(), num_var_means[c]).otherwise(df_num[c]).alias(c) for c in num_vars])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Indicate missing values in categorical columns. Merge with other missing value indicators.

# COMMAND ----------

df_cat = df.select(id_vars + [F.when(df[c].isNull(), 'NA_').otherwise(df[c].cast('string')).alias(c) for c in cat_vars])
df_cat = df_cat.join(df_mvi, id_vars, 'inner')
cat_vars = [x for x in df_cat.columns if x not in id_vars]
df_cat.persist()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create the string indexing pipeline

# COMMAND ----------

s_indexers = [StringIndexer(inputCol=x, outputCol=x + '__indexed__') for x in cat_vars]
si_pipe = Pipeline(stages=s_indexers)
si_pipe_model = si_pipe.fit(df_cat)
df_cat = si_pipe_model.transform(df_cat)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Remove from consideration any categorical variables that have only one level

# COMMAND ----------

cat_col_var = df_cat.select([F.variance(df_cat[c]).alias(c + '_sd') for \
                             c in [cv + '__indexed__' for cv in cat_vars]]).rdd.flatMap(lambda x: x).collect()
cat_vars = [cat_vars[i] for i in range(len(cat_col_var)) if cat_col_var[i] != 0]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Perform one-hot encoding

# COMMAND ----------

oh_encoders = [OneHotEncoder(inputCol=x + '__indexed__', outputCol=x + '__encoded__')
              for x in cat_vars]
df_cat = df_cat.select(id_vars + [x + '__indexed__' for x in cat_vars])
oh_pipe_model = Pipeline(stages=oh_encoders).fit(df_cat)
df_cat = oh_pipe_model.transform(df_cat)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Assemble categorical features into one vector

# COMMAND ----------

df_cat = df_cat.select([df_cat[c].alias(c.replace('__encoded__', ''))
                         for c in id_vars + [x + '__encoded__' for x in cat_vars]])
va = VectorAssembler(inputCols=cat_vars, outputCol='cat_features')
df_cat = va.transform(df_cat).select(id_vars + ['cat_features'])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Map labels correctly

# COMMAND ----------

label_map = {'NO': 0, '>30': 0, '<30': 1}
def map_label(label):
    return(label_map[label])

df_label = df.select(id_vars + label_var)
udf_map_label = F.udf(map_label, StringType())
df_label = df_label.withColumn('readmitted', udf_map_label(df_label['readmitted']))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create string indexer

# COMMAND ----------

m_si_label = StringIndexer(inputCol='readmitted', outputCol='label').fit(df_label)
df_label = m_si_label.transform(df_label)
df_label = df_label.drop('readmitted')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Merge dataframes

# COMMAND ----------

df = df_label.join(df_num, id_vars, 'inner').join(df_cat, id_vars, 'inner')

# COMMAND ----------

va = VectorAssembler(inputCols=(num_vars + ['cat_features']), outputCol='features')
df = va.transform(df).select('label', 'features')

# COMMAND ----------

# MAGIC %md
# MAGIC # DataFrame: `AFTER PREPROCESSING`

# COMMAND ----------

display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Split Train and Test set

# COMMAND ----------

train, test = df.randomSplit([0.8, 0.2], seed=0)
train = train.sampleBy('label', fractions={0.0: 0.2, 1.0: 0.8}, seed=0)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Classifier

# COMMAND ----------

clf = RandomForestClassifier(seed=0)
evaluator = BinaryClassificationEvaluator()
paramGrid = ParamGridBuilder().addGrid(clf.maxDepth, [5, 10]).addGrid(clf.maxBins, [32, 64]).build()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3-fold CrossValidator

# COMMAND ----------

cv = CrossValidator(estimator=clf, estimatorParamMaps=paramGrid, evaluator=evaluator, numFolds=3)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Train
# MAGIC #### `2*2=4` parameter settings for each model, each of which trains with `3` training sets

# COMMAND ----------

cvModel = cv.fit(train)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Best Model

# COMMAND ----------

trained_model = cvModel.bestModel

# COMMAND ----------

print trained_model

# COMMAND ----------

# MAGIC %md
# MAGIC ### Evaluate Best Model

# COMMAND ----------

predictions = trained_model.transform(test)

# COMMAND ----------

display(predictions)

# COMMAND ----------

print evaluator.getMetricName()

# COMMAND ----------

# MAGIC %md
# MAGIC ### AUC

# COMMAND ----------

evaluator.evaluate(predictions)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Confusion matrix

# COMMAND ----------

pred_pd = predictions.toPandas()
confuse = pd.crosstab(pred_pd['label'],pred_pd['prediction'])
confuse.columns = confuse.columns.map(str)
print confuse

# COMMAND ----------

# MAGIC %md
# MAGIC ### Evaluation Metrics

# COMMAND ----------

tp = confuse['1.0'][1]
fp = np.sum(np.sum(confuse[['1.0']])) - tp
tn = confuse['0.0'][0]
fn = np.sum(np.sum(confuse[['0.0']])) - tn
acc_n = tn + tp
acc_d = np.sum(np.sum(confuse[['0.0','1.0']]))
acc = float(acc_n)/acc_d
prec = float(tp)/(tp+fp)
rec = float(tp)/(tp+fn)

print("Accuracy = %g" % acc)
print("Precision = %g" % prec)
print("Recall = %g" % rec )
print("F1 = %g" % (2.0 * prec * rec/(prec + rec)))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save Best Model

# COMMAND ----------

trained_model.save('/mnt/roy/readmissions/dataset_diabetes/model')

# COMMAND ----------

# MAGIC %fs ls /mnt/roy/readmissions/dataset_diabetes/model/data

# COMMAND ----------

display(
  spark.read.parquet('/mnt/roy/readmissions/dataset_diabetes/model/data')
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load Model in `Scala`

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.ml.classification.RandomForestClassificationModel
# MAGIC 
# MAGIC val model = RandomForestClassificationModel.load("/mnt/roy/readmissions/dataset_diabetes/model")
# MAGIC println(s"Loaded classification forest model:\n${model.toDebugString}")

# COMMAND ----------

# MAGIC %md 
# MAGIC ![ml](https://s3.us-east-2.amazonaws.com/databricks-roy/MLDB.jpeg)

# COMMAND ----------

