# Databricks notebook source
# MAGIC %run ./setup

# COMMAND ----------

mlflow = init_mlflow()

# COMMAND ----------

# DBTITLE 1,Read in from our Delta table...
table_version = 0

df = (
  spark.read
    .format("delta")
    .option("versionAsOf", table_version)
    .table("aweaver.turbine_gold")
)

# COMMAND ----------

# DBTITLE 1,Convert to a pandas df to train sklearn model on
df_pd = df.filter("turbine_id=100").toPandas()
display(df_pd)

# COMMAND ----------

# DBTITLE 1,Authenticate with Azure SP
sp = get_service_principal()
azure_workspace = get_azure_workspace(sp)

# COMMAND ----------

# DBTITLE 1,Function to train sklearn model...
def train_sklearn(data):
  from sklearn.ensemble import RandomForestClassifier
  from sklearn import preprocessing
  from sklearn.metrics import roc_auc_score
  import mlflow.sklearn
  from sklearn.model_selection import train_test_split
  
  # Remove the irrelevant columns
  data = data.drop(["read_time", "turbine_id"], axis=1)
  
  # Convert the ReadingType to a 0/1 label
  le = preprocessing.LabelEncoder()
  data["ReadingType"] = le.fit_transform(data["ReadingType"])
  
  # Train, test split
  train, test = train_test_split(data)

  # The predicted column is "ReadingType" which is says which historic sensors are damaged and which are healthy
  train_x = train.drop(["ReadingType"], axis=1)
  test_x = test.drop(["ReadingType"], axis=1)
  train_y = train[["ReadingType"]]
  test_y = test[["ReadingType"]]
  
  # log the run into mlflow
  with mlflow.start_run() as run:
    rf = RandomForestClassifier(n_estimators=100, max_depth=2,random_state=0)
    rf.fit(train_x, train_y)

    predicted_qualities = rf.predict(test_x)
    auc = roc_auc_score(test_y, predicted_qualities)
    
    # Log mlflow attributes for mlflow UI
    mlflow.log_param("timetravel_version", table_version)
    mlflow.log_param("max_depth", 2)
    mlflow.log_param("n_estimators", 100)
    mlflow.log_metric("ROC AUC", auc)
    mlflow.sklearn.log_model(rf, "model")
    return rf, mlflow.get_artifact_uri()

# COMMAND ----------

# DBTITLE 1,Train the model
model, artifact_uri = train_sklearn(df_pd)

# COMMAND ----------

# MAGIC %md
# MAGIC [Open MLflow](https://eastus2.azuredatabricks.net/mlflow/#/experiments/18)

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY aweaver.turbine_gold

# COMMAND ----------

from azureml.core.authentication import ServicePrincipalAuthentication

sp = ServicePrincipalAuthentication("9f37a392-f0ae-4280-9796-f1864a10effc", "29844ed9-47a1-490d-ac2c-6f000b18ba33", "4jLp3BIl4Q5KtfBIWNch0zyoXqIKmDscjnlTLpT/Z48=")
print(sp.get_authentication_header())

# COMMAND ----------

from azureml.core import Workspace

azure_workspace = Workspace.get(name="IoT_Models", auth=sp, subscription_id="3f2e4d32-8e8d-46d6-82bc-5bb8d962328b", resource_group="field-eng")

# COMMAND ----------

# DBTITLE 1,Build an Azure ML ContainerImage
from mlflow import azureml

azure_image, azure_model = azureml.build_image(model_path="/dbfs/databricks/mlflow/18/cbaafb7b68d04b41a6a65edc76deb620/artifacts/model", image_name="windturbines", model_name="windturbines", workspace=azure_workspace, synchronous=True)

# COMMAND ----------

# DBTITLE 1,Deploy to Azure Kubernetes Service
from azureml.core.webservice import AksWebservice, Webservice

webservice_deployment_config = AksWebservice.deploy_configuration()
webservice = Webservice.deploy_from_image(image=azure_image, workspace=azure_workspace, name="windturbines2")
webservice.wait_for_deployment()

# COMMAND ----------

# DBTITLE 1,Call the deployed model for inference
web_service = Webservice(azure_workspace, "windturbines")
features = '{"columns":["AN3 ","AN4","AN5","AN6","AN7","AN8","AN9","AN10"],"index":[0],"data":[[4.6816,-0.1447,0.67837,0.24603,8.6602,2.811,-3.4252,0.8056]]}'
web_service.run(features)

# COMMAND ----------

# DBTITLE 1,Make multiple predictions...
import pandas as pd 

multiple_features = pd.DataFrame(data={'AN3': [0.39624, 0.52982, -1.8748, -0.30597, -5.8551], 'AN4': [-0.18844, 2.5549, 1.1651, -1.0725, 2.9113], 'AN5': [-2.3349, 0.62939, 0.23451, -1.5054, 0.76963], 'AN6': [-1.6337, -3.1414, -3.6414, 2.0511, -5.8789], 'AN7': [-3.2393, -1.0031, 1.7199, -5.8427, 4.529], 'AN8': [-0.69343, -1.6114, 1.2235, -1.8538, -3.4039],'AN9': [0.51986, -0.60015, -1.0728, -0.46044, 16.562], 'AN10': [-0.99175, 1.0142, -1.4955, -0.57462, 0.88898]}).to_json(orient='split')

web_service.run(multiple_features)