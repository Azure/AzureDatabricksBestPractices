# Databricks notebook source
import matplotlib.pyplot as plt
import numpy as np

# functions to show an image
def imshow(img):
    img = img / 2 + 0.5     # unnormalize
    npimg = img.numpy()
    plt.imshow(np.transpose(npimg, (1, 2, 0)))
    plt.show()

# COMMAND ----------

def log_mnist_model(model, run_id=None):
  #create a conda env file which has requirement frameworks
  from mlflow.utils.environment import _mlflow_conda_env
  model_env = _mlflow_conda_env(
      additional_pip_deps=[
          "cloudpickle=={}".format(cloudpickle.__version__),
          "torch=={}".format(torch.__version__),
          "torchvision=={}".format(torchvision.__version__),
          "pillow=={}".format("6.0.0")
      ]
  )
  
  if run_id:
    with mlflow.start_run(run_id = run_id):
      mlflow.pytorch.log_model(model, MODEL_SAVE_PATH, conda_env=model_env)
  else:
      mlflow.pytorch.log_model(model, MODEL_SAVE_PATH, conda_env=model_env)