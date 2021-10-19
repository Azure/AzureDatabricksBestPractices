# Databricks notebook source
# MAGIC %md Copyright (c) Microsoft Corporation. All rights reserved.
# MAGIC 
# MAGIC Licensed under the MIT License.

# COMMAND ----------

# MAGIC %md ## Use MLflow with Azure Machine Learning
# MAGIC 
# MAGIC This example shows you how to use MLflow with Azure Machine Learning services. You'll learn how to:
# MAGIC 
# MAGIC  1. Set up MLflow tracking URI to Azure ML
# MAGIC  2. Create MLflow experiment
# MAGIC  3. Train a PyTorch model on Azure Databricks while logging metrics and artifacts
# MAGIC  4. View your experiment within your Azure ML Workspace in Azure Portal.
# MAGIC  5. Deploy the Model to ACI/AKS
# MAGIC  
# MAGIC *This PyTorch notebook requires Python 3.6.x and torch, torchvision and pillow for Training & Deployment. Please install it on your training cluster before proceeding.*

# COMMAND ----------

# MAGIC %md ## High level overview
# MAGIC 
# MAGIC Install *azureml-mlflow* package before running this notebook on your cluster. This single package includes MLflow and Azure ML SDK.

# COMMAND ----------

# MAGIC %md ![MLflow-AzureML](https://raw.githubusercontent.com/parasharshah/mlflow-azureml/master/MLflow%20with%20Azure%20ML.jpg)

# COMMAND ----------

import mlflow
import mlflow.azureml
import azureml.mlflow
import azureml.core

from azureml.core import Workspace

from azureml.mlflow import get_portal_url

print("SDK version:", azureml.core.VERSION)
print("MLflow version:", mlflow.version.VERSION)

# COMMAND ----------

# DBTITLE 1,Details of Azure ML workspace
subscription_id = "3f2e4d32-8e8d-46d6-82bc-5bb8d962328b" #you should be owner or contributor
resource_group = "joel-simple"                       #you should be owner or contributor
workspace_name = "joel-aml"                         # your workspace name - needs to be unique - can be anything

# COMMAND ----------

# DBTITLE 1,Instantiate AML workspace
ws = Workspace.get(name = workspace_name,
                      subscription_id = subscription_id,
                      resource_group = resource_group)

# COMMAND ----------

# MAGIC %md ## Set Your Tracking URL Using Your Workspace
# MAGIC Link the MLflow tracking to Azure ML Workspace.  After this, all your experiments will land in the managed AzureML tracking service.

# COMMAND ----------

uri = ws.get_mlflow_tracking_uri()
mlflow.set_tracking_uri(uri)

# COMMAND ----------

# MAGIC %md ## Create an Experiment and Train on Azure Databricks
# MAGIC 
# MAGIC In both MLflow and Azure ML, training runs are grouped into experiments. Let's create one for our experimentation.  We'll use pytorch to train MNIST, because, well it's basically required.
# MAGIC 
# MAGIC ![MNIST](https://docs.azuredatabricks.net/_images/mnist.png)

# COMMAND ----------

experiment_name = "pytorch-mlflow-2"
mlflow.set_experiment(experiment_name)

# COMMAND ----------

import warnings
warnings.filterwarnings("ignore")

import cloudpickle

import torch
import torch.nn as nn
import torch.nn.functional as F
import torch.optim as optim
import torchvision
from torchvision import datasets, transforms

PREDICTION_DATA_TENSOR_LABEL = None

# COMMAND ----------

class Net(nn.Module):
    def __init__(self):
        super(Net, self).__init__()
        self.conv1 = nn.Conv2d(1, 20, 5, 1)
        self.conv2 = nn.Conv2d(20, 50, 5, 1)
        self.fc1 = nn.Linear(4*4*50, 500)
        self.fc2 = nn.Linear(500, 10)

    def forward(self, x):
        x = x.view(-1, 1, 28, 28) # Added the view for reshaping score requests
        x = F.relu(self.conv1(x))
        x = F.max_pool2d(x, 2, 2)
        x = F.relu(self.conv2(x))
        x = F.max_pool2d(x, 2, 2)
        x = x.view(-1, 4*4*50)
        x = F.relu(self.fc1(x))
        x = self.fc2(x)
        return F.log_softmax(x, dim=1)
    
def train(args, model, device, train_loader, optimizer, epoch):
    model.train()
    for batch_idx, (data, target) in enumerate(train_loader):
        data, target = data.to(device), target.to(device)
        optimizer.zero_grad()
        output = model(data)
        loss = F.nll_loss(output, target)
        loss.backward()
        optimizer.step()
        if batch_idx % args.log_interval == 0:
            print('Train Epoch: {} [{}/{} ({:.0f}%)]\tLoss: {:.6f}'.format(
                epoch, batch_idx * len(data), len(train_loader.dataset),
                100. * batch_idx / len(train_loader), loss.item()))
            #####################################
            # LOG METRICS WITH MLFLOW to Azure ML
            #####################################
            mlflow.log_metric("epoch_loss", loss.item())

def test(args, model, device, test_loader):
    model.eval()
    test_loss = 0
    correct = 0
    with torch.no_grad():
        for data, target in test_loader:
            data, target = data.to(device), target.to(device)
            
            # For convenience, store a batch of images for scoring later
            global PREDICTION_DATA_TENSOR_LABEL
            if PREDICTION_DATA_TENSOR_LABEL is None:
                PREDICTION_DATA_TENSOR_LABEL = data, data.view(len(data), data.shape[1]* data.shape[2] * data.shape[3]), target
                
            output = model(data)
            test_loss += F.nll_loss(output, target, reduction='sum').item() # sum up batch loss
            pred = output.argmax(dim=1, keepdim=True) # get the index of the max log-probability
            correct += pred.eq(target.view_as(pred)).sum().item()

    test_loss /= len(test_loader.dataset)

    print('\nTest set: Average loss: {:.4f}, Accuracy: {}/{} ({:.0f}%)\n'.format(
        test_loss, correct, len(test_loader.dataset),
        100. * correct / len(test_loader.dataset)))
    ###########################################
    # LOG METRIC WITH MLFLOW
    ###########################################
    mlflow.log_metric("average_loss", test_loss)



# COMMAND ----------

class Args(object):
  pass

# Training settings
args = Args()
setattr(args, 'batch_size', 64)
setattr(args, 'test_batch_size', 1000)
setattr(args, 'epochs', 3) # Higher number for better convergence
setattr(args, 'lr', 0.01)
setattr(args, 'momentum', 0.5)
setattr(args, 'no_cuda', True)
setattr(args, 'seed', 1)
setattr(args, 'log_interval', 50)
setattr(args, 'save_model', True)

use_cuda = not args.no_cuda and torch.cuda.is_available()

torch.manual_seed(args.seed)

device = torch.device("cuda" if use_cuda else "cpu")

kwargs = {'num_workers': 1, 'pin_memory': True} if use_cuda else {}
train_loader = torch.utils.data.DataLoader(
    datasets.MNIST('../data', train=True, download=True,
                   transform=transforms.Compose([
                       transforms.ToTensor(),
                       transforms.Normalize((0.1307,), (0.3081,))
                   ])),
    batch_size=args.batch_size, shuffle=True, **kwargs)
test_loader = torch.utils.data.DataLoader(
    datasets.MNIST('../data', train=False, transform=transforms.Compose([
                       transforms.ToTensor(),
                       transforms.Normalize((0.1307,), (0.3081,))
                   ])),
    batch_size=args.test_batch_size, shuffle=False, **kwargs)

MODEL_SAVE_PATH = 'pytorchmodel'

# COMMAND ----------

################################
# START MLFLOW EXPERIMENT
################################
with mlflow.start_run() as run:
    displayHTML("<a href={} target='_blank'>Azure Portal Run Details Page: {}</a>".format(get_portal_url(run), run.info.run_uuid))
    model = Net().to(device)
    optimizer = optim.SGD(model.parameters(), lr=args.lr, momentum=args.momentum)

    for epoch in range(1, args.epochs + 1):
        train(args, model, device, train_loader, optimizer, epoch)
        test(args, model, device, test_loader)

    if args.save_model:
        
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
        #############################
        # LOG MODEL USING MLFLOW
        #############################
        import mlflow.pytorch
        mlflow.pytorch.log_model(model, MODEL_SAVE_PATH, conda_env=model_env)

# COMMAND ----------

# MAGIC %md ![Workspace](https://github.com/parasharshah/automl-handson/raw/master/image4deploy.JPG)

# COMMAND ----------

####################################################
# Retreive id from experiment to deploy
####################################################
runid = run.info.run_id

# COMMAND ----------

####################################################
# Build an Azure ML Container Image for an MLflow 
####################################################

azure_image, azure_model = mlflow.azureml.build_image(model_uri='runs:/{}/{}'.format(runid, MODEL_SAVE_PATH),
                                                      workspace=ws,
                                                      model_name='pytorch_mnist',
                                                      image_name='pytorch-mnist-img',
                                                      synchronous=True)

# COMMAND ----------

from azureml.core.webservice import AciWebservice, Webservice

import random
import string
deployment_stub = ''.join([random.choice(string.ascii_lowercase) for i in range(5)])

aci_config = AciWebservice.deploy_configuration(cpu_cores=2, 
                                                memory_gb=5, 
                                                tags={"data": "RUL",  "method" : "pytorch"}, 
                                                description='Predict using webservice',
                                                location='eastus2')


# Deploy the image to Azure Container Instances (ACI) for real-time serving
webservice = Webservice.deploy_from_image(
    image=azure_image, workspace=ws, name="mlflow-demo-"+deployment_stub, deployment_config=aci_config)

webservice.wait_for_deployment()

# COMMAND ----------

# After the image deployment completes, requests can be posted via HTTP to the new ACI
# webservice's scoring URI.
print("Scoring URI is: {}".format(webservice.scoring_uri))

# COMMAND ----------

data, tensor, label = PREDICTION_DATA_TENSOR_LABEL
TEST_DATA = datasets.MNIST('../data', train=False)

import base64
from io import BytesIO

def show_index(index):
    global TEST_DATA
    image, label = TEST_DATA[index]
    buffered = BytesIO()
    image.save(buffered, format="JPEG")
    img_str = base64.b64encode(buffered.getvalue())
    displayHTML('<img src="data:image/jpeg;base64,{}" style="width:100px;height:120px;"">'.format(img_str.decode('utf-8')))

# COMMAND ----------

import requests
import json

def score_index(index):
    sample_input = {
        "data": [tensor[index].tolist()]
    }

    response = requests.post(
                  url=webservice.scoring_uri, data=json.dumps(sample_input),
                  headers={"Content-type": "application/json"})
    
    response_json = json.loads(response.text)
    scores = response_json[0]
    max_score = max(scores.values())
    all_predicted_labels = [x for x , y in scores.items() if y == max_score]
    displayHTML('<h3>Predicted Digit: {}</h3>'.format(all_predicted_labels[0]))


# COMMAND ----------

#There are 1000 examples in our test set for the MNIST image recognition challenge
#Pick one at random and display it
indexpred = 201
show_index(indexpred)

# COMMAND ----------

