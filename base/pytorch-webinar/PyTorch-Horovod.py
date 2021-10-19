# Databricks notebook source
# MAGIC %md ## PyTorch - Distributed using Horovod (Using MNIST Dataset)
# MAGIC #### Notable Features: Track with MLflow in Azure ML, Build and Deploy Scoring with ACI, Graphviz NN Model, Horovod Timeline View, etc.
# MAGIC *Following notebook can be run on either on a GPU or CPU instance*   
# MAGIC **Dependencies**  
# MAGIC  *This notebook requires Databricks Runtime 5.5 ML (Python 3.6.x), mlflow, azureml-mlflow, graphviz, pillow, and torchviz for Training & Deployment. Please install it on your training cluster before proceeding.*

# COMMAND ----------

# DBTITLE 0,Objective
# MAGIC %md
# MAGIC ## Objective: Model to recognize handwritten digits using MNIST Dataset
# MAGIC 
# MAGIC To showcase PyTorch (in a single node and with Horovod in distributed learning mode) to **automate the identification of handwritten digits** from the  [MNIST Database of Handwritten Digits](http://yann.lecun.com/exdb/mnist/) database.
# MAGIC 
# MAGIC ![](https://upload.wikimedia.org/wikipedia/commons/2/27/MnistExamples.png)

# COMMAND ----------

# DBTITLE 1,Initialize Config
# MAGIC %run ./config/init

# COMMAND ----------

# DBTITLE 1,Utility functions for building MNIST model
# MAGIC %run ./mnist_utils

# COMMAND ----------

# MAGIC %md ## MLflow & AzureML

# COMMAND ----------

# DBTITLE 1,Initialize AzureML Integrations & Setup AzureML Workspace
# MAGIC %run ./azureml/init

# COMMAND ----------

ws = azureml_workspace(auth_type = 'service_princpal') # If you don't have a service principal, use 'interactive' for interactive login

# COMMAND ----------

# DBTITLE 1,Use AzureML as MLfow Tracking Server
azureml_mlflow_uri = auzreml_mlflow_tracking_uri(ws)
mlflow.set_tracking_uri(azureml_mlflow_uri)

# COMMAND ----------

experiment_name = "pytorch-webinar"
mlflow.set_experiment(experiment_name)

# COMMAND ----------

# MAGIC %md ## Imports for MNIST PyTorch & Others

# COMMAND ----------

import warnings
warnings.filterwarnings("ignore")

import cloudpickle

import os
import tempfile
import torch
import torch.nn as nn
import torch.nn.functional as F
import torch.optim as optim
import torchvision
from torchvision import datasets, transforms
from torch.autograd import Variable
import mlflow.pytorch

# COMMAND ----------

# MAGIC %md ## Input Arguments for Training

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
setattr(args, 'cuda', True)
setattr(args, 'seed', 1)
setattr(args, 'log_interval', 50)
setattr(args, 'save_model', True)
setattr(args, 'distributed_training', True)

NODE_COUNT = 2 # Number of nodes in the cluster

PYTORCH_CHECKPOINT_DIR = '/dbfs/ml/tmp/horovod_pytorch'
MODEL_VISUALIZE_LOC = '/tmp/mnist_nn_model'
MODEL_SAVE_PATH = 'pytorchmodel'

torch.manual_seed(args.seed)

# COMMAND ----------

# DBTITLE 1,Initialize Horovod Checkpoint & Timeline
# MAGIC %run ./horovod/init

# COMMAND ----------

# MAGIC %md ## Explore Data

# COMMAND ----------

# DBTITLE 1,Sampling Data Locally
use_cuda = args.cuda and torch.cuda.is_available()
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
    datasets.MNIST('../data', train=False, 
                   transform=transforms.Compose([
                       transforms.ToTensor(),
                       transforms.Normalize((0.1307,), (0.3081,))
                   ])),
    batch_size=args.test_batch_size, shuffle=False, **kwargs)

classes = ('0', '1', '2', '3', '4', '5', '6', '7', '8', '9')

# COMMAND ----------

# DBTITLE 1,Display Some Sample Images & Labels
# get some random training images
dataiter = iter(train_loader)
images, labels = dataiter.next()

# show images
display(imshow(torchvision.utils.make_grid(images)))

# COMMAND ----------

# print labels
print('Labels: ', ' '.join('%5s' % classes[labels[j]] for j in range(8)))

# COMMAND ----------

# MAGIC %md ## Data Prep

# COMMAND ----------

def data_prep(hvd):
  from torch.utils.data.distributed import DistributedSampler
  
  # Training dataset and distribute across nodes
  train_dataset = datasets.MNIST(
    root='data-%d'% hvd.rank(),  # Use different root directory for each worker to avoid race conditions.
    train=True, 
    download=True,
    transform=transforms.Compose([transforms.ToTensor(), transforms.Normalize((0.1307,), (0.3081,))])
  )
  # Configure the sampler such that each worker obtains a distinct sample of input dataset.
  train_sampler = DistributedSampler(train_dataset, num_replicas=hvd.size(), rank=hvd.rank())
  # Use train_sampler to load a different sample of data on each worker.
  train_loader = torch.utils.data.DataLoader(train_dataset, batch_size=args.batch_size, sampler=train_sampler)
  
  # Test dataset and distribute across nodes
  test_dataset = datasets.MNIST(
    root='data-test-%d'% hvd.rank(),  # Use different root directory for each worker to avoid race conditions.
    train=False, 
    download=True,
    transform=transforms.Compose([transforms.ToTensor(), transforms.Normalize((0.1307,), (0.3081,))])
  )
  # Configure the sampler such that each worker obtains a distinct sample of input dataset.
  test_sampler = DistributedSampler(test_dataset, num_replicas=hvd.size(), rank=hvd.rank())
  # Use train_sampler to load a different sample of data on each worker.
  test_loader = torch.utils.data.DataLoader(test_dataset, batch_size=args.batch_size, sampler=test_sampler)
  
  return train_loader, test_loader

# COMMAND ----------

# MAGIC %md ## MNIST PyTorch Model

# COMMAND ----------

# DBTITLE 1,Define the network
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

# COMMAND ----------

# DBTITLE 1,Train
def train_one_epoch(args, model, device, data_loader, optimizer, epoch):
    model.train()
    for batch_idx, (data, target) in enumerate(data_loader):
        data, target = data.to(device), target.to(device)
        optimizer.zero_grad()
        output = model(data)
        loss = F.nll_loss(output, target)
        loss.backward()
        optimizer.step()
        if batch_idx % args.log_interval == 0:
            print('Train Epoch: {} [{}/{} ({:.0f}%)]\tLoss: {:.6f}'.format(
                epoch, batch_idx * len(data), len(data_loader) * len(data),
                100. * batch_idx / len(data_loader), loss.item()))
            if (hvd.rank() == 0 and args.distributed_training) or not args.distributed_training:
              with mlflow.start_run(run_id = active_run_uuid):
                for key, value in vars(args).items():
                  mlflow.log_param(key, value)
                step = epoch * len(data_loader) + batch_idx
                mlflow.log_metric('train_loss', loss.data.item(), step)

# COMMAND ----------

# DBTITLE 1,Test
def test(args, model, device, test_loader, epoch):
    model.eval()
    test_loss = 0
    correct = 0
    total_target_items = 0
    with torch.no_grad():
        for data, target in test_loader:
            data, target = data.to(device), target.to(device)
            output = model(data)
            test_loss += F.nll_loss(output, target, reduction='sum').item() # sum up batch loss
            pred = output.argmax(dim=1, keepdim=True) # get the index of the max log-probability
            correct += pred.eq(target.view_as(pred)).sum().item()
            total_target_items += len(target)

    test_loss /= total_target_items
    test_accuracy = 100.0 * correct / total_target_items
    print('\nTest set: Average loss: {:.4f}, Accuracy: {}/{} ({:.0f}%)\n'.format(
        test_loss, correct, total_target_items,
        100. * correct / total_target_items))
    step = (epoch + 1) * len(test_loader)
    with mlflow.start_run(run_id = active_run_uuid):
      mlflow.log_metric('test_loss', test_loss)
      mlflow.log_metric('test_accuracy', test_accuracy)

# COMMAND ----------

# DBTITLE 1,Orchestrate Model Building with Horovod
def generate_model_horovod(args):
  # Initialize Horovod
  hvd.init()
  
  # Set MLflow tracking URI in each node
  mlflow.mlflow.set_tracking_uri(azureml_mlflow_uri)
  
  # Define the device type
  device = torch.device('cuda' if torch.cuda.is_available() and args.cuda else 'cpu')  
  # Horovod: pin GPU to local rank.
  if device.type == 'cuda': torch.cuda.set_device(hvd.local_rank()) 
  
  #Prepare data for train and test
  train_loader, test_loader = data_prep(hvd)

  # Define NN model
  model = Net().to(device)
  
  # Effective batch size in synchronous distributed training is scaled by the number of workers.
  # An increase in learning rate compensates for the increased batch size.
  optimizer = optim.SGD(model.parameters(), lr=args.lr * hvd.size(), momentum=args.momentum)

  # Wrap the optimizer with Horovod's DistributedOptimizer.
  optimizer = hvd.DistributedOptimizer(optimizer, named_parameters=model.named_parameters())
  
  # Broadcast initial parameters so all workers start with the same parameters.
  hvd.broadcast_parameters(model.state_dict(), root_rank=0)

  for epoch in range(1, args.epochs + 1):
    print("Active Run ID: %s, Epoch: %s/%s \n" % (active_run_uuid, epoch, args.epochs))
    train_one_epoch(args, model, device, train_loader, optimizer, epoch)
    test(args, model, device, test_loader, epoch)
    # Only save checkpoints on the first worker.
    if hvd.rank() == 0: save_checkpoint(model, optimizer, epoch) 
      
  if hvd.rank() == 0 and args.save_model: log_mnist_model(model, active_run_uuid)

# COMMAND ----------

# MAGIC %md ## Generate MNIST Model

# COMMAND ----------

with mlflow.start_run() as run:
  active_run_uuid = mlflow.active_run().info.run_uuid
  hr = HorovodRunner(np=NODE_COUNT) # Number of nodes in the cluster.
  hr.run(generate_model_horovod, args = args)

# COMMAND ----------

# MAGIC %md ## Horovod Timeline Tracing

# COMMAND ----------

# MAGIC %md
# MAGIC You can copy the file to the FileStore and download it to local machine. Files stored in /FileStore are accessible in your web browser.
# MAGIC - First copy the generated hvd-demo_timeline.json file so it is accessible from /FileStore using command in the next cell  
# MAGIC - Download the file by opening your browser to https://demo.cloud.databricks.com/files/hvd-demo/hvd-demo_timeline.json  
# MAGIC - Open up Chrome Tracing via chrome://tracing  
# MAGIC - Click on Load to open up the downloaded file to view the trace  

# COMMAND ----------

# Contents of `/FileStore/something` would be accessible at `https://{{region}}.azuredatabricks.net/files/something?o={{workspace_id}}
# Example: https://eastus2.azuredatabricks.net/files/joel/hvd-demo_timeline.json?o=984752964297111
dbutils.fs.cp(timeline_root, '/FileStore/joel/hvd-demo_timeline.json')

# COMMAND ----------

# MAGIC %md ## Test Inference Locally

# COMMAND ----------

# Run saved in AzureML using MLflow
runid = run.info.run_id
print('runid: '+ runid)

# COMMAND ----------

# DBTITLE 1,Load PyTorch Model back from MLflow
# Use the runid to pull in PyTorch Model saved
pytorch_model = mlflow.pytorch.load_model("runs:/" + runid + "/" + MODEL_SAVE_PATH)

# COMMAND ----------

# DBTITLE 1,Sample Test Images
dataiter = iter(test_loader)
images, labels = dataiter.next()

# print images
display(imshow(torchvision.utils.make_grid(images[0:8])))

# COMMAND ----------

print('GroundTruth: ', ' '.join('%5s' % classes[labels[j]] for j in range(8)))

# COMMAND ----------

# DBTITLE 1,Score Test Images
outputs = pytorch_model(images.to(device))
_, predicted = torch.max(outputs, 1)

print('Predicted: ', ' '.join('%5s' % classes[predicted[j]]
                              for j in range(8)))

# COMMAND ----------

# MAGIC %md ## Visualize MNIST Network

# COMMAND ----------

# MAGIC %run ./graphviz/init

# COMMAND ----------

visualize_model(outputs, '/dbfs/FileStore'+MODEL_VISUALIZE_LOC)
displayHTML('''<img src="files'''+MODEL_VISUALIZE_LOC+'''.svg">''')

# COMMAND ----------

# MAGIC %md ## Deploy an Inference Server in ACI

# COMMAND ----------

webservice = azureml_build_deploy(runid, ws, 'pytorch-mnist', 'pytorch-mnist-img', 'pytorch-aci-deploy')

# COMMAND ----------

# MAGIC %md ## Test Scoring from Inference URI

# COMMAND ----------

global PREDICTION_DATA_TENSOR_LABEL
for data, target in test_loader:
    data, target = data.to(device), target.to(device)
    PREDICTION_DATA_TENSOR_LABEL = data, data.view(len(data), data.shape[1]* data.shape[2] * data.shape[3]), target

# COMMAND ----------

data, tensor, label = PREDICTION_DATA_TENSOR_LABEL
TEST_DATA = datasets.MNIST('../data', train=False)

import base64
from io import BytesIO

def show_image(index):
    global TEST_DATA
    image, label = TEST_DATA[index]
    buffered = BytesIO()
    image.save(buffered, format="JPEG")
    img_str = base64.b64encode(buffered.getvalue())
    displayHTML('<img src="data:image/jpeg;base64,{}" style="width:100px;height:120px;"">'.format(img_str.decode('utf-8')))

# COMMAND ----------

import requests
import json

def score_image(index):
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

#There are 1000 examples in our test set for the MNIST image recognition challenge. Pick one at random and display it
index_to_predict = 0
show_image(index_to_predict)

# COMMAND ----------

# Score and display result
score_image(index_to_predict)

# COMMAND ----------

