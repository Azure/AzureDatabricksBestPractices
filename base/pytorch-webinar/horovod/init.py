# Databricks notebook source
import horovod.torch as hvd
from sparkdl import HorovodRunner
from torch.utils.data.distributed import DistributedSampler

# COMMAND ----------

from time import time
import os

LOG_DIR = os.path.join(PYTORCH_CHECKPOINT_DIR, str(time()), 'mnist_demo')
os.makedirs(LOG_DIR)
dbutils.fs.mkdirs(LOG_DIR)

# COMMAND ----------

# Save Horovod timeline
timeline_root = LOG_DIR + "/hvd-demo_timeline.json"
timeline_path = "/dbfs" + timeline_root
os.environ['HOROVOD_TIMELINE'] = timeline_path

# COMMAND ----------

def save_checkpoint(model, optimizer, epoch):
  filepath = LOG_DIR + '/checkpoint-{epoch}.pth.tar'.format(epoch=epoch)
  state = {
    'model': model.state_dict(),
    'optimizer': optimizer.state_dict(),
  }
  torch.save(state, filepath)