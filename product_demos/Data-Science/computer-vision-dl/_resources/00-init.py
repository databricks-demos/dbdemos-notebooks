# Databricks notebook source
dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")
#Empty value will try default: dbdemos with a fallback to hive_metastore
dbutils.widgets.text("catalog", "dbdemos", "Catalog")
dbutils.widgets.text("db", "manufacturing_pcb", "Database")

# COMMAND ----------

# MAGIC %run ../../../../_resources/00-global-setup $reset_all_data=$reset_all_data $catalog=$catalog $db=$db

# COMMAND ----------

import os
import torch
import mlflow

reset_all_data = dbutils.widgets.get("reset_all_data") == "true"
folder = "/dbdemos/manufacturing/pcb/"

if reset_all_data:
  dbutils.fs.rm("/dbdemos/manufacturing/pcb/", True)
if reset_all_data or is_folder_empty(folder+"/labels"):
  #data generation on another notebook to avoid installing libraries (takes a few seconds to setup pip env)
  print(f"Loading raw data under {folder} , please wait a few minutes as we extract all images...")
  path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
  parent_count = path[path.rfind("computer-vision-dl"):].count('/') - 1
  prefix = "./" if parent_count == 0 else parent_count*"../"
  prefix = f'{prefix}_resources/'
  dbutils.notebook.run(prefix+"01-load-data", 600)
else:
  print("data already existing. Run with reset_all_data=true to force a data cleanup for your local demo.")

# COMMAND ----------

import time
import requests

class EndpointApiClient:
    def __init__(self):
        self.base_url =dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get()
        self.token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
        self.headers = {"Authorization": f"Bearer {self.token}", "Content-Type": "application/json"}

    def create_inference_endpoint(self, endpoint_name, served_models):
        data = {"name": endpoint_name, "config": {"served_models": served_models}}
        return self._post("api/2.0/serving-endpoints", data)

    def get_inference_endpoint(self, endpoint_name):
        return self._get(f"api/2.0/serving-endpoints/{endpoint_name}", allow_error=True)
      
      
    def inference_endpoint_exists(self, endpoint_name):
      ep = self.get_inference_endpoint(endpoint_name)
      if 'error_code' in ep and ep['error_code'] == 'RESOURCE_DOES_NOT_EXIST':
          return False
      if 'error_code' in ep and ep['error_code'] != 'RESOURCE_DOES_NOT_EXIST':
          raise Exception(f"endpoint exists ? {ep}")
      return True

    def create_endpoint_if_not_exists(self, endpoint_name, model_name, model_version, workload_size, scale_to_zero_enabled=True, wait_start=True):
      models = [{
            "model_name": model_name,
            "model_version": model_version,
            "workload_size": workload_size,
            "scale_to_zero_enabled": scale_to_zero_enabled,
      }]
      if not self.inference_endpoint_exists(endpoint_name):
        r = self.create_inference_endpoint(endpoint_name, models)
      #Make sure we have the proper version deployed
      else:
        ep = self.get_inference_endpoint(endpoint_name)
        if 'pending_config' in ep:
            self.wait_endpoint_start(endpoint_name)
            ep = self.get_inference_endpoint(endpoint_name)
        if 'pending_config' in ep:
            model_deployed = ep['pending_config']['served_models'][0]
            print(f"Error with the model deployed: {model_deployed} - state {ep['state']}")
        else:
            model_deployed = ep['config']['served_models'][0]
        if model_deployed['model_version'] != model_version:
          print(f"Current model is version {model_deployed['model_version']}. Updating to {model_version}...")
          u = self.update_model_endpoint(endpoint_name, {"served_models": models})
      if wait_start:
        self.wait_endpoint_start(endpoint_name)
        time.sleep(10)
      
    def list_inference_endpoints(self):
        return self._get("api/2.0/serving-endpoints")

    def update_model_endpoint(self, endpoint_name, conf):
        return self._put(f"api/2.0/serving-endpoints/{endpoint_name}/config", conf)

    def delete_inference_endpoint(self, endpoint_name):
        return self._delete(f"api/2.0/serving-endpoints/{endpoint_name}")

    def wait_endpoint_start(self, endpoint_name):
      i = 0
      while self.get_inference_endpoint(endpoint_name)['state']['config_update'] == "IN_PROGRESS" and i < 50:
        print("waiting for endpoint to build model image and start")
        time.sleep(30)
        i += 1
      state = self.get_inference_endpoint(endpoint_name)
      if state['state']['config_update'] == "UPDATE_FAILED":
        print(f'WARN: ENDPOINT UPDATE FAILED: {state}')
      else:
        i = 0
        while self.get_inference_endpoint(endpoint_name)['state']['ready'] != "READY" and i < 10:
          print("Endpoint not ready...")
          time.sleep(10)
          i += 1

      
    # Making predictions

    def query_inference_endpoint(self, endpoint_name, data):
        return self._post(f"realtime-inference/{endpoint_name}/invocations", data)

    # Debugging

    def get_served_model_build_logs(self, endpoint_name, served_model_name):
        return self._get(
            f"api/2.0/serving-endpoints/{endpoint_name}/served-models/{served_model_name}/build-logs"
        )

    def get_served_model_server_logs(self, endpoint_name, served_model_name):
        return self._get(
            f"api/2.0/serving-endpoints/{endpoint_name}/served-models/{served_model_name}/logs"
        )

    def get_inference_endpoint_events(self, endpoint_name):
        return self._get(f"api/2.0/serving-endpoints/{endpoint_name}/events")

    def _get(self, uri, data = {}, allow_error = False):
        r = requests.get(f"{self.base_url}/{uri}", params=data, headers=self.headers)
        return self._process(r, allow_error)

    def _post(self, uri, data = {}, allow_error = False):
        return self._process(requests.post(f"{self.base_url}/{uri}", json=data, headers=self.headers), allow_error)

    def _put(self, uri, data = {}, allow_error = False):
        return self._process(requests.put(f"{self.base_url}/{uri}", json=data, headers=self.headers), allow_error)

    def _delete(self, uri, data = {}, allow_error = False):
        return self._process(requests.delete(f"{self.base_url}/{uri}", json=data, headers=self.headers), allow_error)

    def _process(self, r, allow_error = False):
      if r.status_code == 500 or r.status_code == 403 or not allow_error:
        print(r.text)
        r.raise_for_status()
      return r.json()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extra: ligthning dataloader from hugging face dataset
# MAGIC
# MAGIC If your dataset is small, you could also load it from your spark dataframe with the huggingface dataset library:

# COMMAND ----------

def define_lightning_dataset_moduel():

  import pytorch_lightning as pl
  from datasets import Dataset
  class DeltaDataModuleHF(pl.LightningDataModule):
      from torch.utils.data import random_split, DataLoader
      def __init__(self, df, batch_size: int = 64):
          super().__init__()
          # For big dataset, you can use IterableDataset.from_spark()
          self.dataset = Dataset.from_spark(df.select('content', 'label'))
          self.splits = self.dataset.train_test_split(test_size=0.1)
          self.batch_size = batch_size
          self.transform = tf.Compose([
                  tf.Lambda(lambda b: Image.open(io.BytesIO(b)).convert("RGB")),
                  tf.Resize(256),
                  tf.CenterCrop(224),
                  tf.ToTensor(),
                  tf.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225])
              ])
          
          self.train_ds = self.splits['train'].map(lambda e: {'content': self.transform(e['content']), 'label': e['label']})
          self.train_ds.set_format(type='torch')
          self.val_ds = self.splits['test'].map(lambda e: {'content': self.transform(e['content']), 'label': e['label']})
          self.val_ds.set_format(type='torch')


      def setup(self, stage: str):
          print(f"preparing dataset, stage: {stage}")

      def train_dataloader(self):
          return torch.utils.data.DataLoader(self.train_ds, batch_size=self.batch_size, num_workers=8)
        
      def val_dataloader(self):
          return torch.utils.data.DataLoader(self.val_ds, batch_size=self.batch_size, num_workers=8)

      def test_dataloader(self):
          return torch.utils.data.DataLoader(self.val_ds, batch_size=self.batch_size, num_workers=8)
