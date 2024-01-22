# Databricks notebook source
dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")
dbutils.widgets.text('catalog', 'dbdemos', 'Catalog')
dbutils.widgets.text('db', 'hls_patient_readmission', 'Database')

# COMMAND ----------

# MAGIC %run ../../../_resources/00-global-setup $reset_all_data=$reset_all_data $catalog=$catalog $db=$db

# COMMAND ----------

#This  will download the data from our github repo to accelerate the demo start.
#Alternatively, you can run [00-generate-synthea-data]($./00-generate-synthea-data) to generate the data yourself with synthea.

reset_all_data = dbutils.widgets.get("reset_all_data") == "true"
import os
import requests
import timeit
import time
folder = "/dbdemos/hls/synthea"

folders = ["/landing_zone/encounters", "/landing_zone/patients", "/landing_zone/conditions", "/landing_zone/medications", "/landing_zone/immunizations", "/landing_zone/location_ref", "/landing_vocab/CONCEPT", "/landing_vocab/CONCEPT_RELATIONSHIP"]
                               
if reset_all_data or any([is_folder_empty(folder+f) for f in folders]):
  if reset_all_data:
    assert len(folder) > 5
    dbutils.fs.rm(folder, True)
  for f in folders:
      download_file_from_git('/dbfs'+folder+f, "databricks-demos", "dbdemos-dataset", "/hls/synthea"+f.replace("landing_zone", "landing_zone_parquet"))

else:
  print("data already existing. Run with reset_all_data=true to force a data cleanup for your local demo.")

# COMMAND ----------

# Try to set all users as owner or full privilege
spark.sql(f'alter schema {catalog}.{dbName} owner to `account users`')
for t in spark.sql(f'SHOW TABLES in {catalog}.{dbName}').collect():
    try:
        spark.sql(f'GRANT ALL PRIVILEGES ON TABLE {catalog}.{dbName}.{t["tableName"]} TO `account users`')
        spark.sql(f'ALTER TABLE {catalog}.{dbName}.{t["tableName"]} OWNER TO `account users`')
    except Exception as e:
        if "TRANSFER_MATERIALIZED_VIEW_OWNERSHIP" not in str(e) and "UNSUPPORTED_OPERATION" not in str(e) :
            print(str(e))
            print(f'WARN: Couldn t set table {catalog}.{dbName}.{t["tableName"]} owner to account users, error: {e}')

# COMMAND ----------

db_name = dbName 
import mlflow
import time 
import plotly.express as px
import shap
import pandas as pd
import numpy as np

if "evaluate" not in dir(mlflow):
    raise Exception("ERROR - YOU NEED MLFLOW 2.0 for this demo. Select DBRML 12+")
    
from databricks.feature_store import FeatureStoreClient
fs = FeatureStoreClient()
from mlflow.store.artifact.models_artifact_repo import ModelsArtifactRepository
import plotly.express as px
from plotly.subplots import make_subplots
import plotly.graph_objects as go
from mlflow.models.model import Model
from databricks import feature_store
from pyspark.sql.types import *
from pyspark.sql.functions import *

from datetime import date

def drop_fs_table(table_name):
  try:
    fs.drop_table(table_name)  
  except Exception as e:
    print(f"Can't drop the fs table, probably doesn't exist? {e}")
  try:
    spark.sql(f"DROP TABLE IF EXISTS `{table_name}`")
  except Exception as e:
    print(f"Can't drop the delta table, probably doesn't exist? {e}")


# COMMAND ----------

import urllib
import json
import mlflow
import time

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
      
      
    def list_inference_endpoints(self):
        return self._get("api/2.0/serving-endpoints")

    def update_model_endpoint(self, endpoint_name, conf):
        return self._put(f"api/2.0/serving-endpoints/{endpoint_name}/config", conf)

    def delete_inference_endpoint(self, endpoint_name):
        return self._delete(f"api/2.0/serving-endpoints/{endpoint_name}")

    def wait_endpoint_start(self, endpoint_name):
      i = 0
      while self.get_inference_endpoint(endpoint_name)['state']['config_update'] == "IN_PROGRESS" and i < 500:
        print("waiting for endpoint to build model image and start")
        time.sleep(30)
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
