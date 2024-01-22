# Databricks notebook source
dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")

# COMMAND ----------

# MAGIC %run ../../../_resources/00-global-setup $reset_all_data=$reset_all_data $catalog=dbdemos $db=fsi_fraud_detection

# COMMAND ----------

# MAGIC %run ./01-load-data $reset_all_data=$reset_all_data

# COMMAND ----------

import mlflow
if "evaluate" not in dir(mlflow):
    raise Exception("ERROR - YOU NEED MLFLOW 2.0 for this demo. Select DBRML 12+")
    
from databricks.feature_store import FeatureStoreClient
fs = FeatureStoreClient()
from mlflow.store.artifact.models_artifact_repo import ModelsArtifactRepository
import plotly.express as px
from plotly.subplots import make_subplots
import plotly.graph_objects as go
from mlflow.models.model import Model

fraud_automl_run_name = "lakehouse_fsi_auto_ml"
fraud_model_name = "dbdemos_fsi_fraud"

#As we have lot of data, autoML run can take some time. Let's reduce the size to make it faster & work on 10min in small nodes (keeping all fraud transactions)
def reduce_df(df):
  fraud = df.where('is_fraud')
  non_fraud = df.where('not is_fraud')
  return non_fraud.sample(fraud.count()/non_fraud.count()*2).union(fraud)

def display_automl_fraud_link(dataset, model_name, force_refresh = False): 
  if force_refresh:
    reset_automl_run(fraud_automl_run_name)
  display_automl_link(fraud_automl_run_name, model_name, reduce_df(dataset), "is_fraud", 10, move_to_production=False)

def get_automl_fraud_run(force_refresh = False): 
  if force_refresh:
    reset_automl_run(fraud_automl_run_name)
  from_cache, r = get_automl_run_or_start(fraud_automl_run_name, fraud_model_name, reduce_df(fs.read_table(f'dbdemos.fsi_fraud_detection.transactions_features')), "is_fraud", 5, move_to_production=False)
  return r

# COMMAND ----------

import urllib
import json
import mlflow

class EndpointApiClient:
    def __init__(self):
        self.base_url =dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get()
        self.token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
        self.headers = {"Authorization": f"Bearer {self.token}", "Content-Type": "application/json"}

    def create_inference_endpoint(self, endpoint_name, served_models):
        data = {"name": endpoint_name, "config": {"served_models": served_models}}
        return self._post("api/2.0/serving-endpoints", data)

    def get_inference_endpoint(self, endpoint_name):
        return self._get(f"api/2.0/serving-endpoints/{endpoint_name}")
      
      
    def inference_endpoint_exists(self, endpoint_name):
      ep = self.get_inference_endpoint(endpoint_name)
      if 'error_code' in ep and ep['error_code'] == 'RESOURCE_DOES_NOT_EXIST':
          return False
      if 'error_code' in ep and ep['error_code'] != 'RESOURCE_DOES_NOT_EXIST':
          raise Exception(f"endpoint exists ? {ep}")
      return True

    def create_endpoint_if_not_exists(self, endpoint_name, model_name, model_version, workload_size, scale_to_zero_enabled=True, wait_start=True):
      if not self.inference_endpoint_exists(endpoint_name):
        models = [{
              "model_name": model_name,
              "model_version": model_version,
              "workload_size": workload_size,
              "scale_to_zero_enabled": scale_to_zero_enabled,
        }]
        self.create_inference_endpoint(endpoint_name, models)
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
        time.sleep(5)
        i += 1
        print("waiting for endpoint to start")
      
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

    def _get(self, uri, data = {}):
        return requests.get(f"{self.base_url}/{uri}", params=data, headers=self.headers).json()

    def _post(self, uri, data = {}):
        return requests.post(f"{self.base_url}/{uri}", json=data, headers=self.headers).json()

    def _put(self, uri, data = {}):
        return requests.put(f"{self.base_url}/{uri}", json=data, headers=self.headers).json()

    def _delete(self, uri, data = {}):
        return requests.delete(f"{self.base_url}/{uri}", json=data, headers=self.headers).json()


serving_client = EndpointApiClient()


# COMMAND ----------

spark.sql(f"GRANT ALL PRIVILEGES ON SCHEMA dbdemos.fsi_fraud_detection TO `account users`")
for table in spark.sql("show tables in dbdemos.fsi_fraud_detection").collect():
    spark.sql(f"GRANT ALL PRIVILEGES ON TABLE dbdemos.fsi_fraud_detection.`{table['tableName']}` TO `account users`")
