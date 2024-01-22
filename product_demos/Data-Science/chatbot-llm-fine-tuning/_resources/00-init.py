# Databricks notebook source
# MAGIC %md 
# MAGIC # init notebook setting up the backend. 
# MAGIC
# MAGIC Do not edit the notebook, it contains import and helpers for the demo
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Ffeatures%2Fdolly-chatbot%2Finit&dt=ML">

# COMMAND ----------

dbutils.widgets.text("catalog", "hive_metastore", "Catalog")
dbutils.widgets.text("db", "dbdemos_llm", "Database")
dbutils.widgets.text("reset_all_data", "false", "Reset Data")
reset_all_data = dbutils.widgets.get("reset_all_data") == "true"

catalog = dbutils.widgets.get("catalog")
db = dbutils.widgets.get("db")
db_name = db

import pyspark.sql.functions as F
from pyspark.sql.functions import col, udf, length, pandas_udf


# COMMAND ----------

# MAGIC %run ../../../../_resources/00-global-setup $reset_all_data=$reset_all_data $catalog=$catalog $db=$db

# COMMAND ----------

import gc
from pyspark.sql.functions import pandas_udf
import pandas as pd
from typing import Iterator
import torch

demo_path =  "/dbdemos/product/llm"

# Cache our model to dbfs to avoid loading them everytime
hugging_face_cache = "/dbfs"+demo_path+"/cache/hf"

import os
os.environ['TRANSFORMERS_CACHE'] = hugging_face_cache

# COMMAND ----------

import requests
import time
import re

#Helper to send REST queries. This will try to use an existing warehouse or create a new one.
class SQLStatementAPI:
    def __init__(self, warehouse_name = "dbdemos-shared-endpoint", catalog = "dbdemos", schema = "openai_demo"):
        self.base_url =dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get()
        self.token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
        self.headers = {"Authorization": f"Bearer {self.token}", "Content-Type": "application/json"}
        username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
        username = re.sub("[^A-Za-z0-9]", '_', username)
        warehouse = self.get_or_create_endpoint(username, warehouse_name)
        #Try to create it
        if warehouse is None:
          raise Exception(f"Couldn't find or create a warehouse named {warehouse_name}. If you don't have warehouse creation permission, please change the name to an existing one or ask an admin to create the warehouse with this name.")
        self.warehouse_id = warehouse['warehouse_id']
        self.catalog = catalog
        self.schema = schema
        self.wait_timeout = "50s"

    def get_or_create_endpoint(self, username, endpoint_name):
        ds = self.get_demo_datasource(endpoint_name)
        if ds is not None:
            return ds
        def get_definition(serverless, name):
            return {
                "name": name,
                "cluster_size": "Small",
                "min_num_clusters": 1,
                "max_num_clusters": 1,
                "tags": {
                    "project": "dbdemos"
                },
                "warehouse_type": "PRO",
                "spot_instance_policy": "COST_OPTIMIZED",
                "enable_photon": "true",
                "enable_serverless_compute": serverless,
                "channel": { "name": "CHANNEL_NAME_CURRENT" }
            }
        def try_create_endpoint(serverless):
            w = self._post("api/2.0/sql/warehouses", get_definition(serverless, endpoint_name))
            if "message" in w and "already exists" in w['message']:
                w = self._post("api/2.0/sql/warehouses", get_definition(serverless, endpoint_name+"-"+username))
            if "id" in w:
                return w
            print(f"WARN: Couldn't create endpoint with serverless = {endpoint_name} and endpoint name: {endpoint_name} and {endpoint_name}-{username}. Creation response: {w}")
            return None

        if try_create_endpoint(True) is None:
            #Try to fallback with classic endpoint?
            try_create_endpoint(False)
        ds = self.get_demo_datasource(endpoint_name)
        if ds is not None:
            return ds
        print(f"ERROR: Couldn't create endpoint.")
        return None      
      
    def get_demo_datasource(self, datasource_name):
        data_sources = self._get("api/2.0/preview/sql/data_sources")
        for source in data_sources:
            if source['name'] == datasource_name:
                return source
        """
        #Try to fallback to an existing shared endpoint.
        for source in data_sources:
            if datasource_name in source['name'].lower():
                return source
        for source in data_sources:
            if "dbdemos-shared-endpoint" in source['name'].lower():
                return source
        for source in data_sources:
            if "shared-sql-endpoint" in source['name'].lower():
                return source
        for source in data_sources:
            if "shared" in source['name'].lower():
                return source"""
        return None
      
    def execute_sql(self, sql):
      x = self._post("api/2.0/sql/statements", {"statement": sql, "warehouse_id": self.warehouse_id, "catalog": self.catalog, "schema": self.schema, "wait_timeout": self.wait_timeout})
      return self.result_as_df(x, sql)
    
    def wait_for_statement(self, results, timeout = 600):
      sleep_time = 3
      i = 0
      while i < timeout:
        if results['status']['state'] not in ['PENDING', 'RUNNING']:
          return results
        time.sleep(sleep_time)
        i += sleep_time
        results = self._get(f"api/2.0/sql/statements/{results['statement_id']}")
      self._post(f"api/2.0/sql/statements/{results['statement_id']}/cancel")
      return self._get(f"api/2.0/sql/statements/{results['statement_id']}")
        
      
    def result_as_df(self, results, sql):
      results = self.wait_for_statement(results)
      if results['status']['state'] != 'SUCCEEDED':
        print(f"Query error: {results}")
        return pd.DataFrame([[results['status']['state'],{results['status']['error']['message']}, results]], columns = ['state', 'message', 'results'])
      if results["manifest"]['schema']['column_count'] == 0:
        return pd.DataFrame([[results['status']['state'], sql]], columns = ['state', 'sql'])
      cols = [c['name'] for c in results["manifest"]['schema']['columns']]
      results = results["result"]["data_array"] if "data_array" in results["result"] else []
      return pd.DataFrame(results, columns = cols)

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
        r.raise_for_status()
      return r.json()
    
#sql_api = SQLStatementAPI(warehouse_name = "dbdemos-shared-endpoint-test", catalog = "dbdemos", schema = "openai_demo")
#sql_api.execute_sql("select 'test'")

# COMMAND ----------

import warnings

with warnings.catch_warnings():
    warnings.simplefilter('ignore', SyntaxWarning)
    warnings.simplefilter('ignore', DeprecationWarning)
    warnings.simplefilter('ignore', UserWarning)

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
          raise Exception(f"enpoint exists ? {ep}")
      return True

    def create_enpoint_if_not_exists(self, endpoint_name, model_name, model_version, workload_size, scale_to_zero_enabled=True, wait_start=True, environment_vars = {}):
      models = [{
            "model_name": model_name,
            "model_version": model_version,
            "workload_size": workload_size,
            "scale_to_zero_enabled": scale_to_zero_enabled,
            "environment_vars": environment_vars
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
        if i % 10 == 0:
          print("waiting for endpoint to build model image and start...")
        time.sleep(10)
        i += 1
      ep = self.get_inference_endpoint(endpoint_name)
      if ep['state'].get("ready", None) != "READY":
        print(f"Error creating the endpoint: {ep}")
        
      
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

def wait_for_vs_catalog_to_be_ready(catalog, timeout_sec = 1800):
    for i in range(int(timeout_sec/10)):
        c = vsc.get_catalog(catalog)
        if 'error_code' in c:
            raise Exception(f"Error getting catalog. Is it enabled in this workspace? {c} - {catalog}")
        if c['catalog_status']['state'] != 'STATE_NOT_READY':
            print(f'Your catalog {catalog} is ready!: {vsc.get_catalog(catalog)}')
            return
        if i % 20 == 0:
          print(f'waiting for Vector Search catalog to be ready, this can take a few min... {c}')
        time.sleep(10)
    raise Exception(f"Vector search catalog isn't ready after {i*10}sec")
  


def wait_for_index_to_be_ready(index_name):
  for i in range(180):
    idx = vsc.get_index(index_name)
    if "error_code" in idx:
        raise Exception(f'''Error with the index - this shouldn't happen. DLT pipeline might have been killed.\n Please delete it and re-run the previous cell: vsc.delete_index("{vs_index_fullname}") \nIndex status: {idx}''')
    if vsc.get_index(index_name)['index_status']['state'] == 'NOT_READY':
      if i % 20 == 0: print(f"Waiting for index to build, this can take a few min... {vsc.get_index(index_name)['index_status']['message']}")
      time.sleep(10)
    else:
      return vsc.get_index(index_name)
  raise Exception(f"Timeout, your index isn't ready yet: {vsc.get_index(index_name)}")

# COMMAND ----------

def display_answer(question, answer):
  prompt = answer[0]["prompt"].replace('\n', '<br/>')
  answer = answer[0]["answer"].replace('\n', '<br/>').replace('Answer: ', '')
  #Tune the message with the user running the notebook. In real workd example we'd have a table with the customer details. 
  displayHTML(f"""
              <div style="float: right; width: 600px;">
                <h3>Debugging:</h3>
                <div style="border-radius: 10px; background-color: #ebebeb; padding: 10px; box-shadow: 2px 2px 2px #F7f7f7; margin-bottom: 10px; color: #363636"><strong>Prompt sent to the model:</strong><br/><i>{prompt}</i></div>
              </div>
              <h3>Chatbot:</h3>
              <div style="border-radius: 10px; background-color: #e3f6fc; padding: 10px; width: 600px; box-shadow: 2px 2px 2px #F7f7f7; margin-bottom: 10px; margin-left: 40px; font-size: 14px">
                <img style="float: left; width:40px; margin: -10px 5px 0px -10px" src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/chatbot-rag/robot.png?raw=true"/>Hey! I'm your Databricks assistant. How can I help?
              </div>
              <div style="border-radius: 10px; background-color: #c2efff; padding: 10px; width: 600px; box-shadow: 2px 2px 2px #F7f7f7; margin-bottom: 10px; font-size: 14px">{question}</div>
                <div style="border-radius: 10px; background-color: #e3f6fc; padding: 10px; width: 600px; box-shadow: 2px 2px 2px #F7f7f7; margin-bottom: 10px; margin-left: 40px; font-size: 14px">
                <img style="float: left; width:40px; margin: -10px 5px 0px -10px" src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/chatbot-rag/robot.png?raw=true"/> {answer}
                </div>
        """)

# COMMAND ----------

# DBTITLE 1,Optional: Allowing Model Serving IPs
#If your workspace has ip access list, you need to allow your model serving endpoint to hit your AI gateway. Based on your region, IPs might change. Please reach out your Databrics Account team for more details.

# def allow_serverless_ip():
#   base_url =dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get(),
#   headers = {"Authorization": f"Bearer {<Your PAT Token>}", "Content-Type": "application/json"}
#   return requests.post(f"{base_url}/api/2.0/ip-access-lists", json={"label": "serverless-model-serving", "list_type": "ALLOW", "ip_addresses": ["<IP RANGE>"], "enabled": "true"}, headers = headers).json()
