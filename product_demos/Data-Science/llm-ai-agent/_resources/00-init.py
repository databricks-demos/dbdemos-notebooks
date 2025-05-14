# Databricks notebook source
# MAGIC %md 
# MAGIC # init notebook setting up the backend. 
# MAGIC
# MAGIC Do not edit the notebook, it contains import and helpers for the demo
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection or disable tracker during installation. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=data-science&org_id=1444828305810485&notebook=00-init&demo_name=chatbot-rag-llm&event=VIEW">

# COMMAND ----------

# MAGIC %run ../config

# COMMAND ----------

from pyspark.sql.functions import pandas_udf
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql.functions import col, udf, length, pandas_udf
import os
import mlflow
import yaml
from typing import Iterator
from mlflow import MlflowClient
mlflow.set_registry_uri('databricks-uc')

# Workaround for a bug fix that is in progress
mlflow.spark.autolog(disable=True)

import warnings
warnings.filterwarnings("ignore")
# Disable MLflow warnings
import logging
logging.getLogger('mlflow').setLevel(logging.ERROR)

# COMMAND ----------

def use_and_create_db(catalog, dbName, cloud_storage_path = None):
  print(f"USE CATALOG `{catalog}`")
  spark.sql(f"USE CATALOG `{catalog}`")
  spark.sql(f"""create database if not exists `{dbName}` """)

assert catalog not in ['hive_metastore', 'spark_catalog'], "Please use a UC schema"
#If the catalog is defined, we force it to the given value and throw exception if not.
if len(catalog) > 0:
  current_catalog = spark.sql("select current_catalog()").collect()[0]['current_catalog()']
  if current_catalog != catalog:
    catalogs = [r['catalog'] for r in spark.sql("SHOW CATALOGS").collect()]
    if catalog not in catalogs:
      spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
  use_and_create_db(catalog, dbName)


print(f"using catalog.database `{catalog}`.`{dbName}`")
spark.sql(f"""USE `{catalog}`.`{dbName}`""")    

# COMMAND ----------

# DBTITLE 1,Optional: Allowing Model Serving IPs
#If your workspace has ip access list, you need to allow your model serving endpoint to hit your AI gateway. Based on your region, IPs might change. Please reach out your Databrics Account team for more details.

#def allow_serverless_ip():
#  from databricks.sdk import WorkspaceClient
#  from databricks.sdk.service import settings
#
#  w = WorkspaceClient()
#
#  # cleanup
#  w.ip_access_lists.delete(ip_access_list_id='xxxx')
#  created = w.ip_access_lists.create(label=f'serverless-model-serving',
#                                    ip_addresses=['xxxx/32'],
#                                    list_type=settings.ListType.ALLOW)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## Helpers to get catalog and index status:

# COMMAND ----------

# Helper function
def get_latest_model_version(model_name):
    mlflow_client = MlflowClient(registry_uri="databricks-uc")
    latest_version = 1
    for mv in mlflow_client.search_model_versions(f"name='{model_name}'"):
        version_int = int(mv.version)
        if version_int > latest_version:
            latest_version = version_int
    return latest_version

# COMMAND ----------

# DBTITLE 1,endpoint
import time

def endpoint_exists(vsc, vs_endpoint_name):
  try:
    return vs_endpoint_name in [e['name'] for e in vsc.list_endpoints().get('endpoints', [])]
  except Exception as e:
    #Temp fix for potential REQUEST_LIMIT_EXCEEDED issue
    if "REQUEST_LIMIT_EXCEEDED" in str(e):
      print("WARN: couldn't get endpoint status due to REQUEST_LIMIT_EXCEEDED error. The demo will consider it exists")
      return True
    else:
      raise e

def wait_for_vs_endpoint_to_be_ready(vsc, vs_endpoint_name):
  for i in range(180):
    try:
      endpoint = vsc.get_endpoint(vs_endpoint_name)
    except Exception as e:
      #Temp fix for potential REQUEST_LIMIT_EXCEEDED issue
      if "REQUEST_LIMIT_EXCEEDED" in str(e):
        print("WARN: couldn't get endpoint status due to REQUEST_LIMIT_EXCEEDED error. Please manually check your endpoint status")
        return
      else:
        raise e
    status = endpoint.get("endpoint_status", endpoint.get("status"))["state"].upper()
    if "ONLINE" in status:
      return endpoint
    elif "PROVISIONING" in status or i <6:
      if i % 20 == 0: 
        print(f"Waiting for endpoint to be ready, this can take a few min... {endpoint}")
      time.sleep(10)
    else:
      raise Exception(f'''Error with the endpoint {vs_endpoint_name}. - this shouldn't happen: {endpoint}.\n Please delete it and re-run the previous cell: vsc.delete_endpoint("{vs_endpoint_name}")''')
  raise Exception(f"Timeout, your endpoint isn't ready yet: {vsc.get_endpoint(vs_endpoint_name)}")

# COMMAND ----------

# DBTITLE 1,index
def index_exists(vsc, endpoint_name, index_full_name):
    try:
        vsc.get_index(endpoint_name, index_full_name).describe()
        return True
    except Exception as e:
        if 'RESOURCE_DOES_NOT_EXIST' not in str(e):
            print(f'Unexpected error describing the index. This could be a permission issue.')
            raise e
    return False
    
def wait_for_index_to_be_ready(vsc, vs_endpoint_name, index_name):
  for i in range(180):
    idx = vsc.get_index(vs_endpoint_name, index_name).describe()
    index_status = idx.get('status', idx.get('index_status', {}))
    status = index_status.get('detailed_state', index_status.get('status', 'UNKNOWN')).upper()
    url = index_status.get('index_url', index_status.get('url', 'UNKNOWN'))
    if "ONLINE" in status:
      return
    if "UNKNOWN" in status:
      print(f"Can't get the status - will assume index is ready {idx} - url: {url}")
      return
    elif "PROVISIONING" in status:
      if i % 40 == 0: print(f"Waiting for index to be ready, this can take a few min... {index_status} - pipeline url:{url}")
      time.sleep(10)
    else:
        raise Exception(f'''Error with the index - this shouldn't happen. DLT pipeline might have been killed.\n Please delete it and re-run the previous cell: vsc.delete_index("{index_name}, {vs_endpoint_name}") \nIndex details: {idx}''')
  raise Exception(f"Timeout, your index isn't ready yet: {vsc.get_index(index_name, vs_endpoint_name)}")

def wait_for_model_serving_endpoint_to_be_ready(ep_name):
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.serving import EndpointStateReady, EndpointStateConfigUpdate
    import time

    # TODO make the endpoint name as a param
    # Wait for it to be ready
    w = WorkspaceClient()
    state = ""
    for i in range(200):
        state = w.serving_endpoints.get(ep_name).state
        if state.config_update == EndpointStateConfigUpdate.IN_PROGRESS:
            if i % 40 == 0:
                print(f"Waiting for endpoint to deploy {ep_name}. Current state: {state}")
            time.sleep(10)
        elif state.ready == EndpointStateReady.READY:
          print('endpoint ready.')
          return
        else:
          break
    raise Exception(f"Couldn't start the endpoint, timeout, please check your endpoint for more details: {state}")

# COMMAND ----------

#Display a better quota message 
def display_quota_error(e, ep_name):
  if "QUOTA_EXCEEDED" in str(e): 
    displayHTML(f'<div style="background-color: #ffd5b8; border-radius: 15px; padding: 20px;"><h1>Error: Vector search Quota exceeded in endpoint {ep_name}</h1><p>Please select another endpoint in the ../config file (VECTOR_SEARCH_ENDPOINT_NAME="<your-endpoint-name>"), or <a href="/compute/vector-search" target="_blank">open the vector search compute page</a> to cleanup resources.</p></div>')

# COMMAND ----------

# DBTITLE 1,Cleanup utility to remove demo assets
def cleanup_demo(catalog, db, serving_endpoint_name, vs_index_fullname):
  vsc = VectorSearchClient()
  try:
    vsc.delete_index(endpoint_name = VECTOR_SEARCH_ENDPOINT_NAME, index_name=vs_index_fullname)
  except Exception as e:
    print(f"can't delete index {VECTOR_SEARCH_ENDPOINT_NAME} {vs_index_fullname} - might not be existing: {e}")
  try:
    WorkspaceClient().serving_endpoints.delete(serving_endpoint_name)
  except Exception as e:
    print(f"can't delete serving endpoint {serving_endpoint_name} - might not be existing: {e}")
  spark.sql(f'DROP SCHEMA `{catalog}`.`{db}` CASCADE')

# COMMAND ----------

def pprint(obj):
  import pprint
  pprint.pprint(obj, compact=True, indent=1, width=100)
