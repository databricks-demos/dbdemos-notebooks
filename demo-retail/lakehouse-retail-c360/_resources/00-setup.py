# Databricks notebook source
# MAGIC %run ../config

# COMMAND ----------

dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")
reset_all_data = dbutils.widgets.get("reset_all_data") == "true"

# COMMAND ----------

import sys
major, minor = sys.version_info[:2]
assert (major, minor) >= (3, 11), f"This demo expect python version 3.11, but found {major}.{minor}. \nUse DBR15.4 or above. \nIf you're on serverless compute, open the 'Environment' menu on the right of your notebook, set it to >=2 and apply."

# COMMAND ----------

# MAGIC %run ../../../_resources/00-global-setup-v2

# COMMAND ----------

DBDemos.setup_schema(catalog, db, reset_all_data, volume_name)
volume_folder =  f"/Volumes/{catalog}/{db}/{volume_name}"

# COMMAND ----------

def get_last_model_version(model_full_name):
    from mlflow import MlflowClient
    mlflow_client = MlflowClient(registry_uri="databricks-uc")
    # Use the MlflowClient to get a list of all versions for the registered model in Unity Catalog
    all_versions = mlflow_client.search_model_versions(f"name='{model_full_name}'")
    # Sort the list of versions by version number and get the latest version
    latest_version = max([int(v.version) for v in all_versions])
    # Use the MlflowClient to get the latest version of the registered model in Unity Catalog
    return mlflow_client.get_model_version(model_full_name, str(latest_version)).version

# COMMAND ----------

import json
from datetime import datetime
import time
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, sha1, col, initcap, to_timestamp

folder = f"/Volumes/{catalog}/{db}/{volume_name}"

if reset_all_data or DBDemos.is_any_folder_empty([folder+"/orders", folder+"/users", folder+"/events"]):
  #data generation on another notebook to avoid installing libraries (takes a few seconds to setup pip env)
  print(f"Generating data under {folder} , please wait a few sec...")
  path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
  parent_count = path[path.rfind("lakehouse-retail-c360"):].count('/') - 1
  prefix = "./" if parent_count == 0 else parent_count*"../"
  prefix = f'{prefix}_resources/'
  dbutils.notebook.run(prefix+"01-load-data", 600, {"reset_all_data": dbutils.widgets.get("reset_all_data")})
else:
  print("data already existing. Run with reset_all_data=true to force a data cleanup for your local demo.")
