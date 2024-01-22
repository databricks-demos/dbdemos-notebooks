# Databricks notebook source
# MAGIC %run ../config

# COMMAND ----------

dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")
reset_all_data = dbutils.widgets.get("reset_all_data") == "true"

# COMMAND ----------

# MAGIC %run ../../../_resources/00-global-setup-v2

# COMMAND ----------

DBDemos.setup_schema(catalog, db, reset_all_data, volume_name)
volume_folder =  f"/Volumes/{catalog}/{db}/{volume_name}"

# COMMAND ----------

import json
import time

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, sha1, col, initcap, to_timestamp
import pyspark.sql.functions as F
from datetime import datetime

import mlflow
if "evaluate" not in dir(mlflow):
    raise Exception("ERROR - YOU NEED MLFLOW 2.0 for this demo. Select DBRML 12+")
    

folder = f"/Volumes/{catalog}/{db}/{volume_name}"

if reset_all_data or DBDemos.is_any_folder_empty([folder+"/historical_turbine_status", folder+"/parts", folder+"/turbine", folder+"/incoming_data"]):
  #data generation on another notebook to avoid installing libraries (takes a few seconds to setup pip env)
  print(f"Generating data under {folder} , please wait a few sec...")
  path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
  parent_count = path[path.rfind("lakehouse-iot-platform"):].count('/') - 1
  prefix = "./" if parent_count == 0 else parent_count*"../"
  prefix = f'{prefix}_resources/'
  dbutils.notebook.run(prefix+"01-load-data", 600, {"reset_all_data": dbutils.widgets.get("reset_all_data"), "catalog": catalog, "db": db})
else:
  print("data already existing. Run with reset_all_data=true to force a data cleanup for your local demo.")
