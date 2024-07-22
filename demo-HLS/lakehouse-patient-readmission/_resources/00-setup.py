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

#This  will download the data from our github repo to accelerate the demo start.
#Alternatively, you can run [00-generate-synthea-data]($./00-generate-synthea-data) to generate the data yourself with synthea.

reset_all_data = dbutils.widgets.get("reset_all_data") == "true"
import os
import requests
import timeit
import time

folders = ["/landing_zone/encounters", "/landing_zone/patients", "/landing_zone/conditions", "/landing_zone/medications", "/landing_zone/immunizations", "/landing_zone/location_ref", "/landing_vocab/CONCEPT", "/landing_vocab/CONCEPT_RELATIONSHIP"]
                               
if reset_all_data or DBDemos.is_any_folder_empty(folders):
  if reset_all_data:
    assert len(volume_folder) > 20 and volume_folder.startswith('/Volumes/')
    dbutils.fs.rm(volume_folder, True)
  for f in folders:
      DBDemos.download_file_from_git(volume_folder+'/'+f, "databricks-demos", "dbdemos-dataset", "/hls/synthea"+f.replace("landing_zone", "landing_zone_parquet"))
else:
  print("data already existing. Run with reset_all_data=true to force a data cleanup for your local demo.")

# COMMAND ----------

import mlflow
import time 
import plotly.express as px
import shap
import pandas as pd
import numpy as np
    
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
import pyspark.sql.functions as F

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

