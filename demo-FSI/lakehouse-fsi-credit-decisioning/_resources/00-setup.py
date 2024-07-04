# Databricks notebook source
# MAGIC %run ../config

# COMMAND ----------

dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")
reset_all_data = dbutils.widgets.get("reset_all_data") == "true"

# COMMAND ----------

# MAGIC %run ../../../_resources/00-global-setup-v2

# COMMAND ----------

# MAGIC %run ./01-load-data $reset_all_data=$reset_all_data

# COMMAND ----------

import mlflow
import time 
import plotly.express as px
import shap
import pandas as pd
import numpy as np

from mlflow.store.artifact.models_artifact_repo import ModelsArtifactRepository
import plotly.express as px
from plotly.subplots import make_subplots
import plotly.graph_objects as go
from mlflow.models.model import Model
from databricks import feature_store
import pyspark.sql.functions as F
from pyspark.sql.functions import col
from datetime import date

#credit_automl_run_name = "lakehouse_fsi_credit_decisioning_auto_ml"
#credit_model_name = "dbdemos_fsi_credit_decisioning"


# Helper function
def get_latest_model_version(model_name):
    from mlflow.tracking import MlflowClient
    mlflow_client = MlflowClient(registry_uri="databricks-uc")
    latest_version = 1
    for mv in mlflow_client.search_model_versions(f"name='{model_name}'"):
        version_int = int(mv.version)
        if version_int > latest_version:
            latest_version = version_int
    return latest_version
  
  
def drop_fs_table(table_name):
  from databricks.feature_store import FeatureStoreClient
  fs = FeatureStoreClient()
  try:
    fs.drop_table(table_name)  
  except Exception as e:
    print(f"Can't drop the fs table, probably doesn't exist? {e}")
  try:
    spark.sql(f"DROP TABLE IF EXISTS `{table_name}`")
  except Exception as e:
    print(f"Can't drop the delta table, probably doesn't exist? {e}")
