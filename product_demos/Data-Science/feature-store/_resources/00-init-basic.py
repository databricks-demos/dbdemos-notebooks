# Databricks notebook source
# MAGIC %md 
# MAGIC # init notebook setting up the backend. 
# MAGIC
# MAGIC Do not edit the notebook, it contains import and helpers for the demo

# COMMAND ----------

dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")
reset_all_data = dbutils.widgets.get("reset_all_data") == "true"

# COMMAND ----------

catalog = "main__build"
main_naming = "dbdemos_fs_travel"
schema = dbName = db = "dbdemos_fs_travel"

# COMMAND ----------

# MAGIC %run ../../../../_resources/00-global-setup-v2

# COMMAND ----------

DBDemos.setup_schema(catalog, db, reset_all_data)

# COMMAND ----------

# *****
# Loading Modules
# *****
import pyspark.sql.functions as F
import pyspark.sql.types as T
import pyspark.sql.window as w

from databricks import feature_store
from databricks.feature_store import feature_table, FeatureLookup

import mlflow
import mlflow.sklearn
from mlflow.models.signature import infer_signature
from mlflow.tracking import MlflowClient

import uuid
import os
import requests

from pyspark.sql.functions import lit, expr, rand, col, count, mean, unix_timestamp, window, when
from pyspark.sql.types import StringType, DoubleType, IntegerType, LongType
import numpy as np

#Add features from the time variable 
def add_time_features_spark(df):
    return add_time_features(df.pandas_api()).to_spark()

def add_time_features(df):
    # Extract day of the week, day of the month, and hour from the ts column
    df['day_of_week'] = df['ts'].dt.dayofweek
    df['day_of_month'] = df['ts'].dt.day
    df['hour'] = df['ts'].dt.hour
    
    # Calculate sin and cos values for the day of the week, day of the month, and hour
    df['day_of_week_sin'] = np.sin(df['day_of_week'] * (2 * np.pi / 7))
    df['day_of_week_cos'] = np.cos(df['day_of_week'] * (2 * np.pi / 7))
    df['day_of_month_sin'] = np.sin(df['day_of_month'] * (2 * np.pi / 30))
    df['day_of_month_cos'] = np.cos(df['day_of_month'] * (2 * np.pi / 30))
    df['hour_sin'] = np.sin(df['hour'] * (2 * np.pi / 24))
    df['hour_cos'] = np.cos(df['hour'] * (2 * np.pi / 24))
    df = df.drop(['day_of_week', 'day_of_month', 'hour'], axis=1)
    return df

# COMMAND ----------

# Below are initialization related functions
def get_cloud_name():
    return spark.conf.get("spark.databricks.clusterUsageTags.cloudProvider").lower()

def get_current_url():
  return dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get()

def get_username() -> str:  # Get the user's username
    return dbutils().notebook.entry_point.getDbutils().notebook().getContext().tags().apply("user").lower().split("@")[0].replace(".", "_")
cleaned_username = get_username()


def get_request_headers() -> str:
    return {
        "Authorization": f"""Bearer {dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()}"""
    }

def get_instance() -> str:
    return dbutils().notebook.entry_point.getDbutils().notebook().getContext().tags().apply("browserHostName")

# COMMAND ----------

def delete_fs(fs_table_name):
  print("Deleting Feature Table", fs_table_name)
  try:
    fs = feature_store.FeatureStoreClient()
    fs.drop_table(name=fs_table_name)
    spark.sql(f"DROP TABLE IF EXISTS {fs_table_name}")
  except Exception as e:
    print("Can't delete table, likely not existing: "+str(e))  

def delete_fss(catalog, db, tables):
  for table in tables:
    delete_fs(f"{catalog}.{db}.{table}")

def get_last_model_version(model_full_name):
    mlflow_client = MlflowClient(registry_uri="databricks-uc")
    # Use the MlflowClient to get a list of all versions for the registered model in Unity Catalog
    all_versions = mlflow_client.search_model_versions(f"name='{model_full_name}'")
    # Sort the list of versions by version number and get the latest version
    latest_version = max([int(v.version) for v in all_versions])
    # Use the MlflowClient to get the latest version of the registered model in Unity Catalog
    return mlflow_client.get_model_version(model_full_name, str(latest_version))


# COMMAND ----------

from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, FunctionTransformer, StandardScaler
from sklearn.model_selection import train_test_split
import mlflow
from databricks.automl_runtime.sklearn.column_selector import ColumnSelector
import databricks.automl_runtime


import lightgbm
from lightgbm import LGBMClassifier
import pandas as pd

params = {
  "colsample_bytree": 0.40,
  "lambda_l1": 0.15,
  "lambda_l2": 2.1,
  "learning_rate": 4.1,
  "max_bin": 14,
  "max_depth": 12,
  "min_child_samples": 182,
  "n_estimators": 100,
  "num_leaves": 790,
  "path_smooth": 68.0,
  "subsample": 0.52,
  "random_state": 607,
}

# COMMAND ----------

travel_purchase_df = spark.read.option("inferSchema", "true").load("/databricks-datasets/travel_recommendations_realtime/raw_travel_data/fs-demo_vacation-purchase_logs/", format="csv", header="true")
travel_purchase_df = travel_purchase_df.withColumn("id", F.monotonically_increasing_id())
travel_purchase_df.withColumn("booking_date", F.col("booking_date").cast('date')).write.mode('overwrite').saveAsTable('travel_purchase')

# COMMAND ----------

import warnings

with warnings.catch_warnings():
    warnings.simplefilter('ignore', SyntaxWarning)
    warnings.simplefilter('ignore', DeprecationWarning)
    warnings.simplefilter('ignore', UserWarning)
    warnings.simplefilter('ignore', FutureWarning)
    

# COMMAND ----------

#dbdemos__delete_this_cell
#force the experiment to the field demos one. Required to launch as a batch
def init_experiment_for_batch(demo_name, experiment_name):
  from databricks.sdk import WorkspaceClient
  w = WorkspaceClient()
  xp_root_path = f"/Shared/dbdemos/experiments/{demo_name}"
  try:
    r = w.workspace.mkdirs(path=xp_root_path)
  except Exception as e:
    print(f"ERROR: couldn't create a folder for the experiment under {xp_root_path} - please create the folder manually or  skip this init (used for job only: {e})")
    raise e
  xp = f"{xp_root_path}/{experiment_name}"
  print(f"Using common experiment under {xp}")
  mlflow.set_experiment(xp)
  
init_experiment_for_batch("feature_store", "introduction")
