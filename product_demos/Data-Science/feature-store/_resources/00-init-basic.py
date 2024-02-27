# Databricks notebook source
# MAGIC %md 
# MAGIC # init notebook setting up the backend. 
# MAGIC
# MAGIC Do not edit the notebook, it contains import and helpers for the demo

# COMMAND ----------

dbutils.widgets.text("catalog", "feat_eng", "Catalog")
catalog = dbutils.widgets.get("catalog")
main_naming = "dbdemos_fs_travel"
database_name = f"{main_naming}_shared"

# COMMAND ----------

# MAGIC %run ../../../../_resources/00-global-setup $reset_all_data=false $catalog=feat_eng $db=dbdemos_fs_travel_shared

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

model_registry_name = f"{main_naming}_shared_model"
model_name_advanced = f"{main_naming}_advanced_shared_model"
model_name_expert = f"{main_naming}_expert_shared_model"

# COMMAND ----------

def delete_fs(fs_table_name):
  print("Deleting Feature Table", fs_table_name)
  try:
    fs = feature_store.FeatureStoreClient()
    fs.drop_table(name=fs_table_name)
    spark.sql(f"DROP TABLE IF EXISTS {fs_table_name}")
  except Exception as e:
    print("Can't delete table, likely not existing: "+str(e))  

def delete_fss(catalog, tables):
  for table in tables:
    delete_fs(catalog+"."+table)


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

#Once the automl experiment is created, we assign CAN MANAGE to all users as it's shared in the workspace
def set_experiment_permission(experiment_id, experiment_path):
  url = get_current_url()
  import requests
  pat_token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
  headers =  {"Authorization": "Bearer " + pat_token, 'Content-type': 'application/json'}
  status = requests.get(url+"/api/2.0/workspace/get-status", params = {"path": experiment_path}, headers=headers).json()
  #Set can manage to all users to the experiment we created as it's shared among all
  params = {"access_control_list": [{"group_name": "users","permission_level": "CAN_MANAGE"}]}
  permissions = requests.patch(f"{url}/api/2.0/permissions/experiments/{status['object_id']}", json = params, headers=headers)
  if permissions.status_code != 200:
    print("ERROR: couldn't set permission to all users to the autoML experiment")
  path = experiment_path
  path = path[:path.rfind('/')]
  path = path[:path.rfind('/')]
  #List to get the folder with the notebooks from the experiment
  folders = requests.get(url+"/api/2.0/workspace/list", params = {"path": path}, headers=headers).json()
  for f in folders['objects']:
    if f['object_type'] == 'DIRECTORY' and path == f['path']:
        #Set the permission of the experiment notebooks to all
        permissions = requests.patch(f"{url}/api/2.0/permissions/directories/{f['object_id']}", json = params, headers=headers)
        if permissions.status_code != 200:
          print("ERROR: couldn't set permission to all users to the autoML experiment notebooks")
          
import mlflow          
def init_experiment_for_batch(demo_name, experiment_name):
  #You can programatically get a PAT token with the following
  pat_token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
  url = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get()
  #current_user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
  import requests
  xp_root_path = f"/dbdemos/experiments/{demo_name}"
  requests.post(f"{get_current_url()}/api/2.0/workspace/mkdirs", headers = get_request_headers(), json={ "path": xp_root_path})
  xp_path = f"{xp_root_path}/{experiment_name}"
  mlflow.set_experiment(xp_path)
  xp = mlflow.get_experiment_by_name(xp_path)
  set_experiment_permission(xp.experiment_id, xp.name)

init_experiment_for_batch("feature_store", "introduction")
