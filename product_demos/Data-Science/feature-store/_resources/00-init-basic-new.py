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

from databricks.sdk.service.serving import (
    EndpointCoreConfigInput,
    ServedModelInput,
    #ServedModelInputWorkloadSize,
    ServingEndpointDetailed,
)

# COMMAND ----------

# *****
# Loading Modules
# *****
import pyspark.sql.functions as F
import pyspark.sql.types as T
import pyspark.sql.window as w
import timeit
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

def online_table_exists(table_name):
    w = WorkspaceClient()
    try:
        w.online_tables.get(name=table_name)
        return True
    except Exception as e:
        print(str(e))
        return 'already exists' in str(e)
    return False
  
def wait_for_online_tables(catalog, schema, tables, waiting_time = 300):
    sleep_time = 10
    import time
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()
    for table in tables:
        for i in range(int(waiting_time/sleep_time)):
            state = w.online_tables.get(name=f"{catalog}.{db}.{table}").status.detailed_state.value
            if state.startswith('ONLINE'):
                print(f'Table {table} online: {state}')
                break
            time.sleep(sleep_time)
            
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

# DBTITLE 1,Generate the User Demo Dataset
import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta

# 1. Extract unique users
user_ids = (
    travel_purchase_df
    .select("user_id")
    .distinct()
    .toPandas()
)


# 2. State → Cities mapping (all 50 states + DC)
states_cities = {
    "AL": ["Birmingham", "Montgomery", "Mobile"],
    "AK": ["Anchorage", "Fairbanks", "Juneau"],
    "AZ": ["Phoenix", "Tucson", "Mesa"],
    "AR": ["Little Rock", "Fayetteville", "Fort Smith"],
    "CA": ["Los Angeles", "San Francisco", "San Diego", "Sacramento"],
    "CO": ["Denver", "Colorado Springs", "Boulder"],
    "CT": ["Hartford", "New Haven", "Stamford"],
    "DE": ["Wilmington", "Dover"],
    "FL": ["Miami", "Orlando", "Tampa", "Jacksonville"],
    "GA": ["Atlanta", "Savannah", "Augusta"],
    "HI": ["Honolulu", "Hilo"],
    "ID": ["Boise", "Idaho Falls", "Twin Falls"],
    "IL": ["Chicago", "Springfield", "Naperville"],
    "IN": ["Indianapolis", "Fort Wayne", "Evansville"],
    "IA": ["Des Moines", "Cedar Rapids", "Iowa City"],
    "KS": ["Wichita", "Topeka", "Kansas City"],
    "KY": ["Louisville", "Lexington", "Bowling Green"],
    "LA": ["New Orleans", "Baton Rouge", "Shreveport"],
    "ME": ["Portland", "Augusta", "Bangor"],
    "MD": ["Baltimore", "Annapolis", "Rockville"],
    "MA": ["Boston", "Cambridge", "Worcester"],
    "MI": ["Detroit", "Grand Rapids", "Ann Arbor"],
    "MN": ["Minneapolis", "Saint Paul", "Duluth"],
    "MS": ["Jackson", "Biloxi", "Hattiesburg"],
    "MO": ["St. Louis", "Kansas City", "Springfield"],
    "MT": ["Billings", "Bozeman", "Missoula"],
    "NE": ["Omaha", "Lincoln", "Grand Island"],
    "NV": ["Las Vegas", "Reno", "Carson City"],
    "NH": ["Manchester", "Concord", "Nashua"],
    "NJ": ["Newark", "Jersey City", "Trenton"],
    "NM": ["Albuquerque", "Santa Fe", "Las Cruces"],
    "NY": ["New York", "Buffalo", "Rochester", "Albany"],
    "NC": ["Charlotte", "Raleigh", "Durham", "Greensboro"],
    "ND": ["Fargo", "Bismarck", "Grand Forks"],
    "OH": ["Columbus", "Cleveland", "Cincinnati"],
    "OK": ["Oklahoma City", "Tulsa", "Norman"],
    "OR": ["Portland", "Eugene", "Salem"],
    "PA": ["Philadelphia", "Pittsburgh", "Harrisburg"],
    "RI": ["Providence", "Warwick", "Cranston"],
    "SC": ["Charleston", "Columbia", "Greenville"],
    "SD": ["Sioux Falls", "Rapid City", "Pierre"],
    "TN": ["Nashville", "Memphis", "Knoxville"],
    "TX": ["Houston", "Dallas", "Austin", "San Antonio"],
    "UT": ["Salt Lake City", "Provo", "Ogden"],
    "VT": ["Burlington", "Montpelier", "Rutland"],
    "VA": ["Richmond", "Virginia Beach", "Norfolk"],
    "WA": ["Seattle", "Spokane", "Tacoma"],
    "WV": ["Charleston", "Morgantown", "Huntington"],
    "WI": ["Milwaukee", "Madison", "Green Bay"],
    "WY": ["Cheyenne", "Casper", "Laramie"],
    "DC": ["Washington"]
}


# 3. Generate synthetic demographics
np.random.seed(42)
today = datetime.today()

def random_zip():
    # Generate 5-digit random zip (10000–99999)
    return str(random.randint(10000, 99999))

demography = pd.DataFrame({
    "user_id": user_ids["user_id"],
    "age": np.random.randint(18, 65, size=len(user_ids)),
    "gender": np.random.choice(["M", "F", "Other"], size=len(user_ids)),
    "income_bracket": np.random.choice(["low", "medium", "high"], size=len(user_ids)),
    "loyalty_tier": np.random.choice(["silver", "gold", "platinum"], size=len(user_ids))
})

# First login dates within last 5 years
demography["first_login_date"] = [
    today - timedelta(days=random.randint(365, 5*365)) for _ in range(len(user_ids))
]
#demography["tenure_days"] = (today - demography["first_login_date"]).dt.days

# Billing state, city, and ZIP
demography["billing_state"] = np.random.choice(list(states_cities.keys()), size=len(user_ids))
demography["billing_city"] = [
    np.random.choice(states_cities[state]) for state in demography["billing_state"]
]
demography["billing_zip"] = [random_zip() for _ in range(len(user_ids))]

# 4. Save as UC table
demography_spark = spark.createDataFrame(demography)
demography_spark.write.mode("overwrite").saveAsTable("user_demography")

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

# COMMAND ----------

from datetime import datetime
import mlflow
import mlflow.sklearn
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from lightgbm import LGBMClassifier
from sklearn.metrics import roc_auc_score, accuracy_score

# COMMAND ----------

from databricks.feature_engineering import FeatureEngineeringClient
import time

# COMMAND ----------

from databricks.feature_engineering import FeatureLookup

# COMMAND ----------

import mlflow.deployments

# COMMAND ----------

def wait_for_lakebase_tables(catalog, schema, tables, waiting_time=900, sleep_time=30):
    from databricks.sdk import WorkspaceClient
    import time
    w = WorkspaceClient()
    ready = []

    for table in tables:
        print(f"Waiting for online sync: {catalog}.{schema}.{table}")
        for i in range(int(waiting_time / sleep_time)):
            try:
                state = (
                    w.database.get_synced_database_table(
                        name=f"{catalog}.{schema}.{table}"
                    )
                    .data_synchronization_status.detailed_state.name
                )
            except Exception as e:
                print(f"Could not check state yet ({e}), retrying...")
                state = "UNKNOWN"

            if "FAIL" in state:
                print(f"Online table {table} failed to synchronize.")
                break
            elif "ONLINE" in state:
                print(f"Online table {table} is ready.")
                ready.append(table)
                break

            time.sleep(sleep_time)
        else:
            print(f"Timed out waiting for {table}.")
    return len(ready) == len(tables)




# COMMAND ----------

import time
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import EndpointStateReady

def is_endpoint_ready(endpoint_name: str) -> bool:
    w = WorkspaceClient()
    endpoint = w.serving_endpoints.get(name=endpoint_name)

    # Primary check (Model Serving)
    if endpoint.state.ready == EndpointStateReady.READY:
        return True

    # Secondary check (Feature Serving)
    if getattr(endpoint.state, "served_entities", None):
        served_states = [se.state for se in endpoint.state.served_entities if hasattr(se, "state")]
        if any(s in ("READY", "DEPLOYMENT_READY") for s in served_states):
            return True

    return False


def wait_until_endpoint_ready(endpoint_name: str, timeout: int = 1800, sleep_time: int = 30) -> bool:
    w = WorkspaceClient()
    start_time = time.time()
    print(f"Waiting for endpoint '{endpoint_name}' to become READY...")

    while time.time() - start_time < timeout:
        try:
            endpoint = w.serving_endpoints.get(name=endpoint_name)
            state_summary = f"ready={endpoint.state.ready.value}, config_update={endpoint.state.config_update.value}"
            print(f"   Current state → {state_summary}")

            if is_endpoint_ready(endpoint_name):
                print(f"Endpoint '{endpoint_name}' is READY for requests.")
                return True
        except Exception as e:
            print(f"Error checking endpoint state: {e}")

        time.sleep(sleep_time)

    print(f"Timeout reached after {timeout/60:.1f} min — endpoint not ready.")
    return False


# COMMAND ----------

def init_experiment_for_batch(demo_name, experiment_name):
  from databricks.sdk import WorkspaceClient
  import mlflow
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
