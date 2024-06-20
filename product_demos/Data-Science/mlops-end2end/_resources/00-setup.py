# Databricks notebook source
dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")
dbutils.widgets.dropdown("setup_inference_data", "false", ["true", "false"], "Setup inference data")
reset_all_data = dbutils.widgets.get("reset_all_data") == "true"
setup_inference_data = dbutils.widgets.get("setup_inference_data") == "true"

# COMMAND ----------

catalog = "main__build"

# COMMAND ----------

# MAGIC %run ../../../../_resources/00-global-setup-v2

# COMMAND ----------

import mlflow
import pandas as pd
import re
#remove warnings for nicer display
import warnings
warnings.filterwarnings("ignore")
import logging
logging.getLogger("mlflow").setLevel(logging.ERROR)

from mlflow import MlflowClient

# COMMAND ----------

DBDemos.setup_schema(catalog, db, reset_all_data)

# COMMAND ----------

# Set UC Model Registry as default
mlflow.set_registry_uri("databricks-uc")
client = MlflowClient()

# COMMAND ----------

# DBTITLE 1,Create Raw/Bronze customer data from IBM Telco public dataset and sanitize column name
bronze_table_name = "mlops_churn_bronze_customers"
if reset_all_data or not spark.catalog.tableExists(bronze_table_name):
  import requests
  from io import StringIO
  #Dataset under apache license: https://github.com/IBM/telco-customer-churn-on-icp4d/blob/master/LICENSE
  csv = requests.get("https://raw.githubusercontent.com/IBM/telco-customer-churn-on-icp4d/master/data/Telco-Customer-Churn.csv").text
  df = pd.read_csv(StringIO(csv), sep=",")
  def cleanup_column(pdf):
    # Clean up column names
    pdf.columns = [re.sub(r'(?<!^)(?=[A-Z])', '_', name).lower().replace("__", "_") for name in pdf.columns]
    pdf.columns = [re.sub(r'[\(\)]', '', name).lower() for name in pdf.columns]
    pdf.columns = [re.sub(r'[ -]', '_', name).lower() for name in pdf.columns]
    return pdf.rename(columns = {'streaming_t_v': 'streaming_tv', 'customer_i_d': 'customer_id'})

  df = cleanup_column(df)
  print(f"creating `{bronze_table_name}` raw table")
  spark.createDataFrame(df).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(bronze_table_name)

# COMMAND ----------

def delete_feature_store_table(catalog, db, feature_table_name):
  from databricks.feature_engineering import FeatureEngineeringClient
  fe = FeatureEngineeringClient()
  try:
    # Drop existing table from Feature Store
    fe.drop_table(name=f"{catalog}.{db}.{feature_table_name}")
    # Delete underyling delta tables
    spark.sql(f"DROP TABLE IF EXISTS {catalog}.{db}.{feature_table_name}")
    print(f"Dropping Feature Table {catalog}.{db}.{feature_table_name}")
  except ValueError as ve:
    print(f"Feature Table {catalog}.{db}.{feature_table_name} doesn't exist")

# COMMAND ----------

training_table_name = "mlops_churn_training"
infrerence_table_name = "mlops_churn_inference"

if setup_inference_data:
  # Check that the training table exists first, as we'll be creating a copy of it
  if spark.catalog.tableExists(f"{catalog}.{db}.{training_table_name}"):
    # This should only be called from the quickstart challenger validation or batch inference notebooks
    if not spark.catalog.tableExists(f"{catalog}.{db}.{infrerence_table_name}"):
      print("Creating table for inference...")
      # Drop the label column for inference
      spark.read.table(training_table_name).drop("churn").write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(infrerence_table_name)
  else:
    print("Training table doesn't exist, please run the notebook '01_feature_engineering'")

# COMMAND ----------

# DBTITLE 1,Get slack webhook
# Replace this with your Slack webhook
try:
  slack_webhook = dbutils.secrets.get(scope="dbdemos", key=f"slack_webhook")
except:
  slack_webhook = "" # https://hooks.slack.com/services/T00000000/B00000000/XXXXXXXXXXXXXXXXXXXXXXXX
