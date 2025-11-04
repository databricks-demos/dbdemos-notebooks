# Databricks notebook source
dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")
dbutils.widgets.dropdown("gen_synthetic_data", "false", ["true", "false"], "Generate Synthetic data for Drift Detection")
dbutils.widgets.dropdown("adv_mlops", "false", ["true", "false"], "Setup for advanced MLOps demo")
dbutils.widgets.dropdown("setup_inference_data", "false", ["true", "false"], "Setup inference data for quickstart")
dbutils.widgets.dropdown("setup_adv_inference_data", "false", ["true", "false"], "Setup inference data for advanced demo")
reset_all_data = dbutils.widgets.get("reset_all_data") == "true"
setup_inference_data = dbutils.widgets.get("setup_inference_data") == "true"
setup_adv_inference_data = dbutils.widgets.get("setup_adv_inference_data") == "true"
generate_synthetic_data = dbutils.widgets.get("gen_synthetic_data") == "true"
is_advanced_mlops_demo = dbutils.widgets.get("adv_mlops") == "true"

# COMMAND ----------

current_user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
reformat_current_user = current_user.split("@")[0].lower().replace(".", "_")

catalog = "main__build"
dbName = db = "dbdemos_mlops"
online_store_name = "dbdemosonlinestore" # "fe_shared_demo"

# COMMAND ----------

# MAGIC %run ../../../../_resources/00-global-setup-v2

# COMMAND ----------

import mlflow
import pandas as pd
import random
import re
#remove warnings for nicer display
import warnings
import logging
from mlflow import MlflowClient


warnings.filterwarnings("ignore")
logging.getLogger("mlflow").setLevel(logging.ERROR)

# COMMAND ----------

DBDemos.setup_schema(catalog, db, False)

# COMMAND ----------

# Set UC Model Registry as default
mlflow.set_registry_uri("databricks-uc")
xp_name = "dbdemos_mlops_churn_demo_experiment"
xp_path = f"/Users/{current_user}/dbdemos_mlops"
if is_advanced_mlops_demo:
  # Set Experiment name as default
  xp_name = "dbdemos_advanced_mlops_churn_demo_experiment"
try:
  from databricks.sdk import WorkspaceClient
  w = WorkspaceClient()
  r = w.workspace.mkdirs(path=xp_path)
except Exception as e:
  print(f"ERROR: couldn't create a folder for the experiment under {xp_path} - please create the folder manually or  skip this init (used for job only: {e})")
  raise e

client = MlflowClient()

# COMMAND ----------

# DBTITLE 1,Create Raw/Bronze customer data from IBM Telco public dataset and sanitize column name
# Default for quickstart
bronze_table_name = "mlops_churn_bronze_customers"

# Bronze table name for advanced
if is_advanced_mlops_demo:
  bronze_table_name = "advanced_churn_bronze_customers"

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
  
  if is_advanced_mlops_demo:
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()
    try:
      print(f"Deleting existing monitors for {catalog}.{db}.advanced_churn_inference_table")
      w.quality_monitors.delete(table_name=f"{catalog}.{db}.advanced_churn_inference_table")
    except Exception as error:
      print(f"Error deleting monitor: {type(error).__name__}")
    experiment_details = client.get_experiment_by_name(f"{xp_path}/{xp_name}")
    if experiment_details:
      print(f' Deleting experiment: {experiment_details.experiment_id}')
      client.delete_experiment(f'{experiment_details.experiment_id}')
  
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

# This setup is used in the quickstart demo only
quickstart_training_table_name = "mlops_churn_training"
quickstart_unlabelled_table_name = "mlops_churn_inference"

if setup_inference_data:
  # Check that the training table exists first, as we'll be creating a copy of it
  if spark.catalog.tableExists(f"{catalog}.{db}.{quickstart_training_table_name}"):
    # This should only be called from the quickstart challenger validation or batch inference notebooks
    if not spark.catalog.tableExists(f"{catalog}.{db}.{quickstart_unlabelled_table_name}"):
      print("Creating unlabelled data table for performing inference...")
      # Drop the label column for inference
      spark.read.table(quickstart_training_table_name).drop("churn").write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(quickstart_unlabelled_table_name)
  else:
    print("Training table doesn't exist, please run the notebook '01_feature_engineering'")

# COMMAND ----------

# This setup is used in the advanced demo only
#advanced_label_table_name = "churn_label_table"
#advanced_unlabelled_table_name = "mlops_churn_advanced_cust_ids"

if setup_adv_inference_data:
  # Check that the label table exists first, as we'll be creating a copy of it
  if spark.catalog.tableExists(f"advanced_churn_label_table"):
    # This should only be called from the advanced batch inference notebook
    # if not spark.catalog.tableExists(f"advanced_churn_cust_ids"):
    print("Creating table with customer records for inference...")
    # Drop the label column for inference
    spark.read.table("advanced_churn_label_table").drop("churn").write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("advanced_churn_cust_ids")
  else:
    print("Label table `advanced_churn_label_table` doesn't exist, please run the notebook '01_feature_engineering'")

# COMMAND ----------

# DBTITLE 1,Generate inference synthetic data
def generate_synthetic(inference_table):
  import dbldatagen as dg
  import pyspark.sql.types
  import pyspark.sql.functions as F
  from datetime import datetime, timedelta

  model_version = client.get_model_version_by_alias(name=model_name, alias="Champion").version
  n_days_to_back_fill = 30 # Change this

  # Column definitions are based on original dataset schema
  generation_spec = (
    dg.DataGenerator(sparkSession=spark, 
                    name='synthetic_data', 
                    rows=7000*n_days_to_back_fill,
                    random=True,
                    )
    .withColumn('customer_id', 'string', template=r'dddd-AAAA')
    .withColumn('transaction_ts', 'timestamp', begin=(datetime.now() + timedelta(days=-n_days_to_back_fill)), end=datetime.now(), interval="1 hour")
    .withColumn('gender', 'string', values=['Female', 'Male'], random=True, weights=[0.5, 0.5])
    .withColumn('senior_citizen', 'string', values=['No', 'Yes'], random=True, weights=[0.85, 0.15])
    .withColumn('partner', 'string', values=['No', 'Yes'], random=True, weights=[0.5, 0.5])
    .withColumn('dependents', 'string', values=['No', 'Yes'], random=True, weights=[0.7, 0.3])
    .withColumn('tenure', 'double', minValue=0.0, maxValue=72.0, step=1.0)
    .withColumn('phone_service', values=['No', 'Yes'], random=True, weights=[0.9, 0.1])
    .withColumn('multiple_lines', 'string', values=['No', 'Yes'], random=True, weights=[0.5, 0.5])
    .withColumn('internet_service', 'string', values=['Fiber optic', 'DSL', 'No'], random=True, weights=[0.5, 0.3, 0.2])
    .withColumn('online_security', 'string', values=['No', 'Yes'], random=True, weights=[0.5, 0.5])
    .withColumn('online_backup', 'string', values=['No', 'Yes'], random=True, weights=[0.5, 0.5])
    .withColumn('device_protection', 'string', values=['No', 'Yes'], random=True, weights=[0.5, 0.5])
    .withColumn('tech_support', 'string', values=['No', 'Yes'], random=True, weights=[0.5, 0.5])
    .withColumn('streaming_tv', 'string', values=['No', 'Yes', 'No internet service'], random=True, weights=[0.4, 0.4, 0.2])
    .withColumn('streaming_movies', 'string', values=['No', 'Yes', 'No internet service'], random=True, weights=[0.4, 0.4, 0.2])
    .withColumn('contract', 'string', values=['Month-to-month', 'One year','Two year'], random=True, weights=[0.5, 0.25, 0.25])
    .withColumn('paperless_billing', 'string', values=['No', 'Yes'], random=True, weights=[0.6, 0.4])
    .withColumn('payment_method', 'string', values=['Credit card (automatic)', 'Mailed check', 'Bank transfer (automatic)', 'Electronic check'], weights=[0.2, 0.2, 0.2, 0.4])
    .withColumn('monthly_charges', 'double', minValue=18.0, maxValue=118.0, step=0.5)
    .withColumn('total_charges', 'double', minValue=0.0, maxValue=8684.0, step=20)
    .withColumn('num_optional_services', 'double', minValue=0.0, maxValue=6.0, step=1)
    .withColumn('avg_price_increase', 'float', minValue=-19.0, maxValue=130.0, step=20)
    .withColumn('churn', 'string', values=['No', 'Yes'], random=True, weights=[0.2, 0.8], percentNulls=0.8)
    .withColumn('inference_timestamp', 'timestamp', begin=(datetime.now() + timedelta(days=-n_days_to_back_fill)), end=(datetime.now()), interval="1 hour")
    .withColumn('prediction', 'string', values=['No', 'Yes'], random=True, weights=[0.6, 0.4])
    )

  # Generate Synthetic Data
  df_synthetic_data = generation_spec.build()

  ## Append relevant/monitoring columns
  preds_df = df_synthetic_data \
    .withColumn('model_name', F.lit(model_name)) \
    .withColumn('model_version', F.lit(model_version)) \
    # .withColumn('inference_timestamp', F.lit(datetime.now())) # + timedelta(days=1))) 

  preds_df.write.mode("append").option("mergeSchema", "true").saveAsTable(f"{catalog}.{db}.{inference_table_name}")

if is_advanced_mlops_demo:
  model_name = f"{catalog}.{db}.advanced_mlops_churn"
  inference_table_name = "advanced_churn_inference_table"
  if generate_synthetic_data:
    generate_synthetic(inference_table=inference_table_name)

# COMMAND ----------


