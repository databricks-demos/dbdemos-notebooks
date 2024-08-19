# Databricks notebook source
dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")
dbutils.widgets.dropdown("gen_synthetic_data", "false", ["true", "false"], "Generate Synthetic data for Drift Detection")
dbutils.widgets.dropdown("setup_inference_data", "false", ["true", "false"], "Setup inference data for quickstart")
dbutils.widgets.dropdown("setup_adv_inference_data", "false", ["true", "false"], "Setup inference data for advanced demo")
reset_all_data = dbutils.widgets.get("reset_all_data") == "true"
setup_inference_data = dbutils.widgets.get("setup_inference_data") == "true"
setup_adv_inference_data = dbutils.widgets.get("setup_adv_inference_data") == "true"
generate_synthetic_data = dbutils.widgets.get("gen_synthetic_data") == "true"

# COMMAND ----------

catalog = "aminen_catalog"
db = "advanced_mlops"
model_name = f"{catalog}.{db}.mlops_churn"
model_alias = "Champion"
inference_table_name = "mlops_churn_advanced_inference_table"


# COMMAND ----------

# MAGIC %run ../../../../_resources/00-global-setup-v2

# COMMAND ----------

import mlflow
import pandas as pd
import random
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

# Set Experiment name as default
current_user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
xp_path = f"/Users/{current_user}/databricks_automl"
xp_name = "advanced_mlops_churn_demo_experiment"

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
advanced_label_table_name = "churn_label_table"
advanced_unlabelled_table_name = "mlops_churn_advanced_cust_ids"

if setup_adv_inference_data:
  # Check that the label table exists first, as we'll be creating a copy of it
  if spark.catalog.tableExists(f"{catalog}.{db}.{advanced_label_table_name}"):
    # This should only be called from the advanced batch inference notebook
    if not spark.catalog.tableExists(f"{catalog}.{db}.{advanced_unlabelled_table_name}"):
      print("Creating table with customer records for inference...")
      # Drop the label column for inference
      spark.read.table(advanced_label_table_name).drop("churn","split").write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(advanced_unlabelled_table_name)
  else:
    print("Label table doesn't exist, please run the notebook '01_feature_engineering'")

# COMMAND ----------

# DBTITLE 1,Generate inference synthetic data
gen_synthetic_data = False
def generate_synthetic(inference_table, drift_type="label_drift"):
  import dbldatagen as dg
  import pyspark.sql.types
  from databricks.feature_engineering import FeatureEngineeringClient
  import pyspark.sql.functions as F
  from datetime import datetime, timedelta
  # Column definitions are stubs only - modify to generate correct data  
  #
  generation_spec = (
      dg.DataGenerator(sparkSession=spark, 
                      name='synthetic_data', 
                      rows=5000,
                      random=True,
                      )
      .withColumn('customer_id', 'string', template=r'dddd-AAAA')
      .withColumn('transaction_ts', 'timestamp', begin=(datetime.now() + timedelta(days=-30)), end=(datetime.now() + timedelta(days=-1)), interval="1 hour")
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
      .withColumn('payment_method', 'string', values=['Credit card (automatic)', 'Mailed check',
  'Bank transfer (automatic)', 'Electronic check'], weights=[0.2, 0.2, 0.2, 0.4])
      .withColumn('monthly_charges', 'double', minValue=18.0, maxValue=118.0, step=0.5)
      .withColumn('total_charges', 'double', minValue=0.0, maxValue=8684.0, step=20)
      .withColumn('num_optional_services', 'double', minValue=0.0, maxValue=6.0, step=1)
      .withColumn('avg_price_increase', 'float', minValue=-19.0, maxValue=130.0, step=20)
      .withColumn('churn', 'string', values=[ 'Yes'], random=True)
      )


  # Generate Synthetic Data
  df_synthetic_data = generation_spec.build()

  fe = FeatureEngineeringClient()

  # Model URI
  model_uri = f"models:/{model_name}@{model_alias}"

  # Batch score
  preds_df = fe.score_batch(df=df_synthetic_data, model_uri=model_uri, result_type="string")
  preds_df = preds_df \
    .withColumn('model_name', F.lit(f"{model_name}")) \
    .withColumn('model_version', F.lit(2)) \
    .withColumn('model_alias', F.lit("Champion")) \
    .withColumn('inference_timestamp', F.lit(datetime.now())) 

  preds_df.write.mode("append").saveAsTable(f"{catalog}.{db}.{inference_table_name}")

if generate_synthetic_data:
  generate_synthetic(inference_table=inference_table_name)

# COMMAND ----------

# DBTITLE 1,Get slack webhook
# Replace this with your Slack webhook
try:
  slack_webhook = dbutils.secrets.get(scope="dbdemos", key=f"slack_webhook")
except:
  slack_webhook = "" # https://hooks.slack.com/services/T00000000/B00000000/XXXXXXXXXXXXXXXXXXXXXXXX

# COMMAND ----------

from pyspark.sql.functions import col
#from databricks.feature_store import FeatureStoreClient
import mlflow

import databricks
from databricks import automl
from datetime import datetime

def get_automl_run(name):
  #get the most recent automl run
  df = spark.table("field_demos_metadata.automl_experiment").filter(col("name") == name).orderBy(col("date").desc()).limit(1)
  return df.collect()

#Get the automl run information from the field_demos_metadata.automl_experiment table. 
#If it's not available in the metadata table, start a new run with the given parameters
def get_automl_run_or_start(name, model_name, dataset, target_col, timeout_minutes):
  spark.sql("create database if not exists field_demos_metadata")
  spark.sql("create table if not exists field_demos_metadata.automl_experiment (name string, date string)")
  result = get_automl_run(name)
  if len(result) == 0:
    print("No run available, start a new Auto ML run, this will take a few minutes...")
    start_automl_run(name, model_name, dataset, target_col, timeout_minutes)
    result = get_automl_run(name)
  return result[0]


#Start a new auto ml classification task and save it as metadata.
def start_automl_run(name, model_name, dataset, target_col, timeout_minutes = 5):
  automl_run = databricks.automl.classify(
    dataset = dataset,
    target_col = target_col,
    timeout_minutes = timeout_minutes
  )
  experiment_id = automl_run.experiment.experiment_id
  path = automl_run.experiment.name
  data_run_id = mlflow.search_runs(experiment_ids=[automl_run.experiment.experiment_id], filter_string = "tags.mlflow.source.name='Notebook: DataExploration'").iloc[0].run_id
  exploration_notebook_id = automl_run.experiment.tags["_databricks_automl.exploration_notebook_id"]
  best_trial_notebook_id = automl_run.experiment.tags["_databricks_automl.best_trial_notebook_id"]

  cols = ["name", "date", "experiment_id", "experiment_path", "data_run_id", "best_trial_run_id", "exploration_notebook_id", "best_trial_notebook_id"]
  spark.createDataFrame(data=[(name, datetime.today().isoformat(), experiment_id, path, data_run_id, automl_run.best_trial.mlflow_run_id, exploration_notebook_id, best_trial_notebook_id)], schema = cols).write.mode("append").option("mergeSchema", "true").saveAsTable("field_demos_metadata.automl_experiment")
  #Create & save the first model version in the MLFlow repo (required to setup hooks etc)
  mlflow.register_model(f"runs:/{automl_run.best_trial.mlflow_run_id}/model", model_name)
  return get_automl_run(name)

#Generate nice link for the given auto ml run
def display_automl_link(name, model_name, dataset, target_col, force_refresh=False, timeout_minutes = 5):
  r = get_automl_run_or_start(name, model_name, dataset, target_col, timeout_minutes)
  html = f"""For exploratory data analysis, open the <a href="/#notebook/{r["exploration_notebook_id"]}">data exploration notebook</a><br/><br/>"""
  html += f"""To view the best performing model, open the <a href="/#notebook/{r["best_trial_notebook_id"]}">best trial notebook</a><br/><br/>"""
  html += f"""To view details about all trials, navigate to the <a href="/#mlflow/experiments/{r["experiment_id"]}/s?orderByKey=metrics.%60val_f1_score%60&orderByAsc=false">MLflow experiment</>"""
  displayHTML(html)


def display_automl_churn_link(): 
  display_automl_link("churn_auto_ml", "field_demos_customer_churn", spark.table("churn_features"), "churn", 5)

def get_automl_churn_run(): 
  return get_automl_run_or_start("churn_auto_ml", "field_demos_customer_churn", spark.table("churn_features"), "churn", 5)
