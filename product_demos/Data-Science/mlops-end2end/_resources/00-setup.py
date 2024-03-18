# Databricks notebook source
dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")
dbutils.widgets.text("catalog","dbdemos", "Catalog to use")

# COMMAND ----------

# MAGIC %run ../../../../_resources/00-global-setup $reset_all_data=$reset_all_data $db_prefix=retail $min_dbr_version=13.3 $catalog=$catalog

# COMMAND ----------

import mlflow
if "evaluate" not in dir(mlflow):
    raise Exception("ERROR - YOU NEED MLFLOW 2.5 for this demo. Select DBRML 13.3LTS+")

from databricks.feature_store import FeatureStoreClient
from mlflow import MlflowClient

# COMMAND ----------

# Set UC Model Registry as default
mlflow.set_registry_uri("databricks-uc")
client = MlflowClient()

# COMMAND ----------

# DBTITLE 1,Create Raw/Bronze customer data from IBM Telco public dataset and sanitize column name
bronze_table_name = "mlops_churn_bronze_customers"
if reset_all:
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

# Define Feature, Labels and Inference Table specs
feature_table_name = "mlops_churn_features"
primary_key = "customer_id"
timestamp_col = "scoring_timestamp"
label_col = "churn"
labels_table_name = "mlops_churn_labels"
inference_table_name = "mlops_churn_inference_log"

# COMMAND ----------

# MAGIC %run ./API_Helpers

# COMMAND ----------

# DBTITLE 1,Additional AutoML/MLflow experiment helpers [Required for MLR<14.0]
import uuid

churn_experiment_name = "churn_auto_ml"
model_name = f"{catalog}.{dbName}.mlops_churn"
model_serving_pandas_ver = "1.5.3" # Temporary needed to pin to model's depdency if trained using autoML

# Delete all existing model version and remove from registry
if reset_all:
  cleanup_registered_model(model_name)

def fix_automl_pandas_dependency(run_id:str, pandas_ver:str = model_serving_pandas_ver):
  """
  Helper function to manually append pandas dependency for Model Serving endpoints (currently 1.5.3 vs. 1.4.4 for MLR<14.0)
  More info here: https://docs.databricks.com/machine-learning/automl/train-ml-model-automl-api.html#no-module-named-pandascoreindexesnumeric

  Note: Only call/invoke this function if pandas version is < 1.5.3
  """

  import yaml
  import tempfile

  try:
    # setup local dir for downloading the artifacts
    tmp_dir = f"/tmp/{current_user_no_at}/{str(tempfile.TemporaryDirectory())}"
    dbutils.fs.mkdirs(tmp_dir)

    # fix conda.yaml
    conda_file_path = mlflow.artifacts.download_artifacts(artifact_uri = f"runs:/{run_id}/model/conda.yaml", dst_path=tmp_dir)
    with open(conda_file_path) as f:
      conda_libs = yaml.load(f, Loader=yaml.FullLoader)
    pandas_lib_exists = any([lib.startswith("pandas==") for lib in conda_libs["dependencies"][-1]["pip"]])
    client = MlflowClient()
    if not pandas_lib_exists:
      print(f"Adding pandas {pandas_ver} dependency to conda.yaml")
      conda_libs["dependencies"][-1]["pip"].append(f"pandas=={pandas_ver}")

      with open(f"{tmp_dir}/conda.yaml", "w") as f:
        f.write(yaml.dump(conda_libs))
      client.log_artifact(run_id=run_id, local_path=conda_file_path, artifact_path="model")

    # fix requirements.txt
    venv_file_path = mlflow.artifacts.download_artifacts(artifact_uri = f"runs:/{run_id}/model/requirements.txt", dst_path=tmp_dir)

    with open(venv_file_path) as f:
      venv_libs = f.readlines()
    venv_libs = [lib.strip() for lib in venv_libs]
    pandas_lib_exists = any([lib.startswith("pandas==") for lib in venv_libs])
    if not pandas_lib_exists:
      print(f"Adding pandas {pandas_ver} dependency to requirements.txt")
      venv_libs.append(f"pandas=={pandas_ver}")

      with open(f"{tmp_dir}/requirements.txt", "w") as f:
        f.write("\n".join(venv_libs))
      client.log_artifact(run_id=run_id, local_path=venv_file_path, artifact_path="model")

    dbutils.fs.rm(tmp_dir, True)
    return True

  except Exception as e:
    print(e)
    return False

def display_automl_churn_link(table_name, force_refresh = False, use_feature_table = False):
  if force_refresh:
    reset_automl_run(churn_experiment_name)
  display_automl_link(churn_experiment_name, model_name, table_name, label_col, 10, False, use_feature_table)

def get_automl_churn_run(table_name = f"{catalog}.{schema}.{feature_table_name}", force_refresh = False, use_feature_table = True):
  if force_refresh:
    reset_automl_run(churn_experiment_name)
  from_cache, r = get_automl_run_or_start(name=churn_experiment_name, model_name=model_name, table_name=table_name, target_col=label_col, timeout_minutes=5, move_to_production=True, use_feature_table=use_feature_table)
  return r

# Override automl helper functions here
def get_automl_run(name):
  #get the most recent automl run
  df = spark.table("hive_metastore.dbdemos_metadata.automl_experiment").filter((col("name")==name) & (col("experiment_path").like(f"/Users/{current_user}/databricks_automl/%"))).orderBy(col("date").desc()).limit(1)
  return df.collect()

# Get the automl run information from the hive_metastore.dbdemos_metadata.automl_experiment table.
#If it's not available in the metadata table, start a new run with the given parameters
def get_automl_run_or_start(name, model_name, table_name, target_col, timeout_minutes=10, move_to_production = True, use_feature_table = True):
  spark.sql("create database if not exists hive_metastore.dbdemos_metadata")
  spark.sql("create table if not exists hive_metastore.dbdemos_metadata.automl_experiment (name string, date string)")
  result = get_automl_run(name)
  if len(result) == 0:
    print("No run available, start a new Auto ML run, this will take a few minutes...")
    start_automl_run(name, model_name, table_name, target_col, timeout_minutes, move_to_production, use_feature_table)
    return (False, get_automl_run(name))
  return (True, result[0])


# Start a new auto ml classification task and save it as metadata.
def start_automl_run(name, model_name, table_name, target_col=label_col, timeout_minutes = 10, move_to_production = True, use_feature_table = True):
  import databricks.automl
  import pandas as pd


  # Additionnal check if ghost experiment exists, rename it
  client = mlflow.tracking.MlflowClient()
  existing_exp = client.get_experiment_by_name(f"/Users/{current_user}/databricks_automl/{name}")
  if existing_exp:
    print(f"Existing/Ghost experiment {name} found, renaming it...")
    client.rename_experiment(existing_exp.experiment_id, new_name=f"{existing_exp.name}_old_{str(uuid.uuid4())[:4]}")

  if use_feature_table:
    fl = [{"table_name" : table_name, "lookup_key" : [primary_key], "timestamp_lookup_key" : timestamp_col}]
    automl_run = databricks.automl.classify(
      dataset = f"{catalog}.{dbName}.{labels_table_name}",
      target_col = target_col,
      feature_store_lookups=fl,
      timeout_minutes = timeout_minutes,
      experiment_name=name,
      exclude_cols=[primary_key, timestamp_col],
      pos_label="Yes"
    )

  else:
    automl_run = databricks.automl.classify(
      dataset = table_name,
      target_col = target_col,
      timeout_minutes = timeout_minutes,
      experiment_name=name,
      exclude_cols=[primary_key, timestamp_col],
      pos_label="Yes"
    )

  # Find best run in last experiment
  experiment_id = automl_run.experiment.experiment_id
  path = automl_run.experiment.name
  data_run_id = mlflow.search_runs(experiment_ids=[automl_run.experiment.experiment_id], filter_string = "tags.mlflow.source.name='Notebook: DataExploration'").iloc[0].run_id
  exploration_notebook_id = automl_run.experiment.tags["_databricks_automl.exploration_notebook_id"]
  best_trial_notebook_id = automl_run.experiment.tags["_databricks_automl.best_trial_notebook_id"]

  # Create internal metadata table entry of automl experiment
  cols = ["name", "date", "experiment_id", "experiment_path", "data_run_id", "best_trial_run_id", "exploration_notebook_id", "best_trial_notebook_id"]
  spark.createDataFrame(data=[(name, datetime.today().isoformat(), experiment_id, path, data_run_id, automl_run.best_trial.mlflow_run_id, exploration_notebook_id, best_trial_notebook_id)], schema = cols).write.mode("append").option("mergeSchema", "true").saveAsTable("hive_metastore.dbdemos_metadata.automl_experiment")

  # Update/Fix model dependency (for model serving)
  if pd.__version__ != "1.5.3":
    fix_automl_pandas_dependency(automl_run.best_trial.mlflow_run_id)

  # Create & save the first model version in the MLFlow repo (required to setup hooks, showcase model serving upfront etc.)
  model_registered = mlflow.register_model(f"runs:/{automl_run.best_trial.mlflow_run_id}/model", model_name)

  set_experiment_permission(path)

  # Set as "baseline" model using @Aliases
  print("registering model version "+model_registered.version+" as baseline model")
  client.set_registered_model_alias(name = model_name, alias = "Baseline", version = model_registered.version)

  return get_automl_run(name)

# Generate nice link for the given auto ml run
def display_automl_link(name, model_name, table_name, target_col, timeout_minutes = 5, move_to_production = True, use_feature_table = True):
  from_cache, r = get_automl_run_or_start(name, model_name, table_name, target_col, timeout_minutes, move_to_production, use_feature_table)
  if from_cache:
    html = f"""For exploratory data analysis, open the <a href="/#notebook/{r["exploration_notebook_id"]}">data exploration notebook</a><br/><br/>"""
    html += f"""To view the best performing model, open the <a href="/#notebook/{r["best_trial_notebook_id"]}">best trial notebook</a><br/><br/>"""
    html += f"""To view details about all trials, navigate to the <a href="/#mlflow/experiments/{r["experiment_id"]}/s?orderByKey=metrics.%60val_f1_score%60&orderByAsc=false">MLflow experiment</>"""
    displayHTML(html)

def reset_automl_run(name):
  # First rename old experiment in MLflow tracking server
  run = get_automl_run(name)
  if run:
    client.rename_experiment(run[0]['experiment_id'], new_name=f"{run[0]['experiment_path']}_old_{str(uuid.uuid4())[:4]}")

  # Delete entry from dbdemos metadata
  if spark._jsparkSession.catalog().tableExists('hive_metastore.dbdemos_metadata.automl_experiment'):
      spark.sql(f"delete from hive_metastore.dbdemos_metadata.automl_experiment where name='{name}' and experiment_path like '/Users/{current_user}/databricks_automl/%'")

# COMMAND ----------

# DBTITLE 1,Get slack webhook
# Replace this with your Slack webhook
try:
  slack_webhook = dbutils.secrets.get(scope="fieldeng", key=f"{get_current_username()}_slack_webhook")
except:
  slack_webhook = "" # https://hooks.slack.com/services/T00000000/B00000000/XXXXXXXXXXXXXXXXXXXXXXXX

# COMMAND ----------

# DBTITLE 1,Create baseline table for Model Monitoring performance if doesn't exist
# One-Time operation
# For demo purposes, we'll use a fraction of the whole training set as baseline (assumed to be the test/held-out set)

baseline_table_name = f"{catalog}.{dbName}.{inference_table_name}_baseline"
model_versions = client.search_model_versions(f"name='{model_name}'")

# Check if baseline table and model exists
if spark._jsparkSession.catalog().tableExists(f"{catalog}.{dbName}.{labels_table_name}") and  not spark._jsparkSession.catalog().tableExists(baseline_table_name) and len(model_versions) > 0:
  from databricks.feature_engineering import FeatureEngineeringClient

  fe = FeatureEngineeringClient()

  # Get list of observations to score
  labelsDF = spark.read.table(f"{catalog}.{dbName}.{labels_table_name}")
  
  # Get baseline model version
  try:
    baseline_model_version = client.get_model_version_by_alias(name=model_name, alias="Baseline").version

  except:  
    baseline_model_version = model_versions[0].version

  predictions = fe.score_batch(
    df=labelsDF,
    model_uri=f"models:/{model_name}/{baseline_model_version}",
    result_type=labelsDF.schema[label_col].dataType
  ).withColumn("Model_Version", F.lit(baseline_model_version))

  (
    predictions.drop(timestamp_col).sample(fraction=0.2, seed=42) # Pick a random sample (assumed to be the test set) for demo purposes
    .write
    .format("delta")
    .mode("overwrite") # "append" also works if baseline evolves
    .option("overwriteSchema",True)
    .option("delta.enableChangeDataFeed", "true")
    .saveAsTable(baseline_table_name)
  )
