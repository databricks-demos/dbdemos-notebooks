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

# Workaround for dbdemos to support automl the time being, creates a mock run simulating automl results
def create_mockup_automl_run_for_dbdemos(full_xp_path, df):
    print('Creating mockup automl run...')
    xp = mlflow.create_experiment(full_xp_path)
    mlflow.set_experiment(experiment_id=xp)
    with mlflow.start_run(run_name="DBDemos automl mock autoML run", experiment_id=xp) as run:
        mlflow.set_tag('mlflow.source.name', 'Notebook: DataExploration')
        mlflow.log_metric('val_f1_score', 0.81)
        split_choices = ['train', 'val', 'test']
        split_probabilities = [0.7, 0.2, 0.1]  # 70% train, 20% val, 10% test
        # Add a new column with random assignments
        import numpy as np
        df['_automl_split_col'] = np.random.choice(split_choices, size=len(df), p=split_probabilities)
        df.to_parquet('/tmp/dataset.parquet', index=False)
        mlflow.log_artifact('/tmp/dataset.parquet', artifact_path='data/training_data')

# COMMAND ----------

import json
from datetime import datetime
import time
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, sha1, col, initcap, to_timestamp

folder = f"/Volumes/{catalog}/{db}/{volume_name}"

if reset_all_data or DBDemos.is_any_folder_empty([folder+"/orders", folder+"/users", folder+"/events"]):
  #data generation on another notebook to avoid installing libraries (takes a few seconds to setup pip env)
  print(f"Generating data under {folder} , please wait a few sec...")
  path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
  parent_count = path[path.rfind("lakehouse-retail-c360"):].count('/') - 1
  prefix = "./" if parent_count == 0 else parent_count*"../"
  prefix = f'{prefix}_resources/'
  dbutils.notebook.run(prefix+"01-load-data", 600, {"reset_all_data": dbutils.widgets.get("reset_all_data")})
else:
  print("data already existing. Run with reset_all_data=true to force a data cleanup for your local demo.")
