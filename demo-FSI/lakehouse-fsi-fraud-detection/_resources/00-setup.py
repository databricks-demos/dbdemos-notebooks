# Databricks notebook source
dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")
reset_all_data = dbutils.widgets.get("reset_all_data") == "true"

# COMMAND ----------

# MAGIC %run ../config

# COMMAND ----------

# MAGIC %run ../../../_resources/00-global-setup-v2

# COMMAND ----------

DBDemos.setup_schema(catalog, db, reset_all_data, volume_name)
folder = f"/Volumes/{catalog}/{db}/{volume_name}"

data_missing = DBDemos.is_any_folder_empty([folder+"/customers", folder+"/transactions", folder+"/country_code", folder+"/fraud_report"])

# COMMAND ----------

import os
import requests
import timeit
import time
from datetime import datetime

if reset_all_data or data_missing:
  print("data_missing, re-loading data")
  if reset_all_data:
    assert len(folder) > 15 and folder.startswith("/Volumes/")
    dbutils.fs.rm(folder, True)
  try:
    #credit_bureau
    DBDemos.download_file_from_git(folder+'/customers_parquet', "databricks-demos", "dbdemos-dataset", "/fsi/fraud-transaction/customers")
    #transactions
    DBDemos.download_file_from_git(folder+'/transactions_parquet', "databricks-demos", "dbdemos-dataset", "/fsi/fraud-transaction/transactions")
    #countries
    DBDemos.download_file_from_git(folder+'/country_code', "databricks-demos", "dbdemos-dataset", "/fsi/fraud-transaction/country_code")
    #countries
    DBDemos.download_file_from_git(folder+'/fraud_report_parquet', "databricks-demos", "dbdemos-dataset", "/fsi/fraud-transaction/fraud_report")
    def write_to(folder, output_format, output_folder):
      spark.read.format('parquet').load(folder).repartition(16).write.format(output_format).option('header', 'true').mode('overwrite').save(output_folder)
    
    from concurrent.futures import ThreadPoolExecutor, as_completed
    with ThreadPoolExecutor(max_workers=3) as executor:
      [future.result() for future in as_completed([
          executor.submit(write_to, folder+'/transactions_parquet', 'json', folder+'/transactions'),
          executor.submit(write_to, folder+'/customers_parquet', 'csv', folder+'/customers'),
          executor.submit(write_to, folder+'/fraud_report_parquet', 'csv', folder+'/fraud_report')
      ])]
    
  except Exception as e: 
    print(f"Error trying to download the file from the repo: {str(e)}.")    

# COMMAND ----------

import plotly.express as px
from plotly.subplots import make_subplots
import plotly.graph_objects as go

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


#Fix pandas version to avoid conflict and support most DBR in the demo
def force_pandas_version(run_id):
    import os
    import shutil
    import yaml
    import tempfile
    import mlflow
    from mlflow.tracking import MlflowClient

    # setup local dir for downloading the artifacts
    tmp_dir = str(tempfile.TemporaryDirectory().name)
    os.makedirs(tmp_dir)

    # fix conda.yaml
    conda_file_path = mlflow.artifacts.download_artifacts(artifact_uri = f"runs:/{run_id}/model/conda.yaml", dst_path=tmp_dir)
    with open(conda_file_path) as f:
        conda_libs = yaml.load(f, Loader=yaml.FullLoader)
    pandas_lib_exists = any([lib.startswith("pandas==") for lib in conda_libs["dependencies"][-1]["pip"]])
    client = MlflowClient()
    if not pandas_lib_exists:
        print("Adding pandas dependency to conda.yaml")
        conda_libs["dependencies"][-1]["pip"].append("pandas==1.5.3")

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
        print("Adding pandas dependency to requirements.txt")
        venv_libs.append("pandas==1.5.3")

        with open(f"{tmp_dir}/requirements.txt", "w") as f:
            f.write("\n".join(venv_libs))
        client.log_artifact(run_id=run_id, local_path=venv_file_path, artifact_path="model")

    shutil.rmtree(tmp_dir)
