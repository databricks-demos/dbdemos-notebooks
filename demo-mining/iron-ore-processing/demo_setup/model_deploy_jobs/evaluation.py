# Databricks notebook source
# MAGIC %md
# MAGIC This notebook should only be run in a Databricks Job, as part of MLflow 3.0 Deployment Jobs.

# COMMAND ----------

# MAGIC %pip install mlflow  --upgrade --pre
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("model_name", "")
dbutils.widgets.text("model_version", "")

# COMMAND ----------

import pandas as pd
from sklearn.datasets import load_iris

def sample_iris_data():
  iris = load_iris()
  iris_df = pd.DataFrame(data=iris.data, columns=iris.feature_names)
  iris_df['quality'] = (iris.target == 2).astype(int)
  return iris_df

# COMMAND ----------

import mlflow

# TODO: add evaluation dataset and target here
data = sample_iris_data()
target = "quality"
# TODO: add model type here (e.g. "regressor", "databricks-agent", etc.)
model_type = "regressor"

model_name = dbutils.widgets.get("model_name")
model_version = dbutils.widgets.get("model_version")
model_uri = "models:/" + model_name + "/" + model_version 
# can also fetch model ID and use that for URI instead as described below

with mlflow.start_run(run_name="evaluation") as run:
  mlflow.evaluate(
    model=model_uri,
    data=data,
    targets=target,
    model_type=model_type
  )