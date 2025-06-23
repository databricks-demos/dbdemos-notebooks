# Databricks notebook source
# MAGIC %md
# MAGIC This notebook should only be run in a Databricks Job, as part of MLflow 3.0 Deployment Jobs.

# COMMAND ----------

# MAGIC %pip install mlflow  --upgrade --pre
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("model_name", "")
dbutils.widgets.text("model_version", "")
dbutils.widgets.text("approval_tag_name", "")

# COMMAND ----------

from mlflow import MlflowClient

client = MlflowClient(registry_uri="databricks-uc")
model_name = dbutils.widgets.get("model_name")
model_version = dbutils.widgets.get("model_version")

# by default, the approval tag name here is populated with the approval task name
tag_name = dbutils.widgets.get("approval_tag_name")

# fetch the model version's UC tags
tags = client.get_model_version(model_name, model_version).tags

# check if any tag matches the approval tag name
if not any(tag.lower() == tag_name.lower() for tag in tags.keys()):
  raise Exception("Model version not approved for deployment")
else:
  # if tag is found, check if it is approved
  if tags.get(tag_name).lower() == "approved":
    print("Model version approved for deployment")
  else:
    raise Exception("Model version not approved for deployment")