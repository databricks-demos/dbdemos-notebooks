# Databricks notebook source
# MAGIC %md
# MAGIC This notebook should only be run in a Databricks Job, as part of MLflow 3.0 Deployment Jobs.

# COMMAND ----------

dbutils.widgets.text("model_name", "")
dbutils.widgets.text("model_version", "")

# COMMAND ----------

model_name = dbutils.widgets.get("model_name")
model_version = dbutils.widgets.get("model_version")

#remove the catalog_name
model_name_parts = model_name.split(".")
short_model_name = ".".join(model_name_parts[1:])

# TODO: Enter serving endpoint name
serving_endpoint_name = short_model_name.replace('.', '-') + "-serving-endpoint"

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import (
  ServedEntityInput,
  EndpointCoreConfigInput
)
from databricks.sdk.errors import ResourceDoesNotExist
w = WorkspaceClient()  # Assumes DATABRICKS_HOST and DATABRICKS_TOKEN are set
served_entities=[
  ServedEntityInput(
    entity_name=model_name,
    entity_version=model_version,
    workload_size="Small",
    scale_to_zero_enabled=True
  )
]

# Update serving endpoint if it already exists, otherwise create the serving endpoint
try:
  w.serving_endpoints.update_config(name=serving_endpoint_name, served_entities=served_entities)
except ResourceDoesNotExist:
  w.serving_endpoints.create(name=serving_endpoint_name, config=EndpointCoreConfigInput(served_entities=served_entities))