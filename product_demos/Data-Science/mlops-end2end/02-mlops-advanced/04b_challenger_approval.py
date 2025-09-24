# Databricks notebook source
# MAGIC %md
# MAGIC #  `Challenger` Model approval (or Model "Deployment" Job)
# MAGIC
# MAGIC This notebook checks for approval tags on the new candidate model (i.e. __Challenger__).
# MAGIC
# MAGIC It will enable having a "Human-In-The-Loop" to approve a new model version _OPTIONNAL_ as part of the model's [deployment job](https://docs.databricks.com/aws/en/mlflow/deployment-job)
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/mlops/advanced/banners/mlflow-uc-end-to-end-advanced-4b-v2.png?raw=true" width="1200">
# MAGIC
# MAGIC *Note: In a typical mlops setup, this would run as part of an automated deployment job to validate a new model. We'll run this demo as an interactive notebook.*
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable the collection or the tracker during installation. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=lakehouse&notebook=04_challenger_validation&demo_name=mlops-end2end&event=VIEW">

# COMMAND ----------

# MAGIC %pip install --quiet mlflow-skinny --upgrade --pre
# MAGIC
# MAGIC
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %run ../_resources/00-setup $adv_mlops=true

# COMMAND ----------

dbutils.widgets.text("model_name", f"{catalog}.{db}.advanced_mlops_churn", "Model Name") # Will be populated from Deployment Jobs Parameters
dbutils.widgets.text("model_version", "1", "Model Version") # Will be populated from Deployment Jobs Parameters
dbutils.widgets.text("approval_tag_name", "Approval_Check", "Approval Tag to check")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fetch Model information
# MAGIC
# MAGIC We will fetch the model information for the latest model version from Unity Catalog.

# COMMAND ----------

import mlflow
from mlflow.tracking.client import MlflowClient


client = MlflowClient(registry_uri="databricks-uc")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validating the new version/Challenger to Champion
# MAGIC
# MAGIC Tag/Verifications check.

# COMMAND ----------

model_name = dbutils.widgets.get("model_name")
model_version = dbutils.widgets.get("model_version")
tag_name = dbutils.widgets.get("approval_tag_name") # Default = approval task name

# Fetch model version's UC tags
tags = client.get_model_version(model_name, model_version).tags

# Check if any tag matches the approval tag name
if not any(tag == tag_name for tag in tags.keys()):
  raise Exception("Model version not approved for deployment")

else:
  # if tag is found, check if it is approved
  if tags.get(tag_name).lower() == "approved":
    print("Model version approved for deployment")
    
    client.set_registered_model_alias(
      name=model_name,
      alias="Champion",
      version=model_version
    )

  else:
    raise Exception("Model version not approved for deployment")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Congratulations, our model is now "Approved" accordingly
# MAGIC
# MAGIC We now know that our model is ready to be used in inference pipelines and real-time serving endpoints, as it matches our validation standards.
# MAGIC
# MAGIC
# MAGIC Next: [Run batch inference from our newly promoted Champion model]($./05_batch_inference)
