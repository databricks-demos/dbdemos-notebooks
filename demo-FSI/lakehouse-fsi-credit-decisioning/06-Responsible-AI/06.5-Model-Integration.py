# Databricks notebook source
# MAGIC %run ../_resources/00-setup $reset_all_data=false

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC <img src="https://github.com/manganganath/dbdemos-notebooks/blob/main/demo-FSI/lakehouse-fsi-credit-decisioning/06-Responsible-AI/images/architecture_5.png?raw=true" 
# MAGIC      style="width: 100%; height: auto; display: block; margin: 0;" />

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## Identify the current champion and challenger models
# MAGIC
# MAGIC If there is a model already in production, we define it as the current `champion model`. The model in staging is defined as the `challenger model`.

# COMMAND ----------

import mlflow

# Retrieve model info run by alias
client = mlflow.tracking.MlflowClient()

challenger_model_info = client.get_model_version_by_alias(name=f"{catalog}.{db}.{model_name}", alias="Staging")
challenger_run = client.get_run(challenger_model_info.run_id)

try:
    champion_model_info = client.get_model_version_by_alias(name=f"{catalog}.{db}.{model_name}", alias="Production")
except Exception as e:
    print(e)
    champion_model_info = None
if champion_model_info is not None:
  champion_run = client.get_run(champion_model_info.run_id)

# COMMAND ----------

# MAGIC %md
# MAGIC Next, we compare the perfomance of the two models. In this case, we use the `val_f1_score` metric. The model with the highest `val_f1_score` is the new `champion model`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deploying our model in production
# MAGIC
# MAGIC If `staging model` is the new `champion model`, we deploy that in prodcution and archive the exsiting `production model`. Otheriwse, we archive the `staging model` and make no changes to the production model.
# MAGIC
# MAGIC Before moving the model into production, there can be a human validation of the model involved.

# COMMAND ----------

# Indicate that validation checks are passed
client.set_model_version_tag(f"{catalog}.{db}.{model_name}", challenger_model_info.version, "validation_status", "approved")

if champion_model_info is None:
  # Archive the challenger model
  client.delete_registered_model_alias(name=f"{catalog}.{db}.{model_name}", alias="Staging")
  client.set_registered_model_alias(name=f"{catalog}.{db}.{model_name}", alias="Production", version=challenger_model_info.version)
  print(f'Deployed {catalog}.{db}.{model_name} in Production.')
elif challenger_run.data.metrics['val_f1_score'] > champion_run.data.metrics['val_f1_score']:
  # Archive the production model
  client.delete_registered_model_alias(name=f"{catalog}.{db}.{model_name}", alias="Production")
  client.set_registered_model_alias(name=f"{catalog}.{db}.{model_name}", alias="Archived", version=champion_model_info.version)
  # Move challenger model to production
  client.delete_registered_model_alias(name=f"{catalog}.{db}.{model_name}", alias="Staging")
  client.set_registered_model_alias(name=f"{catalog}.{db}.{model_name}", alias="Production", version=challenger_model_info.version)
  print(f'{challenger_model_info.version} of {catalog}.{db}.{model_name} is now the production version.')
else:
  # Archive the challenger model
  client.delete_registered_model_alias(name=f"{catalog}.{db}.{model_name}", alias="Staging")
  client.set_registered_model_alias(name=f"{catalog}.{db}.{model_name}", alias="Archived", version=challenger_model_info.version)
  print(f'No changes made to the production version of {catalog}.{db}.{model_name}.')

# COMMAND ----------

# MAGIC %md
# MAGIC We just moved our automl model as production ready! 
# MAGIC
# MAGIC Open model in Unity Catalog to explore its artifact and analyze the parameters used, including traceability to the notebook used for its creation.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Our model predicting default risks is now deployed in production
# MAGIC
# MAGIC
# MAGIC So far we have:
# MAGIC * ingested all required data in a single source of truth,
# MAGIC * properly secured all data (including granting granular access controls, masked PII data, applied column level filtering),
# MAGIC * enhanced that data through feature engineering,
# MAGIC * used MLFlow AutoML to track experiments and build a machine learning model,
# MAGIC * registered the model.

# COMMAND ----------


