# Databricks notebook source
# MAGIC %md
# MAGIC # Model retrain based on detected drift metric(s)
# MAGIC
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/mlops-end2end-flow-7.png" width="1200">
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Ffeatures%2Fmlops%2F07_retrain_automl&dt=MLOPS">
# MAGIC <!-- [metadata={"description":"MLOps end2end workflow: Batch to automatically retrain model on a monthly basis.",
# MAGIC  "authors":["quentin.ambard@databricks.com"],
# MAGIC  "db_resources":{},
# MAGIC   "search_tags":{"vertical": "retail", "step": "Model testing", "components": ["mlflow"]},
# MAGIC                  "canonicalUrl": {"AWS": "", "Azure": "", "GCP": ""}}] -->

# COMMAND ----------

# MAGIC %md ## Synchronous re-training job
# MAGIC
# MAGIC We can programatically schedule a job to retrain our model, or retrain it based on an event if we realize that our model doesn't behave as expected.
# MAGIC
# MAGIC This notebook should be run as a job. It'll call the Databricks Auto-ML API, get the best model and request a validation to get the `Challenger` alias.

# COMMAND ----------

# MAGIC %run ../_resources/00-setup $reset_all_data=false

# COMMAND ----------

# DBTITLE 1,Create new experiment
import uuid


this_experiment_name = f"{churn_experiment_name}_{str(uuid.uuid4())[:4]}"
print(f"Running new autoML experiment {this_experiment_name} and pushing best model to {model_name}")

# COMMAND ----------

from databricks import automl

# COMMAND ----------

# MAGIC %md
# MAGIC ### Train using Feature Store tables

# COMMAND ----------

# DBTITLE 1,Define feature lookups
churn_feature_lookups = [
  {
    "table_name" : f"{catalog}.{dbName}.{feature_table_name}",
    "lookup_key" : [primary_key],
    "timestamp_lookup_key": timestamp_col
  }
]

# COMMAND ----------

# DBTITLE 1,Run AutoML
model = automl.classify(
  dataset=f"{catalog}.{dbName}.{labels_table_name}",
  target_col=label_col,
  exclude_cols=[primary_key, timestamp_col],
  feature_store_lookups=churn_feature_lookups,
  timeout_minutes=20,
  experiment_name=this_experiment_name,
  pos_label="Yes"
)

# COMMAND ----------

# DBTITLE 1,Register the Best Run
import mlflow
from mlflow.tracking.client import MlflowClient


run_id = model.best_trial.mlflow_run_id
model_uri = f"runs:/{run_id}/model"

client.set_tag(run_id, key='demographic_vars', value="senior_citizen,gender")
client.set_tag(run_id, key='feature_table', value=f"{catalog}.{dbName}.{feature_table_name}")
client.set_tag(run_id, key='labels_table', value=f"{catalog}.{dbName}.{labels_table_name}")

model_details = mlflow.register_model(model_uri, model_name)

# COMMAND ----------

# DBTITLE 1,Add Descriptions
best_score = model.best_trial.metrics['test_f1_score']
run_name = model.best_trial.model_description.split("(")[0]

client.update_model_version(
  name=model_details.name,
  version=model_details.version,
  description=f"[AutoML] This model version was built using automated retraining with an accuracy/F1 validation metric of {round(best_score,2)*100}%"
)

# COMMAND ----------

# DBTITLE 1,Request transition to Challenger
r_t = request_transition(
  model_name = model_name,
  version = model_details.version,
  stage = "Challenger"
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Next: Building a dashboard with Customer Churn information & Creating model monitor
# MAGIC
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/mlops-end2end-flow-dashboard.png" width="600px" style="float:right"/>
# MAGIC
# MAGIC We now have all our data ready, including customer churn.
# MAGIC
# MAGIC The Churn table containing analysis and Churn predictions can be shared with the Analyst and Marketing team.
# MAGIC
# MAGIC With Databricks SQL, we can build our Customer Churn monitoring Dashboard to start tracking our Marketing campaign effect!
# MAGIC
# MAGIC Next:
# MAGIC * [Explore DBSQL Churn Dashboard](TO-DO/ add-link)
