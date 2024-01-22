# Databricks notebook source
# MAGIC %md
# MAGIC ## Monthly AutoML Retrain
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

# MAGIC %md ## Monthly training job
# MAGIC 
# MAGIC We can programatically schedule a job to retrain our model, or retrain it based on an event if we realize that our model doesn't behave as expected.
# MAGIC 
# MAGIC This notebook should be run as a job. It'll call the Databricks Auto-ML API, get the best model and request a transition to Staging.

# COMMAND ----------

# MAGIC %run ./_resources/00-setup $reset_all_data=false $catalog="hive_metastore"

# COMMAND ----------

# DBTITLE 1,Load Features
fs = FeatureStoreClient()
features = fs.read_table(f'{dbName}.dbdemos_mlops_churn_features')

# COMMAND ----------

# DBTITLE 1,Run AutoML
import databricks.automl
model = databricks.automl.classify(features, target_col = "churn", data_dir= "dbfs:/tmp/", timeout_minutes=5) 

# COMMAND ----------

# DBTITLE 1,Register the Best Run
import mlflow
from mlflow.tracking.client import MlflowClient

client = MlflowClient()

run_id = model.best_trial.mlflow_run_id
model_name = "dbdemos_mlops_churn"
model_uri = f"runs:/{run_id}/model"

client.set_tag(run_id, key='db_table', value=f'{dbName}.dbdemos_mlops_churn_features')
client.set_tag(run_id, key='demographic_vars', value='seniorCitizen,gender_Female')

model_details = mlflow.register_model(model_uri, model_name)

# COMMAND ----------

# DBTITLE 1,Add Descriptions
model_version_details = client.get_model_version(name=model_name, version=model_details.version)

client.update_model_version(
  name=model_details.name,
  version=model_details.version,
  description="This model version was built using autoML and automatically getting the best model."
)

# COMMAND ----------

# DBTITLE 1,Request transition to Staging
# Transition request to staging
staging_request = {'name': model_name, 'version': model_details.version, 'stage': 'Staging', 'archive_existing_versions': 'true'}
mlflow_call_endpoint('transition-requests/create', 'POST', json.dumps(staging_request))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ## Next: Building a dashboard with Customer Churn information
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/mlops-end2end-flow-dashboard.png" width="600px" style="float:right"/>
# MAGIC 
# MAGIC We now have all our data ready, including customer churn. 
# MAGIC 
# MAGIC The Churn table containing analysis and Churn predictions can be shared with the Analyst and Marketing team.
# MAGIC 
# MAGIC With Databricks SQL, we can build our Customer Churn monitoring Dashboard to start tracking our Marketing campaign effect!

# COMMAND ----------

# MAGIC %md
# MAGIC For a complete Customer Churn example & dashboards, run `dbdemos.install('lakehouse-retail-churn')`.
