# Databricks notebook source
# MAGIC %md
# MAGIC # Churn Prediction Model Serving
# MAGIC
# MAGIC **Work-In-Progress**
# MAGIC Only works when deploying models without feature functions for now
# MAGIC
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/mlops-end2end-flow-6.png" width="1200">
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Ffeatures%2Fmlops%2F06_staging_inference&dt=MLOPS">
# MAGIC <!-- [metadata={"description":"MLOps end2end workflow: Load the model from MLFLow and run inferences, in batch or realtime.",
# MAGIC  "authors":["quentin.ambard@databricks.com"],
# MAGIC  "db_resources":{},
# MAGIC   "search_tags":{"vertical": "retail", "step": "Model testing", "components": ["mlflow"]},
# MAGIC                  "canonicalUrl": {"AWS": "", "Azure": "", "GCP": ""}}] -->

# COMMAND ----------

# MAGIC %md
# MAGIC To manage model serving endpoints we'll leverage the databricks pyhton sdk ([AWS](https://docs.databricks.com/en/dev-tools/sdk-python.html#)|[Azure](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/sdk-python)|[GCP](https://docs.gcp.databricks.com/dev-tools/sdk-python.html)) available in Databricks Runtime 13.3LTS+

# COMMAND ----------

# DBTITLE 1,Remove once databricks-sdk version in MLR will be >=0.9.0
# MAGIC %pip install databricks-sdk --upgrade
# MAGIC
# MAGIC
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./_resources/00-setup $reset_all_data=false $catalog="dbdemos"

# COMMAND ----------

endpoint_name = "dbdemos_mlops_churn"

model_version_champion = client.get_model_version_by_alias(name=model_name, alias="Champion").version # Get champion version
model_version_challenger = client.get_model_version_by_alias(name=model_name, alias="Challenger").version # Get challenger version
print(f"Deploying {model_name} versions {model_version_champion} (champion) & {model_version_challenger} (challenger) to endpoint {endpoint_name}")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC #Deploying the model for real-time inferences
# MAGIC
# MAGIC <img style="float: right; margin-left: 20px" width="600" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/churn_realtime_inference.gif" />
# MAGIC
# MAGIC Our marketing team also needs to run inferences in real-time using REST api (send a customer ID and get back the inference).
# MAGIC
# MAGIC While Feature store integration in real-time serving will come with Model Serving v2, you can deploy your Databricks Model in a single click.
# MAGIC
# MAGIC Open the Model page and click on "Serving". It'll start your model behind a REST endpoint and you can start sending your HTTP requests!

# COMMAND ----------

# MAGIC %md
# MAGIC ## Enable model serving endpoint via API call
# MAGIC
# MAGIC After calling `log_model`, a new version of the model is saved. To provision a serving endpoint, follow the steps below.
# MAGIC
# MAGIC 1. Click **Serving** in the left sidebar. If you don't see it, switch to the Machine Learning Persona ([AWS](https://docs.databricks.com/workspace/index.html#use-the-sidebar)|[Azure](https://docs.microsoft.com/azure/databricks//workspace/index#use-the-sidebar)).
# MAGIC 2. Enable serving for your model. See the Databricks documentation for details ([AWS](https://docs.databricks.com/machine-learning/model-inference/serverless/create-manage-serverless-endpoints.html)|[Azure](https://learn.microsoft.com/en-us/azure/databricks/machine-learning/model-inference/serverless/create-manage-serverless-endpoints)).
# MAGIC
# MAGIC The code below automatically creates a model serving endpoint for you.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create/Update serving endpoint

# COMMAND ----------

# Parse model name from UC namespace
served_model_name =  model_name.split('.')[-1]

# COMMAND ----------

from databricks.sdk.service.serving import EndpointCoreConfigInput


endpoint_config_dict = {
    "served_models": [
        {
            "model_name": model_name,
            "model_version": model_version_champion,
            "scale_to_zero_enabled": True,
            "workload_size": "Small",
            "instance_profile_arn": dbutils.secrets.get(scope="fieldeng", key="oneenv_ip_arn"),
        },
        {
            "model_name": model_name,
            "model_version": model_version_challenger,
            "scale_to_zero_enabled": True,
            "workload_size": "Small",
            "instance_profile_arn": dbutils.secrets.get(scope="fieldeng", key="oneenv_ip_arn"),
        },
    ],
    "traffic_config": {
        "routes": [
            {"served_model_name": f"{served_model_name}-{model_version_champion}", "traffic_percentage": 50},
            {"served_model_name": f"{served_model_name}-{model_version_challenger}", "traffic_percentage": 50},
        ]
    },
    "auto_capture_config":{
        "catalog_name": catalog,
        "schema_name": schema,
        "table_name_prefix": "mlops_churn_served"
    }
}


endpoint_config = EndpointCoreConfigInput.from_dict(endpoint_config_dict)

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import EndpointTag


# Create/Update endpoint and deploy model+version
w = WorkspaceClient()

# COMMAND ----------

try:
  w.serving_endpoints.create_and_wait(
    name=endpoint_name,
    config=endpoint_config,
    tags=[EndpointTag.from_dict({"key": "db_demos", "value": "mlops_churn"})]
  )
  
  print(f"Creating endpoint {endpoint_name} with models {model_name} versions {model_version_champion} & {model_version_challenger}")

except Exception as e:
  if "already exists" in e.args[0]:
    print(f"Endpoint with name {endpoint_name} already exists, updating it with model {model_name}-{model_version_champion}/{model_version_challenger}")

    # TO-DO:
    # w.serving_endpoints.update_config_and_wait(
    #   name=endpoint_name,
    #   end=TrafficConfig(routes=routes_handle),
    #   served_models=served_models_handle
    # )
  else:
    raise(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Wait/Verify that endpoint is ready

# COMMAND ----------

endpoint = w.serving_endpoints.wait_get_serving_endpoint_not_updating(endpoint_name)

assert endpoint.state.config_update.value == "NOT_UPDATING" and endpoint.state.ready.value == "READY" , "Endpoint not ready or failed"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Send payloads via REST call
# MAGIC Example payload format expected by endpoint
# MAGIC ```
# MAGIC {
# MAGIC   "dataframe_records": [
# MAGIC     {"customer_id": "0002-ORFBO", "scoring_timestamp": "2024-02-05"},
# MAGIC     {"customer_id": "0003-MKNFE", "scoring_timestamp": "2024-02-05"}
# MAGIC   ]
# MAGIC }
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Get input example directly from mlfow model or hard-code
from mlflow.store.artifact.models_artifact_repo import ModelsArtifactRepository
from mlflow.models import Model

p = ModelsArtifactRepository(f"models:/{model_name}/{model_version_challenger}").download_artifacts("") 
input_example =  Model.load(p).load_input_example(p)

if input_example:
  # Only works if model NOT logged with feature store client
  dataframe_records =  [{input_example.to_dict(orient='records')}]

else:
  # Hard-code test-sample
  dataframe_records = [
    {primary_key: "0002-ORFBO", timestamp_col: "2024-02-05"},
    {primary_key: "0003-MKNFE", timestamp_col: "2024-02-05"}
  ]

# COMMAND ----------

# DBTITLE 1,Query endpoint
print("Churn inference:")
response = w.serving_endpoints.query(name=endpoint_name, dataframe_records=dataframe_records)
print(response.predictions)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Next: Monitor model performance [OPTIONAL]
# MAGIC
# MAGIC With inference tables availables we can create a monitor to track our ML's system behavior over time (feature drifts, prediction drift, label drift, model accuracy and metrics etc.)
# MAGIC
# MAGIC Next steps:
# MAGIC * [Create monitor for model performance]($./07_model_monitoring)
# MAGIC * [Automate model re-training]($./08_retrain_churn_automl)
