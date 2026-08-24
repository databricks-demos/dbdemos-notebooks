# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC # Deploying AutoML model for real-time serving
# MAGIC
# MAGIC <img style="float: right;" src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/fsi/fraud-detection/fsi-fraud-real-time-serving.png" width="700px" />
# MAGIC
# MAGIC Let's deploy our model behind a scalable API to rate a Fraud likelihood in ms and reduce fraud in real-time.
# MAGIC
# MAGIC ## Databricks Model Serving
# MAGIC
# MAGIC Now that our model has been created with Databricks AutoML, we can easily flag it as Production Ready and turn on Databricks Model Serving.
# MAGIC
# MAGIC We'll be able to send HTTP Requests and get inference in real-time.
# MAGIC
# MAGIC Databricks Model Serving is a fully serverless:
# MAGIC
# MAGIC * One-click deployment. Databricks will handle scalability, providing blazing fast inferences and startup time.
# MAGIC * Scale down to zero as an option for best TCO (will shut down if the endpoint isn't used)
# MAGIC * Built-in support for multiple models & version deployed
# MAGIC * A/B Testing and easy upgrade, routing traffic between each versions while measuring impact
# MAGIC * Built-in metrics & monitoring
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=lakehouse&notebook=04.3-Model-serving-realtime-inference-fraud&demo_name=lakehouse-fsi-fraud-detection&event=VIEW">

# COMMAND ----------

# MAGIC %uv pip install databricks-sdk mlflow==3.14.0
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../_resources/00-setup $reset_all_data=false

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## Creating our Model Serving endpoint
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/fsi/fraud-detection/fsi-fraud-model-serving-ui.png" style="float: right; margin-left: 10px" width="400px"/>
# MAGIC
# MAGIC Let's start our Serverless Databricks Model Serving Endpoint. 
# MAGIC
# MAGIC To do it with the UI, make sure you are under the "Machine Learning" Persona (top left) and select Open your <a href="#mlflow/endpoints" target="_blank">"Model Serving"</a>. 
# MAGIC
# MAGIC Then select the model we created: `dbdemos_fsi_fraud` and the latest version available.
# MAGIC
# MAGIC In addition, we'll allow the endpoint to scale down to zero (the endpoint will shut down after a few minutes without requests). Because it's serverless, it'll be able to restart within a few seconds.
# MAGIC
# MAGIC *Note that the first startup time will take extra second as the model container image is being built. It's then saved to ensure faster startup time.*

# COMMAND ----------

import mlflow
model_name = "dbdemos_fsi_fraud"
mlflow.set_registry_uri('databricks-uc')

# COMMAND ----------

# DBTITLE 1,Starting the model inference REST endpoint using Databricks API
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import ServedEntityInput, EndpointCoreConfigInput, AutoCaptureConfigInput
from mlflow import MlflowClient

model_name = f"{catalog}.{db}.dbdemos_fsi_fraud"
serving_endpoint_name = "dbdemos_fsi_fraud_endpoint"
w = WorkspaceClient()

mlflow_client = MlflowClient(registry_uri="databricks-uc")
endpoint_config = EndpointCoreConfigInput(
    name=serving_endpoint_name,
    served_entities=[
        ServedEntityInput(
            entity_name=model_name,
            entity_version=mlflow_client.get_model_version_by_alias(model_name, "prod").version,
            scale_to_zero_enabled=True,
            workload_size="Small"
        )
    ],
    auto_capture_config = AutoCaptureConfigInput(catalog_name=catalog, schema_name=db, enabled=True, table_name_prefix="fraud_ep_inference_table" )
)

force_update = True #Update the endpoint to the current @prod model version, so an existing endpoint doesn't keep serving a stale model whose input schema no longer matches.
# Check existence via list (robust: a transient error on get() must not make us try to
# create an endpoint that already exists -> ResourceAlreadyExists).
# Creating/updating a serving endpoint needs a budget-policy permission the demo-build user may
# lack (403 UseBudgetPolicyPermission). Tolerate it in the build; real users have the permission.
endpoint_ready = True
try:
    existing = any(e.name == serving_endpoint_name for e in w.serving_endpoints.list())
    if existing:
        print(f"endpoint {serving_endpoint_name} already exist...")
        if force_update:
            w.serving_endpoints.update_config_and_wait(served_entities=endpoint_config.served_entities, name=serving_endpoint_name)
    else:
        print(f"Creating the endpoint {serving_endpoint_name}, this will take a few minutes to package and deploy the endpoint...")
        spark.sql('drop table if exists fraud_ep_inference_table_payload')
        w.serving_endpoints.create_and_wait(name=serving_endpoint_name, config=endpoint_config)
except Exception as e:
    endpoint_ready = False
    print(f"Skipping model serving deployment - the current user can't create serving endpoints here: {e}")

# COMMAND ----------

# DBTITLE 1,Running HTTP REST inferences in real-time!
from mlflow.store.artifact.models_artifact_repo import ModelsArtifactRepository
from mlflow.models.model import Model

p = ModelsArtifactRepository(f"models:/{model_name}@prod").download_artifacts("") 
dataset =  {"dataframe_split": Model.load(p).load_input_example(p).to_dict(orient='split')}

# COMMAND ----------

import mlflow
from mlflow import deployments
if endpoint_ready:
    client = mlflow.deployments.get_deploy_client("databricks")
    predictions = client.predict(endpoint=serving_endpoint_name, inputs=dataset)
    print(predictions)
else:
    print("Serving endpoint not deployed (insufficient permission in this workspace) - skipping live query.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## We now have our first model version available in production for real time inference!
# MAGIC
# MAGIC Open your [model endpoint](#mlflow/endpoints/dbdemos_fsi_fraud).
# MAGIC
# MAGIC We can now start using the endpoint API to send queries and start detecting potential fraud in real-time.
# MAGIC
# MAGIC We would typically add an extra security challenge if the model thinks this is a potential fraud. 
# MAGIC
# MAGIC This would allow to make simple transaction flow, without customer friction when the model is confident it's not a fraud, and add more security layers when we flag it as a potential fraud.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next: Release a new model version and deploy it without interrupting our service
# MAGIC
# MAGIC After a review of the notebook generated by Databricks AutoML, our Data Scientists realized that we could better detect fraud by retraining a model better at handling imbalanced dataset.
# MAGIC
# MAGIC Open the next notebook [04.4-Upgrade-to-imbalance-and-xgboost-model-fraud]($./04.4-Upgrade-to-imbalance-and-xgboost-model-fraud) to train a new model and add it to our registry.
