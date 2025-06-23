# Databricks notebook source
# MAGIC %run ../demo_setup/00.Initial_library_install

# COMMAND ----------

# MAGIC %md
# MAGIC # 4. 🚀 Model Serving
# MAGIC
# MAGIC <div style="text-align: center;">
# MAGIC   <img src="../demo_setup/images/model_serving.png" width="1200px"/> 
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC #### Simplified deployment for all AI models and agents
# MAGIC
# MAGIC Deploy any model type, from pretrained open source models to custom models built on your own data — on both CPUs and GPUs. Automated container build and infrastructure management reduce maintenance costs and speed up deployment so you can focus on building your AI agent systems and delivering value faster for your business
# MAGIC
# MAGIC #### Unified management for all models
# MAGIC
# MAGIC Manage all models, including custom ML models like PyFunc, scikit-learn and LangChain, foundation models (FMs) on Databricks like Llama 3, MPT and BGE, and foundation models hosted elsewhere like ChatGPT, Claude 3, Cohere and Stable Diffusion. Model Serving makes all models accessible in a unified user interface and API, including models hosted by Databricks, or from another model provider on Azure or AWS.
# MAGIC
# MAGIC
# MAGIC #### Governance built-in
# MAGIC
# MAGIC Integrate with Mosaic AI Gateway to meet stringent security and advanced governance requirements. You can enforce proper permissions, monitor model quality, set rate limits, and track lineage across all models whether they are hosted by Databricks or on any other model provider.
# MAGIC
# MAGIC ![](https://www.databricks.com/sites/default/files/2023-09/simplified-deployment.png?v=1696033263)

# COMMAND ----------

# MAGIC %md
# MAGIC # 4.1 🚀 CICD with Deployment Jobs
# MAGIC
# MAGIC A key final step in the end-to-end machine learning lifecycle is to deploy our model as a REST API endpoint using **Databricks Model Serving.**
# MAGIC
# MAGIC To accomplish this we will leverage best practice with **CICD** (continuous integration, continuous deployment) to automatically deploy our model once a new version is registered into unity catalog. We accomplish this via **Deployment Jobs.**
# MAGIC
# MAGIC ![](../demo_setup/images/simple-deployment-job.png)
# MAGIC
# MAGIC Our Deployment job has the below steps:
# MAGIC - Approve model deployment
# MAGIC - Deploy new version of the model as a REST endpoint
# MAGIC
# MAGIC This process is fully automated, all it requires is someone to approve the deployment after the job detects a new model version has been registered.
# MAGIC
# MAGIC ![](../demo_setup/images/deployment_approval.png)
# MAGIC
# MAGIC See documentation here: [Deployment Jobs](https://docs.databricks.com/aws/en/mlflow/deployment-job)
# MAGIC
# MAGIC To view our deployment job we can follow this link:
# MAGIC [Deployment Job for Si Model](https://e2-demo-field-eng.cloud.databricks.com/explore/data/models/mining_iron_ore_processing_demo_catalog/iop_schema/si_model?o=1444828305810485)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # 4.2 🚀 (Optional) Manually Serving the Model as a Real-Time Endpoint
# MAGIC
# MAGIC This code demonstrates how to deploy a trained ML model as a real-time REST API endpoint using **Databricks Model Serving** — often we would leverage automation to deploy model serving endpoints, however sometimes we may want to serve a model outside of a **CICD** workflow.
# MAGIC
# MAGIC Here's what the code does:
# MAGIC - Inputs: It takes a model name and version (typically registered in the MLflow Model Registry).
# MAGIC - Constructs a fully qualified model name (catalog.database.model_name) for serving.
# MAGIC - Creates or updates a serving endpoint using the WorkspaceClient from the Databricks SDK.
# MAGIC - Defines the endpoint configuration:
# MAGIC   - Uses ServedEntityInput to specify the model version, workload size, and whether to enable scale-to-zero (cost-efficient).
# MAGIC   - Enables auto-capture of inference inputs and outputs into a Delta table for auditing, monitoring, or retraining (via AutoCaptureConfigInput).
# MAGIC   - If the endpoint already exists and force_update=True, it updates the configuration to serve the new model version.
# MAGIC
# MAGIC 💡 This allows teams to rapidly deploy production-grade ML models with built-in observability and governance — without needing to manage infrastructure manually.

# COMMAND ----------

import mlflow
from mlflow.deployments import get_deploy_client
from mlflow.tracking import MlflowClient
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import ServedEntityInput, EndpointCoreConfigInput, AutoCaptureConfigInput

def serve_model(model_name, model_version):
    model_FQDN = f"{catalog_name}.{schema_name}.{model_name}"
    serving_endpoint_name = f"process_demo_{model_name}"
    w = WorkspaceClient()
    endpoint_config = EndpointCoreConfigInput(
        name=serving_endpoint_name,
        served_entities=[
            ServedEntityInput(
                entity_name=model_FQDN,
                entity_version=model_version,
                scale_to_zero_enabled=True,
                workload_size="Small"
            )
        ],
        auto_capture_config = AutoCaptureConfigInput(catalog_name=catalog_name, schema_name=schema_name, enabled=True, table_name_prefix=f"{model_name}_payload_inference_table" )
    )

    force_update = True #Set this to True to release a newer version (the demo won't update the endpoint to a newer model version by default)
    existing_endpoint = next((e for e in w.serving_endpoints.list() if e.name == serving_endpoint_name), None)
    if existing_endpoint == None:
        print(f"Creating the endpoint {serving_endpoint_name}, this will take a few minutes to package and deploy the endpoint...")
        w.serving_endpoints.create_and_wait(name=serving_endpoint_name, config=endpoint_config)
    else:
        print(f"endpoint {serving_endpoint_name} already exist...")
        if force_update:
            w.serving_endpoints.update_config_and_wait(served_entities=endpoint_config.served_entities, name=serving_endpoint_name)

# COMMAND ----------

mlflow.set_tracking_uri("databricks")
mlflow.set_registry_uri("databricks-uc")
client = mlflow.tracking.MlflowClient()

fe_model_version_uri = f"{catalog_name}.{schema_name}.fe_model"
si_model_version_uri = f"{catalog_name}.{schema_name}.si_model"

# Load the model from Unity Catalog via the alias "Champion"
fe_model_details = client.get_model_version_by_alias(fe_model_version_uri, "Champion")
fe_model_version = fe_model_details.version

# Load the model from Unity Catalog via the alias "Champion"
si_model_details = client.get_model_version_by_alias(si_model_version_uri, "Champion")
si_model_version = si_model_details.version

# Serve the models
serve_model("fe_model", fe_model_version)
serve_model("si_model", si_model_version)