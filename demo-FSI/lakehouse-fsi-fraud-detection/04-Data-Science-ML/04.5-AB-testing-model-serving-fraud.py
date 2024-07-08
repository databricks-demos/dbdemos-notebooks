# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC # New Model deployement with A/B testing 
# MAGIC
# MAGIC
# MAGIC <img style="float: right;" src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/fsi/fraud-detection/model-serving-ab-testing.png" width="800px" />
# MAGIC
# MAGIC Our new model is now saved in our Registry.
# MAGIC
# MAGIC Our next step is now to deploy it while ensuring that it's behaving as expected. We want to be able to deploy the new version in the REST API:
# MAGIC
# MAGIC * Without making any production outage
# MAGIC * Slowly routing requests to the new model
# MAGIC * Supporting auto-scaling & potential bursts
# MAGIC * Performing some A/B testing ensuring the new model is providing better outcomes
# MAGIC * Monitorig our model outcome and technical metrics (CPU/load etc)
# MAGIC
# MAGIC Databricks makes this process super simple with Serverless Model Serving endpoint.
# MAGIC
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=lakehouse&notebook=04.5-AB-testing-model-serving-fraud&demo_name=lakehouse-fsi-fraud-detection&event=VIEW">

# COMMAND ----------

# MAGIC %run ../_resources/00-setup $reset_all_data=false

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## Routing our Model Serving endpoint to multiple models
# MAGIC <img style="float: right; margin-left: 10px" width="700px" src="https://cms.databricks.com/sites/default/files/inline-images/db-498-blog-imgs-1.png" />
# MAGIC
# MAGIC Databricks Model Serving endpoints allow you to serve different models and dynamically redirect a subset of the traffic to a given model.
# MAGIC
# MAGIC Open your <a href="#mlflow/endpoints/dbdemos_fsi_fraud" target="_blank"> Model Serving Endpoint</a>, edit the configuration and add our second model.
# MAGIC
# MAGIC Select the traffic ratio you want to send to the new model (20%), save and Databricks will handle the rest for you. 
# MAGIC
# MAGIC Your endpoint will automatically bundle the new model, and start routing a subset of your queries to this model.
# MAGIC
# MAGIC Let's see how this can be done using the API.

# COMMAND ----------

from databricks.sdk.service.serving import ServedEntityInput, EndpointCoreConfigInput, AutoCaptureConfigInput, TrafficConfig, Route
from databricks.sdk import WorkspaceClient
from mlflow import MlflowClient

model_name = f"{catalog}.{db}.dbdemos_fsi_fraud"
serving_endpoint_name = "dbdemos_fsi_fraud_endpoint"

w = WorkspaceClient()
mlflow_client = MlflowClient(registry_uri="databricks-uc")
served_entities=[
        ServedEntityInput(
            name="prod_model",
            entity_name=model_name,
            entity_version=mlflow_client.get_model_version_by_alias(model_name, "prod").version,
            scale_to_zero_enabled=True,
            workload_size="Small"
        ),
        ServedEntityInput(
            name="candidate_model",
            entity_name=model_name,
            entity_version=mlflow_client.get_model_version_by_alias(model_name, "candidate").version,
            scale_to_zero_enabled=True,
            workload_size="Small"
        )
    ]
traffic_config=TrafficConfig(routes=[
        Route(
            served_model_name="prod_model",
            traffic_percentage=90
        ),
        Route(
            served_model_name="candidate_model",
            traffic_percentage=10
        )
    ])

print('Updating the endpoint, this will take a few sec, please wait...')
w.serving_endpoints.update_config_and_wait(name=serving_endpoint_name, served_entities=served_entities, traffic_config=traffic_config)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Our new model is now serving 10% of our requests
# MAGIC
# MAGIC Open your <a href="#mlflow/endpoints/dbdemos_fsi_fraud_endpoint" target="_blank"> Model Serving Endpoint</a> to view the changes and track the 2 models performance

# COMMAND ----------

mlflow.set_registry_uri('databricks-uc') 
p = ModelsArtifactRepository(f"models:/{model_name}@prod").download_artifacts("") 
dataset =  {"dataframe_split": Model.load(p).load_input_example(p).to_dict(orient='split')}

# COMMAND ----------

dataset

# COMMAND ----------

# DBTITLE 1,Trying our new Model Serving setup
from mlflow import deployments
client = mlflow.deployments.get_deploy_client("databricks")
#Let's do multiple call to track the results in the model endpoint inference table
for row in dataset:
    predictions = client.predict(endpoint=serving_endpoint_name, inputs=[row])
    print(predictions)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## Model monitoring and A/B testing analysis
# MAGIC
# MAGIC Because the Model Serving runs within our Lakehouse, Databricks will automatically save and track all our Model Endpoint results as a Delta Table.
# MAGIC
# MAGIC We can then easily plug a feedback loop to start analysing the revenue in $ each model is offering. 
# MAGIC
# MAGIC All these metrics, including A/B testing validation (p-values etc) can then be pluged into a Model Monitoring Dashboard and alerts can be sent for errors, potentially triggering new model retraining or programatically updating the Endpoint routes to fallback to another model.
# MAGIC
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/fsi/fraud-detection/model-serving-monitoring.png" width="1200px" />

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## Conclusion: the power of the Lakehouse
# MAGIC
# MAGIC In this demo, we've seen an end 2 end flow with the Lakehouse:
# MAGIC
# MAGIC - Data ingestion made simple with Delta Live Table
# MAGIC - Leveraging Databricks warehouse to Analyze existing Fraud
# MAGIC - Model Training with AutoML for citizen Data Scientist
# MAGIC - Ability to tune our model for better results, improving our revenue
# MAGIC - Ultimately, the ability to Deploy and track our models in real time, made possible with the full lakehouse capabilities.
# MAGIC
# MAGIC [Go back to the introduction]($../00-FSI-fraud-detection-introduction-lakehouse) or discover how to use Databricks Workflow to orchestrate this tasks: [05-Workflow-orchestration-fsi-fraud]($../05-Workflow-orchestration/05-Workflow-orchestration-fsi-fraud)
