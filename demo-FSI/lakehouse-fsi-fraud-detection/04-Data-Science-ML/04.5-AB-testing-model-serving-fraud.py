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
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Ffsi%2Flakehouse_fsi_fraud%2Fml-ab-testing&dt=LAKEHOUSE_FSI_FRAUD">

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

# DBTITLE 1,Move the new model in production if it's not already the case
client = mlflow.tracking.MlflowClient()
latest_model = client.get_latest_versions("dbdemos_fsi_fraud")[0]
if latest_model.current_stage != 'Production':
  client.transition_model_version_stage("dbdemos_fsi_fraud", latest_model.version, stage = "Production", archive_existing_versions=True)

# COMMAND ----------

# DBTITLE 1,Deploying our model with a new route using the API
model_currently_served = serving_client.get_inference_endpoint("dbdemos_fsi_fraud")['config']['served_models'][0]
conf = {
    "served_models":[
       {  
          "name": "model_A",
          "model_name": "dbdemos_fsi_fraud",
          "model_version": latest_model.version,
          "workload_size": "Small",
          "scale_to_zero_enabled": True
       }, 
       {
          "name": "new_mode_B",
          "model_name": "dbdemos_fsi_fraud",
          "model_version": model_currently_served['model_version'],
          "workload_size": "Small",
          "scale_to_zero_enabled": True
      }
    ],
    "traffic_config": {
      "routes": [
        {
          "served_model_name": "model_A",
          "traffic_percentage": 80
        },
        {
          "served_model_name": "new_mode_B",
          "traffic_percentage": 20
        }
      ]
    }
  }

serving_client.update_model_endpoint("dbdemos_fsi_fraud", conf)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Our new model is now serving 20% of our requests
# MAGIC 
# MAGIC Open your <a href="#mlflow/endpoints/dbdemos_fsi_fraud" target="_blank"> Model Serving Endpoint</a> to view the changes and track the 2 models performance

# COMMAND ----------

# DBTITLE 1,Trying our new Model Serving setup
from mlflow.store.artifact.models_artifact_repo import ModelsArtifactRepository

#Let's get our dataset example
p = ModelsArtifactRepository("models:/dbdemos_fsi_fraud/Production").download_artifacts("") 
dataset =  {"dataframe_split": Model.load(p).load_input_example(p).to_dict(orient='split')}

endpoint_url = f"{serving_client.base_url}/realtime-inference/dbdemos_fsi_fraud/invocations"
inferences = requests.post(endpoint_url, json=dataset, headers=serving_client.headers).json()
print("Fraud inference:")
print(inferences)

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
