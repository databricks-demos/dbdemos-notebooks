# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC # Getting realtime patient risks
# MAGIC
# MAGIC Let's leverage the model we trained to deploy real-time inferences behind a REST API.
# MAGIC
# MAGIC This will provide instant recommandations for any new patient, on demand, potentially also explaining the recommendation (see [next notebook]($./03.5-Explainability-patient-readmission) for Explainability) 
# MAGIC
# MAGIC Now that our model has been created with Databricks AutoML, we can easily flag it as Production Ready and turn on Databricks Model Serving.
# MAGIC
# MAGIC We'll be able to send HTTP REST Requests and get inference (risk probability) in real-time.
# MAGIC
# MAGIC
# MAGIC ## Databricks Model Serving
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/hls/patient-readmission/patient-risk-ds-flow-4.png?raw=true" width="700px" style="float: right; margin-left: 10px;" />
# MAGIC
# MAGIC
# MAGIC Databricks Model Serving is fully serverless:
# MAGIC
# MAGIC * One-click deployment. Databricks will handle scalability, providing blazing fast inferences and startup time.
# MAGIC * Scale down to zero as an option for best TCO (will shut down if the endpoint isn't used).
# MAGIC * Built-in support for multiple models & version deployed.
# MAGIC * A/B Testing and easy upgrade, routing traffic between each versions while measuring impact.
# MAGIC * Built-in metrics & monitoring.
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=lakehouse&notebook=04.4-Model-Serving-patient-readmission&demo_name=lakehouse-patient-readmission&event=VIEW">

# COMMAND ----------

# DBTITLE 1,Make sure we have the latset sdk (used in the helper)
# MAGIC %pip install databricks-sdk -U
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../_resources/00-setup $reset_all_data=false

# COMMAND ----------

# DBTITLE 1,Load the model with "prod" alias from Unity Catalog Registry
model_name = "dbdemos_hls_patient_readmission"

#Enable Unity Catalog with mlflow registry
mlflow.set_registry_uri('databricks-uc')
client = mlflow.tracking.MlflowClient()
#Get model with PROD alias (make sure you run the notebook 04.2 to save the model in UC)
latest_model = client.get_model_version_by_alias(f"{catalog}.{db}.dbdemos_hls_patient_readmission", "prod")

# COMMAND ----------

# DBTITLE 1,Deploy the model in our serving endpoint
#See helper in companion notebook
serving_client = EndpointApiClient()

#The first deployment will build the image and take a few extra minute. Stop/Start is then instantly.
serving_client.create_endpoint_if_not_exists(
    "dbdemos_hls_patient_readmission_endpoint",
    model_name=f"{catalog}.{db}.dbdemos_hls_patient_readmission",
    model_version=latest_model.version,
    workload_size="Small",
    scale_to_zero_enabled=True,
    wait_start=True
)


#Make sure all users can access our endpoint for this demo
set_model_endpoint_permission("dbdemos_embedding_endpoint", "CAN_MANAGE", "users")

# COMMAND ----------

# MAGIC %md 
# MAGIC Our model endpoint was automatically created. 
# MAGIC
# MAGIC Open the [endpoint UI](#mlflow/endpoints/dbdemos_hls_patient_readmission_endpoint) to explore your endpoint and use the UI to send queries.
# MAGIC
# MAGIC *Note that the first deployment will build your model image and take a few minutes. It'll then stop & start instantly.*

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Testing the model
# MAGIC
# MAGIC Now that the model is deployed, let's test it with information from one of our patient. *Note that we could also chose to return a risk percentage instead of a binary result.*

# COMMAND ----------

p = ModelsArtifactRepository(f"models:/{catalog}.{db}.dbdemos_hls_patient_readmission@prod").download_artifacts("") 
dataset =  {"dataframe_split": Model.load(p).load_input_example(p).to_dict(orient='split')}

# COMMAND ----------

from mlflow.store.artifact.models_artifact_repo import ModelsArtifactRepository

endpoint_url = f"{serving_client.base_url}/realtime-inference/dbdemos_hls_patient_readmission_endpoint/invocations"
inferences = requests.post(
    endpoint_url, json=dataset, headers=serving_client.headers
).json()

prediction_score = list(inferences["predictions"])[0]
print(f"Patient readmission risk: {prediction_score}.")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC # Updating your model and monitoring its performance with A/B testing 
# MAGIC
# MAGIC Databricks Model Serving let you easily deploy & test new versions of your model.
# MAGIC
# MAGIC You can dynamically reconfigure your endpoint to route a subset of your traffic to a newer version. In addition, you can leverage endpoint monitoring to understand your model behavior and track your A/B deployment.
# MAGIC
# MAGIC * Without making any production outage
# MAGIC * Slowly routing requests to the new model
# MAGIC * Supporting auto-scaling & potential bursts
# MAGIC * Performing some A/B testing ensuring the new model is providing better outcomes
# MAGIC * Monitorig our model outcome and technical metrics (CPU/load etc)
# MAGIC
# MAGIC Databricks makes this process super simple with Serverless Model Serving endpoint.
# MAGIC

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
# MAGIC ## Next
# MAGIC
# MAGIC Making sure your model doesn't have bias and being able to explain its behavior is extremely important to increase health care quality and personalize patient journey. <br/>
# MAGIC Explore your model with [04.5-Explainability-patient-readmission]($./04.5-Explainability-patient-readmission) on the Lakehouse.
# MAGIC
# MAGIC ## Conclusion: the power of the Lakehouse
# MAGIC
# MAGIC In this demo, we've seen an end 2 end flow with the Lakehouse:
# MAGIC
# MAGIC - Data ingestion made simple with Delta Live Table
# MAGIC - Leveraging Databricks notebooks and SQL warehouse to create, anaylize and share our dashboards 
# MAGIC - Model Training with AutoML for citizen Data Scientist
# MAGIC - Ability to tune our model for better results, improving our patient journey quality
# MAGIC - Ultimately, the ability to deploy and make explainable ML predictions, made possible with the full Lakehouse capabilities.
# MAGIC
# MAGIC [Go back to the introduction]($../00-patient-readmission-introduction) or discover how to use Databricks Workflow to orchestrate everything together through the [05-Workflow-Orchestration-patient-readmission]($../05-Workflow-Orchestration/05-Workflow-Orchestration-patient-readmission).
