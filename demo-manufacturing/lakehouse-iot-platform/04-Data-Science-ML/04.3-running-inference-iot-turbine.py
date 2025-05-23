# Databricks notebook source
# MAGIC %md
# MAGIC # Predictive Maintenance Inference - Batch or serverless real-time
# MAGIC
# MAGIC
# MAGIC With AutoML, our best model was automatically saved in our MLFlow registry.
# MAGIC
# MAGIC All we need to do now is use this model to run Inferences. A simple solution is to share the model name to our Data Engineering team and they'll be able to call this model within the pipeline they maintained. That's what we did in our Delta Live Table pipeline!
# MAGIC
# MAGIC Alternatively, this can be schedule in a separate job. Here is an example to show you how MLFlow can be directly used to retriver the model and run inferences.
# MAGIC
# MAGIC *Make sure you run the previous notebook to be able to access the data.*
# MAGIC
# MAGIC ## Environment Recreation
# MAGIC The cell below downloads the model artifacts associated with your model in the remote registry, which include `conda.yaml` and `requirements.txt` files. In this notebook, `pip` is used to reinstall dependencies by default.
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=lakehouse&notebook=04.3-running-inference-iot-turbine&demo_name=lakehouse-patient-readmission&event=VIEW">

# COMMAND ----------

# MAGIC %pip install mlflow==2.22.0 databricks-sdk==0.40.0

# COMMAND ----------

# MAGIC %run ../_resources/00-setup $reset_all_data=false

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##Deploying the model for batch inferences
# MAGIC
# MAGIC <img style="float: right; margin-left: 20px" width="800" src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/retail/lakehouse-churn/ep_model_serving_creation.gif?raw=true" />
# MAGIC
# MAGIC Now that our model is available in the Registry, we can load it to compute our inferences and save them in a table to start building dashboards.
# MAGIC
# MAGIC We will use MLFlow function to load a pyspark UDF and distribute our inference in the entire cluster. If the data is small, we can also load the model with plain python and use a pandas Dataframe.
# MAGIC
# MAGIC If you don't know how to start, Databricks can generate a batch inference notebook in just one click from the model registry: Open MLFlow model registry and click the "User model for inference" button!

# COMMAND ----------

# MAGIC %md ### Scaling inferences using Spark 
# MAGIC We'll first see how it can be loaded as a spark UDF and called directly in a SQL function:

# COMMAND ----------

import mlflow
mlflow.set_registry_uri('databricks-uc')
#                                                                                                    Stage/version  
#                                                                                       Model name         |        
#                                                                                           |              |        
predict_maintenance = mlflow.pyfunc.spark_udf(spark, f"models:/{catalog}.{db}.dbdemos_turbine_maintenance@prod", "string", env_manager='virtualenv')
#We can use the function in SQL
spark.udf.register("predict_maintenance", predict_maintenance)
columns = predict_maintenance.metadata.get_input_schema().input_names()

# COMMAND ----------

# DBTITLE 1,Run inferences
columns = predict_maintenance.metadata.get_input_schema().input_names()
spark.table('turbine_hourly_features').withColumn("dbdemos_turbine_maintenance", predict_maintenance(*columns)).display()

# COMMAND ----------

# DBTITLE 1,Or in SQL directly
# MAGIC %sql
# MAGIC SELECT turbine_id, predict_maintenance(hourly_timestamp, avg_energy, std_sensor_A, std_sensor_B, std_sensor_C, std_sensor_D, std_sensor_E, std_sensor_F, location, model, state) as prediction FROM turbine_hourly_features

# COMMAND ----------

# MAGIC %md ### Pure pandas inference
# MAGIC If we have a small dataset, we can also compute our segment using a single node and pandas API:

# COMMAND ----------

from mlflow.store.artifact.models_artifact_repo import ModelsArtifactRepository
mlflow.set_registry_uri('databricks-uc')
local_path = ModelsArtifactRepository(f"models:/{catalog}.{db}.dbdemos_turbine_maintenance@prod").download_artifacts("") # download model from remote registry

requirements_path = os.path.join(local_path, "requirements.txt")
if not os.path.exists(requirements_path):
  dbutils.fs.put("file:" + requirements_path, "", True)

# COMMAND ----------

# MAGIC %pip install -r $requirements_path
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../_resources/00-setup $reset_all_data=false

# COMMAND ----------

import mlflow
mlflow.set_registry_uri('databricks-uc')
model = mlflow.pyfunc.load_model(f"models:/{catalog}.{db}.dbdemos_turbine_maintenance@prod")
columns = model.metadata.get_input_schema().input_names()
df = spark.table('turbine_hourly_features').select(*columns).limit(10).toPandas()
df['churn_prediction'] = model.predict(df)
display(df)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## Realtime model serving with Databricks serverless serving
# MAGIC
# MAGIC <img style="float: right; margin-left: 20px" width="800" src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/retail/lakehouse-churn/ep_model_serving_creation.gif?raw=true" />
# MAGIC
# MAGIC Databricks also provides serverless serving.
# MAGIC
# MAGIC Click on model Serving, enable realtime serverless and your endpoint will be created, providing serving over REST api within a Click.
# MAGIC
# MAGIC Databricks Serverless offer autoscaling, including downscaling to zero when you don't have any traffic to offer best-in-class TCO while keeping low-latencies model serving.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Real time model inference
# MAGIC
# MAGIC Let's now deploy our model behind a realtime model serving endpoint.
# MAGIC
# MAGIC We'll then use this endpoint in our GenAI Agentic demo to be able to fetch a turbine status in realtime
# MAGIC

# COMMAND ----------

from mlflow.deployments import get_deploy_client

client = get_deploy_client("databricks")
try:
    endpoint = client.create_endpoint(
        name=MODEL_SERVING_ENDPOINT_NAME,
        config={
            "served_entities": [
                {
                    "name": "iot-maintenance-serving-endpoint",
                    "entity_name": f"{catalog}.{db}.{model_name}",
                    "entity_version": get_last_model_version(f"{catalog}.{db}.{model_name}"),
                    "workload_size": "Small",
                    "scale_to_zero_enabled": True
                }
            ]
        }
    )
except Exception as e:
    if "already exists" in str(e):
        print(f"Endpoint {catalog}.{db}.{MODEL_SERVING_ENDPOINT_NAME} already exists. Skipping creation.")
    else:
        raise e

while client.get_endpoint(MODEL_SERVING_ENDPOINT_NAME)['state']['config_update'] == 'IN_PROGRESS':
    time.sleep(10)

# COMMAND ----------

# MAGIC %md You can now view the status of the Feature Serving Endpoint in the table on the **Serving endpoints** page. Click **Serving** in the sidebar to display the page.

# COMMAND ----------

# DBTITLE 1,Call the REST API deployed using standard python
from mlflow import deployments


def score_model(dataset):
  client = mlflow.deployments.get_deploy_client("databricks")
  predictions = client.predict(endpoint=MODEL_SERVING_ENDPOINT_NAME, inputs=dataset.to_dict(orient='split'))

dataset = spark.table(f'turbine_hourly_features').select(*columns).limit(3).toPandas()
#Deploy your model and uncomment to run your inferences live!
#score_model(dataset)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Next step: Leverage inferences and automate action to lower cost
# MAGIC
# MAGIC ## Automate action to react on potential turbine failure
# MAGIC
# MAGIC We now have an end 2 end data pipeline analizing and predicting churn. We can now easily trigger actions to reduce downtime, such as dispatching a team earlier to fix the issue before an actual outage!
# MAGIC
# MAGIC ## Track windturbine failure and impact
# MAGIC
# MAGIC Of course, this prediction can be re-used in our dashboard to analyse future failure and measure impact. 
# MAGIC
# MAGIC <img width="800px" src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/manufacturing/lakehouse-iot-turbine/lakehouse-manuf-iot-dashboard-2.png">
# MAGIC
# MAGIC <a dbdemos-dashboard-id="turbine-predictive" href="/sql/dashboardsv3/01ef3a4263bc1180931f6ae733179956">Open the Predictive Maintenance AI/BI dashboard</a> | [Go back to the introduction]($../00-IOT-wind-turbine-introduction-DI-platform)
