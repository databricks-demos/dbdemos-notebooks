# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC # Model Inference
# MAGIC
# MAGIC In this notebook, we demonstrate how to operationalize machine learning models on Databricks by enabling both batch and real-time inference. For real-time serving, we leverage Databricks Model Serving, which allows us to expose the model through a low-latency REST API endpoint, enabling scalable and responsive predictions for downstream applications. We also implement batch inference using pyspark for large-scale processing. This dual approach illustrates how Databricks supports a unified platform for production-grade ML inference across diverse use cases.
# MAGIC <img src="https://github.com/manganganath/dbdemos-notebooks/blob/main/demo-FSI/lakehouse-fsi-credit-decisioning/_resources/images/architecture_6.png?raw=true" 
# MAGIC      style="width: 100%; height: auto; display: block; margin: 0;" />

# COMMAND ----------

# MAGIC %run ../_resources/00-setup $reset_all_data=false

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## Running batch inference
# MAGIC
# MAGIC Now that our model has been successfully trained and deployed to production within the MLFlow registry, we can seamlessly integrate it into various Data Engineering workflows. 
# MAGIC  
# MAGIC We can now easily load it calling the `Production` stage, and use it in any Data Engineering pipeline (a job running every night, in streaming or even within a Delta Live Table pipeline).

# COMMAND ----------

import mlflow

mlflow.set_registry_uri('databricks-uc')

# Load model as a Spark UDF.
loaded_model = mlflow.pyfunc.spark_udf(spark, 
                                       model_uri=f"models:/{catalog}.{db}.{model_name}@production", result_type='double', 
                                       env_manager="virtualenv")

# COMMAND ----------

from pyspark.sql import functions as F

features = loaded_model.metadata.get_input_schema().input_names()

feature_df = spark.table("credit_decisioning_features_labels").fillna(0)

prediction_df = feature_df.withColumn("prediction", loaded_model(F.struct(*features)).cast("integer")) \
                   .withColumn("model_id", F.lit(1)).cache()

display(prediction_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC In the scored data frame above, we have essentially created an end-to-end process to predict credit worthiness for any customer, regardless of whether the customer has an existing bank account. We have a binary prediction which captures this and incorporates all the intellience from Databricks AutoML and curated features from our feature store.

# COMMAND ----------

(prediction_df
  .write
  .format("delta")
  .mode("overwrite")
  .option("overwriteSchema",True)
  .option("delta.enableChangeDataFeed", "true")
  .saveAsTable("credit_decisioning_baseline_predictions")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deploying the Credit Scoring model for real-time serving
# MAGIC
# MAGIC
# MAGIC Let's deploy our model behind a scalable API to evaluate credit-worthiness in real-time.
# MAGIC
# MAGIC Now that our model has been created with Databricks AutoML, we can easily flag it as Production Ready and turn on Databricks Model Serving.
# MAGIC
# MAGIC We'll be able to send HTTP Requests and get inference in real-time.
# MAGIC
# MAGIC Databricks Model Serving is fully serverless:
# MAGIC
# MAGIC * One-click deployment. Databricks will handle scalability, providing blazing fast inferences and startup time.
# MAGIC * Scale down to zero as an option for best TCO (will shut down if the endpoint isn't used).
# MAGIC * Built-in support for multiple models & version deployed.
# MAGIC * A/B Testing and easy upgrade, routing traffic between each versions while measuring impact.
# MAGIC * Built-in metrics & monitoring.

# COMMAND ----------

import mlflow

# Retrieve model info run by alias
client = mlflow.tracking.MlflowClient()

production_model_info = client.get_model_version_by_alias(name=f"{catalog}.{db}.{model_name}", alias="Production")

# COMMAND ----------

from mlflow.deployments import get_deploy_client

mlflow.set_registry_uri("databricks-uc")
deploy_client = get_deploy_client("databricks")

endpoint = deploy_client.create_endpoint(
  name=f"{endpoint_name}",
  config={
    "served_entities": [
        {
            "name": f"{model_name}-{production_model_info.version}",
            "entity_name": f"{catalog}.{db}.{model_name}",
            "entity_version": f"{production_model_info.version}",
            "workload_size": "Small",
            "scale_to_zero_enabled": True
        }
    ],
    "traffic_config": {
        "routes": [
            {
                "served_model_name": f"{model_name}-{production_model_info.version}",
               "traffic_percentage": "100"
            }
        ]
    },
    "auto_capture_config":{
        "catalog_name": f"{catalog}",
        "schema_name": f"{db}",
        "table_name_prefix": f"{endpoint_name}"
    },
    "environment_variables": {
        "ENABLE_MLFLOW_TRACING": "True"
    }
  }
)

# COMMAND ----------

from time import sleep

# Wait for endpoint to be ready before running inference
# This can take 10 minutes or so

endpoint_udpate = endpoint
is_not_ready = True
is_not_updated = True

print("Waiting for endpoint to be ready...")
while(is_not_ready | is_not_updated):
  sleep(10)
  endpoint_udpate = deploy_client.get_endpoint(endpoint_name)
  is_not_ready = endpoint_udpate["state"]["ready"] != "READY"
  is_not_updated = endpoint_udpate["state"]["config_update"] != "NOT_UPDATING"

print(f"Endpoint status: {endpoint_udpate['state']['ready']}; {endpoint_udpate['state']['config_update']}")

# COMMAND ----------

# MAGIC %md 
# MAGIC Our model endpoint was automatically created. 
# MAGIC
# MAGIC Open the [endpoint UI](#mlflow/endpoints) to explore your endpoint and use the UI to send queries.
# MAGIC
# MAGIC *Note that the first deployment will build your model image and take a few minutes. It'll then stop & start instantly.*

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Testing the model serving endpoint
# MAGIC
# MAGIC Now that the model is deployed, let's test it with some queries and investigate the inferance table.

# COMMAND ----------

from mlflow.store.artifact.models_artifact_repo import ModelsArtifactRepository
from mlflow.models.model import Model

p = ModelsArtifactRepository(f"models:/{model_name}@production").download_artifacts("") 
dataset =  {"dataframe_split": Model.load(p).load_input_example(p).to_dict(orient='split')}
predictions = deploy_client.predict(endpoint=endpoint_name, inputs=dataset)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Next Steps
# MAGIC
# MAGIC With model inference in place, the next critical step is [06.7-Model-Monitoring]($./06-Responsible-AI/06.7-Model-Monitoring). In this final stage, we will continuously track data drift and model performance degradation using Lakehouse Monitoring. By integrating real-time monitoring with inference, we can proactively manage model health and maintain responsible AI principles throughout the credit scoring lifecycle.
