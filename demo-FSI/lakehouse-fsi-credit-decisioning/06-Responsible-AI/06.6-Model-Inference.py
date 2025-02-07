# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC # Use the production model to batch score credit worthiness
# MAGIC
# MAGIC
# MAGIC <img src="https://github.com/manganganath/dbdemos-notebooks/blob/main/demo-FSI/lakehouse-fsi-credit-decisioning/06-Responsible-AI/images/architecture_6.png?raw=true" 
# MAGIC      style="width: 100%; height: auto; display: block; margin: 0;" />
# MAGIC
# MAGIC
# MAGIC Databricks AutoML runs experiments across a grid and creates many models and metrics to determine the best models among all trials. This is a glass-box approach to create a baseline model, meaning we have all the code artifacts and experiments available afterwards. 
# MAGIC
# MAGIC Here, we selected the Notebook from the best run from the AutoML experiment.
# MAGIC
# MAGIC All the code below has been automatically generated. As data scientists, we can tune it based on our business knowledge, or use the generated model as-is.
# MAGIC
# MAGIC This saves data scientists hours of developement and allows team to quickly bootstrap and validate new projects, especally when we may not know the predictors for alternative data such as the telco payment data.
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=lakehouse&org_id=1444828305810485&notebook=%2F03-Data-Science-ML%2F03.3-Batch-Scoring-credit-decisioning&demo_name=lakehouse-fsi-credit&event=VIEW&path=%2F_dbdemos%2Flakehouse%2Flakehouse-fsi-credit%2F03-Data-Science-ML%2F03.3-Batch-Scoring-credit-decisioning&version=1">

# COMMAND ----------

# MAGIC %run ../_resources/00-setup $reset_all_data=false

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## Running batch inference to score our existing database
# MAGIC
# MAGIC Now that our model was created and deployed in production within the MLFlow registry.
# MAGIC
# MAGIC <br/>
# MAGIC We can now easily load it calling the `Production` stage, and use it in any Data Engineering pipeline (a job running every night, in streaming or even within a Delta Live Table pipeline).
# MAGIC
# MAGIC <br/>
# MAGIC
# MAGIC We'll then save this information as a new table without our FS database, and start building dashboards and alerts on top of it to run live analysis.

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
# MAGIC # Deploying the Credit Scoring model for real-time serving
# MAGIC
# MAGIC
# MAGIC Let's deploy our model behind a scalable API to evaluate credit-worthiness in real-time.
# MAGIC
# MAGIC ## Databricks Model Serving
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
