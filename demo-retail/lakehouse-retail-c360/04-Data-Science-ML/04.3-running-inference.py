# Databricks notebook source
# MAGIC %md
# MAGIC # Churn Prediction Inference - Batch or serverless real-time
# MAGIC
# MAGIC
# MAGIC With AutoML, our best model was automatically saved in our MLFlow registry.
# MAGIC
# MAGIC All we need to do now is use this model to run Inferences. A simple solution is to share the model name to our Data Engineering team and they'll be able to call this model within the pipeline they maintained. That's what we did in our Spark Declarative Pipelines pipeline!
# MAGIC
# MAGIC Alternatively, this can be schedule in a separate job. Here is an example to show you how MLFlow can be directly used to retriver the model and run inferences.
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection or disable tracker during installation. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=lakehouse&notebook=04.3-running-inference&demo_name=lakehouse-retail-c360&event=VIEW">

# COMMAND ----------

# MAGIC %pip install mlflow==3.1.0
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../_resources/00-setup $reset_all_data=false

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##Deploying the model for batch inferences
# MAGIC
# MAGIC Now that our model is available in the Registry, we can load it to compute our inferences and save them in a table to start building dashboards.
# MAGIC
# MAGIC We will use MLFlow function to load a pyspark UDF and distribute our inference in the entire cluster. If the data is small, we can also load the model with plain python and use a pandas Dataframe.

# COMMAND ----------

# MAGIC %md ### Scaling inferences using Spark 
# MAGIC We'll first see how it can be loaded as a spark UDF and called directly in a SQL function:

# COMMAND ----------

import mlflow
model_name = "dbdemos_customer_churn"
mlflow.set_registry_uri("databricks-uc")
#                                                                                                Alias
#                                                                                  Model name       |
#                                                                                        |          |
predict_churn_udf = mlflow.pyfunc.spark_udf(spark, model_uri=f"models:/{catalog}.{db}.{model_name}@prod", env_manager='virtualenv', result_type='long')
# Note: virtualenv will recreate an env from scratch which can take some time, but prevent any version issue. If you're using the same compute as for training, you can remove it to use the local env instead (just install the lib from the requirements.txt file as below)
#We can use the function in SQL
spark.udf.register("predict_churn", predict_churn_udf)

# COMMAND ----------

# DBTITLE 1,Run inferences
columns = predict_churn_udf.metadata.get_input_schema().input_names()
spark.table('churn_features').withColumn("churn_prediction", predict_churn_udf(*columns)).display()

# COMMAND ----------

# MAGIC %md ### Pure pandas inference
# MAGIC If we have a small dataset, we can also compute our segment using a single node and pandas API:

# COMMAND ----------

# DBTITLE 1,Load the model dependencies from MLFlow registry
from mlflow.store.artifact.models_artifact_repo import ModelsArtifactRepository
import mlflow
# Use the Unity Catalog model registry
mlflow.set_registry_uri("databricks-uc")
# download model requirement from remote registry
requirements_path = ModelsArtifactRepository(f"models:/{catalog}.{db}.dbdemos_customer_churn@prod").download_artifacts(artifact_path="requirements.txt") 

# COMMAND ----------

# MAGIC %pip install -r $requirements_path
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../_resources/00-setup $reset_all_data=false

# COMMAND ----------

import mlflow
mlflow.set_registry_uri("databricks-uc")
model_name = "dbdemos_customer_churn"
model = mlflow.pyfunc.load_model(f"models:/{catalog}.{db}.{model_name}@prod")
columns = model.metadata.get_input_schema().input_names()
df = spark.table('churn_features').select(*columns).limit(10).toPandas()
df['churn_prediction'] = model.predict(df)
df.head(3)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## Realtime model serving with Databricks serverless serving
# MAGIC
# MAGIC <img style="float: right; margin-left: 20px" width="700" src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/retail/lakehouse-churn/lakehouse-c360-model-serving.png?raw=true" />
# MAGIC
# MAGIC Databricks also provides serverless serving.
# MAGIC
# MAGIC Click on model Serving, enable realtime serverless and your endpoint will be created, providing serving over REST api within a Click.
# MAGIC
# MAGIC Databricks Serverless offer autoscaling, including downscaling to zero when you don't have any traffic to offer best-in-class TCO while keeping low-latencies model serving.
# MAGIC
# MAGIC To deploy your serverless model, open the [Model Serving menu](https://e2-demo-tools.cloud.databricks.com/?o=1660015457675682#mlflow/endpoints), and select the model you registered within Unity Catalog.

# COMMAND ----------

# DBTITLE 1,Deploy the endpoint via databricks sdk client
from mlflow.deployments import get_deploy_client
model_endpoint_name = "dbdemos_customer_churn_endpoint"
last_version = get_last_model_version(f"{catalog}.{db}.{model_name}")
client = get_deploy_client("databricks")
try:
    endpoint = client.create_endpoint(
        name=model_endpoint_name,
        config={
            "served_entities": [
                {
                    "name": f"dbdemos_customer_churn_endpoint_{last_version}",
                    "entity_name": f"{catalog}.{db}.{model_name}",
                    "entity_version": last_version,
                    "workload_size": "Small",
                    "scale_to_zero_enabled": True
                }
            ]
        }
    )
except Exception as e:
    if "already exists" in str(e).lower():
        print(f"Endpoint {catalog}.{db}.{model_endpoint_name} already exists. Skipping creation.")
    else:
        raise e

while client.get_endpoint(model_endpoint_name)['state']['config_update'] == 'IN_PROGRESS':
    time.sleep(10)

# COMMAND ----------

dataset = spark.table('churn_features').select(*columns).limit(3).toPandas()
#Make it a string to send to the inference endpoint
dataset['last_transaction'] = dataset['last_transaction'].astype(str)
dataset

# COMMAND ----------

# DBTITLE 1,Call the REST API deployed using standard python
from mlflow import deployments

def score_model(dataset):
  client = mlflow.deployments.get_deploy_client("databricks")
  payload = {"dataframe_split": dataset.to_dict(orient='split')}
  predictions = client.predict(endpoint=model_endpoint_name, inputs=payload)
  print(predictions)

#Deploy your model and uncomment to run your inferences live!
score_model(dataset)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Next step: Leverage inferences and automate actions to increase revenue
# MAGIC
# MAGIC ## Automate action to reduce churn based on predictions
# MAGIC
# MAGIC We now have an end 2 end data pipeline analizing and predicting churn. We can now easily trigger actions to reduce the churn based on our business:
# MAGIC
# MAGIC - Send targeting email campaign to the customer the most likely to churn
# MAGIC - Phone campaign to discuss with our customers and understand what's going
# MAGIC - Understand what's wrong with our line of product and fixing it
# MAGIC
# MAGIC These actions are out of the scope of this demo and simply leverage the Churn prediction field from our ML model.
# MAGIC
# MAGIC ## Track churn impact over the next month and campaign impact
# MAGIC
# MAGIC Of course, this churn prediction can be re-used in our dashboard to analyse future churn and measure churn reduction. 
# MAGIC
# MAGIC The pipeline created with the Lakehouse will offer a strong ROI: it took us a few hours to setup this pipeline end 2 end and we have potential gain for $129,914 / month!
# MAGIC
# MAGIC <img width="800px" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/lakehouse-retail-churn-dbsql-prediction-dashboard.png">
# MAGIC
# MAGIC <a dbdemos-dashboard-id="churn-prediction" href='/sql/dashboardsv3/01ef00cc36721f9e9f2028ee75723cc1'>Open the Churn prediction DBSQL dashboard</a>
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Reducing churn leveraging Databricks GenAI and LLMs capabilities 
# MAGIC
# MAGIC GenAI provides unique capabilities to improve your customer relationship, providing better services but also better analyzing your churn risk.
# MAGIC
# MAGIC Databricks provides built-in GenAI capabilities for you to accelerate such GenAI apps deployment. 
# MAGIC
# MAGIC Discover how with the [Agent Tools]($../05-Generative-AI/05.1-Agent-Functions-Creation) Notebook in the new Generative AI section of this demo!
# MAGIC
# MAGIC [Go back to the introduction]($../00-churn-introduction-lakehouse)
