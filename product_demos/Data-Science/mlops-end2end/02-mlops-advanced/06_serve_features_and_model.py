# Databricks notebook source
# MAGIC %md
# MAGIC # Churn Prediction Realtime Inference
# MAGIC
# MAGIC We have just seen how to get predictions in batches. Now, we will deploy the features and model to make real-time predictions via a REST API call. Customer application teams can embed this predictive capability into customer-facing applications and apply a retention strategy for customers predicted to churn as they interact with the application.
# MAGIC
# MAGIC Because the predictions are to be made in a customer-facing application as the customer interacts with it, they have to be returned with low latency.
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/mlops/advanced/banners/mlflow-uc-end-to-end-advanced-6-v2.png?raw=true" width="1200">
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Ffeatures%2Fmlops%2F06_serve_features_and_model&dt=MLOPS">
# MAGIC <!-- [metadata={"description":"MLOps end2end workflow: Load the model from MLFLow and run inferences, in batch or real-time.",
# MAGIC  "authors":["quentin.ambard@databricks.com"],
# MAGIC  "db_resources":{},
# MAGIC   "search_tags":{"vertical": "retail", "step": "Model testing", "components": ["mlflow"]},
# MAGIC                  "canonicalUrl": {"AWS": "", "Azure": "", "GCP": ""}}] -->
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC To serve the features and model, we will:
# MAGIC
# MAGIC - Make the features available for low-latency retrieval by the model through Databricks' online tables
# MAGIC - Deploy the registered model from Unity Catalog to a Model Serving endpoint for low latency serving
# MAGIC
# MAGIC These tasks can be done in the UI. They can also be automated by leveraging the Databricks Python SDK ([AWS](https://docs.databricks.com/en/dev-tools/sdk-python.html#)|[Azure](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/sdk-python)|[GCP](https://docs.gcp.databricks.com/dev-tools/sdk-python.html)) available in Databricks Runtime 13.3LTS+

# COMMAND ----------

# MAGIC %md
# MAGIC Last environment tested:
# MAGIC ```
# MAGIC databricks-feature-engineering==0.13.0a8
# MAGIC mlflow==3.3.2
# MAGIC ```

# COMMAND ----------

# MAGIC %pip install --quiet databricks-feature-engineering>=0.13.0a8 mlflow --upgrade
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %run ../_resources/00-setup $adv_mlops=true

# COMMAND ----------

dbutils.widgets.text("model_name", f"{catalog}.{db}.advanced_mlops_churn", "Model Name") # Will be populated from Deployment Jobs Parameters
dbutils.widgets.text("model_version", "1", "Model Version") # Will be populated from Deployment Jobs Parameters
dbutils.widgets.text("online_store_name", online_store_name, "Online Store Name")
dbutils.widgets.dropdown("drop_online_store", "False", ["True", "False"], "Reset Online Table(s)")
dbutils.widgets.dropdown("smoke_test", "False", ["True", "False"], "Smoke Test Flag")

# COMMAND ----------

# MAGIC %md
# MAGIC # Serve features with Databricks Lakebase's Online Tables (`beta`)
# MAGIC
# MAGIC To serve prediction queries with low latency, publish the features to Databricks online tables and serve them in real time to the model.
# MAGIC
# MAGIC During the feature engineering step, we have created a Delta Table as an offline feature table. Recall that any Delta Table with a primary key can be a feature table in Databricks.

# COMMAND ----------

is_smoke_test = dbutils.widgets.get("smoke_test").lower() == "true"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Create Databricks Online Feature Store
# MAGIC
# MAGIC You can create an online table from the Catalog Explorer UI, or by using the API. The steps are described below. For more details, see the Databricks documentation ([AWS](https://docs.databricks.com/aws/en/machine-learning/feature-store/online-feature-store)|[Azure](https://learn.microsoft.com/en-us/azure/databricks/machine-learning/feature-store/online-feature-store)). For information about required permissions, see Permissions ([AWS](https://docs.databricks.com/en/machine-learning/feature-store/online-tables.html#user-permissions)|[Azure](https://learn.microsoft.com/azure/databricks/machine-learning/feature-store/online-tables#user-permissions)).
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Enable Change-Data-Feed on Feature Table for performance considerations
# MAGIC
# MAGIC An online table is a read-only copy of a Delta Table stored in row-oriented format optimized for online access. 
# MAGIC
# MAGIC Databricks allows the online tables to be refreshed efficiently whenever there are updates to the underlying feature tables. This is enabled through Delta Lake's Change Data Feed feature. Let us first enable Change Data Feed on the underlying feature table `churn_feature_table`.

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE advanced_churn_feature_table SET TBLPROPERTIES (delta.enableChangeDataFeed = true)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### OPTION 1: Use the Catalog Explorer UI
# MAGIC In Catalog Explorer, navigate to the source table that you want to sync to an online table. From the **Create** menu, select **Synched table**.
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/mlops/advanced/06_create_online_table.gif?raw=true" width="1200">
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC Fill in the following fields:
# MAGIC
# MAGIC * **Name**: `churn_feature_table_online_table`
# MAGIC   * This is the name to use for the online table in Unity Catalog.
# MAGIC * **Primary Key**: `customer_id`
# MAGIC   * This is the column in the source table to use as the primary key in the online table.
# MAGIC * **Timeseries Key**: `transaction_ts`
# MAGIC   * This is the column in the source table to use as the timeseries key.
# MAGIC
# MAGIC Leave the **Sync mode** as **Snapshot**. This synchronization strategy updates the pipeline from its source feature table. Refer to the documentation to learn more ([AWS](https://docs.databricks.com/en/machine-learning/feature-store/online-tables.html)|[Azure](https://learn.microsoft.com/en-us/azure/databricks/machine-learning/feature-store/online-tables)).
# MAGIC
# MAGIC When you are done, click Confirm.
# MAGIC
# MAGIC You are brought to the online table page. Wait for the synchronization to complete.
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/mlops/advanced/06_online_table.png?raw=true" width="1200">
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC *The new online table is created under the catalog, schema, and name specified in the creation dialog. In Catalog Explorer, the online table is indicated by the online table icon.*

# COMMAND ----------

# MAGIC %md
# MAGIC ### OPTION 2: Use the Databricks Feature Engineering SDK 
# MAGIC
# MAGIC The other alternative is the [Databricks Feature Engineering SDK](https://api-docs.databricks.com/python/feature-engineering/latest/index.html). Let's  first define the table specifications, then create the table.

# COMMAND ----------



# COMMAND ----------

from databricks.feature_engineering import FeatureEngineeringClient


# Initialize the client
fe = FeatureEngineeringClient()

# Set Online Store Name
# Change it to avoid conflict if you want to redeploy a new version
online_store_name = dbutils.widgets.get("online_store_name") # dbdemosonlinestore
print(f'Using online store: {online_store_name}')

endpoint_name = ("advanced_mlops_churn_" + current_user_az)[:50]
print(f'Using endoint name: {endpoint_name}')

# Check if exists
online_store = fe.get_online_store(name=online_store_name)

# COMMAND ----------

from time import time


if online_store:
  print(f"Online store exists:")
  print(f"Store: {online_store.name}, State: {online_store.state}, Capacity: {online_store.capacity}")

  # Update the capacity of online store
  # updated_store = fe.update_online_store(
  #     name=online_store_name,
  #     capacity="CU_2"  # Upgrade to higher capacity
  # )

  if dbutils.widgets.get("drop_online_store") == "True" and not is_smoke_test:
    # Drop & wait
    fe.delete_online_store(name=online_store_name)
    time.sleep(60)

    print(f"Dropping/Recreating it.")
    online_store = fe.create_online_store(
      name=online_store_name,
      capacity="CU_1"
    )

elif not online_store and not is_smoke_test:
  # Create an online store with specified capacity & wait
  print(f"Creating Online store: {online_store_name}")
  online_store = fe.create_online_store(
      name=online_store_name,
      capacity="CU_1"  # Valid options: "CU_1", "CU_2", "CU_4", "CU_8"
  )

# COMMAND ----------

import time


if not is_smoke_test:
    print(f"Publishing feature table to online store...")
    max_retries = 5
    retry_count = 0
    while retry_count < max_retries:
        try:
            publish_state = fe.publish_table(
                online_store=online_store,
                source_table_name=f"{catalog}.{db}.advanced_churn_feature_table",
                online_table_name=f"{catalog}.{db}.advanced_churn_feature_online_table",
                # streaming=True
            )
            break
        except Exception as e:
            if "feature sync is currently in progress" in str(e):
                print("Feature sync in progress, retrying...")
                retry_count += 1
                time.sleep(10)  # Wait for 10 seconds before retrying
            else:
                raise e
    else:
        print("Failed to publish after multiple retries.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Review Featurization Logic to compute features on-demand
# MAGIC
# MAGIC We have deployed the online table and features are now available on-demand at low latency to the model.
# MAGIC
# MAGIC Recall that we have also defined a function earlier to calculate the `avg_price_increase` feature on-demand. Let's review the function here.
# MAGIC
# MAGIC This function was specified as a feature function when creating the training dataset with the Feature Engineering Client in the model training notebook. This information is logged with the model in MLflow. That means that at serving time, not only does the model know to retrieve features from the online table, but it also knows that the `avg_price_increase` feature has to be computed on-demand using this function.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE FUNCTION EXTENDED avg_price_increase;

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC # Deploying the model for real-time inference
# MAGIC
# MAGIC To make the model available for real-time inference through a REST API, we will deploy to a Model Serving endpoint.
# MAGIC
# MAGIC Our marketing team can point customer-facing applications used by many concurrent customers to this endpoint. Databricks makes it easy for ML teams to deploy this type of low-latency and high-concurrency applications. Model Serving handles all the infrastructure, deployment and scaling for you. You just need to deploy the model!

# COMMAND ----------

# MAGIC %md
# MAGIC ## Promote model for real-time serving
# MAGIC
# MAGIC We have seen earlier how to use the `@Champion` and `@Challenger` aliases to promote models to be called in a batch pipeline.
# MAGIC
# MAGIC Since real-time inference is another way the model will be consumed, we need a workflow to ensure the model is deployed safely. You can adopt different strategies to deploy a new model version as an endpoint. We will look at  A/B testing as an example here. Other strategies include canary deploying, shadow testing, etc.
# MAGIC
# MAGIC In A/B testing, there is a 'live' model served in production. When we have a new model version, we want to divert a percentage of our online traffic to this new model version. We let the new model version predict if these customers will churn, and compare the results to that of the 'live' model. We monitor the results. If the results are acceptable, then we divert all traffic to the new model version, and it now becomes the new 'live' version.
# MAGIC
# MAGIC Let's introduce a third alias, `@Production`, to track this workflow:
# MAGIC
# MAGIC - `@Production`: Production model that is 'live'
# MAGIC - `@Champion`: New model version that was promoted after Champion-Challenger testing
# MAGIC - `@Challenger`: Model that challenges the Champion. Never gets deployed for real-time serving unless promoted to Champion.
# MAGIC
# MAGIC Model Serving lets you deploy multiple model versions to a serving endpoint and specify a traffic split between these versions. For example, when there is a new Champion model to be A/B tested, you can route 20% of the traffic to the the Champion model and let the Production model process only 80%.
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/mlops/advanced/06_traffic_percent.png?raw=true" width="450">
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC Since we are deploying a model as an endpoint for the first time, we will promote the `@Production` alias to the Champion model and deploy it for serving against 100% traffic.
# MAGIC

# COMMAND ----------

# Fully qualified model name
# model_name = f"{catalog}.{db}.advanced_mlops_churn"
# model_version = client.get_model_version_by_alias(name=model_name, alias="Champion").version # Get champion version

model_name = dbutils.widgets.get("model_name")
model_version = dbutils.widgets.get("model_version") # Should pull @Champion Model by default

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Create the Model Serving endpoint
# MAGIC
# MAGIC We'll now create the Model Serving endpoint. This is very simple once the model has been registered in Unity Catalog. You can do it through the UI or by using the API.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### OPTION 1: Use the UI
# MAGIC
# MAGIC Go to the **Serving** section under **Machine Learning** and click **Create serving endpoint**.
# MAGIC
# MAGIC Open the Model page and click on "Serving". It'll start your model behind a REST endpoint and you can start sending your HTTP requests!
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/mlops/advanced/06_create_serving_endpoint.gif?raw=true" width="854">
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC Fill in the following fields:
# MAGIC
# MAGIC * **Name**: `dbdemos_mlops_advanced_churn`
# MAGIC   * This is the name of the serving endpoint
# MAGIC * **Entity**: Type `mlops_churn` and choose the model registered from the previous notebooks.
# MAGIC   * This is the Unity Catalog-registered model you want to serve.
# MAGIC * **Compute type**: Leave it as **CPU**
# MAGIC   * This is the column in the source table to use as the timeseries key.
# MAGIC * **Compute scale out**: Choose **Small**
# MAGIC   * This determines how many concurrent requests the endpoint can handle.
# MAGIC * **Scale to zero**: Keep it checked
# MAGIC   * This allows the serving endpoint to scale down to zero when there are no requests
# MAGIC
# MAGIC Click **Create** and wait for the endpoint to provision. Be patient, as this can take more than an hour. Take a break and check back later.
# MAGIC
# MAGIC When the endpoint is ready, it should show the status is **Ready**.
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/mlops/advanced/06_served_model.png?raw=true" width="854">
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC Refer to the documentation to learn more about creating and managing serving endpoints. ([AWS](https://docs.databricks.com/machine-learning/model-inference/serverless/create-manage-serverless-endpoints.html)|[Azure](https://learn.microsoft.com/en-us/azure/databricks/machine-learning/model-inference/serverless/create-manage-serverless-endpoints))

# COMMAND ----------

# MAGIC %md
# MAGIC ### OPTION 2: Enable model serving endpoint via API call
# MAGIC
# MAGIC What is done above using the UI to create a serving endpoint can also be done programmatically. The code below automatically creates a model serving endpoint for you.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create/Update serving endpoint

# COMMAND ----------

# Parse model name from UC namespace
served_model_name =  model_name.split('.')[-1]

# COMMAND ----------

from databricks.sdk.service.serving import EndpointCoreConfigInput


endpoint_config_dict = {
    "served_entities": [
        # Add models to be served to this list
        {
            "entity_name": model_name,
            "entity_version": model_version,
            "scale_to_zero_enabled": True,
            "workload_size": "Small",
        },
        # {
        #     "entity_name": model_name,
        #     "entity_version": model_version_b,
        #     "scale_to_zero_enabled": True,
        #     "workload_size": "Small",
        # },
    ],
    "traffic_config": {
        "routes": [
            # Add versions of the model to be served to this list
            # Make sure that traffic_percentage adds up to 100 over all served models
            # Naming convention for served_model_name: <registered_model_name>-<model_version>
            {"served_model_name": f"{served_model_name}-{model_version}", "traffic_percentage": 100},
            # {"served_model_name": f"{served_model_name}-{model_version_b}", "traffic_percentage": 10},
        ]
    },
    "auto_capture_config":{
        "catalog_name": catalog,
        "schema_name": db,
        "table_name_prefix": "advanced_churn_served"
    }
}

endpoint_config = EndpointCoreConfigInput.from_dict(endpoint_config_dict)

# COMMAND ----------

from databricks.sdk.service.serving import EndpointTag
from databricks.sdk.errors import ResourceDoesNotExist


if not is_smoke_test:
  try:
    # Update with new model version
    w.serving_endpoints.update_config(
        name=endpoint_name,
        served_entities=endpoint_config.served_entities,
        traffic_config=endpoint_config.traffic_config
      )
    
    print(f"Updating endpoint {endpoint_name} with models {model_name} version {model_version}")

  except ResourceDoesNotExist:
    w.serving_endpoints.create(
      name=endpoint_name,
      config=endpoint_config,
      tags=[EndpointTag.from_dict({"key": "dbdemos", "value": "advanced_mlops_churn"})]
    )
    
    print(f"Creating endpoint {endpoint_name} with models {model_name} version {model_version}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Wait/Verify that endpoint is ready
# MAGIC
# MAGIC Leave the following cell to run. It may take an hour or so for the endpoint to be ready. Take a break and check back later.

# COMMAND ----------

from datetime import timedelta


if not is_smoke_test:
  # Wait for the endpoint to be ready or finish updating
  endpoint = w.serving_endpoints.wait_get_serving_endpoint_not_updating(endpoint_name, timeout=timedelta(minutes=30))

  assert endpoint.state.config_update.value == "NOT_UPDATING" and endpoint.state.ready.value == "READY" , "Endpoint not ready or failed"

# COMMAND ----------

# MAGIC %md
# MAGIC # Send payloads via REST call
# MAGIC
# MAGIC You can test the endpoint on the UI. Copy and paste this JSON input to the UI to test the endpoint.
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC ```
# MAGIC {
# MAGIC   "dataframe_records": [
# MAGIC     {"customer_id": "0002-ORFBO", "transaction_ts": "2025-08-19", "split":"test"},
# MAGIC     {"customer_id": "0003-MKNFE", "transaction_ts": "2025-08-19", "split":"test"}
# MAGIC   ]
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/mlops/advanced/06_online_scoring.gif?raw=true" width="950">
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC Run the next cells to call the endpoint programatically.
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,Get input example directly from mlfow model or hard-code
from mlflow.store.artifact.models_artifact_repo import ModelsArtifactRepository
from mlflow.models import Model


# Setting these variables again in case the user skipped running the cells to deploy the model
# model_version = client.get_model_version_by_alias(name=model_name, alias="Champion").version # Get champion version

p = ModelsArtifactRepository(f"models:/{model_name}/{model_version}").download_artifacts("") 
input_example =  Model.load(p).load_input_example(p)

if input_example:
  # Only works if model NOT logged with feature store client
  dataframe_records =  [{input_example.to_dict(orient='records')}]
  
else:
  # Hard-code test-sample
  dataframe_records = [
    {"customer_id": "0002-ORFBO", "transaction_ts": "2025-08-19", "split":"test"},
    {"customer_id": "0003-MKNFE", "transaction_ts": "2025-08-19", "split":"test"}
  ]

# COMMAND ----------

# DBTITLE 1,Query endpoint
import time


# Wait 60 seconds for the endpoint so that the endpoint is fully ready to handle errors in the next command
# time.sleep(60)

print("Churn inference:")
response = w.serving_endpoints.query(name=f"{endpoint_name}", dataframe_records=dataframe_records)
print(response.predictions)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Congratulations! You have deployed a feature and model serving endpoint.
# MAGIC
# MAGIC Now that we are able to both make predictions in batches and predict a customer's propensity to churn in real-time, we will next look at how we can monitor the model's performance.
# MAGIC
# MAGIC With inference tables available, we can create a monitor to track our ML system's behavior over time (feature drifts, prediction drift, label drift, model accuracy, and metrics, etc)
# MAGIC
# MAGIC Next steps:
# MAGIC * [Create monitor for model performance]($./07_model_monitoring)
# MAGIC * [Detect drift and trigger model retrain]($./08_drift_detection)
