# Databricks notebook source
# MAGIC %md
# MAGIC # Churn Prediction Model Serving
# MAGIC
# MAGIC **Work-In-Progress**
# MAGIC Only works when deploying models without feature functions for now
# MAGIC
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/mlops-end2end-flow-6.png" width="1200">
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Ffeatures%2Fmlops%2F06_staging_inference&dt=MLOPS">
# MAGIC <!-- [metadata={"description":"MLOps end2end workflow: Load the model from MLFLow and run inferences, in batch or realtime.",
# MAGIC  "authors":["quentin.ambard@databricks.com"],
# MAGIC  "db_resources":{},
# MAGIC   "search_tags":{"vertical": "retail", "step": "Model testing", "components": ["mlflow"]},
# MAGIC                  "canonicalUrl": {"AWS": "", "Azure": "", "GCP": ""}}] -->

# COMMAND ----------

# MAGIC %md
# MAGIC To manage model serving endpoints we'll leverage the databricks pyhton sdk ([AWS](https://docs.databricks.com/en/dev-tools/sdk-python.html#)|[Azure](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/sdk-python)|[GCP](https://docs.gcp.databricks.com/dev-tools/sdk-python.html)) available in Databricks Runtime 13.3LTS+

# COMMAND ----------

# DBTITLE 1,Install MLflow version for model lineage in UC [for MLR < 15.2]
# MAGIC %pip install --quiet mlflow==2.14.3
# MAGIC %pip install -U databricks-sdk
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../_resources/00-setup $reset_all_data=false

# COMMAND ----------

feature_table_name = "churn_feature_table"
primary_key = "customer_id"
timestamp_col ="transaction_ts"
model_name = f"{catalog}.{dbName}.mlops_advanced_churn"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Feature serving with Databrick's Online tables
# MAGIC
# MAGIC For serving predictions queries with low-latency, publish the features to Databricks online tables and serve them in real time.
# MAGIC
# MAGIC Databricks allows the online tables to be refreshed efficiently whenever there are updates to the underlying feature tables. This is enabled through the Change Data Feed feature of Delta Lake. Let us first enable Change Data Feed on the underlying feature table `churn_feature_table`.

# COMMAND ----------

# DBTITLE 1,Enable Change-Data-Feed on Feature Table for performance considerations
spark.sql(f"ALTER TABLE {catalog}.{db}.{feature_table_name} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Creating the Online Table
# MAGIC
# MAGIC You create an online table from the Catalog Explorer, or using the API. The steps are described below. For more details, see the Databricks documentation ([AWS](https://docs.databricks.com/en/machine-learning/feature-store/online-tables.html#create)|[Azure](https://learn.microsoft.com/azure/databricks/machine-learning/feature-store/online-tables#create)). For information about required permissions, see Permissions ([AWS](https://docs.databricks.com/en/machine-learning/feature-store/online-tables.html#user-permissions)|[Azure](https://learn.microsoft.com/azure/databricks/machine-learning/feature-store/online-tables#user-permissions)).
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### OPTION 1: Use the Catalog Explorer UI
# MAGIC In Catalog Explorer, navigate to the source table that you want to sync to an online table. From the kebab menu, select **Create online table**.
# MAGIC
# MAGIC * Use the selectors in the dialog to configure the online table.
# MAGIC   * Name: Name to use for the online table in Unity Catalog.
# MAGIC   * Primary Key: Column(s) in the source table to use as primary key(s) in the online table.
# MAGIC   * Timeseries Key: (Optional). Column in the source table to use as timeseries key. When specified, the online table includes only the row with the latest timeseries key value for each primary key.
# MAGIC   * Sync mode: Specifies how the synchronization pipeline updates the online table. Select one of Snapshot, Triggered, or Continuous.
# MAGIC   * Policy
# MAGIC     * Snapshot - The pipeline runs once to take a snapshot of the source table and copy it to the online table. Subsequent changes to the source table are automatically reflected in the online table by taking a new snapshot of the source and creating a new copy. The content of the online table is updated atomically.
# MAGIC     * Triggered - The pipeline runs once to create an initial snapshot copy of the source table in the online table. Unlike the Snapshot sync mode, when the online table is refreshed, only changes since the last pipeline execution are retrieved and applied to the online table. The incremental refresh can be manually triggered or automatically triggered according to a schedule.
# MAGIC     * Continuous - The pipeline runs continuously. Subsequent changes to the source table are incrementally applied to the online table in real time streaming mode. No manual refresh is necessary.
# MAGIC * When you are done, click Confirm. The online table page appears.
# MAGIC
# MAGIC The new online table is created under the catalog, schema, and name specified in the creation dialog. In Catalog Explorer, the online table is indicated by online table icon.

# COMMAND ----------

# MAGIC %md
# MAGIC #### OPTION 2: Use the Databricks SDK 
# MAGIC
# MAGIC The other alternative is the Databricks' python-sdk [AWS](https://docs.databricks.com/en/machine-learning/feature-store/online-tables.html#api-sdk) | [Azure](https://learn.microsoft.com/en-us/azure/databricks/machine-learning/feature-store/online-tables). Let's  first define the table specifications, then create the table.
# MAGIC
# MAGIC **🚨 Note:** The workspace must be enabled for using the SDK for creating and managing online tables. You can run following code blocks if your workspace is enabled for this feature (fill this [form](https://forms.gle/9jLZkpXnJF9ZcxtQA) to enable your workspace)

# COMMAND ----------

from databricks.sdk import WorkspaceClient

# Create workspace client [OPTIONAL: for publishing to online tables]
w = WorkspaceClient()

# COMMAND ----------

# DBTITLE 1,Drop any existing online table (optional)
from pprint import pprint

try:

  online_table_specs = w.online_tables.get(f"{catalog}.{db}.{feature_table_name}_online_table")
  
  # Drop existing online feature table
  w.online_tables.delete(f"{catalog}.{db}.{feature_table_name}_online_table")
  print(f"Dropping online feature table: {catalog}.{db}.{feature_table_name}_online_table")

except Exception as e:
  pprint(e)

# COMMAND ----------

from databricks.sdk.service.catalog import OnlineTableSpec, OnlineTableSpecTriggeredSchedulingPolicy

# Create an online table specification
churn_features_online_store_spec = OnlineTableSpec(
  primary_key_columns = [primary_key],
  timeseries_key = timestamp_col,
  source_table_full_name = f"{catalog}.{db}.{feature_table_name}",
  run_triggered=OnlineTableSpecTriggeredSchedulingPolicy.from_dict({'triggered': 'true'})
)

# COMMAND ----------

# Create the online table
w.online_tables.create(
  name=f"{catalog}.{db}.{feature_table_name}_online_table",
  spec=churn_features_online_store_spec
)

# COMMAND ----------

# DBTITLE 1,Check status of Online Table
from pprint import pprint

try:
  online_table_exist = w.online_tables.get(f"{catalog}.{db}.{feature_table_name}_online_table")
  pprint(online_table_exist)

except Exception as e:
  pprint(e)

# COMMAND ----------

# DBTITLE 1,Refresh Online Table (optional in case new data was added or offline table was dropped and re-created with new data))
# Trigger an online table refresh by calling the pipeline API
# w.pipelines.start_update(pipeline_id=online_table_spec.pipeline_id, full_refresh=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Review Featurization Logic for realtime/on-demand feature functions
# MAGIC
# MAGIC To recap, we have defined a function earlier to calculate the `avg_price_increase` feature on demand. Let's review the function here.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE FUNCTION avg_price_increase
# MAGIC ;

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC #Deploying the model for real-time inferences
# MAGIC
# MAGIC <img style="float: right; margin-left: 20px" width="600" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/churn_realtime_inference.gif" />
# MAGIC
# MAGIC Our marketing team also needs to run inferences in real-time using REST api (send a customer ID and get back the inference).
# MAGIC
# MAGIC While Feature store integration in real-time serving will come with Model Serving v2, you can deploy your Databricks Model in a single click.
# MAGIC
# MAGIC Open the Model page and click on "Serving". It'll start your model behind a REST endpoint and you can start sending your HTTP requests!

# COMMAND ----------

# MAGIC %md
# MAGIC ##
# MAGIC
# MAGIC TODO
# MAGIC
# MAGIC Explain the flow for promoting models with canary deployment, or A/B testing.
# MAGIC
# MAGIC The flow will involve 3 aliases:
# MAGIC - Production (for online serving - set to 100% traffic when this is the only version running, or 80% when doing "online testing")
# MAGIC - Champion (20% traffic when doing online testing)
# MAGIC - Challenger (This alias is used in the batch testing workflow. In our flow, the Challenger model never gets deployed in the serving endpoint.)
# MAGIC
# MAGIC To simplify packaging and presentation of the demo within dbdemo, we will use one catalog and schema for models of all aliases. (i.e. we are not keeping separate dev/qa/prod catalogs or schemas)
# MAGIC
# MAGIC -----
# MAGIC
# MAGIC Here, we are going to promote the Champion model to Production and deploy it for serving.
# MAGIC

# COMMAND ----------

endpoint_name = "dbdemos_mlops_advanced_churn"

model_version = client.get_model_version_by_alias(name=model_name, alias="Champion").version # Get champion version

# Promote Champion model to Production
client.set_registered_model_alias(
    name=model_name,
    alias="Production",
    version=model_version
)

print(f"Promoting {model_name} versions {model_version} from Champion to Production")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Enable model serving endpoint via API call
# MAGIC
# MAGIC To deploy a model for online serving, we need to provision a serving endpoint.
# MAGIC
# MAGIC Do it in the UI by following the steps below.
# MAGIC
# MAGIC 1. Click **Serving** in the left sidebar. If you don't see it, switch to the Machine Learning Persona ([AWS](https://docs.databricks.com/workspace/index.html#use-the-sidebar)|[Azure](https://docs.microsoft.com/azure/databricks//workspace/index#use-the-sidebar)).
# MAGIC 2. Enable serving for your model. See the Databricks documentation for details ([AWS](https://docs.databricks.com/machine-learning/model-inference/serverless/create-manage-serverless-endpoints.html)|[Azure](https://learn.microsoft.com/en-us/azure/databricks/machine-learning/model-inference/serverless/create-manage-serverless-endpoints)).
# MAGIC
# MAGIC
# MAGIC Alternatively, you can create the serving endpoint programatically. The code below automatically creates a model serving endpoint for you.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create/Update serving endpoint

# COMMAND ----------

# Parse model name from UC namespace
served_model_name =  model_name.split('.')[-1]

# COMMAND ----------

from databricks.sdk.service.serving import EndpointCoreConfigInput


endpoint_config_dict = {
    "served_models": [
        # Add models to be served to this list
        {
            "model_name": model_name,
            "model_version": model_version,
            "scale_to_zero_enabled": True,
            "workload_size": "Small",
            "instance_profile_arn": dbutils.secrets.get(scope="fieldeng", key="oneenv_ip_arn"),
        },
    ],
    "traffic_config": {
        "routes": [
            # Add versions of the model to be served to this list
            # Make sure that traffic_percentage adds up to 100 over all served models
            # Naming convention for served_model_name: <registered_model_name>-<model_version>
            {"served_model_name": f"{served_model_name}-{model_version}", "traffic_percentage": 100},
        ]
    },
    "auto_capture_config":{
        "catalog_name": catalog,
        "schema_name": schema,
        "table_name_prefix": "mlops_churn_served"
    }
}


endpoint_config = EndpointCoreConfigInput.from_dict(endpoint_config_dict)

# COMMAND ----------

from databricks.sdk.service.serving import EndpointTag

try:
  #w.serving_endpoints.create_and_wait(
  # Create and do not wait. Check readiness of endpoint in next cell.
  w.serving_endpoints.create(
    name=endpoint_name,
    config=endpoint_config,
    tags=[EndpointTag.from_dict({"key": "db_demos", "value": "mlops_advanced_churn"})]
  )
  
  print(f"Creating endpoint {endpoint_name} with models {model_name} version {model_version}")

except Exception as e:
  print(e)
  # TODO: When inference table already exists, "already exists" phrase is also present in exception message. Make sure that endpoint already exists case is properly handled. Otherwise, the following error message can cause a lot of confusion when debugging.

  if "already exists" in e.args[0]:
    print(f"Endpoint with name {endpoint_name} already exists, updating it with model {model_name}-{model_version}")
    print("--- TODO: Code to be implemented ---")

    # TO-DO:
    # w.serving_endpoints.update_config_and_wait(
    #   name=endpoint_name,
    #   end=TrafficConfig(routes=routes_handle),
    #   served_models=served_models_handle
    # )
  else:
    raise(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Wait/Verify that endpoint is ready
# MAGIC
# MAGIC Leave the following cell to run. It may take an hour or so for the endpoint to be ready.

# COMMAND ----------

from datetime import timedelta

# Wait for endpoint to be ready or finish updating
endpoint = w.serving_endpoints.wait_get_serving_endpoint_not_updating(endpoint_name, timeout=timedelta(minutes=120))

assert endpoint.state.config_update.value == "NOT_UPDATING" and endpoint.state.ready.value == "READY" , "Endpoint not ready or failed"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Send payloads via REST call
# MAGIC Example payload format expected by endpoint
# MAGIC ```
# MAGIC {
# MAGIC   "dataframe_records": [
# MAGIC     {"customer_id": "0002-ORFBO", "scoring_timestamp": "2024-02-05"},
# MAGIC     {"customer_id": "0003-MKNFE", "scoring_timestamp": "2024-02-05"}
# MAGIC   ]
# MAGIC }
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Get input example directly from mlfow model or hard-code
from mlflow.store.artifact.models_artifact_repo import ModelsArtifactRepository
from mlflow.models import Model

p = ModelsArtifactRepository(f"models:/{model_name}/{model_version}").download_artifacts("") 
input_example =  Model.load(p).load_input_example(p)

if input_example:
  # Only works if model NOT logged with feature store client
  dataframe_records =  [{input_example.to_dict(orient='records')}]

else:
  # Hard-code test-sample
  dataframe_records = [
    {primary_key: "0002-ORFBO", timestamp_col: "2024-02-05"},
    {primary_key: "0003-MKNFE", timestamp_col: "2024-02-05"}
  ]

# COMMAND ----------

# Wait 20 sec for endpoint to be available to avoid errors in the next command
import time
time.sleep(20)

# COMMAND ----------

# DBTITLE 1,Query endpoint
print("Churn inference:")
response = w.serving_endpoints.query(name=f"{endpoint_name}", dataframe_records=dataframe_records)
print(response.predictions)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Next: Monitor model performance [OPTIONAL]
# MAGIC
# MAGIC With inference tables availables we can create a monitor to track our ML's system behavior over time (feature drifts, prediction drift, label drift, model accuracy and metrics etc.)
# MAGIC
# MAGIC Next steps:
# MAGIC * [Create monitor for model performance]($./07_model_monitoring)
# MAGIC * [Automate model re-training]($./08_retrain_churn_automl)
