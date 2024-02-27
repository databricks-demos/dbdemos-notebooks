# Databricks notebook source
# MAGIC %md 
# MAGIC
# MAGIC # Feature store - full example for Travel recommendation
# MAGIC
# MAGIC This notebook will illustrate the full capabilities of the feature store to provide recommendation for a Travel website and increase or conversion rate.
# MAGIC
# MAGIC If you don't know about feature store yet, we recommand you start with the first version to cover the basics.
# MAGIC
# MAGIC We'll go in details and introduce:
# MAGIC
# MAGIC * Streaming feature store tables, to refresh your data in near realtime
# MAGIC * Live feature computation, reusing the same code for training and inference with the Pandas On Spark APIs (current booking time & distance to location)
# MAGIC * Point in time lookup with multiple feature table
# MAGIC * Automl to bootstrap model creation
# MAGIC
# MAGIC In addition, we'll see how we can perform realtime inference:
# MAGIC
# MAGIC * Create online backed for the feature store table (dynamoDB / cosmosDB)
# MAGIC * Deploy the model using realtime serverless Model Serving fetching features in the online store
# MAGIC * Send realtime REST queries for live inference.
# MAGIC
# MAGIC *Note: For more detail on this notebook, you can read the [Databricks blog post](https://www.databricks.com/blog/2023/02/16/best-practices-realtime-feature-computation-databricks.html) .*

# COMMAND ----------

# MAGIC %run ./_resources/00-init-expert $catalog="feat_eng"

# COMMAND ----------

# MAGIC %md ## New Streaming dataset
# MAGIC
# MAGIC For this full example demo, we will add a Streaming dataset containing the destination availability that we'll update live. We'll use Spark Streming to ingest the data live. 
# MAGIC
# MAGIC *Note: the streaming flow would typically connect to a message queue like kafka. For this demo we'll consider that our data is updated as file in a blob storage* 

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # 1: Create the feature tables
# MAGIC
# MAGIC The first step is to create our feature store tables. We'add a new datasource that we'll consume in streaming, making sure our Feature Table is refreshed in near realtime.
# MAGIC
# MAGIC In addition, we'll compute the "on-demande" feature (distance between the user and a destination, booking time) using the pandas API during training, this will allow us to use the same code for realtime inferences.
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/product/feature_store/feature_store_expert_training.png" width="1200px"/>

# COMMAND ----------

# MAGIC %md ## Compute batch features
# MAGIC
# MAGIC Calculate the aggregated features from the vacation purchase logs for destination and users. The destination features include popularity features such as impressions, clicks, and pricing features like price at the time of booking. The user features capture the user profile information such as past purchased price. Because the booking data does not change very often, it can be computed once per day in batch.

# COMMAND ----------

# DBTITLE 1,Review our silver data
# MAGIC %sql SELECT * FROM travel_purchase 

# COMMAND ----------

# DBTITLE 1,Destination metadata
# MAGIC %sql SELECT * FROM destination_location

# COMMAND ----------

# DBTITLE 1,Compute features & create our 3 Feature Table
#Delete potential existing tables to reset all the demo
# delete_fss(database_name, ["user_features", "destination_features", "destination_location_features", "availability_features"])

from databricks.feature_engineering import FeatureEngineeringClient
fe = FeatureEngineeringClient()

# Reuse the same features as the previous example 02_Feature_store_advanced
# For more details these functions are available under ./_resources/00-init-expert
user_features_df = create_user_features(spark.table('travel_purchase'))
fe.create_table(name=f"{database_name}.user_features",
                primary_keys=["user_id", "ts"], 
                timestamp_keys="ts", 
                df=user_features_df, 
                description="User Features")

destination_features_df = destination_features_fn(spark.table('travel_purchase'))
fe.create_table(name=f"{database_name}.destination_features", 
                primary_keys=["destination_id", "ts"], 
                timestamp_keys="ts", 
                df=destination_features_df, 
                description="Destination Popularity Features")


#Add the destination location dataset
destination_location = spark.table("destination_location")
fe.create_table(name=f"{database_name}.destination_location_features", 
                primary_keys="destination_id", 
                df=destination_location, 
                description="Destination location features.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Compute streaming features
# MAGIC
# MAGIC Availability of the destination can hugely affect the prices. Availability can change frequently especially around the holidays or long weekends during busy season. This data has a freshness requirement of every few minutes, so we use Spark structured streaming to ensure data is fresh when doing model prediction. 

# COMMAND ----------

# MAGIC %md <img src="https://docs.databricks.com/_static/images/machine-learning/feature-store/realtime/streaming.png"/>

# COMMAND ----------

destination_availability_stream = (
  spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "json") #Could be "kafka" to consume from a message queue
  .option("cloudFiles.inferSchema", "true")
  .option("cloudFiles.inferColumnTypes", "true")
  .option("cloudFiles.schemaHints", "event_ts timestamp, booking_date date, destination_id int")
  .option("cloudFiles.schemaLocation", current_user_location+"/availability_schema")
  .option("cloudFiles.maxFilesPerTrigger", 100) #Simulate streaming
  .option("cloudFiles.schemaEvolutionMode", "none")
  .load("/databricks-datasets/travel_recommendations_realtime/raw_travel_data/fs-demo_destination-availability_logs/json")
  .drop("_rescued_data")
  .withColumnRenamed("event_ts", "ts")
)

display(destination_availability_stream)

# COMMAND ----------

fe.create_table(
    name=f"{database_name}.availability_features", 
    primary_keys=["destination_id", "booking_date", "ts"],
    timestamp_keys=["ts"],
    schema=destination_availability_stream.schema,
    description="Destination Availability Features"
)

# Now write the data to the feature table in "merge" mode using a stream
fe.write_table(
    name=f"{database_name}.availability_features", 
    df=destination_availability_stream,
    mode="merge",
    checkpoint_location=current_user_location+"/availability_checkpoint",
    trigger={'processingTime': '1 minute'} #Refresh the feature store table with availability every minute
)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## Compute on-demand live features
# MAGIC
# MAGIC User location is a context feature that is captured at the time of the query. This data is not known in advance, hence the derived feature. 
# MAGIC
# MAGIC For example, user distance from destination can only be computed in realtime at the prediction time. We'll use a custom MLflow model `PythonModel` to ship this transformation as part of the model we save.
# MAGIC
# MAGIC For training we'll leverage the spark Pandas API to compute these feature at scale on the entire dataset before training our model.
# MAGIC
# MAGIC Because it's shipped within the model, the same code will be used at inference time, offering a garantee that we compute the feature the same way, and adding flexibility while increasing model version.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE FUNCTION IF NOT EXISTS feat_eng.dbdemos_fs_travel_shared.extract_week_udf(datetime TIMESTAMP)
# MAGIC RETURNS INT
# MAGIC LANGUAGE PYTHON
# MAGIC COMMENT 'Extracts week number from datetime'
# MAGIC AS $$
# MAGIC from datetime import datetime as dt
# MAGIC
# MAGIC def extract_week(datetime):
# MAGIC     return datetime.isocalendar()[1] if datetime else None
# MAGIC
# MAGIC return extract_week(datetime)
# MAGIC $$

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE FUNCTION IF NOT EXISTS feat_eng.dbdemos_fs_travel_shared.extract_month_udf(datetime TIMESTAMP)
# MAGIC RETURNS INT
# MAGIC LANGUAGE PYTHON
# MAGIC COMMENT 'Extracts month from datetime'
# MAGIC AS $$
# MAGIC from datetime import datetime as dt
# MAGIC
# MAGIC def extract_month(datetime):
# MAGIC     return datetime.month if datetime else None
# MAGIC
# MAGIC return extract_month(datetime)
# MAGIC $$
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE FUNCTION IF NOT EXISTS feat_eng.dbdemos_fs_travel_shared.extract_month_udf2(datetime TIMESTAMP)
# MAGIC RETURNS INT
# MAGIC LANGUAGE PYTHON
# MAGIC COMMENT 'Extracts month from datetime'
# MAGIC AS $$
# MAGIC from datetime import datetime as dt
# MAGIC
# MAGIC def extract_month(datetime):
# MAGIC     return datetime.month if datetime else None
# MAGIC
# MAGIC return extract_month(datetime)
# MAGIC $$

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE FUNCTION IF NOT EXISTS feat_eng.dbdemos_fs_travel_shared.extract_month_udf3(datetime TIMESTAMP)
# MAGIC RETURNS INT
# MAGIC LANGUAGE PYTHON
# MAGIC COMMENT 'Extracts month from datetime'
# MAGIC AS $$
# MAGIC from datetime import datetime as dt
# MAGIC
# MAGIC def extract_month(datetime):
# MAGIC     return datetime.month if datetime else None
# MAGIC
# MAGIC return extract_month(datetime)
# MAGIC $$

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE FUNCTION IF NOT EXISTS feat_eng.dbdemos_fs_travel_shared.extract_month_udf4(datetime TIMESTAMP)
# MAGIC RETURNS INT
# MAGIC LANGUAGE PYTHON
# MAGIC COMMENT 'Extracts month from datetime'
# MAGIC AS $$
# MAGIC from datetime import datetime as dt
# MAGIC
# MAGIC def extract_month(datetime):
# MAGIC     return datetime.month if datetime else None
# MAGIC
# MAGIC return extract_month(datetime)
# MAGIC $$

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT ts, feat_eng.dbdemos_fs_travel_shared.extract_week_udf(ts) AS week, feat_eng.dbdemos_fs_travel_shared.extract_month_udf(ts) AS month FROM feat_eng.dbdemos_fs_travel_shared.destination_features

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE FUNCTION feat_eng.dbdemos_fs_travel_shared.distance_udf(lat1 FLOAT, lon1 FLOAT, lat2 FLOAT, lon2 FLOAT)
# MAGIC RETURNS FLOAT
# MAGIC LANGUAGE PYTHON
# MAGIC COMMENT 'Calculate hearth distance from latitude and longitude'
# MAGIC AS $$
# MAGIC import numpy as np
# MAGIC
# MAGIC def compute_hearth_distance(lat1, lon1, lat2, lon2):
# MAGIC   dlat, dlon = np.radians(lat2 - lat1), np.radians(lon2 - lon1)
# MAGIC   a = np.sin(dlat/2)**2 + np.cos(np.radians(lat1)) * np.cos(np.radians(lat2)) * np.sin(dlon/2)**2
# MAGIC   return 2 * 6371 * np.arcsin(np.sqrt(a))
# MAGIC
# MAGIC return compute_hearth_distance(lat1, lon1, lat2, lon2)
# MAGIC $$

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *, feat_eng.dbdemos_fs_travel_shared.distance_udf(user_latitude, user_longitude, latitude, longitude) AS hearth_distance
# MAGIC FROM feat_eng.dbdemos_fs_travel_shared.destination_location_features
# MAGIC JOIN feat_eng.dbdemos_fs_travel_shared.destination_features
# MAGIC ON destination_location_features.destination_id = destination_features.destination_id
# MAGIC JOIN feat_eng.dbdemos_fs_travel_shared.user_features
# MAGIC ON user_features.ts = destination_features.ts
# MAGIC

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC # 2: Train a custom model with batch, on-demand and streaming features
# MAGIC
# MAGIC The following uses all the features created above to train a ranking model.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get ground-truth training labels and key + timestamp

# COMMAND ----------

# Random split to define a training and inference set
training_keys = spark.table('feat_eng.dbdemos_fs_travel_shared.travel_purchase').select('ts', 'purchased', 'destination_id', 'user_id', 'user_latitude', 'user_longitude', 'booking_date')
training_df = training_keys.where("ts < '2022-11-23'")

test_df = training_keys.where("ts >= '2022-11-23'")

display(training_df.limit(5))

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## Create the training set

# COMMAND ----------

# DBTITLE 1,Define Feature Lookups (for batch and streaming input features)
from databricks.feature_engineering import FeatureEngineeringClient
from databricks.feature_engineering.entities.feature_function import FeatureFunction
from databricks.feature_engineering.entities.feature_lookup import FeatureLookup

fe = FeatureEngineeringClient()

feature_lookups = [ # Grab all useful features from different feature store tables
  FeatureLookup(
      table_name="user_features", 
      lookup_key="user_id",
      timestamp_lookup_key="ts",
      feature_names=["mean_price_7d"]
  ),
  FeatureLookup(
      table_name="destination_features", 
      lookup_key="destination_id",
      timestamp_lookup_key="ts"
  ),
  FeatureLookup(
      table_name="destination_location_features",  
      lookup_key="destination_id",
      feature_names=["latitude", "longitude"]
  ),
  FeatureLookup(
      table_name="availability_features", 
      lookup_key=["destination_id", "booking_date"],
      timestamp_lookup_key="ts",
      feature_names=["availability"]
  ),
#   FeatureFunction(
#       udf_name="distance_udf",
#       input_bindings={"lat1": "user_latitude", "lon1": "user_longitude", "lat2": "latitude", "lon2": "longitude"},
#       output_name="distance_udf_output"
#   ),
  FeatureFunction(
      udf_name="extract_week_udf",
      input_bindings={"datetime": "ts"},
      output_name="extract_week_udf_output"
  ),
  FeatureFunction(
      udf_name="extract_month_udf",
      input_bindings={"datetime": "ts"},
      output_name="extract_month_udf_output"
  ),]

training_set = fe.create_training_set(
    df=training_df,
    feature_lookups=feature_lookups,
    exclude_columns=['user_id', 'destination_id', 'booking_date', 'clicked', 'price'],
    label='purchased'
)

# COMMAND ----------


training_set_df = training_set.load_df()
#Add our live features. Transformations are written using Pandas API and will use spark backend for the training steps.
#This will allow us to use the exact same code for real time inferences!
# training_features_df = OnDemandCovmputationModelWrapper.add_live_features(training_set_df)
#Let's cache the training dataset for automl (to avoid recomputing it everytime)
training_features_df = training_set_df.cache()

display(training_features_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Use automl to build an ML model out of the box

# COMMAND ----------

import databricks.automl as db_automl

summary_cl = db_automl.classify(training_features_df, target_col="purchased", primary_metric="log_loss", timeout_minutes=10, experiment_dir = "/dbdemos/experiments/feature_store", time_col='ts')
print(f"Best run id: {summary_cl.best_trial.mlflow_run_id}")

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Save our best model to MLflow registry
# MAGIC
# MAGIC Next, we'll get Automl best model and add it to our registry. Because we the feature store to keep track of our model & features, we'll log the best model as a new run using the `FeatureStoreClient.log_model()` function.
# MAGIC
# MAGIC Because our model need live features, we'll wrap our best model with `OnDemandComputationModelWrapper`
# MAGIC
# MAGIC Because we re-define the `.predict()` function, the wrapper will automatically add our live feature depending on the input.

# COMMAND ----------

import mlflow
import os 

# creating sample input to be logged (do not include the live features in the schema as they'll be computed within the model)
df_sample = training_set_df.limit(10).toPandas()
x_sample = df_sample.drop(columns=["purchased"])

# getting the model created by AutoML 
best_model = summary_cl.best_trial.load_model()
# model = OnDemandComputationModelWrapper(best_model)

#Get the conda env from automl run
artifacts_path = mlflow.artifacts.download_artifacts(run_id=summary_cl.best_trial.mlflow_run_id)
env = mlflow.pyfunc.get_default_conda_env()
with open(artifacts_path+"model/requirements.txt", 'r') as f:
    env['dependencies'][-1]['pip'] = f.read().split('\n')

#Create a new run in the same experiment as our automl run.
with mlflow.start_run(run_name="best_fs_model", experiment_id=summary_cl.experiment.experiment_id) as run:
  #Use the feature store client to log our best model
  #TODO: need to add the conda env from the automl run
  fe.log_model(
              model=best_model, # object of your model
              artifact_path="model", #name of the Artifact under MlFlow
              flavor=mlflow.sklearn, # flavour of the model (our LightGBM model has a SkLearn Flavour)
              training_set=training_set, # training set you used to train your model with AutoML
              input_example=x_sample, # Dataset example (Pandas dataframe)
              conda_env=env)

  #Copy automl images & params to our FS run
  for item in os.listdir(artifacts_path):
    if item.endswith(".png"):
      mlflow.log_artifact(artifacts_path+item)
  mlflow.log_metrics(summary_cl.best_trial.metrics)
  mlflow.log_params(summary_cl.best_trial.params)
  mlflow.log_param("automl_run_id", summary_cl.best_trial.mlflow_run_id)
  mlflow.set_tag(key='feature_store', value='expert_demo')

# COMMAND ----------

import mlflow
model_name_expert = "dbdemos_fs_travel_shared_expert_model"
model_registered = mlflow.register_model(f"runs:/{run.info.run_id}/model", model_name_expert)

# COMMAND ----------

# DBTITLE 1,Save best model in the registry & flag it as Production ready
#Move the model in production by setting the "Production" alias
client = mlflow.tracking.MlflowClient()
print("registering model version "+model_registered.version+" as production model")

destination_alias = "Production"
client.set_registered_model_alias(model_name_expert, destination_alias, version=model_registered.version)

# COMMAND ----------

# MAGIC %md
# MAGIC Our model is ready! you can open [the dbdemos_fs_travel_shared_model](/#mlflow/models/dbdemos_fs_travel_shared_model) to review it.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Batch score test set
# MAGIC
# MAGIC Let's make sure our model is working as expected and try to score our test dataset

# COMMAND ----------

# scored_df = fe.score_batch(model_uri=f"models:/{model_name_expert}@production", df=test_df, result_type="boolean")
scored_df = fe.score_batch(model_uri=f"models:/dbdemos_fs_travel_shared_expert_model@production", df=test_df, result_type="boolean")
display(scored_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test Accuracy

# COMMAND ----------

from sklearn.metrics import accuracy_score

# simply convert the original probability predictions to true or false
pd_scoring = scored_df.select("purchased", "prediction").toPandas()
print("Accuracy: ", accuracy_score(pd_scoring["purchased"], pd_scoring["prediction"]))

# COMMAND ----------

# MAGIC %md 
# MAGIC # 3: Real time serving and inference
# MAGIC
# MAGIC We're now going to deploy our model supporting real time inference.
# MAGIC
# MAGIC To provide inference with ms response time, we need to be able to lookup the features for a single user or destination with low latencies.
# MAGIC
# MAGIC To do that, we'll deploy online store (K/V store like Mysql, dynamoDB, CosmoDB...) and Databricks Feature Store will automatically synchronize them with the Feature Store table.
# MAGIC
# MAGIC During inference, the engine will automatically use the same primary keys to do the timestamp.
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/product/feature_store/feature_store_expert_inference.png" width="1200px" />

# COMMAND ----------

# MAGIC %md ## Publish feature tables as Databricks-managed online tables
# MAGIC
# MAGIC By publishing our tables to a Databricks-managed online table, Databricks will automatically synchronize the data written to your feature store to the realtime backend.
# MAGIC
# MAGIC Apart from Databricks-managed online tables, Databricks also supports different third-party backends. You can find more information about integrating Databricks feature tables with third-party online stores in the links below.
# MAGIC
# MAGIC * AWS dynamoDB ([doc](https://docs.databricks.com/machine-learning/feature-store/online-feature-stores.html))
# MAGIC * Azure cosmosDB [doc](https://learn.microsoft.com/en-us/azure/databricks/machine-learning/feature-store/online-feature-stores)
# MAGIC
# MAGIC
# MAGIC **Important note for Azure users:** please make sure you have installed [Azure Cosmos DB Apache Spark 3 OLTP Connector for API for NoSQL](https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/sdk-java-spark-v3) (i.e. `com.azure.cosmos.spark:azure-cosmos-spark_3-2_2-12:4.17.2`) to your cluster before running this demo.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Publish the feature store with online table specs

# COMMAND ----------

# MAGIC %md ## Deploy Serverless Model serving Endpoint
# MAGIC
# MAGIC We're now ready to deploy our model using Databricks Model Serving endpoints. This will provide a REST API to serve our model in realtime.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Enable model inference via the UI
# MAGIC
# MAGIC After calling `log_model`, a new version of the model is saved. To provision a serving endpoint, follow the steps below.
# MAGIC
# MAGIC 1. Within the Machine Learning menu, click **Serving** in the left sidebar. 
# MAGIC 2. Create a new endpoint, select the most recent model version and start the serverless model serving
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/product/feature_store/notebook4_ff_model_serving_screenshot2_1.png" alt="step12" width="1500"/>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Set up & start a Serverless model serving endpoint using the API:
# MAGIC
# MAGIC We will use the API to programatically start the endpoint:

# COMMAND ----------

import time
import requests

class EndpointApiClient:
    def __init__(self):
        self.base_url =dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get()
        self.token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
        self.headers = {"Authorization": f"Bearer {self.token}", "Content-Type": "application/json"}

    def create_inference_endpoint(self, endpoint_name, served_models):
        data = {"name": endpoint_name, "config": {"served_models": served_models}}
        return self._post("api/2.0/serving-endpoints", data)

    def get_inference_endpoint(self, endpoint_name):
        return self._get(f"api/2.0/serving-endpoints/{endpoint_name}", allow_error=True)
      
      
    def inference_endpoint_exists(self, endpoint_name):
      ep = self.get_inference_endpoint(endpoint_name)
      if 'error_code' in ep and ep['error_code'] == 'RESOURCE_DOES_NOT_EXIST':
          return False
      if 'error_code' in ep and ep['error_code'] != 'RESOURCE_DOES_NOT_EXIST':
          raise Exception(f"enpoint exists ? {ep}")
      return True

    def create_endpoint_if_not_exists(self, endpoint_name, model_name, model_version, workload_size, scale_to_zero_enabled=True, wait_start=True):
      models = [{
            "model_name": model_name,
            "model_version": model_version,
            "workload_size": workload_size,
            "scale_to_zero_enabled": scale_to_zero_enabled,
      }]
      if not self.inference_endpoint_exists(endpoint_name):
        r = self.create_inference_endpoint(endpoint_name, models)
      #Make sure we have the proper version deployed
      else:
        ep = self.get_inference_endpoint(endpoint_name)
        if 'pending_config' in ep:
            self.wait_endpoint_start(endpoint_name)
            ep = self.get_inference_endpoint(endpoint_name)
        if 'pending_config' in ep:
            model_deployed = ep['pending_config']['served_models'][0]
            print(f"Error with the model deployed: {model_deployed} - state {ep['state']}")
        else:
            model_deployed = ep['config']['served_models'][0]
        if model_deployed['model_version'] != model_version:
          print(f"Current model is version {model_deployed['model_version']}. Updating to {model_version}...")
          u = self.update_model_endpoint(endpoint_name, {"served_models": models})
      if wait_start:
        self.wait_endpoint_start(endpoint_name)
      
      
    def list_inference_endpoints(self):
        return self._get("api/2.0/serving-endpoints")

    def update_model_endpoint(self, endpoint_name, conf):
        return self._put(f"api/2.0/serving-endpoints/{endpoint_name}/config", conf)

    def delete_inference_endpoint(self, endpoint_name):
        return self._delete(f"api/2.0/serving-endpoints/{endpoint_name}")

    def wait_endpoint_start(self, endpoint_name):
      i = 0
      while self.get_inference_endpoint(endpoint_name)['state']['config_update'] == "IN_PROGRESS" and i < 500:
        print("waiting for endpoint to build model image and start")
        time.sleep(30)
        i += 1
      
    # Making predictions

    def query_inference_endpoint(self, endpoint_name, data):
        return self._post(f"realtime-inference/{endpoint_name}/invocations", data)

    # Debugging

    def get_served_model_build_logs(self, endpoint_name, served_model_name):
        return self._get(
            f"api/2.0/serving-endpoints/{endpoint_name}/served-models/{served_model_name}/build-logs"
        )

    def get_served_model_server_logs(self, endpoint_name, served_model_name):
        return self._get(
            f"api/2.0/serving-endpoints/{endpoint_name}/served-models/{served_model_name}/logs"
        )

    def get_inference_endpoint_events(self, endpoint_name):
        return self._get(f"api/2.0/serving-endpoints/{endpoint_name}/events")

    def _get(self, uri, data = {}, allow_error = False):
        r = requests.get(f"{self.base_url}/{uri}", params=data, headers=self.headers)
        return self._process(r, allow_error)

    def _post(self, uri, data = {}, allow_error = False):
        return self._process(requests.post(f"{self.base_url}/{uri}", json=data, headers=self.headers), allow_error)

    def _put(self, uri, data = {}, allow_error = False):
        return self._process(requests.put(f"{self.base_url}/{uri}", json=data, headers=self.headers), allow_error)

    def _delete(self, uri, data = {}, allow_error = False):
        return self._process(requests.delete(f"{self.base_url}/{uri}", json=data, headers=self.headers), allow_error)

    def _process(self, r, allow_error = False):
      if r.status_code == 500 or r.status_code == 403 or not allow_error:
        print(r.text)
        r.raise_for_status()
      return r.json()

# COMMAND ----------

catalog_name = "feat_eng"

# COMMAND ----------

# Use the MlflowClient to get a list of all versions for the registered model in Unity Catalog

all_versions = client.search_model_versions(f"name='{catalog_name}.{database_name}.{model_name_expert}'")
# # Sort the list of versions by version number and get the latest version
latest_version = max([int(v.version) for v in all_versions])
# # Use the MlflowClient to get the latest version of the registered model in Unity Catalog
latest_model = client.get_model_version(f"{catalog_name}.{database_name}.{model_name_expert}", str(latest_version))

#See the 00-init-expert notebook for the endpoint API details
serving_client = EndpointApiClient()

#Start the endpoint using the REST API (you can do it using the UI directly)
serving_client.create_endpoint_if_not_exists("dbdemos_feature_store_endpoint", model_name="feat_eng.dbdemos_fs_travel_shared.dbdemos_fs_travel_shared_expert_model", model_version = latest_model.version, workload_size="Small", scale_to_zero_enabled=True, wait_start = True)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC Your endpoint was created. Open the [Endpoint UI](/#mlflow/endpoints/dbdemos_feature_store_endpoint) to see the creation logs.
# MAGIC
# MAGIC
# MAGIC ### Send payloads via REST call
# MAGIC
# MAGIC With Databricks's Serverless Model Serving, the endpoint takes a different score format.
# MAGIC You can see that users in New York can see high scores for Florida, whereas users in California can see high scores for Hawaii.
# MAGIC
# MAGIC ```
# MAGIC {
# MAGIC   "dataframe_records": [
# MAGIC     {"user_id": 4, "ts": "2022-11-23 00:28:40.053000", "booking_date": "2022-11-23", "destination_id": 16, "user_latitude": 40.71277, "user_longitude": -74.005974}, 
# MAGIC     {"user_id": 39, "ts": "2022-11-23 00:30:29.345000", "booking_date": "2022-11-23", "destination_id": 1, "user_latitude": 37.77493, "user_longitude": -122.41942}
# MAGIC   ]
# MAGIC }
# MAGIC ```

# COMMAND ----------

import timeit
import requests

dataset = {
  "dataframe_records": [
    {"user_id": 4, "ts": "2022-11-23 00:28:40.053000", "booking_date": "2022-11-23", "destination_id": 16, "user_latitude": 40.71277, "user_longitude": -74.005974}, 
    {"user_id": 39, "ts": "2022-11-23 00:30:29.345000", "booking_date": "2022-11-23", "destination_id": 1, "user_latitude": 37.77493, "user_longitude": -122.41942}
  ]
}

# endpoint_url = f"{serving_client.base_url}/serving-endpoints/dbdemos_fs_travel_shared_expert_model/invocations"

endpoint_url = "https://e2-dogfood-brickstore-mt-bug-bash.staging.cloud.databricks.com/serving-endpoints/dbdemos_fs_travel_shared_expert_model/invocations"

print(f"Sending requests to {endpoint_url}")
for i in range(3):
    starting_time = timeit.default_timer()
    inferences = requests.post(endpoint_url, json=dataset, headers=serving_client.headers).json()
    print(f"Inference time, end 2 end :{round((timeit.default_timer() - starting_time)*1000)}ms")
    print(inferences)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Sending inference using the UI
# MAGIC
# MAGIC Once your serving endpoint is ready, your previous cell's `score_model` code should give you the model inference result. 
# MAGIC
# MAGIC You can also directly use the model serving UI to try your realtime inference. Click on "Query endpoint" (upper right corner) to test your model. 
# MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/product/feature_store/notebook4_ff_model_serving_screenshot2_3.png" alt="step12" width="1500"/>

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Conclusion
# MAGIC
# MAGIC In this series of demos you've learned how to use **Databricks Feature Store** in 3 different manner:
# MAGIC - `batch (offline Feature Store)`
# MAGIC - `streaming (offline Feature Store)`
# MAGIC - `real-time (online Feature Store)`
# MAGIC
# MAGIC The use of the each from the above would depend whether your organization requires scheduled batch jobs, near real-time streaming or real-time on the fly computations. 
# MAGIC
# MAGIC To summarize, if you required to have a real-time feature computations, then figure out what type of data you have, data freshness and latency requirements and make sure to:
# MAGIC
# MAGIC - Map your data to batch, streaming, and on-demand computational architecture based on data freshness requirements.
# MAGIC - Use spark structured streaming to stream the computation to offline store and online store
# MAGIC - Use on-demand computation with MLflow pyfunc
# MAGIC - Use Databricks Serverless realtime inference to perform low-latency predictions on your model

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## Make sure we stop all existing streams

# COMMAND ----------

stop_all_streams()
