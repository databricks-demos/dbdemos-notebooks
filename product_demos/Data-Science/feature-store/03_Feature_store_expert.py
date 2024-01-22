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

# MAGIC %run ./_resources/00-init-expert $catalog="hive_metastore"

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
#delete_fss(database_name, ["user_features", "destination_features", "destination_location_features", "availability_features"])

from databricks import feature_store
fs = feature_store.FeatureStoreClient()

# Reuse the same features as the previous example 02_Feature_store_advanced
# For more details these functions are available under ./_resources/00-init-expert
user_features_df = create_user_features(spark.table('travel_purchase'))
fs.create_table(name=f"{database_name}.user_features",
                primary_keys=["user_id"], 
                timestamp_keys="ts", 
                df=user_features_df, 
                description="User Features")

destination_features_df = destination_features_fn(spark.table('travel_purchase'))
fs.create_table(name=f"{database_name}.destination_features", 
                primary_keys=["destination_id"], 
                timestamp_keys="ts", 
                df=destination_features_df, 
                description="Destination Popularity Features")


#Add the destination location dataset
destination_location = spark.table("destination_location")
fs.create_table(name=f"{database_name}.destination_location_features", 
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
  .load("/databricks-datasets/travel_recommendations_realtime/raw_travel_data/fs-demo_destination-availability_logs/json")
  .drop("_rescued_data")
  .withColumnRenamed("event_ts", "ts")
)

display(destination_availability_stream)

# COMMAND ----------

fs.create_table(
    name=f"{database_name}.availability_features", 
    primary_keys=["destination_id", "booking_date"],
    timestamp_keys=["ts"],
    schema=destination_availability_stream.schema,
    description="Destination Availability Features"
)

# Now write the data to the feature table in "merge" mode using a stream
fs.write_table(
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

# DBTITLE 1,Custom model wrapper for on demand feature computation
# Define the model class with on-demand computation model wrapper
class OnDemandComputationModelWrapper(mlflow.pyfunc.PythonModel):
  def __init__(self, model):
    self.model = model
    
  @staticmethod
  def add_live_features(df):
    """
    Leveraging spark pandas API, the same live transformation can be applied for live and batch dataframes.
    """
    import pandas as pd
    # Live deployment won't have reference to spark, so we test against pandas
    is_spark_df = not isinstance(df, pd.DataFrame)
    if is_spark_df:
      df = df.pandas_api()
    df = OnDemandComputationModelWrapper.add_time_features(df)
    df = OnDemandComputationModelWrapper.add_distance_features(df)
    if is_spark_df:
      return df.to_spark()
    return df

  @staticmethod
  def add_time_features(df):
    # Extract day of the week, day of the month, and hour from the ts column
    df['day_of_week'] = df['ts'].dt.dayofweek
    df['day_of_month'] = df['ts'].dt.day
    df['hour'] = df['ts'].dt.hour
    
    # Calculate sin and cos values for the day of the week, day of the month, and hour
    df['day_of_week_sin'] = np.sin(df['day_of_week'] * (2 * np.pi / 7))
    df['day_of_week_cos'] = np.cos(df['day_of_week'] * (2 * np.pi / 7))
    df['day_of_month_sin'] = np.sin(df['day_of_month'] * (2 * np.pi / 30))
    df['day_of_month_cos'] = np.cos(df['day_of_month'] * (2 * np.pi / 30))
    df['hour_sin'] = np.sin(df['hour'] * (2 * np.pi / 24))
    df['hour_cos'] = np.cos(df['hour'] * (2 * np.pi / 24))
    return df.drop(['day_of_week', 'day_of_month', 'hour'], axis=1)

  @staticmethod
  def add_distance_features(df):   
    df['distance'] = df.apply(lambda x: compute_hearth_distance(x["user_longitude"], x["user_latitude"], x["longitude"], x["latitude"]), axis=1)
    return df

  def predict(self, model_input: pd.DataFrame)->pd.DataFrame:
    # Supports input as datetime or string (force conversion to datetime)
    model_input["ts"] = pd.to_datetime(model_input['ts'].astype("str"))
    new_model_input = OnDemandComputationModelWrapper.add_live_features(model_input)
    return self.model.predict(new_model_input)

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
training_keys = spark.table('travel_purchase').select('ts', 'purchased', 'destination_id', 'user_id', 'user_latitude', 'user_longitude', 'booking_date')
training_df = training_keys.where("ts < '2022-11-23'")

test_df = training_keys.where("ts >= '2022-11-23'")


display(training_df.limit(5))

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ## Create the training set

# COMMAND ----------

# DBTITLE 1,Define Feature Lookups (for batch and streaming input features)
from databricks.feature_store.client import FeatureStoreClient
from databricks.feature_store.entities.feature_lookup import FeatureLookup

fs = FeatureStoreClient()

feature_lookups = [ # Grab all useful features from different feature store tables
  FeatureLookup(
      table_name=f"{database_name}.user_features", 
      lookup_key="user_id",
      timestamp_lookup_key="ts",
      feature_names=["mean_price_7d"]
  ),
  FeatureLookup(
      table_name=f"{database_name}.destination_features", 
      lookup_key="destination_id",
      timestamp_lookup_key="ts"
  ),
  FeatureLookup(
      table_name=f"{database_name}.destination_location_features",  
      lookup_key="destination_id",
      feature_names=["latitude", "longitude"]
  ),
  FeatureLookup(
      table_name=f"{database_name}.availability_features", 
      lookup_key=["destination_id", "booking_date"],
      timestamp_lookup_key="ts",
      feature_names=["availability"]
  )]

training_set = fs.create_training_set(
    training_df,
    feature_lookups=feature_lookups,
    exclude_columns=['user_id', 'destination_id', 'booking_date', 'clicked', 'price'],
    label='purchased'
)

# COMMAND ----------


training_set_df = training_set.load_df()
#Add our live features. Transformations are written using Pandas API and will use spark backend for the training steps.
#This will allow us to use the exact same code for real time inferences!
training_features_df = OnDemandComputationModelWrapper.add_live_features(training_set_df)
#Let's cache the training dataset for automl (to avoid recomputing it everytime)
training_features_df = training_features_df.cache()

display(training_features_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Use automl to build an ML model out of the box

# COMMAND ----------

import databricks.automl as db_automl

summary_cl = db_automl.classify(training_features_df, target_col="purchased", primary_metric="log_loss", timeout_minutes=5, experiment_dir = "/dbdemos/experiments/feature_store", time_col='ts')
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

# DBTITLE 1,Save best model in the registry & flag it as Production ready
# creating sample input to be logged (do not include the live features in the schema as they'll be computed within the model)
df_sample = training_set_df.limit(10).toPandas()
x_sample = df_sample.drop(columns=["purchased"])

# getting the model created by AutoML 
best_model = summary_cl.best_trial.load_model()
model = OnDemandComputationModelWrapper(best_model)

#Get the conda env from automl run
artifacts_path = mlflow.artifacts.download_artifacts(run_id=summary_cl.best_trial.mlflow_run_id)
env = mlflow.pyfunc.get_default_conda_env()
with open(artifacts_path+"model/requirements.txt", 'r') as f:
    env['dependencies'][-1]['pip'] = f.read().split('\n')

#Create a new run in the same experiment as our automl run.
with mlflow.start_run(run_name="best_fs_model", experiment_id=summary_cl.experiment.experiment_id) as run:
  #Use the feature store client to log our best model
  #TODO: need to add the conda env from the automl run
  fs.log_model(
              model=model, # object of your model
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
    
model_registered = mlflow.register_model(f"runs:/{run.info.run_id}/model", model_name_expert)

#Move the model in production
client = mlflow.tracking.MlflowClient()
print("registering model version "+model_registered.version+" as production model")
client.transition_model_version_stage(model_name_expert, model_registered.version, stage = "Production", archive_existing_versions=True)

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

scored_df = fs.score_batch(f"models:/{model_name_expert}/Production", test_df, result_type="boolean")
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

# MAGIC %md ## Publish feature tables to online store
# MAGIC 
# MAGIC By enabling online store, Databricks will automatically synchronize the data written to your feature store to the realtime backend (Key/Value store).
# MAGIC 
# MAGIC Databricks support different backend. In this demo, we'll show you how to setup 2 databases:
# MAGIC 
# MAGIC * AWS dynamoDB ([doc](https://docs.databricks.com/machine-learning/feature-store/online-feature-stores.html))
# MAGIC * Azure cosmosDB [doc](https://learn.microsoft.com/en-us/azure/databricks/machine-learning/feature-store/online-feature-stores)
# MAGIC 
# MAGIC 
# MAGIC **Important note for Azure users:** please make sure you have installed [Azure Cosmos DB Apache Spark 3 OLTP Connector for API for NoSQL](https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/sdk-java-spark-v3) (i.e. `com.azure.cosmos.spark:azure-cosmos-spark_3-2_2-12:4.17.2`) to your cluster before running this demo.

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Define your secrets
# MAGIC 
# MAGIC We need permissions to be able to read/write data to DynamoDB/Cosmos. The Feature Store leverage Databricks secrets. 
# MAGIC 
# MAGIC Before running the next cell, Use the Databricks CLI to add your keys. The Feature Store Spec will automatically retrive the secret based on the name you give. 
# MAGIC 
# MAGIC For this setup, we used the following:
# MAGIC 
# MAGIC **Read:** `databricks secrets create-scope --scope field-all-users-feature-store-example-read`
# MAGIC 
# MAGIC Access key ID for the IAM user with read-only access to the target online store:  `databricks secrets put --scope field-all-users-feature-store-example-read --key field-eng-access-key-id`
# MAGIC 
# MAGIC Secret access key for the IAM user with read-only access to the target online store:  `databricks secrets put --scope field-all-users-feature-store-example-read --key field-eng-secret-access-key`
# MAGIC 
# MAGIC **Write:** `databricks secrets create-scope --scope field-all-users-feature-store-example-write`
# MAGIC 
# MAGIC Access key ID for the IAM user with read-write access to the target online store: `databricks secrets put --scope field-all-users-feature-store-example-write --key field-eng-access-key-id`
# MAGIC 
# MAGIC Secret access key for the IAM user with read-write access to the target online store: `databricks secrets put --scope field-all-users-feature-store-example-write --key field-eng-secret-access-key`
# MAGIC 
# MAGIC Read the documentation [AWS](https://docs.databricks.com/machine-learning/feature-store/fs-authentication.html) | [Azure](https://learn.microsoft.com/en-us/azure/databricks/machine-learning/feature-store/fs-authentication) for more details on the required permissions.

# COMMAND ----------

# DBTITLE 1,Define your read/write secret using Databricks CLI
# Databricks leverage secrets to read/write data in our online store. 
# For our demo, we'll use the following secret scope:
read_secret_prefix="field-all-users-feature-store-example-read/field-eng"
write_secret_prefix="field-all-users-feature-store-example-write/field-eng"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Publish the feature store with online table specs

# COMMAND ----------

# DBTITLE 1,Creates the online table Spec & publish them, depending on your cloud
from databricks.feature_store import FeatureStoreClient
from databricks.feature_store.online_store_spec import AzureCosmosDBSpec, AmazonDynamoDBSpec
fs = FeatureStoreClient()


def delete_online_store_table(table_name):
  if get_cloud_name() == "aws":
    delete_dynamodb_table(online_table_name)
  elif get_cloud_name() == "azure":
    delete_cosmosdb_container(online_table_name)
  else:
    raise Exception(f"cloud not supported : {get_cloud_name()}")
    

cloud_name = get_cloud_name()
def create_realtime_table(fs_table_name, streaming = False, checkpoint_location = None, force_delete = False):
  online_table_name = "feature_store_dbdemos_"+fs_table_name[fs_table_name.rfind('.')+1:]
  print(f"creating and publishing feature table {fs_table_name} in realtime table {online_table_name} on {cloud_name}")
  
  #Force online store the deletion (usefull if you have conflict error)
  if force_delete:
    delete_online_store_table(online_table_name)
  
  if cloud_name == "azure":
    online_store_spec = AzureCosmosDBSpec(
      account_uri=account_uri,
      write_secret_prefix = write_secret_prefix,
      read_secret_prefix = read_secret_prefix,
      database_name = "dbdemos_fs",
      container_name = container_name)
  elif cloud_name == "aws":
    online_store_spec = AmazonDynamoDBSpec(
      region="us-west-2",
      write_secret_prefix=write_secret_prefix, 
      read_secret_prefix=read_secret_prefix,
      table_name = online_table_name)
  else:
    raise Exception('GCP not supported in this demo')
  
  #Publish the feature table content to our online store spec
  fs.publish_table(fs_table_name, online_store_spec, streaming = streaming, checkpoint_location = checkpoint_location) 


create_realtime_table(f"{database_name}.destination_location_features")
create_realtime_table(f"{database_name}.destination_features")
create_realtime_table(f"{database_name}.user_features")
#Keep availability in streaming
create_realtime_table(f"{database_name}.availability_features", 
                      streaming = True, 
                      checkpoint_location = current_user_location+"/availability_checkpoint_online")

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

client = mlflow.tracking.MlflowClient()
latest_model = client.get_latest_versions(model_name_expert, stages=["Production"])[0]

#See the 00-init-expert notebook for the endpoint API details
serving_client = EndpointApiClient()

#Start the endpoint using the REST API (you can do it using the UI directly)
serving_client.create_endpoint_if_not_exists("dbdemos_feature_store_endpoint", model_name=model_name_expert, model_version = latest_model.version, workload_size="Small", scale_to_zero_enabled=True, wait_start = True)

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

dataset = {
  "dataframe_records": [
    {"user_id": 4, "ts": "2022-11-23 00:28:40.053000", "booking_date": "2022-11-23", "destination_id": 16, "user_latitude": 40.71277, "user_longitude": -74.005974}, 
    {"user_id": 39, "ts": "2022-11-23 00:30:29.345000", "booking_date": "2022-11-23", "destination_id": 1, "user_latitude": 37.77493, "user_longitude": -122.41942}
  ]
}

endpoint_url = f"{serving_client.base_url}/realtime-inference/dbdemos_feature_store_endpoint/invocations"
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
