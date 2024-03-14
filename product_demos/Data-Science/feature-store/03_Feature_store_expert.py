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
# MAGIC * Create online backed for the feature store table
# MAGIC * Create online functions to add additional, realtime feature (distance and date)
# MAGIC * Deploy the model using realtime serverless Model Serving fetching features in the online store
# MAGIC * Send realtime REST queries for live inference.
# MAGIC
# MAGIC *Note: For more detail on this notebook, you can read the [Databricks blog post](https://www.databricks.com/blog/2023/02/16/best-practices-realtime-feature-computation-databricks.html) .*

# COMMAND ----------

# MAGIC %pip install databricks-feature-engineering==0.2.0 databricks-sdk==0.20.0
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./_resources/00-init-expert

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
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/feature_store/feature-store-expert-flow-training.png?raw=true" width="1200px"/>

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
delete_fss(catalog, db, ["user_features", "destination_features", "destination_location_features", "availability_features"])

from databricks.feature_engineering import FeatureEngineeringClient
fe = FeatureEngineeringClient()

# Reuse the same features as the previous example 02_Feature_store_advanced
# For more details these functions are available under ./_resources/00-init-expert
user_features_df = create_user_features(spark.table('travel_purchase'))
fe.create_table(name=f"{catalog}.{db}.user_features",
                primary_keys=["user_id", "ts"], 
                timestamp_keys="ts", 
                df=user_features_df, 
                description="User Features")

destination_features_df = destination_features_fn(spark.table('travel_purchase'))
fe.create_table(name=f"{catalog}.{db}.destination_features", 
                primary_keys=["destination_id", "ts"], 
                timestamp_keys="ts", 
                df=destination_features_df, 
                description="Destination Popularity Features")


#Add the destination location dataset
destination_location = spark.table("destination_location")
fe.create_table(name=f"{catalog}.{db}.destination_location_features", 
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

spark.sql('CREATE VOLUME IF NOT EXISTS feature_store_volume')
destination_availability_stream = (
  spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "json") #Could be "kafka" to consume from a message queue
  .option("cloudFiles.inferSchema", "true")
  .option("cloudFiles.inferColumnTypes", "true")
  .option("cloudFiles.schemaEvolutionMode", "rescue")
  .option("cloudFiles.schemaHints", "event_ts timestamp, booking_date date, destination_id int")
  .option("cloudFiles.schemaLocation", f"/Volumes/{catalog}/{db}/feature_store_volume/stream/availability_schema")
  .option("cloudFiles.maxFilesPerTrigger", 100) #Simulate streaming
  .load("/databricks-datasets/travel_recommendations_realtime/raw_travel_data/fs-demo_destination-availability_logs/json")
  .drop("_rescued_data")
  .withColumnRenamed("event_ts", "ts")
)

DBDemos.stop_all_streams_asynch(sleep_time=30)
display(destination_availability_stream)

# COMMAND ----------

fe.create_table(
    name=f"{catalog}.{db}.availability_features", 
    primary_keys=["destination_id", "booking_date", "ts"],
    timestamp_keys=["ts"],
    schema=destination_availability_stream.schema,
    description="Destination Availability Features"
)

# Now write the data to the feature table in "merge" mode using a stream
fe.write_table(
    name=f"{catalog}.{db}.availability_features", 
    df=destination_availability_stream,
    mode="merge",
    checkpoint_location= f"/Volumes/{catalog}/{db}/feature_store_volume/stream/availability_checkpoint",
    trigger={'once': True} #Refresh the feature store table once, or {'processingTime': '1 minute'} for every minute-
)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## Compute on-demand live features
# MAGIC
# MAGIC User location is a context feature that is captured at the time of the query. This data is not known in advance. 
# MAGIC
# MAGIC Derivated features can be computed from this location. For example, user distance from destination can only be computed in realtime at the prediction time.
# MAGIC
# MAGIC This introduce a new challenge, we now have to link some function to transform the data and make sure the same is being used for training and inference (batch or realtime). 
# MAGIC
# MAGIC To solve this, Databricks introduced Feature Spec. With Feature Spec, you can create custom function (SQL/PYTHON) to transform your data into new features, and link them to your model and feature store.
# MAGIC
# MAGIC Because it's shipped as part of your FeatureLookup definition, the same code will be used at inference time, offering a garantee that we compute the feature the same way, and adding flexibility while increasing model version.
# MAGIC
# MAGIC Note that this function will be available as `catalog.schema.distance_udf` in the browser.

# COMMAND ----------

# DBTITLE 1,Define a function to compute distance 
# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION distance_udf(lat1 DOUBLE, lon1 DOUBLE, lat2 DOUBLE, lon2 DOUBLE)
# MAGIC RETURNS DOUBLE
# MAGIC LANGUAGE PYTHON
# MAGIC COMMENT 'Calculate hearth distance from latitude and longitude'
# MAGIC AS $$
# MAGIC   import numpy as np
# MAGIC   dlat, dlon = np.radians(lat2 - lat1), np.radians(lon2 - lon1)
# MAGIC   a = np.sin(dlat/2)**2 + np.cos(np.radians(lat1)) * np.cos(np.radians(lat2)) * np.sin(dlon/2)**2
# MAGIC   return 2 * 6371 * np.arcsin(np.sqrt(a))
# MAGIC $$

# COMMAND ----------

# DBTITLE 1,Try the function to compute the distance between a user and a destination
# MAGIC %sql
# MAGIC SELECT distance_udf(user_latitude, user_longitude, latitude, longitude) AS hearth_distance, *
# MAGIC     FROM destination_location_features
# MAGIC         JOIN destination_features USING (destination_id)
# MAGIC         JOIN user_features USING (ts)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC # 2: Train a custom model with batch, on-demand and streaming features
# MAGIC
# MAGIC That's all we have to do. We're now ready to train our model with this new feature.
# MAGIC
# MAGIC *Note: In a typical deployment, you would add more functions such as timestamp features (cos/sin for the hour/day of the week) etc.*

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get ground-truth training labels and key + timestamp

# COMMAND ----------

# Split to define a training and inference set
training_keys = spark.table('travel_purchase').select('ts', 'purchased', 'destination_id', 'user_id', 'user_latitude', 'user_longitude', 'booking_date')
training_df = training_keys.where("ts < '2022-11-23'")
test_df = training_keys.where("ts >= '2022-11-23'").cache()

display(training_df.limit(5))

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## Create the training set
# MAGIC
# MAGIC Note the use of `FeatureFunction`, pointing to the new distance_udf function that we saved in Unity Catalog.

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
  # Add our function to compute the distance between the user and the destination 
  FeatureFunction(
      udf_name="distance_udf",
      input_bindings={"lat1": "user_latitude", "lon1": "user_longitude", "lat2": "latitude", "lon2": "longitude"},
      output_name="distance"
  )]

#Create the training set
training_set = fe.create_training_set(
    df=training_df,
    feature_lookups=feature_lookups,
    exclude_columns=['user_id', 'destination_id', 'booking_date', 'clicked', 'price'],
    label='purchased'
)

# COMMAND ----------


training_set_df = training_set.load_df()
#Let's cache the training dataset for automl (to avoid recomputing it everytime)
training_features_df = training_set_df.cache()

display(training_features_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Use automl to build an ML model out of the box

# COMMAND ----------

from datetime import datetime
from databricks import automl
xp_path = "/Shared/dbdemos/experiments/feature-store"
xp_name = f"automl_purchase_expert_{datetime.now().strftime('%Y-%m-%d_%H:%M:%S')}"
summary_cl = automl.classify(
    experiment_name = xp_name,
    experiment_dir = xp_path,
    dataset = training_features_df,
    target_col = "purchased",
    primary_metric="log_loss",
    timeout_minutes = 15
)
#Make sure all users can access dbdemos shared experiment
DBDemos.set_experiment_permission(f"{xp_path}/{xp_name}")

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

model_name = "dbdemos_fs_travel_model_expert"
model_full_name = f"{catalog}.{db}.{model_name}"

mlflow.set_registry_uri('databricks-uc')
# creating sample input to be logged (do not include the live features in the schema as they'll be computed within the model)
df_sample = training_set_df.limit(10).toPandas()
x_sample = df_sample.drop(columns=["purchased"])

# getting the model created by AutoML 
best_model = summary_cl.best_trial.load_model()

#Get the conda env from automl run
artifacts_path = mlflow.artifacts.download_artifacts(run_id=summary_cl.best_trial.mlflow_run_id)
env = mlflow.pyfunc.get_default_conda_env()
with open(artifacts_path+"model/requirements.txt", 'r') as f:
    env['dependencies'][-1]['pip'] = f.read().split('\n')

#Create a new run in the same experiment as our automl run.
with mlflow.start_run(run_name="best_fs_model_expert", experiment_id=summary_cl.experiment.experiment_id) as run:
  #Use the feature store client to log our best model
  fe.log_model(
              model=best_model, # object of your model
              artifact_path="model", #name of the Artifact under MlFlow
              flavor=mlflow.sklearn, # flavour of the model (our LightGBM model has a SkLearn Flavour)
              training_set=training_set, # training set you used to train your model with AutoML
              input_example=x_sample, # Dataset example (Pandas dataframe)
              registered_model_name=model_full_name, # register your best model
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

# DBTITLE 1,Save best model in the registry & flag it as Production ready
latest_model = get_last_model_version(model_full_name)
#Move it in Production
production_alias = "production"
if len(latest_model.aliases) == 0 or latest_model.aliases[0] != production_alias:
  print(f"updating model {latest_model.version} to Production")
  mlflow_client = MlflowClient(registry_uri="databricks-uc")
  mlflow_client.set_registered_model_alias(model_full_name, production_alias, version=latest_model.version)

# COMMAND ----------

# MAGIC %md
# MAGIC Our model is ready! you can open the Unity Catalog Explorer to review it.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Batch score test set
# MAGIC
# MAGIC Let's make sure our model is working as expected and try to score our test dataset

# COMMAND ----------

scored_df = fe.score_batch(model_uri=f"models:/{model_full_name}@{production_alias}", df=test_df, result_type="boolean")
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
# MAGIC To do that, we'll deploy online stores. These are fully serverless and managed by Databricks. You can think of them as a  (K/V store, such as Mysql, dynamoDB, CosmoDB...).
# MAGIC
# MAGIC Databricks will automatically synchronize the Delta Live Table content with the online store (you can chose to trigger the update yourself or do it on a schedule).
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/feature_store/feature-store-expert-flow.png?raw=true" width="1200px" />

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

from databricks.sdk import WorkspaceClient

def create_online_table(table_name, pks, timeseries_key=None):
    w = WorkspaceClient()
    online_table_name = table_name+"_online"
    if not online_table_exists(online_table_name):
        from databricks.sdk.service import catalog as c
        print(f"Creating online table for {online_table_name}...")
        spark.sql(f'ALTER TABLE {table_name} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)')
        spec = c.OnlineTableSpec(source_table_full_name=table_name, primary_key_columns=pks, run_triggered={'triggered': 'true'}, timeseries_key=timeseries_key)
        w.online_tables.create(name=online_table_name, spec=spec)

create_online_table(f"{catalog}.{db}.user_features",                 ["user_id"], "ts")
create_online_table(f"{catalog}.{db}.destination_features",          ["destination_id"], "ts")
create_online_table(f"{catalog}.{db}.destination_location_features", ["destination_id"])
create_online_table(f"{catalog}.{db}.availability_features",         ["destination_id", "booking_date"], "ts")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## Deploy Serverless Model serving Endpoint
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/feature_store/feature-store-expert-model-serving.png?raw=true" style="float: right" width="500px">
# MAGIC
# MAGIC Once our Model, Function and Online feature store are in Unity Catalog, we can deploy the model as using Databricks Model Serving.
# MAGIC
# MAGIC This will provide a REST API to serve our model in realtime.
# MAGIC
# MAGIC ### Enable model inference via the UI
# MAGIC
# MAGIC After calling `log_model`, a new version of the model is saved. To provision a serving endpoint, follow the steps below.
# MAGIC
# MAGIC 1. Within the Machine Learning menu, click [Serving menu](ml/endpoints) in the left sidebar. 
# MAGIC 2. Create a new endpoint, select the most recent model version from Unity Catalog and start the serverless model serving
# MAGIC
# MAGIC You can use the UI, in this demo We will use the API to programatically start the endpoint:

# COMMAND ----------

endpoint_name = "dbdemos_feature_store_endpoint_expert"
wc = WorkspaceClient()
served_models =[ServedModelInput(model_full_name, model_version=latest_model.version, workload_size=ServedModelInputWorkloadSize.SMALL, scale_to_zero_enabled=True)]
try:
    print(f'Creating endpoint {endpoint_name} with latest version...')
    wc.serving_endpoints.create_and_wait(endpoint_name, config=EndpointCoreConfigInput(served_models=served_models))
except Exception as e:
    if 'already exists' in str(e):
        print(f'Endpoint exists, updating with latest model version...')
        wc.serving_endpoints.update_config_and_wait(endpoint_name, served_models=served_models)
    else: 
        raise e

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/feature_store/feature-store-expert-model-serving-inference.png?raw=true" style="float: right" width="700"/>
# MAGIC
# MAGIC Once our model deployed, you can easily test your model using the Model Serving endpoint UI.
# MAGIC
# MAGIC Let's call it using the REST API directly.
# MAGIC
# MAGIC The endpoint will answer in millisec, what will happen under the hood is the following:
# MAGIC
# MAGIC * The endpoint receive the REST api call
# MAGIC * It calls our 4 online table to get the features
# MAGIC * Call the `distance_udf` function to compute the distance
# MAGIC * Call the ML model
# MAGIC * Returns the final answer

# COMMAND ----------


lookup_keys = test_df.drop('purchased').limit(2).toPandas().astype({'ts': 'str', 'booking_date': 'str'}).to_dict(orient="records")
print(f'Compute the propensity score for these customers: {lookup_keys}')
#Query the endpoint
for i in range(3):
    starting_time = timeit.default_timer()
    inferences = wc.serving_endpoints.query(endpoint_name, inputs=lookup_keys)
    print(f"Inference time, end 2 end :{round((timeit.default_timer() - starting_time)*1000)}ms")
    print(inferences)

# COMMAND ----------

# MAGIC %md # Optional: Deploy our Function as Feature Spec to compute transformations in realtime
# MAGIC
# MAGIC Our function can be saved as a Feature Spec and deployed standalone Model Serving Endpoint to serve any transformation.
# MAGIC
# MAGIC Here is an example on how you can create a feature spec to compute the distance between 2 points.
# MAGIC
# MAGIC This feature spec will do the lookup for you and call the `distance_df` function (as define in the `feature_lookups`).
# MAGIC
# MAGIC Once the feature spec deployed, you can use the [Serving Endpoint menu](ml/endpoints) to create a new endpoint.

# COMMAND ----------

# DBTITLE 1,Save the feature spec within Unity Catalog
feature_spec_name = f"{catalog}.{db}.travel_feature_spec"
try:
    fe.create_feature_spec(name=feature_spec_name, features=feature_lookups, exclude_columns=['user_id', 'destination_id', 'booking_date', 'clicked', 'price'])
except Exception as e:
    if "RESOURCE_ALREADY_EXISTS" not in str(e): raise e

# COMMAND ----------

# DBTITLE 1,Create the endpoint using the API
from databricks.feature_engineering.entities.feature_serving_endpoint import AutoCaptureConfig, EndpointCoreConfig, ServedEntity

# Create endpoint
feature_endpoint_name = "dbdemos-fse-travel-spec"
try: 
    status = fe.create_feature_serving_endpoint(name=feature_endpoint_name, 
                                                config=EndpointCoreConfig(served_entities=ServedEntity(scale_to_zero_enabled= True, feature_spec_name=feature_spec_name)))
except Exception as e:
    if "already exists" not in str(e): raise e

ep = wait_for_feature_endpoint_to_start(fe, feature_endpoint_name)

# COMMAND ----------

# MAGIC %md
# MAGIC Let's try our new feature spec endpoints. We can send send our queries using the UI or via REST API directly:

# COMMAND ----------

#lookup_keys = test_df.limit(2).toPandas().astype({'ts': 'str', 'booking_date': 'str'})
print(f'Compute the propensity score for these customers: {lookup_keys}')

def query_endpoint(url, lookup_keys):
    return requests.request(method='POST', headers=get_headers(), url=url, json={'dataframe_records': lookup_keys}).json()
query_endpoint(ep.url+"/invocations", lookup_keys)

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
