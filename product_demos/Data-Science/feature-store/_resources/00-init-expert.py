# Databricks notebook source
# MAGIC %md 
# MAGIC Do not edit the notebook, it contains import and helpers for the demo

# COMMAND ----------

# MAGIC %run ./00-init-basic

# COMMAND ----------

# DBTITLE 1,Untitled
def create_user_features(travel_purchase_df):
    """
    Computes the user_features feature group.
    """
    travel_purchase_df = travel_purchase_df.withColumn('ts_l', F.col("ts").cast("long"))
    travel_purchase_df = (
        # Sum total purchased for 7 days
        travel_purchase_df.withColumn("lookedup_price_7d_rolling_sum",
            F.sum("price").over(w.Window.partitionBy("user_id").orderBy(F.col("ts_l")).rangeBetween(start=-(7 * 86400), end=0))
        )
        # counting number of purchases per week
        .withColumn("lookups_7d_rolling_sum", 
            F.count("*").over(w.Window.partitionBy("user_id").orderBy(F.col("ts_l")).rangeBetween(start=-(7 * 86400), end=0))
        )
        # total price 7d / total purchases for 7 d 
        .withColumn("mean_price_7d",  F.col("lookedup_price_7d_rolling_sum") / F.col("lookups_7d_rolling_sum"))
         # converting True / False into 1/0
        .withColumn("tickets_purchased", F.col("purchased").cast('int'))
        # how many purchases for the past 6m
        .withColumn("last_6m_purchases", 
            F.sum("tickets_purchased").over(w.Window.partitionBy("user_id").orderBy(F.col("ts_l")).rangeBetween(start=-(6 * 30 * 86400), end=0))
        )
        .select("user_id", "ts", "mean_price_7d", "last_6m_purchases", "user_longitude", "user_latitude")
    )
    return travel_purchase_df



def destination_features_fn(travel_purchase_df):
    """
    Computes the destination_features feature group.
    """
    return (
        travel_purchase_df
          .withColumn("clicked", F.col("clicked").cast("int"))
          .withColumn("sum_clicks_7d", 
            F.sum("clicked").over(w.Window.partitionBy("destination_id").orderBy(F.col("ts").cast("long")).rangeBetween(start=-(7 * 86400), end=0))
          )
          .withColumn("sum_impressions_7d", 
            F.count("*").over(w.Window.partitionBy("destination_id").orderBy(F.col("ts").cast("long")).rangeBetween(start=-(7 * 86400), end=0))
          )
          .select("destination_id", "ts", "sum_clicks_7d", "sum_impressions_7d")
    )  

#Required for pandas_on_spark assign to work properly
import pyspark.pandas as ps
import timeit
ps.set_option('compute.ops_on_diff_frames', True)

#Compute distance between 2 points. Could use geopy instead
def compute_hearth_distance(lat1, lon1, lat2, lon2):
  dlat, dlon = np.radians(lat2 - lat1), np.radians(lon2 - lon1)
  a = np.sin(dlat/2)**2 + np.cos(np.radians(lat1)) * np.cos(np.radians(lat2)) * np.sin(dlon/2)**2
  return 2 * 6371 * np.arcsin(np.sqrt(a))


# COMMAND ----------

# Below are cleanup related functions   
import boto3
   
def delete_online_store_table(table_name):
  assert table_name.startswith('feature_store_dbdemos')
  if get_cloud_name() == "aws":
    delete_dynamodb_table(online_table_name)
  elif get_cloud_name() == "azure":
    delete_cosmosdb_container(online_table_name)
  else:
    raise Exception(f"cloud not supported : {get_cloud_name()}")
    
def delete_dynamodb_table(table_name):
  #TODO: add your key 
  #AWS_DYNAMO_DB_KEY_ID = aws_access_key_id=dbutils.secrets.get(scope="field-all-users-feature-store-example-write", key="field-eng-access-key-id")
  #AWS_DYNAMO_DB_KEY = aws_secret_access_key=dbutils.secrets.get(scope="field-all-users-feature-store-example-write", key="field-eng-secret-access-key")
  client = boto3.client('dynamodb', aws_access_key_id=AWS_DYNAMO_DB_KEY_ID, aws_secret_access_key=AWS_DYNAMO_DB_KEY, region_name="us-west-2")
  client.delete_table(TableName=table_name)
  waiter = client.get_waiter('table_not_exists')
  waiter.wait(TableName=table_name)
  print(f"table: '{table_name}' was deleted")

from azure.cosmos import cosmos_client
import azure.cosmos.exceptions as exceptions

def delete_cosmosdb_container(container_name, account_uri_field_demo, database = "field_demos"):
  #TODO: add your key
  #COSMO_DB_KEY = dbutils.secrets.get(scope="feature-store-example-write", key="field-eng-authorization-key")
  client = cosmos_client.CosmosClient(account_uri_field_demo, credential=COSMO_DB_KEY)
  database = client.get_database_client(database)
  container = database.get_container_client(container_name)
  try:
      database.delete_container(container)
      print('Container with id \'{0}\' was deleted'.format(container))
  except exceptions.CosmosResourceNotFoundError:
      print('A container with id \'{0}\' does not exist'.format(container))


# COMMAND ----------

def func_stop_streaming_query(query):
  import time
  while len(query.recentProgress) == 0 or query.status["isDataAvailable"]:
    print("waiting for stream to process all data")
    print(query.status)
    time.sleep(10)
  query.stop() 
  print("Just stopped one of the streaming queries.")
  

from mlflow.tracking.client import MlflowClient
def get_latest_model_version(model_name: str):
  client = MlflowClient()
  models = client.get_latest_versions(model_name, stages=["None"])
  for m in models:
    new_model_version = m.version
  return new_model_version


def cleanup(query, query2):
  func_stop_streaming_query(query)
  func_stop_streaming_query(query2)
  
  func_delete_model_serving_endpoint(model_serving_endpoint_name)

  fs_table_names = [
    fs_table_destination_popularity_features,
    fs_table_destination_location_features,
    fs_table_destination_availability_features,
    fs_table_user_features
  ]
  fs_online_table_names = [
    fs_online_destination_popularity_features,
    fs_online_destination_location_features,
    fs_online_destination_availability_features,
    fs_online_user_features
  ]
  db_name = database_name
  delta_checkpoint = fs_destination_availability_features_delta_checkpoint
  online_checkpoint = fs_destination_availability_features_online_checkpoint
  model = model_name

  fs = feature_store.FeatureStoreClient()

  for table_name in fs_table_names:
    try:
      fs.drop_table(name=table_name)
      print(table_name, "is dropped!")
    except Exception as ex:
      print(ex)

  try:
    drop_database(db_name)
  except Exception as ex:
    print(ex)


  for container_name in fs_online_table_names:
    try:
      print("currentlly working on this online table/container dropping: ", container_name)
      if cloud_name == "azure":
        delete_cosmosdb_container(container_name, account_uri_field_demo)
        print("\n")
      elif cloud_name == "aws":
        delete_dynamodb_table(table_name=container_name)
        print("\n")
    except Exception as ex:
      print(ex)

  try:    
    dbutils.fs.rm(
      delta_checkpoint, True
    )
  except Exception as ex:
      print(ex) 

  try:    
    dbutils.fs.rm(
      online_checkpoint, True
    )
  except Exception as ex:
    print(ex)

  try:
    delete_model(model_name)
  except Exception as ex:
    print(ex)  

# COMMAND ----------

destination_location_df = spark.read.option("inferSchema", "true").load("/databricks-datasets/travel_recommendations_realtime/raw_travel_data/fs-demo_destination-locations/",  format="csv", header="true")
destination_location_df.write.mode('overwrite').saveAsTable('destination_location')

# COMMAND ----------

def wait_for_feature_endpoint_to_start(fe, endpoint_name: str):
    for i in range (100):
        ep = fe.get_feature_serving_endpoint(name=endpoint_name)
        if ep.state['config_update'] == 'IN_PROGRESS':
            if i % 10 == 0:
                print(f"deployment in progress, please wait for your feature serving endpoint to be deployed {ep}")
            time.sleep(5)
        else:
            if ep.state['ready'] != 'READY':
                raise Exception(f"Endpoint is in abnormal state: {ep}")
            print(f"Endpoint {endpoint_name} ready - {ep}")
            return ep
        

# COMMAND ----------

import time

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
          raise Exception(f"endpoint exists ? {ep}")
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

def get_active_streams(start_with = ""):
    return [s for s in spark.streams.active if len(start_with) == 0 or (s.name is not None and s.name.startswith(start_with))]
  
  
# Function to stop all streaming queries 
def stop_all_streams(start_with = "", sleep_time=0):
  import time
  time.sleep(sleep_time)
  streams = get_active_streams(start_with)
  if len(streams) > 0:
    print(f"Stopping {len(streams)} streams")
    for s in streams:
        try:
            s.stop()
        except:
            pass
    print(f"All stream stopped {'' if len(start_with) == 0 else f'(starting with: {start_with}.)'}")


def retrain_expert_model():
  return dbutils.widgets.get('retrain_model') == 'true' or not model_exists(model_name_expert)

def model_exists(model_name):
  try:
    client = mlflow.tracking.MlflowClient()             
    latest_model = client.get_latest_versions(model_name)
    return True
  except:
    return False
