# Databricks notebook source
# MAGIC %md 
# MAGIC
# MAGIC # Feature store Travel Agency recommendation - Advanced
# MAGIC
# MAGIC For this demo, we'll go deeper in the Feature Store capabilities, adding multiple Feature Store table and introducing point in time lookup.
# MAGIC
# MAGIC We'll use the same dataset as before and implement the same use-case: recommender model for a Travel agency, pushing personalized offer based on what our customers are the most likely to buy.
# MAGIC
# MAGIC **What you will learn:**
# MAGIC - Build more advanced features with multiple tables
# MAGIC - Introduce timestamp key, to simplify temporal feature management
# MAGIC - Use AutoML to create the best model for us
# MAGIC - Online table, synchronizing Databricks Delta Table with a real time, low latency table automatically used by your model to lookup features.

# COMMAND ----------

# MAGIC %pip install databricks-feature-engineering==0.2.0 databricks-sdk==0.20.0
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./_resources/00-init-basic

# COMMAND ----------

# DBTITLE 1,Review our silver data
# MAGIC %sql SELECT * FROM travel_purchase

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## 1: Create our Feature Tables
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/feature_store/feature-store-advanced-flow.png?raw=true" width="800px" style="float: right">
# MAGIC
# MAGIC In this second example, we'll introduce more tables and new features calculated with window functions.
# MAGIC
# MAGIC To simplify updates & refresh, we'll split them in 2 tables:
# MAGIC
# MAGIC * **User features**: contains all the features for a given user in a given point in time (location, previous purchases if any etc)
# MAGIC * **Destination features**: data on the travel destination for a given point in time (interest tracked by the number of clicks & impression)
# MAGIC
# MAGIC %md 
# MAGIC ### Point-in-time support for feature tables
# MAGIC
# MAGIC Databricks Feature Store supports use cases that require point-in-time correctness.
# MAGIC
# MAGIC The data used to train a model often has time dependencies built into it. In our case, because we are adding rolling-window features, our Feature Table will contain data on all the dataset timeframe. 
# MAGIC
# MAGIC When we build our model, we must consider only feature values up until the time of the observed target value. If you do not explicitly take into account the timestamp of each observation, you might inadvertently use feature values measured after the timestamp of the target value for training. This is called “data leakage” and can negatively affect the model’s performance.
# MAGIC
# MAGIC Time series feature tables include a timestamp key column that ensures that each row in the training dataset represents the latest known feature values as of the row’s timestamp. 
# MAGIC
# MAGIC In our case, this timestamp key will be the `ts` field, present in our 2 feature tables.

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Calculating the features
# MAGIC
# MAGIC Let's calculate the aggregated features from the vacation purchase logs for destinations and users. 
# MAGIC
# MAGIC The user features capture the user profile information such as past purchased price. Because the booking data does not change very often, it can be computed once per day in batch.
# MAGIC
# MAGIC The destination features include popularity features such as impressions and clicks, as well as pricing features such as price at the time of booking.

# COMMAND ----------

# DBTITLE 1,User Features
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
            F.sum("tickets_purchased").over(w.Window.partitionBy("user_id").orderBy(F.col("ts_l")).rangeBetween(start=-(6 * 30 * 86400), end=0)).cast('double')
        )
        .select("user_id", "ts", "mean_price_7d", "last_6m_purchases", "user_longitude", "user_latitude")
    )
    return add_time_features_spark(travel_purchase_df)


user_features_df = create_user_features(spark.table('travel_purchase'))
display(user_features_df)

# COMMAND ----------

# DBTITLE 1,Destination Features
def create_destination_features(travel_purchase_df):
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
destination_features_df = create_destination_features(spark.table('travel_purchase'))
display(destination_features_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Creating the Feature Table
# MAGIC
# MAGIC Let's use the FeatureStore client to save our 2 tables. Note the `timestamp_keys='ts'` parameters that we're adding during the table creation.
# MAGIC
# MAGIC Databricks Feature Store will use this information to automatically filter features and prevent from potential leakage.

# COMMAND ----------

# DBTITLE 1,user_features
from databricks.feature_engineering import FeatureEngineeringClient

fe = FeatureEngineeringClient(model_registry_uri="databricks-uc")
# help(fe.create_table)

# first create a table with User Features calculated above 
fe_table_name_users = f"{catalog}.{db}.user_features_advanced"
#fe.drop_table(name=fe_table_name_users)
fe.create_table(
    name=fe_table_name_users, # unique table name (in case you re-run the notebook multiple times)
    primary_keys=["user_id", "ts"],
    timestamp_keys="ts",
    df=user_features_df,
    description="User Features",
    tags={"team":"analytics"}
)

# COMMAND ----------

# DBTITLE 1,destination_features
fe_table_name_destinations = f"{catalog}.{db}.destination_features_advanced"
# second create another Feature Table from popular Destinations
# for the second table, we show how to create and write as two separate operations
# fe.drop_table(name=fe_table_name_destinations)
fe.create_table(
    name=fe_table_name_destinations, # unique table name (in case you re-run the notebook multiple times)
    primary_keys=["destination_id", "ts"],
    timestamp_keys="ts", 
    schema=destination_features_df.schema,
    description="Destination Popularity Features",
    tags={"team":"analytics"} # if you have multiple team creating tables, maybe worse of adding a tag 
)
fe.write_table(name=fe_table_name_destinations, df=destination_features_df)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/feature_store/feature-store-advanced-tables.png?raw=true" style="float: right; margin-left: 10px" width="550px">
# MAGIC
# MAGIC As in our previous example, the 2 feature store tables were created and are available within Unity Catalog. 
# MAGIC
# MAGIC You can explore Catalog Explorer. You'll find all the features created, including a reference to this notebook and the version used during the feature table creation.
# MAGIC
# MAGIC Note that the id ts are automatically defined as PK and TS PK columns.
# MAGIC
# MAGIC Now that our features are ready, we can start creating the training dataset and train a model using AutoML!

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 2: Train a model with FS and timestamp lookup
# MAGIC
# MAGIC ### Create the training dataset
# MAGIC
# MAGIC The next step is to build a training dataset. 
# MAGIC
# MAGIC Because we have 2 feature tables, we'll add 2 `FeatureLookup` entries, specifying the key so that the feature store engine can join using this field.
# MAGIC
# MAGIC We will also add the `timestamp_lookup_key` property to `ts` so that the engine filter the features based on this key.

# COMMAND ----------

ground_truth_df = spark.table('travel_purchase').select('user_id', 'destination_id', 'purchased', 'ts')

# Split based on time to define a training and inference set (we'll do train+eval on the past & test in the most current value)
training_labels_df = ground_truth_df.where("ts < '2022-11-23'")
test_labels_df = ground_truth_df.where("ts >= '2022-11-23'")

display(test_labels_df)

# COMMAND ----------

# DBTITLE 1,Create the feature lookup with the lookup keys
from databricks.feature_engineering import FeatureEngineeringClient, FeatureLookup
from databricks.feature_store import feature_table, FeatureLookup

fe = FeatureEngineeringClient()

model_feature_lookups = [
      FeatureLookup(
          table_name=fe_table_name_destinations,
          lookup_key="destination_id",
          timestamp_lookup_key="ts"
      ),
      FeatureLookup(
          table_name=fe_table_name_users,
          lookup_key="user_id",
          feature_names=["mean_price_7d", "last_6m_purchases", "day_of_week_sin", "day_of_week_cos", "day_of_month_sin", "day_of_month_cos", "hour_sin", "hour_cos"], # if you dont specify here the FeatureEngineeringClient will take all your feature apart from primary_keys 
          timestamp_lookup_key="ts"
      )
]

# fe.create_training_set will look up features in model_feature_lookups with matched key from training_labels_df
training_set = fe.create_training_set(
    df=training_labels_df, # joining the original Dataset, with our FeatureLookupTable
    feature_lookups=model_feature_lookups,
    exclude_columns=["ts", "destination_id", "user_id"], # exclude id columns as we don't want them as feature
    label='purchased',
)

training_pd = training_set.load_df()
display(training_pd)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Creating the model using Databricks AutoML 
# MAGIC
# MAGIC Instead of creating a basic model like previously, we will use <a href="https://docs.databricks.com/machine-learning/automl/index.html#classification" target="_blank">Databricks AutoML</a> to train our model, using best practices out of the box.
# MAGIC
# MAGIC While you can do that using the UI directly (+New => AutoML), we'll be using the `databricks.automl` API to have a reproductible flow.
# MAGIC
# MAGIC After running the previous cell, you will notice two notebooks and an MLflow experiment:
# MAGIC
# MAGIC * **Data exploration notebook**: we can see a Profiling Report which organizes the input columns and discusses values, frequency and other information
# MAGIC * **Best trial notebook**: shows the source code for reproducing the best trial conducted by AutoML
# MAGIC * **MLflow experiment**: contains high level information, such as the root artifact location, experiment ID, and experiment tags. The list of trials contains detailed summaries of each trial, such as the notebook and model location, training parameters, and overall metrics.

# COMMAND ----------

# DBTITLE 1,Start an AutoML run
from datetime import datetime
from databricks import automl
xp_path = "/Shared/dbdemos/experiments/feature-store"
xp_name = f"automl_purchase_advanced_{datetime.now().strftime('%Y-%m-%d_%H:%M:%S')}"
summary_cl = automl.classify(
    experiment_name = xp_name,
    experiment_dir = xp_path,
    dataset = training_pd,
    target_col = "purchased",
    primary_metric="log_loss",
    timeout_minutes = 10
)
#Make sure all users can access dbdemos shared experiment
DBDemos.set_experiment_permission(f"{xp_path}/{xp_name}")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC #### Get best run from automl MLFlow experiment
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/product/feature_store/automl_experm.png" alt="step12" width="700" style="float: right; margin-left: 10px" />
# MAGIC
# MAGIC Open the **MLflow experiment** from the link above and explore your best run.
# MAGIC
# MAGIC In a real deployment, we would review the notebook generated and potentially improve it using our domain knowledge before deploying it in production.
# MAGIC
# MAGIC For this Feature Store demo, we'll simply get the best model and deploy it in the registry.

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Saving our best model to MLflow registry
# MAGIC
# MAGIC Next, we'll get Automl best model and add it to our registry. Because we the feature store to keep track of our model & features, we'll log the best model as a new run using the `FeatureStoreClient.log_model()` function.
# MAGIC
# MAGIC **summary_cl** provides the automl information required to automate the deployment process. We'll use it to select our best run and deploy as Production production.
# MAGIC
# MAGIC *Note that another way to find the best run would be to use search_runs function from mlflow API, sorting by our accuracy metric.*

# COMMAND ----------

# DBTITLE 1,Save best model in the registry & flag it as Production ready
model_name = "dbdemos_fs_travel_model_advanced"
model_full_name = f"{catalog}.{db}.{model_name}"

mlflow.set_registry_uri('databricks-uc')
# creating sample input to be logged
df_sample = training_pd.limit(10).toPandas()
x_sample = df_sample.drop(columns=["purchased"])
y_sample = df_sample["purchased"]

# getting the model created by AutoML 
best_model = summary_cl.best_trial.load_model()

env = mlflow.pyfunc.get_default_conda_env()
with open(mlflow.artifacts.download_artifacts("runs:/"+summary_cl.best_trial.mlflow_run_id+"/model/requirements.txt"), 'r') as f:
    env['dependencies'][-1]['pip'] = f.read().split('\n')

#Create a new run in the same experiment as our automl run.
with mlflow.start_run(run_name="best_fs_model_advanced", experiment_id=summary_cl.experiment.experiment_id) as run:
  #Use the feature store client to log our best model
  fe.log_model(
              model=best_model, # object of your model
              artifact_path="model", #name of the Artifact under MlFlow
              flavor=mlflow.sklearn, # flavour of the model (our LightGBM model has a SkLearn Flavour)
              training_set=training_set, # training set you used to train your model with AutoML
              input_example=x_sample, # example of the dataset, should be Pandas
              signature=infer_signature(x_sample, y_sample), # schema of the dataset, not necessary with FS, but nice to have 
              registered_model_name=model_full_name, # register your best model
              conda_env = env
          )
  mlflow.log_metrics(summary_cl.best_trial.metrics)
  mlflow.log_params(summary_cl.best_trial.params)
  mlflow.set_tag(key='feature_store', value='advanced_demo')

# COMMAND ----------

latest_model = get_last_model_version(model_full_name)
#Move it in Production
production_alias = "production"
if len(latest_model.aliases) == 0 or latest_model.aliases[0] != production_alias:
  print(f"updating model {latest_model.version} to Production")
  mlflow_client = MlflowClient(registry_uri="databricks-uc")
  mlflow_client.set_registered_model_alias(model_full_name, production_alias, version=latest_model.version)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## 3: Running batch inference
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/product/feature_store/feature_store_inference_advanced.png" style="float: right" width="850px" />
# MAGIC
# MAGIC As previously, we can easily leverage the feature store to get our predictions.
# MAGIC
# MAGIC No need to fetch or recompute the feature, we just need the lookup ids and the feature store will automatically fetch them from the feature store table. 

# COMMAND ----------

## For sake of simplicity, we will just predict on the same inference_data_df
from databricks.feature_engineering import FeatureEngineeringClient
fe = FeatureEngineeringClient(model_registry_uri="databricks-uc")
batch_scoring = test_labels_df.select('user_id', 'destination_id', 'ts', 'purchased')
scored_df = fe.score_batch(model_uri=f"models:/{model_full_name}@{production_alias}", df=batch_scoring, result_type="boolean")
display(scored_df)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## 4: Running realtime inferences: introducing Online Tables
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/feature_store/feature-store-advanced-online.png?raw=true" style="float: right" width="850px" />
# MAGIC
# MAGIC Databricks now has built-in **online table**, providing realtime Key-Value lookup for your inferences.
# MAGIC
# MAGIC Online tables are fully managed and serverless. 
# MAGIC
# MAGIC This let you deploy realtime endpoints using these tables to lookup features and compute your prediction in milliseconds.
# MAGIC
# MAGIC Simply pick a source Detla Table to create your first online table. 
# MAGIC
# MAGIC Databricks will manage the synchronisation between your Delta Live Table and the Online Table for you in the background.

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Creating the online tables
# MAGIC Creating the table is straight forward

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
        
#Note that the timeseries key 'ts' is optional. When defined, the online store will return the most recent entry.
create_online_table(fe_table_name_destinations, ["destination_id"], "ts") 
create_online_table(fe_table_name_users,        ["user_id"], "ts")

#wait for all the tables to be online
wait_for_online_tables(catalog, db, [fe_table_name_destinations+"_online", fe_table_name_users+"_online"])

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Our online realtime tables are available in the Unity Catalog Explorer, like any other tables!
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/feature_store/feature-store-advanced-online-tables.png?raw=true" style="float: right" width="850px" />
# MAGIC
# MAGIC We can see that our online table has been successfully created. 
# MAGIC
# MAGIC Like any other table, it's available within the Unity Catalog explorer, in your catalog -> schema. 
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Let's deploy our realtime model using the online table.
# MAGIC
# MAGIC Because you used the Feature Store to save the features and the model, Databricks knows that your model has to leverage the online tables once deployed.
# MAGIC
# MAGIC All we have to do is simply deploy the Model as a Serving Endpoint, and the Online Tables will automatically be leveraged to lookup features in realtime, all managed by Databricks.

# COMMAND ----------

endpoint_name = "dbdemos_feature_store_endpoint_advanced"
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

# MAGIC %md 
# MAGIC ### Querying the online tables
# MAGIC
# MAGIC Let's query the model. Under the hood, the following will happen:
# MAGIC

# COMMAND ----------

lookup_keys = test_labels_df.drop('purchased', "ts").limit(2).toPandas()
data = lookup_keys.to_dict(orient="records")
print('Data sent to the model:')
print(data)

starting_time = timeit.default_timer()
inferences = wc.serving_endpoints.query(endpoint_name, inputs=lookup_keys.to_dict(orient="records"))
print(f"Inference time, end 2 end :{round((timeit.default_timer() - starting_time)*1000)}ms")
print(inferences.predictions)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## Summary 
# MAGIC
# MAGIC We've seen how the feature store can handle multiple tables, leverage a more advanced model with Databricks AutoML and use point-in-time lookup when your dataset contains temporal information and you don't want the future information to leak.
# MAGIC
# MAGIC On top of that, we saw how Databricks Online table can provide realtime capabilities for K/V queries, automatically backed 
# MAGIC
# MAGIC Databricks Feature store brings you a full traceability, knowing which model is using which feature in which notebook/job.
# MAGIC
# MAGIC It also simplify inferences by always making sure the same features will be used for model training and inference, always querying the same feature table based on your lookup keys.
# MAGIC
# MAGIC ## Next Steps 
# MAGIC
# MAGIC Open the [03_Feature_store_expert notebook]($./03_Feature_store_expert) to explore more Feature Store benefits & capabilities:
# MAGIC
# MAGIC - Multiple lookup tables
# MAGIC - Streaming datasets
# MAGIC - On-demand feature computation wrapped with the model with Feature Spec
# MAGIC - Online feature store
# MAGIC - Real time model serving with Rest Endpoints
