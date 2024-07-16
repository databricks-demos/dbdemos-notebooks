# Databricks notebook source
dbutils.widgets.dropdown("force_refresh_automl", "true", ["false", "true"], "Restart AutoML run")

# COMMAND ----------

# MAGIC %md
# MAGIC # Churn Prediction Feature Engineering
# MAGIC Our first step is to analyze the data and build the features we'll use to train our model. Let's see how this can be done.
# MAGIC
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/mlops-end2end-flow-1.png" width="1200">
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Ffeatures%2Fmlops%2F02_feature_prep&dt=MLOPS">
# MAGIC <!-- [metadata={"description":"MLOps end2end workflow: Feature engineering",
# MAGIC  "authors":["quentin.ambard@databricks.com"],
# MAGIC  "db_resources":{},
# MAGIC   "search_tags":{"vertical": "retail", "step": "Data Engineering", "components": ["feature store"]},
# MAGIC                  "canonicalUrl": {"AWS": "", "Azure": "", "GCP": ""}}] -->

# COMMAND ----------

# DBTITLE 1,Install latest feature engineering client for UC [for MLR < 13.2] and databricks python sdk
# MAGIC %pip install databricks-feature-engineering --upgrade
# MAGIC %pip install databricks-sdk --upgrade
# MAGIC
# MAGIC
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../_resources/00-setup $reset_all_data=false $catalog="aminen_catalog" $db="advanced_mlops"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exploratory Data Anaylsis
# MAGIC To get a feel of the data, what needs cleaning, pre-processing etc.
# MAGIC - **Use Databricks's native visualization tools**
# MAGIC - Bring your own visualization library of choice (i.e. seaborn, plotly)

# COMMAND ----------

# DBTITLE 1,Read in Bronze Delta table using Spark
# Read into Spark
telcoDF = spark.read.table(bronze_table_name)
display(telcoDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Featurization Logic(s) for BATCH feature computation
# MAGIC
# MAGIC 1. Compute number of active services
# MAGIC 2. Clean-up names and manual mapping
# MAGIC
# MAGIC _This can also work for streaming based features_

# COMMAND ----------

# MAGIC %md
# MAGIC ### Using PandasUDF and PySpark
# MAGIC To scale pandas analytics on a spark dataframe

# COMMAND ----------

primary_key = "customer_id"
timestamp_col ="transaction_ts"
label_col = "churn"
labels_table_name = "churn_label_table"
feature_table_name = "churn_feature_table"

# COMMAND ----------

from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql.functions import pandas_udf, col, when, lit

def compute_service_features(inputDF: SparkDataFrame) -> SparkDataFrame:
  """
  Count number of optional services enabled, like streaming TV
  """

  # Create pandas UDF function
  @pandas_udf('double')
  def num_optional_services(*cols):
    """Nested helper function to count number of optional services in a pandas dataframe"""
    return sum(map(lambda s: (s == "Yes").astype('double'), cols))

  return inputDF.\
    withColumn("num_optional_services",
        num_optional_services("online_security", "online_backup", "device_protection", "tech_support", "streaming_tv", "streaming_movies"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Using Pandas On Spark API
# MAGIC
# MAGIC Because our Data Scientist team is familiar with Pandas, we'll use the [pandas on spark API](https://spark.apache.org/docs/latest/api/python/reference/pyspark.pandas/index.html) to scale `pandas` code. The Pandas instructions will be converted in the spark engine under the hood and distributed at scale.
# MAGIC
# MAGIC *Note: Starting from `spark 3.2`, koalas is builtin and we can get an Pandas Dataframe using `pandas_api()`.*

# COMMAND ----------

# DBTITLE 1,Define featurization function
def clean_churn_features(dataDF: SparkDataFrame) -> SparkDataFrame:
  """
  Simple cleaning function leveraging pandas API
  """

  # Convert to pandas on spark dataframe
  data_psdf = dataDF.pandas_api()

  # Convert some columns
  data_psdf["senior_citizen"] = data_psdf["senior_citizen"].map({1 : "Yes", 0 : "No"})
  data_psdf = data_psdf.astype({"total_charges": "double", "senior_citizen": "string"})

  # Fill some missing numerical values with 0
  data_psdf = data_psdf.fillna({"tenure": 0.0})
  data_psdf = data_psdf.fillna({"monthly_charges": 0.0})
  data_psdf = data_psdf.fillna({"total_charges": 0.0})

  # Add/Force semantic data types for specific colums (to facilitate autoML)
  data_cleanDF = data_psdf.to_spark()
  data_cleanDF = data_cleanDF.withMetadata(primary_key, {"spark.contentAnnotation.semanticType":"native"})
  data_cleanDF = data_cleanDF.withMetadata("num_optional_services", {"spark.contentAnnotation.semanticType":"numeric"})

  return data_cleanDF

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## Compute & Write to Feature Store
# MAGIC
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/mlops-end2end-flow-feature-store.png" style="float:right" width="500" />
# MAGIC
# MAGIC Once our features are ready, we'll save them in Databricks Feature Store. Under the hood, features store are backed by a Delta Lake table.
# MAGIC
# MAGIC This will allow discoverability and reusability of our feature accross our organization, increasing team efficiency.
# MAGIC
# MAGIC Feature store will bring traceability and governance in our deployment, knowing which model is dependent of which set of features.
# MAGIC
# MAGIC Make sure you're using the "Machine Learning" menu to have access to your feature store using the UI.

# COMMAND ----------

# DBTITLE 1,Compute Churn Features and append a timestamp
from datetime import datetime

# Add current scoring timestamp
this_time = (datetime.now()).timestamp()
churn_features_n_predsDF = clean_churn_features(compute_service_features(telcoDF)) \
                            .withColumn(timestamp_col, lit(this_time).cast("timestamp"))

display(churn_features_n_predsDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Extract ground-truth labels in a separate table to avoid label leakage

# COMMAND ----------

# DBTITLE 1,Extract ground-truth labels in a separate table and drop from Feature table
# Extract labels in separate table before pushing to Feature Store to avoid label leakage
churn_features_n_predsDF.select(primary_key, timestamp_col, label_col) \
                        .write.format("delta") \
                        .mode("overwrite").option("overwriteSchema", "true") \
                        .saveAsTable(f"{catalog}.{db}.{labels_table_name}")

churn_featuresDF = churn_features_n_predsDF.drop(label_col)

# COMMAND ----------

# MAGIC %md
# MAGIC Add primary keys constraints to labels table for feature lookup

# COMMAND ----------

spark.sql(f"ALTER TABLE {catalog}.{db}.{labels_table_name} ALTER COLUMN {primary_key} SET NOT NULL")
spark.sql(f"ALTER TABLE {catalog}.{db}.{labels_table_name} ALTER COLUMN {timestamp_col} SET NOT NULL")
spark.sql(f"ALTER TABLE {catalog}.{db}.{labels_table_name} ADD CONSTRAINT {labels_table_name}_pk PRIMARY KEY({primary_key}, {timestamp_col})")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Feature Table and write to offline-store
# MAGIC One-time setup

# COMMAND ----------

# DBTITLE 1,Import Feature Store Client
from databricks.feature_engineering import FeatureEngineeringClient

fe = FeatureEngineeringClient()

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

# DBTITLE 1,Drop feature table if it already exists (optional)

try:

  # Drop existing table from Feature Store
  fe.drop_table(name=f"{catalog}.{db}.{feature_table_name}")

  # Delete underyling delta tables
  spark.sql(f"DROP TABLE IF EXISTS {catalog}.{db}.{feature_table_name}")
  print(f"Dropping Feature Table {catalog}.{db}.{feature_table_name}")


except ValueError as ve:
  pass
  print(f"Feature Table {catalog}.{db}.{feature_table_name} doesn't exist")

# COMMAND ----------

churn_feature_table = fe.create_table(
  name=feature_table_name, # f"{catalog}.{dbName}.{feature_table_name}"
  primary_keys=[primary_key, timestamp_col],
  schema=churn_featuresDF.schema,
  timeseries_columns=timestamp_col,
  description=f"These features are derived from the {catalog}.{db}.{bronze_table_name} table in the lakehouse. We created service features, cleaned up their names.  No aggregations were performed. [Warning: This table doesn't store the ground-truth and now can be used with AutoML's Feature Store integration"
)

# COMMAND ----------

# DBTITLE 1,Write feature values to Feature Store
fe.write_table(
  name=f"{catalog}.{db}.{feature_table_name}",
  df=churn_featuresDF, # can be a streaming dataframe as well
  mode='merge' #'merge'/'overwrite' which supports schema evolution
)

# COMMAND ----------

# DBTITLE 1,Enable Change-Data-Feed on Feature Table for performance considerations
spark.sql(f"ALTER TABLE {catalog}.{db}.{feature_table_name} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Publish features to online store
# MAGIC
# MAGIC For serving prediction queries with low-latency. Databricks offer 2 possibilities:
# MAGIC 1. Third-party online store ([AWS](https://docs.databricks.com/en/machine-learning/feature-store/online-feature-stores.html) | [Azure](https://learn.microsoft.com/en-us/azure/databricks/machine-learning/feature-store/online-feature-stores))
# MAGIC 2. **Databricks online tables** ([AWS](https://docs.databricks.com/en/machine-learning/feature-store/online-tables.html#use-online-tables-with-databricks-model-serving) | [Azure](https://learn.microsoft.com/en-us/azure/databricks/machine-learning/feature-store/online-tables))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Publish to 3rd-party online store
# MAGIC
# MAGIC For authentication with 3rd-party online stores, please referr to the different options in ([AWS](https://docs.databricks.com/en/machine-learning/feature-store/fs-authentication.html#authentication-for-working-with-online-stores) | [Azure](https://learn.microsoft.com/en-us/azure/databricks/machine-learning/feature-store/fs-authentication)).
# MAGIC
# MAGIC For production purposes and rotating keys it's better to use instance profiles ([AWS only](https://docs.databricks.com/en/machine-learning/feature-store/fs-authentication.html#provide-lookup-authentication-through-an-instance-profile-configured-to-a-served-model))

# COMMAND ----------

# MAGIC %md
# MAGIC #### For pushing to CosmosDB (Azure), make sure to:
# MAGIC 1. Install the latest cosmosdb connector as a cluster library by pointing to the latest maven coordinates [here](https://github.com/Azure/azure-sdk-for-java/tree/main/sdk/cosmos/azure-cosmos-spark_3-4_2-12#azure-cosmos-spark_3-3_2-12)(i.e. `com.azure.cosmos.spark:azure-cosmos-spark_3-3_2-12:4.21.1`)
# MAGIC 2. Create a CosmosDB instance/account and get the `account_uri` (i.e. "https://field-demo.documents.azure.com:443/" for databricks's field-engineering team).
# MAGIC 3. Create static read/write keys, store in secrets (under 2 different read/write scopes) and embedd in model's fs lookup mechanism (e.g. below).

# COMMAND ----------

# DBTITLE 1,Setup Online Store spec object
from databricks.feature_store.online_store_spec import AmazonDynamoDBSpec, AzureCosmosDBSpec


if get_cloud_name() == "aws":
  # DynamoDB spec
  dynamo_table_prefix = "one-env-feature_store" # [OPTIONAL] If table name has to start with specific prefix given policies
  churn_features_online_store_spec = AmazonDynamoDBSpec(
    region="us-west-2",
    table_name = f"{dynamo_table_prefix}_{db}_{feature_table_name}"
#    ttl= # Publish window of feature values given time-to-live date
)

elif get_cloud_name() == "azure":
  # Shared AWS/Azure field-eng secrets for Dynamo/CosmosDB
  prefix = "field-eng"
  read_scope = "field-all-users-feature-store-example-read"
  write_scope = "field-all-users-feature-store-example-write"

  # CosmosDB spec
  churn_features_online_store_spec = AzureCosmosDBSpec(
    account_uri="https://field-demo.documents.azure.com:443/",
    write_secret_prefix=f"{write_scope}/{prefix}",
    read_secret_prefix=f"{read_scope}/{prefix}"
  )

# COMMAND ----------

# DBTITLE 1,Publish from Offline to Online Store
fe.publish_table(
  name=feature_table_name,
  online_store=churn_features_online_store_spec,
  streaming = False
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Publish to Databrick's Online tables **(Preferred method)**
# MAGIC
# MAGIC You create an online table from the Catalog Explorer. The steps are described below. For more details, see the Databricks documentation ([AWS](https://docs.databricks.com/en/machine-learning/feature-store/online-tables.html#create)|[Azure](https://learn.microsoft.com/azure/databricks/machine-learning/feature-store/online-tables#create)). For information about required permissions, see Permissions ([AWS](https://docs.databricks.com/en/machine-learning/feature-store/online-tables.html#user-permissions)|[Azure](https://learn.microsoft.com/azure/databricks/machine-learning/feature-store/online-tables#user-permissions)).
# MAGIC
# MAGIC
# MAGIC #### OPTION 1: Use UI
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
# MAGIC ## Define Featurization Logic(s) for realtime/on-demand feature functions
# MAGIC
# MAGIC For features that can only/needs to be calculated in "real-time" see more info here ([AWS](https://docs.databricks.com/en/machine-learning/feature-store/on-demand-features.html)|[Azure](https://learn.microsoft.com/en-us/azure/databricks/machine-learning/feature-store/on-demand-features)) 

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Define the Python UDF
# MAGIC CREATE OR REPLACE FUNCTION avg_price_increase(monthly_charges_in DOUBLE, tenure_in DOUBLE, total_charges_in DOUBLE)
# MAGIC RETURNS FLOAT
# MAGIC LANGUAGE PYTHON
# MAGIC COMMENT "[Feature Function] Calculate potential average price increase for tenured customers based on last monthly charges and updated tenure"
# MAGIC AS $$
# MAGIC if tenure_in > 0:
# MAGIC   return monthly_charges_in - total_charges_in/tenure_in
# MAGIC else:
# MAGIC   return 0
# MAGIC $$

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## Accelerating Churn model creation using Databricks Auto-ML
# MAGIC ### A glass-box solution that empowers data teams without taking away control
# MAGIC
# MAGIC Databricks simplify model creation and MLOps. However, bootstraping new ML projects can still be long and inefficient.
# MAGIC
# MAGIC Instead of creating the same boilerplate for each new project, Databricks Auto-ML can automatically generate state of the art models for Classifications, regression, and forecast.
# MAGIC
# MAGIC
# MAGIC <img width="1000" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/auto-ml-full.png"/>
# MAGIC
# MAGIC <img style="float: right" width="600" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/churn-auto-ml.png"/>
# MAGIC
# MAGIC Models can be directly deployed, or instead leverage generated notebooks to boostrap projects with best-practices, saving you weeks of efforts.
# MAGIC
# MAGIC ### Using Databricks Auto ML with our Churn dataset
# MAGIC
# MAGIC Auto ML is available in the "Machine Learning" space. All we have to do is start a new Auto-ML experimentation and select the feature table we just created (`dbdemos.retail_username.mlops_churn_features`)
# MAGIC
# MAGIC Our prediction target is the `churn` column.
# MAGIC
# MAGIC Click on Start, and Databricks will do the rest.
# MAGIC
# MAGIC While this is done using the UI, you can also leverage the [python API](https://docs.databricks.com/applications/machine-learning/automl.html#automl-python-api-1)
# MAGIC
# MAGIC #### Join/Use features directly from the Feature Store from the [UI](https://docs.databricks.com/machine-learning/automl/train-ml-model-automl-ui.html#use-existing-feature-tables-from-databricks-feature-store) or [python API]()
# MAGIC * Select the table containing the ground-truth labels (i.e. `dbdemos.schema.mlops_churn_labels`)
# MAGIC * Join remaining features from the feature table (i.e. `dbdemos.schema.mlops_churn_features`)

# COMMAND ----------

# DBTITLE 1,Run 'baseline' autoML experiment in the back-ground
force_refresh = dbutils.widgets.get("force_refresh_automl") == "true"
display_automl_churn_link(f"{catalog}.{db}.{feature_table_name}", force_refresh = force_refresh, use_feature_table=True)

# COMMAND ----------

from pyspark.sql.functions import col
from databricks.feature_store import FeatureStoreClient
import mlflow

import databricks
from databricks import automl
from datetime import datetime

def get_automl_run(name):
  #get the most recent automl run
  df = spark.table("field_demos_metadata.automl_experiment").filter(col("name") == name).orderBy(col("date").desc()).limit(1)
  return df.collect()

#Get the automl run information from the field_demos_metadata.automl_experiment table. 
#If it's not available in the metadata table, start a new run with the given parameters
def get_automl_run_or_start(name, model_name, dataset, target_col, timeout_minutes):
  spark.sql("create database if not exists field_demos_metadata")
  spark.sql("create table if not exists field_demos_metadata.automl_experiment (name string, date string)")
  result = get_automl_run(name)
  if len(result) == 0:
    print("No run available, start a new Auto ML run, this will take a few minutes...")
    start_automl_run(name, model_name, dataset, target_col, timeout_minutes)
    result = get_automl_run(name)
  return result[0]


#Start a new auto ml classification task and save it as metadata.
def start_automl_run(name, model_name, dataset, target_col, timeout_minutes = 5):
  automl_run = databricks.automl.classify(
    dataset = dataset,
    target_col = target_col,
    timeout_minutes = timeout_minutes
  )
  experiment_id = automl_run.experiment.experiment_id
  path = automl_run.experiment.name
  data_run_id = mlflow.search_runs(experiment_ids=[automl_run.experiment.experiment_id], filter_string = "tags.mlflow.source.name='Notebook: DataExploration'").iloc[0].run_id
  exploration_notebook_id = automl_run.experiment.tags["_databricks_automl.exploration_notebook_id"]
  best_trial_notebook_id = automl_run.experiment.tags["_databricks_automl.best_trial_notebook_id"]

  cols = ["name", "date", "experiment_id", "experiment_path", "data_run_id", "best_trial_run_id", "exploration_notebook_id", "best_trial_notebook_id"]
  spark.createDataFrame(data=[(name, datetime.today().isoformat(), experiment_id, path, data_run_id, automl_run.best_trial.mlflow_run_id, exploration_notebook_id, best_trial_notebook_id)], schema = cols).write.mode("append").option("mergeSchema", "true").saveAsTable("field_demos_metadata.automl_experiment")
  #Create & save the first model version in the MLFlow repo (required to setup hooks etc)
  mlflow.register_model(f"runs:/{automl_run.best_trial.mlflow_run_id}/model", model_name)
  return get_automl_run(name)

#Generate nice link for the given auto ml run
def display_automl_link(name, model_name, dataset, target_col, force_refresh=False, timeout_minutes = 5):
  r = get_automl_run_or_start(name, model_name, dataset, target_col, timeout_minutes)
  html = f"""For exploratory data analysis, open the <a href="/#notebook/{r["exploration_notebook_id"]}">data exploration notebook</a><br/><br/>"""
  html += f"""To view the best performing model, open the <a href="/#notebook/{r["best_trial_notebook_id"]}">best trial notebook</a><br/><br/>"""
  html += f"""To view details about all trials, navigate to the <a href="/#mlflow/experiments/{r["experiment_id"]}/s?orderByKey=metrics.%60val_f1_score%60&orderByAsc=false">MLflow experiment</>"""
  displayHTML(html)


def display_automl_churn_link(): 
  display_automl_link("churn_auto_ml", "field_demos_customer_churn", spark.table("churn_features"), "churn", 5)

def get_automl_churn_run(): 
  return get_automl_run_or_start("churn_auto_ml", "field_demos_customer_churn", spark.table("churn_features"), "churn", 5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Using the generated notebook to build our model
# MAGIC
# MAGIC Next step: [Explore the generated Auto-ML notebook]($./02_automl_champion)
# MAGIC
# MAGIC **Note:**
# MAGIC For demo purposes, run the above notebook OR create and register a new version of the model from your autoML experiment and label/alias the model as "Champion"
