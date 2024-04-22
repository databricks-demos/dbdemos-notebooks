# Databricks notebook source
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
# MAGIC %pip install databricks-sdk==0.23.0
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../_resources/00-setup $reset_all_data='false'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exploratory Data Anaylsis
# MAGIC To get a feel of the data, what needs cleaning, pre-processing etc.
# MAGIC - **Use Databricks's native visualization tools**
# MAGIC - Bring your own visualization library of choice (i.e. seaborn, plotly)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM mlops_churn_bronze_customers

# COMMAND ----------

telco_df = spark.read.table("mlops_churn_bronze_customers").to_pandas_on_spark()
telco_df["internet_service"].value_counts().plot.pie()

# COMMAND ----------

# DBTITLE 1,Read in Bronze Delta table using Spark
# Read into Spark
telcoDF = spark.read.table("mlops_churn_bronze_customers")
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
# MAGIC ### Using Pandas On Spark API
# MAGIC
# MAGIC Because our Data Scientist team is familiar with Pandas, we'll use the [pandas on spark API](https://spark.apache.org/docs/latest/api/python/reference/pyspark.pandas/index.html) to scale `pandas` code. The Pandas instructions will be converted in the spark engine under the hood and distributed at scale.
# MAGIC
# MAGIC *Note: Starting from `spark 3.2`, koalas is builtin and we can get an Pandas Dataframe using `pandas_api()`.*

# COMMAND ----------

# DBTITLE 1,Define featurization function
import pyspark.sql.functions as F
from pyspark.sql import DataFrame

def clean_churn_features(dataDF: DataFrame) -> DataFrame:
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

  def sum_optional_services(df):
      """Count number of optional services enabled, like streaming TV"""
      cols = ["online_security", "online_backup", "device_protection", "tech_support",
              "streaming_tv", "streaming_movies"]
      return sum(map(lambda c: (df[c] == "Yes"), cols))

  data_psdf["num_optional_services"] = sum_optional_services(data_psdf)

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
churn_features = clean_churn_features(telcoDF) 
display(churn_features)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Extract ground-truth labels in a separate table to avoid label leakage

# COMMAND ----------

# DBTITLE 1,Extract ground-truth labels in a separate table and drop from Feature table
# Extract labels in separate table before pushing to Feature Store to avoid label leakage
(churn_features.select("customer_id", "churn")
                .write.mode("overwrite").option("overwriteSchema", "true")
                .saveAsTable("mlops_churn_labels"))

churn_features = churn_features.drop("churn")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Feature Table and write to offline-store
# MAGIC One-time setup

# COMMAND ----------

from databricks.feature_engineering import FeatureEngineeringClient
fe = FeatureEngineeringClient()
# delete a feature store stable if it exists
delete_feature_store_table(catalog, db, "mlops_churn_features")

fe.create_table(
  name="mlops_churn_features",
  primary_keys=["customer_id"],
  schema=churn_features.schema,
  description=f"These features are derived from the {catalog}.{db}.mlops_churn_bronze_customers table in the lakehouse. We created service features, cleaned up their names.  No aggregations were performed. [Warning: This table doesn't store the ground-truth and now can be used with AutoML's Feature Store integration"
)

# COMMAND ----------

# DBTITLE 1,Write feature values to Feature Store
fe.write_table(
  name=f"{catalog}.{db}.mlops_churn_features",
  df=churn_features # can be a streaming dataframe as well
)

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
from databricks import automl
from datetime import datetime

xp_path = "/Shared/dbdemos/experiments/mlops"
xp_name = f"automl_churn_{datetime.now().strftime('%Y-%m-%d_%H:%M:%S')}"
training_dataset = fe.read_table(name=f'{catalog}.{db}.mlops_churn_features').join(spark.table('mlops_churn_labels'), ["customer_id"])
automl_run = automl.classify(
    experiment_name = xp_name,
    experiment_dir = xp_path,
    dataset = training_dataset,
    target_col = "churn",
    timeout_minutes = 5
)
#Make sure all users can access dbdemos shared experiment
DBDemos.set_experiment_permission(f"{xp_path}/{xp_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Using the generated notebook to build our model
# MAGIC
# MAGIC Next step: [Explore the generated Auto-ML notebook]($./02_automl_champion)
# MAGIC
# MAGIC **Note:**
# MAGIC For demo purposes, run the above notebook OR create and register a new version of the model from your autoML experiment and label/alias the model as "Champion"
