# Databricks notebook source
#dbutils.widgets.dropdown("force_refresh_automl", "true", ["false", "true"], "Restart AutoML run")

# COMMAND ----------

# MAGIC %md
# MAGIC # Churn Prediction Feature Engineering
# MAGIC Our first step is to analyze the data and build the features we'll use to train our model. Let's see how this can be done.
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/mlops/advanced/banners/mlflow-uc-end-to-end-advanced-1.png?raw=true" width="1200">
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Ffeatures%2Fmlops%2F02_feature_prep&dt=MLOPS">
# MAGIC <!-- [metadata={"description":"MLOps end2end workflow: Feature engineering",
# MAGIC  "authors":["quentin.ambard@databricks.com"],
# MAGIC  "db_resources":{},
# MAGIC   "search_tags":{"vertical": "retail", "step": "Data Engineering", "components": ["feature store"]},
# MAGIC                  "canonicalUrl": {"AWS": "", "Azure": "", "GCP": ""}}] -->

# COMMAND ----------

# MAGIC %pip install --quiet mlflow==2.19 databricks-feature-engineering==0.8.0
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../_resources/00-setup $reset_all_data=false $adv_mlops=true

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exploratory Data Anaylsis
# MAGIC To get a feel of the data, what needs cleaning, pre-processing etc.
# MAGIC - **Use Databricks's native visualization tools**
# MAGIC - Bring your own visualization library of choice (i.e. seaborn, plotly)

# COMMAND ----------

# DBTITLE 1,Read in Bronze Delta table using Spark
# Read into spark dataframe
telcoDF = spark.read.table("advanced_churn_bronze_customers")
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

from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql.functions import pandas_udf, col, when, lit


#  Count number of optional services enabled, like streaming TV
def compute_service_features(inputDF: SparkDataFrame) -> SparkDataFrame:
  # Create pandas UDF function
  @pandas_udf('double')
  def num_optional_services(*cols):
    # Nested helper function to count number of optional services in a pandas dataframe
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
  data_psdf = data_psdf.astype({"senior_citizen": "string"})
  data_psdf["senior_citizen"] = data_psdf["senior_citizen"].map({"1" : "Yes", "0" : "No"})

  data_psdf["total_charges"] = data_psdf["total_charges"].apply(lambda x: float(x) if x.strip() else 0)

  # Fill some missing numerical values with 0
  data_psdf = data_psdf.fillna({"tenure": 0.0})
  data_psdf = data_psdf.fillna({"monthly_charges": 0.0})
  data_psdf = data_psdf.fillna({"total_charges": 0.0})

  # Add/Force semantic data types for specific colums (to facilitate autoML)
  data_cleanDF = data_psdf.to_spark()
  data_cleanDF = data_cleanDF.withMetadata("customer_id", {"spark.contentAnnotation.semanticType":"native"})
  data_cleanDF = data_cleanDF.withMetadata("num_optional_services", {"spark.contentAnnotation.semanticType":"numeric"})

  return data_cleanDF

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## Compute & Write to Feature Store
# MAGIC
# MAGIC Once our features are ready, we'll save them in Databricks Feature Store. Any Delta Table registered to Unity Catalog can be used as a feature table.
# MAGIC
# MAGIC This will allows us to leverage Unity Catalog for governance, discoverability and reusability of our features accross our organization, as well as increasing team efficiency.
# MAGIC
# MAGIC The lineage capability in Unity Catalog brings traceability and governance in our deployment, knowing which model is dependent of which feature tables.

# COMMAND ----------

# DBTITLE 1,Compute Churn Features and append a timestamp
from datetime import datetime
from pyspark.sql.functions import lit


# Add current scoring timestamp
this_time = (datetime.now()).timestamp()
churn_features_n_predsDF = clean_churn_features(compute_service_features(telcoDF)) \
                            .withColumn("transaction_ts", lit(this_time).cast("timestamp"))

display(churn_features_n_predsDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Extract ground-truth labels in a separate table to avoid label leakage
# MAGIC * In reality ground-truth label data should be in its own separate table

# COMMAND ----------

# DBTITLE 1,Extract ground-truth labels in a separate table and drop from Feature table
import pyspark.sql.functions as F


# Best practice: specify train-val-test split as categorical label (to be used by automl and/or model validation jobs)
train_ratio, val_ratio, test_ratio = 0.7, 0.2, 0.1

churn_features_n_predsDF.select("customer_id", "transaction_ts", "churn") \
                        .withColumn("random", F.rand(seed=42)) \
                        .withColumn("split",
                                    F.when(F.col("random") < train_ratio, "train")
                                    .when(F.col("random") < train_ratio + val_ratio, "validate")
                                    .otherwise("test")) \
                        .drop("random") \
                        .write.format("delta") \
                        .mode("overwrite").option("overwriteSchema", "true") \
                        .saveAsTable(f"advanced_churn_label_table")

churn_featuresDF = churn_features_n_predsDF.drop("churn")

# COMMAND ----------

# MAGIC %md
# MAGIC Add primary keys constraints to labels table for feature lookup

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE advanced_churn_label_table ALTER COLUMN customer_id SET NOT NULL;
# MAGIC ALTER TABLE advanced_churn_label_table ALTER COLUMN transaction_ts SET NOT NULL;
# MAGIC ALTER TABLE advanced_churn_label_table ADD CONSTRAINT advanced_churn_label_table_pk PRIMARY KEY(customer_id, transaction_ts);

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write the feature table to Unity Catalog
# MAGIC
# MAGIC With Unity Catalog, any Delta table with a primary key constraint can be used as a offline feature table.
# MAGIC
# MAGIC Time series feature tables have an additional primary key on the time column.
# MAGIC
# MAGIC After the table is created, you can write data to it like other Delta tables, and use it as a feature table.
# MAGIC
# MAGIC Here, we demonstrate creating the feature table using the `FeatureEngineeringClient` API. You can also easily create it using SQL:
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC ```
# MAGIC CREATE TABLE {catalog}.{db}.{feature_table_name} (
# MAGIC   {primary_key} int NOT NULL,
# MAGIC   {timestamp_col} timestamp NOT NULL,
# MAGIC   feat1 long,
# MAGIC   feat2 varchar(100),
# MAGIC   CONSTRAINT customer_features_pk PRIMARY KEY ({primary_key}, {timestamp_col} TIMESERIES)
# MAGIC );
# MAGIC ```
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC First, since we are creating the feature table from scratch, we want to make sure that our environment is clean and any previously created offline/online feature tables are deleted.

# COMMAND ----------

# DBTITLE 1,Drop any existing online table (optional)
from pprint import pprint
from databricks.sdk import WorkspaceClient


# Create workspace client
w = WorkspaceClient()

# Remove any existing online feature table
try:
  online_table_specs = w.online_tables.get(f"{catalog}.{db}.advanced_churn_feature_table_online_table")
  # Drop existing online feature table
  w.online_tables.delete(f"{catalog}.{db}.advanced_churn_feature_table_online_table")
  print(f"Dropping online feature table: {catalog}.{db}.advanced_churn_feature_table_online_table")

except Exception as e:
  pprint(e)

# COMMAND ----------

# DBTITLE 1,Drop feature table if it already exists
# MAGIC %sql
# MAGIC -- We are creating the feature table from scratch.
# MAGIC -- Let's drop any existing feature table if it exists
# MAGIC DROP TABLE IF EXISTS advanced_churn_feature_table;

# COMMAND ----------

# DBTITLE 1,Import Feature Store Client
from databricks.feature_engineering import FeatureEngineeringClient


fe = FeatureEngineeringClient()

# COMMAND ----------

# DBTITLE 1,Create "feature"/UC table
churn_feature_table = fe.create_table(
  name="advanced_churn_feature_table", # f"{catalog}.{dbName}.{feature_table_name}"
  primary_keys=["customer_id", "transaction_ts"],
  schema=churn_featuresDF.schema,
  timeseries_columns="transaction_ts",
  description=f"These features are derived from the {catalog}.{db}.{bronze_table_name} table in the lakehouse. We created service features, cleaned up their names.  No aggregations were performed. [Warning: This table doesn't store the ground-truth and now can be used with AutoML's Feature Store integration"
)

# COMMAND ----------

# DBTITLE 1,Write feature values to Feature Store
fe.write_table(
  name=f"{catalog}.{db}.advanced_churn_feature_table",
  df=churn_featuresDF, # can be a streaming dataframe as well
  mode='merge' #'merge' supports schema evolution
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Featurization Logic for on-demand feature functions
# MAGIC
# MAGIC We will define a function for features that need to be calculated on-demand. These functions can be used in both batch/offline and serving/online inference.
# MAGIC
# MAGIC It is common that customers who have elevated bills of monthly charges have a higher propensity to churn. The `avg_price_increase` function calculates the potential average price increase based on their historical charges, as well as their current tenure. The function lets the model use this freshly calculated value as a feature for training and, later, scoring.
# MAGIC
# MAGIC This function is defined under Unity Catalog, which provides governance over who can use the function.
# MAGIC
# MAGIC Refer to the documentation for more information. ([AWS](https://docs.databricks.com/en/machine-learning/feature-store/on-demand-features.html)|[Azure](https://learn.microsoft.com/en-us/azure/databricks/machine-learning/feature-store/on-demand-features)) 

# COMMAND ----------

# MAGIC %sql
# MAGIC   CREATE OR REPLACE FUNCTION avg_price_increase(monthly_charges_in DOUBLE, tenure_in DOUBLE, total_charges_in DOUBLE)
# MAGIC   RETURNS FLOAT
# MAGIC   LANGUAGE PYTHON
# MAGIC   COMMENT "[Feature Function] Calculate potential average price increase for tenured customers based on last monthly charges and updated tenure"
# MAGIC   AS $$
# MAGIC   if tenure_in > 0:
# MAGIC     return monthly_charges_in - total_charges_in/tenure_in
# MAGIC   else:
# MAGIC     return 0
# MAGIC   $$

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE FUNCTION avg_price_increase;

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
# MAGIC Models can be directly deployed, or instead leverage generated notebooks to boostrap projects with best-practices, saving you weeks of efforts.
# MAGIC
# MAGIC <img width="1000" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/auto-ml-full.png"/>
# MAGIC
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC ### Using Databricks Auto ML with our Churn dataset
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC <img style="float: right" width="600" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/churn-auto-ml.png"/>
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC Auto ML is available under **Machine Learning - Experiments**. All we have to do is create a new Auto-ML experiment and select the table containing the ground-truth labels and join it with the features in the feature table.
# MAGIC
# MAGIC Our prediction target is the `churn` column.
# MAGIC
# MAGIC Click on **Start**, and Databricks will do the rest.
# MAGIC
# MAGIC While this is done using the UI, you can also leverage the [python API](https://docs.databricks.com/applications/machine-learning/automl.html#automl-python-api-1)
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC #### Join/Use features directly from the Feature Store from the [UI](https://docs.databricks.com/machine-learning/automl/train-ml-model-automl-ui.html#use-existing-feature-tables-from-databricks-feature-store) or [python API]()
# MAGIC * Select the table containing the ground-truth labels (i.e. `dbdemos.schema.churn_label_table`)
# MAGIC * Join remaining features from the feature table (i.e. `dbdemos.schema.churn_feature_table`)
# MAGIC
# MAGIC Refer to the __Quickstart__ version of this demo for an example of AutoML in action.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Using the AutoML-generated notebook to build our model
# MAGIC
# MAGIC We have pre-run AutoML, which generated the notebook that trained the best model in the AutoML run. We take this notebook and improve on the model.
# MAGIC
# MAGIC Next step: [Explore the modfied version of the notebook generated from Auto-ML]($./02_automl_champion)
