# Databricks notebook source
# MAGIC %md
# MAGIC # Model validation JOB
# MAGIC
# MAGIC This notebook execution is automatically triggered using MLFLow webhook. It's defined as a **job** and will programatically validate the model before labelling/alias it to `Challenger`.
# MAGIC
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/mlops-end2end-flow-5.png" width="1200">
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Ffeatures%2Fmlops%2F05_job_staging_validation&dt=MLOPS">
# MAGIC <!-- [metadata={"description":"MLOps end2end workflow: Trigger Model testing and validation job.",
# MAGIC  "authors":["quentin.ambard@databricks.com"],
# MAGIC  "db_resources":{},
# MAGIC   "search_tags":{"vertical": "retail", "step": "Model testing", "components": ["mlflow"]},
# MAGIC                  "canonicalUrl": {"AWS": "", "Azure": "", "GCP": ""}}] -->

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## General Validation Checks
# MAGIC
# MAGIC <img style="float: right" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/churn-mlflow-webhook-1.png" width=600 >
# MAGIC
# MAGIC In the context of MLOps, there are more tests than simply how accurate a model will be.  To ensure the stability of our ML system and compliance with any regulatory requirements, we will subject each model added to the registry to a series of validation checks.  These include, but are not limited to:
# MAGIC <br><br>
# MAGIC * __Inference on production data__
# MAGIC * __Input schema ("signature") compatibility with current model version__
# MAGIC * __Accuracy on multiple slices of the training data__
# MAGIC * __Model documentation__
# MAGIC
# MAGIC In this notebook we explore some approaches to performing these tests, and how we can add metadata to our models with tagging if they have passed a given test or not.
# MAGIC
# MAGIC This part is typically specific to your line of business and quality requirement.
# MAGIC
# MAGIC For each test, we'll add information using tags to know what has been validated in the model. We can also add Comments if needed.

# COMMAND ----------

# MAGIC %run ../_resources/00-setup

# COMMAND ----------

# DBTITLE 1,Create job parameters input widgets
def get_latest_model_version(model_name):
  model_version_infos = MlflowClient().search_model_versions("name = '%s'" % model_name)
  return max([model_version_info.version for model_version_info in model_version_infos])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fetch Model information

# COMMAND ----------

# Get the model in transition, its name and version from the metadata received by the webhook
model_name = f"{catalog}.{db}.mlops_churn"
model_version = get_latest_model_version(model_name)
model_stage = "Challenger" #,["Challenger", "Champion", "Baseline", "Archived"])

print(f"Validating {model_stage} request for model {model_name} version {model_version}")

# COMMAND ----------

client = MlflowClient()
model_details = client.get_model_version(model_name, model_version)
run_info = client.get_run(run_id=model_details.run_id)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Validate prediction
# MAGIC
# MAGIC We want to test to see that the model can predict on production data.  So, we will load the model and the latest from the feature store and test making some predictions.

# COMMAND ----------

feature_df = spark.read.table(run_info.data.tags['feature_table'])
label_df = spark.read.table(run_info.data.tags['labels_table'])
label_df.schema[label_col].dataType

loaded_model = mlflow.pyfunc.spark_udf(spark, model_uri=f"models:/{model_name}/{model_version}", result_type=label_df.schema[label_col].dataType)

# COMMAND ----------

from databricks.feature_engineering import FeatureEngineeringClient
import pandas as pd


# Predict on a Spark DataFrame
try:
  # Read labels and IDs
  feature_df = spark.read.table(run_info.data.tags['feature_table'])
  label_df = spark.read.table(run_info.data.tags['labels_table'])

  # Load model as a Spark UDF
  model_udf = mlflow.pyfunc.spark_udf(spark, model_uri=f"models:/{model_name}/{model_version}")

  # Batch score
  features_w_preds = feature_df.withColumn(label_col, model_udf(*feature_df.columns))
  display(features_w_preds)
  client.set_model_version_tag(name=model_name, version=model_version, key="predicts", value=True)

except Exception as e:
  print(e)
  features_w_preds = spark.createDataFrame([], StructType([]))
  print("Unable to predict on features.")
  client.set_model_version_tag(name=model_name, version=model_version, key="predicts", value=False)
  pass

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Making sure our model behaves as the previous one
# MAGIC
# MAGIC How does the model perform across various slices of the customer base?

# COMMAND ----------

TODO

# COMMAND ----------

# MAGIC %md
# MAGIC #### Description check
# MAGIC
# MAGIC Has the data scientist provided a description of the model being submitted?

# COMMAND ----------

# If there's no description or an insufficient number of charaters, tag accordingly
if not model_details.description:
  client.set_model_version_tag(name=model_name, version=model_version, key="has_description", value=False)
  print("Please add model description")
elif not len(model_details.description) > 20:
  client.set_model_version_tag(name=model_name, version=model_version, key="has_description", value=False)
  print("Please add detailed model description (40 char min).")
else:
  client.set_model_version_tag(name=model_name, version=model_version, key="has_description", value=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Results
# MAGIC
# MAGIC Here's a summary of the testing results:

# COMMAND ----------

results = client.get_model_version(model_name, model_version)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Move to new stage or archive version
# MAGIC
# MAGIC The next phase of this models' lifecycle will be to `Challenger` or `Archived`, depending on how it fared in testing.

# COMMAND ----------

# If any checks failed, reject/set 'validation_status' tag to 'FAILED' and remove an alias
if False in results.tags.values():
  print("Rejecting transition...")
  validation_status = "FAILED"
  alias = "ARCHIVED"

else:
  print("Accepting transition...")
  validation_status = "PASSED"
  alias = model_stage

# COMMAND ----------

# Update validation tag
client.set_model_version_tag(
  name=model_name,
  version=model_version,
  key='validation_status',
  value=validation_status
)

# Update/Set model alias
client.set_registered_model_alias(
  name=model_name,
  alias=alias,
  version=model_version
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Congratulation, our model is now automatically tested and will be transitioned accordingly
# MAGIC
# MAGIC We now have the certainty that our model is ready to be used as it matches our quality standard.
# MAGIC
# MAGIC
# MAGIC Next: [Run batch inference from our Challenger model]($./05_batch_inference)
