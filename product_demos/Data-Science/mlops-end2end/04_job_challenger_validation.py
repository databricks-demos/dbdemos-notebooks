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

# MAGIC %run ./_resources/00-setup $reset_all_data=false $catalog="dbdemos"

# COMMAND ----------

# DBTITLE 1,Create job parameters input widgets
dbutils.widgets.text("model_name",model_name,"Model Name")
dbutils.widgets.text("version",get_latest_model_version(model_name),"Model Version")
dbutils.widgets.dropdown("to_stage","Challenger",["Challenger", "Champion", "Baseline", "Archived"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fetch Model information

# COMMAND ----------

# Get the model in transition, its name and version from the metadata received by the webhook
model_name = dbutils.widgets.get("model_name")
model_version = dbutils.widgets.get("version")
model_stage = dbutils.widgets.get("to_stage")
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

from databricks.feature_engineering import FeatureEngineeringClient
import pandas as pd


fe = FeatureEngineeringClient()

# Load model as a Spark UDF
model_uri = f"models:/{model_name}/{model_version}"

# Predict on a Spark DataFrame
try:
  # Read labels and IDs
  labelsDF = spark.read.table(run_info.data.tags['labels_table'])

  # Batch score
  features_w_preds = fe.score_batch(df=labelsDF, model_uri=model_uri, result_type=labelsDF.schema[label_col].dataType)
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
# MAGIC #### Signature check
# MAGIC
# MAGIC When working with ML models you often need to know some basic functional properties of the model at hand, such as “What inputs does it expect?” and “What output does it produce?”.  The model **signature** defines the schema of a model’s inputs and outputs. Model inputs and outputs can be either column-based or tensor-based.
# MAGIC
# MAGIC See [here](https://mlflow.org/docs/latest/models.html#signature-enforcement) for more details.

# COMMAND ----------

loaded_model = mlflow.pyfunc.spark_udf(spark, model_uri=model_uri)
if not loaded_model.metadata.signature:
  print("This model version is missing a signature.  Please push a new version with a signature!  See https://mlflow.org/docs/latest/models.html#model-metadata for more details.")
  client.set_model_version_tag(name=model_name, version=model_version, key="has_signature", value=False)
else:
  client.set_model_version_tag(name=model_name, version=model_version, key="has_signature", value=True)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Demographic accuracy
# MAGIC
# MAGIC How does the model perform across various slices of the customer base?

# COMMAND ----------

import numpy as np

try:
  # Cast to Pandas given small size
  features_w_preds_pdf = features_w_preds.toPandas()

  # Calculate error
  features_w_preds_pdf['accurate'] = np.where(features_w_preds_pdf.churn == features_w_preds_pdf.prediction, 1, 0)

  # Check run tags for demographic columns and accuracy in each segment
  demographics = run_info.data.tags['demographic_vars'].split(",")
  slices = features_w_preds_pdf.groupby(demographics).accurate.agg(acc = 'sum', obs = lambda x:len(x), pct_acc = lambda x:sum(x)/len(x))

  # Threshold for passing on demographics is 55%
  demo_test = True if slices['pct_acc'].any() > 0.55 else False

  # Set tags in registry
  client.set_model_version_tag(name=model_name, version=model_version, key="demographics_test", value=demo_test)

  print(slices)

except Exception as e:
  print(e)
  print("KeyError: No demographics_vars tagged with this model version.")
  client.set_model_version_tag(name=model_name, version=model_version, key="demographics_test", value=False)
  pass

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
# MAGIC #### Artifact check
# MAGIC Has the data scientist logged supplemental artifacts along with the original model?

# COMMAND ----------

import os

# Create local directory
local_dir = "/tmp/model_artifacts"
if not os.path.exists(local_dir):
    os.mkdir(local_dir)

# Download artifacts from tracking server - no need to specify DBFS path here
local_path = mlflow.artifacts.download_artifacts(run_id=run_info.info.run_id,dst_path=local_dir)

# Tag model version as possessing artifacts or not
if not os.listdir(local_path):
  client.set_model_version_tag(name=model_name, version=model_version, key="has_artifacts", value=False)
  print("There are no artifacts associated with this model.  Please include some data visualization or data profiling.  MLflow supports HTML, .png, and more.")

else:
  client.set_model_version_tag(name=model_name, version=model_version, key = "has_artifacts", value = True)
  print("Artifacts downloaded in: {}".format(local_path))
  print("Artifacts: {}".format(os.listdir(local_path)))

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
# MAGIC ## Slack webhook notification
# MAGIC OPTIONAL

# COMMAND ----------

# MAGIC %md
# MAGIC ### Slack webhook setup (OPTIONAL pre-requisite)
# MAGIC
# MAGIC 1. Create a [new Slack Workspace](https://slack.com/get-started#/create) (call it MLOps-Env for example)
# MAGIC 2. Create a Slack App in this new workspace **(NOT Databricks workspace)**, activate webhooks and **copy the URL** - more info [here](https://api.slack.com/messaging/webhooks#getting_started).
# MAGIC 3. In order NOT TO expose it, store it in a secret using the [Secrets API](https://docs.databricks.com/dev-tools/api/latest/secrets.html#secretsecretservicecreatescope) while [authenticating with a PAT](https://docs.databricks.com/dev-tools/api/latest/authentication.html#generate-a-personal-access-token)
# MAGIC
# MAGIC ```
# MAGIC curl --netrc --request POST \
# MAGIC https://<databricks-instance>/api/2.0/secrets/scopes/create \
# MAGIC --data '{
# MAGIC   "scope": "fieldeng", #This scope already exists on AWS/E2 and Azure Field-Eng shards so no need to do it
# MAGIC   "initial_manage_principal": "users"
# MAGIC }'
# MAGIC ```
# MAGIC then
# MAGIC ```
# MAGIC curl --netrc --request POST \
# MAGIC https://<databricks-instance>/api/2.0/secrets/put \
# MAGIC --data '{
# MAGIC "scope": "fieldeng",
# MAGIC "key": "username_slack_webhook", # username == {current_user_no_at}
# MAGIC "string_value": "https://hooks.slack.com/services/T00000000/B00000000/XXXXXXXXXXXXXXXXXXXXXXXX"
# MAGIC }'
# MAGIC ```
# MAGIC 4. Alternatively you can use the [Databricks CLI](https://docs.databricks.com/dev-tools/cli/index.html)
# MAGIC ```
# MAGIC databricks secrets put --scope fieldeng --key username_slack_webhook --profile <databricks-instance>
# MAGIC ```

# COMMAND ----------

if slack_webhook.strip():
  slack_message = f"Model <b>{model_name}</b> version <b>{model_version}</b> test results: {results.tags}"
  send_notification(slack_message+f"- Staging transition <b>{validation_status}</b>", slack_webhook)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Congratulation, our model is now automatically tested and will be transitioned accordingly
# MAGIC
# MAGIC We now have the certainty that our model is ready to be used as it matches our quality standard.
# MAGIC
# MAGIC
# MAGIC Next: [Run batch inference from our Challenger model]($./05_batch_inference)
