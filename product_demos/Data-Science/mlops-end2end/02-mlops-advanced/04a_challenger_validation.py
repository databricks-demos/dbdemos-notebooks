# Databricks notebook source
# MAGIC %md
# MAGIC #  `Challenger` Model Validation
# MAGIC
# MAGIC This notebook checks for approval tags on the new candidate model (i.e. __Challenger__) and is the first task of the **MLflow 3's Model Deployment Job**.
# MAGIC
# MAGIC [Deployment jobs](https://docs.databricks.com/aws/en/mlflow/deployment-job) integrate naturally with the newly released [Unity Catalog Create Model Version ACL](https://docs.databricks.com/aws/en/data-governance/unity-catalog/manage-privileges/privileges#create-model-version) to complete your MLOps story. Specifically, a user can be granted the **`CREATE MODEL VERSION ACL`** to register new model versions to the model. Once the user deems a model good enough for production, they will register this version to the Unity Catalog model, which will kick off this deployment job to automatically evaluate the model.
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/mlops/advanced/banners/mlflow-uc-end-to-end-advanced-4a-v2.png?raw=true" width="1200">
# MAGIC
# MAGIC *Note: In a typical mlops setup, this would run as part of an automated deployment job to validate a new model. We'll run this demo as an interactive notebook.*
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable the collection or the tracker during installation. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=lakehouse&notebook=04_challenger_validation&demo_name=mlops-end2end&event=VIEW">

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## General Validation Checks
# MAGIC
# MAGIC <!--img style="float: right" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/churn-mlflow-webhook-1.png" width=600 -->
# MAGIC
# MAGIC In the context of MLOps, there are more tests than simply how accurate a model will be.  To ensure the stability of our ML system and compliance with any regulatory requirements, we will subject each model added to the registry to a series of validation checks.  These include, but are not limited to:
# MAGIC <br>
# MAGIC * __Model documentation__
# MAGIC * __Inference on production data__
# MAGIC * __Champion-Challenger testing to ensure that business KPIs are acceptable__
# MAGIC
# MAGIC In this notebook, we explore some approaches to performing these tests, and how we can add metadata to our models by tagging if they have passed a given test.
# MAGIC
# MAGIC This part is typically specific to your line of business and quality requirements.
# MAGIC
# MAGIC For each test, we'll add information using tags to know what has been validated in the model. We can also add Comments to a model if needed.

# COMMAND ----------

# MAGIC %md
# MAGIC Last environment tested:
# MAGIC ```
# MAGIC databricks-feature-engineering==0.13.0a8
# MAGIC mlflow==3.3.2
# MAGIC ```

# COMMAND ----------

# MAGIC %pip install --quiet databricks-feature-engineering>=0.13.0a8 mlflow --upgrade
# MAGIC
# MAGIC
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %run ../_resources/00-setup $adv_mlops=true

# COMMAND ----------

dbutils.widgets.text("model_name", f"{catalog}.{db}.advanced_mlops_churn", "Model Name") # Will be populated from Deployment Jobs Parameters
dbutils.widgets.text("model_version", "1", "Model Version") # Will be populated from Deployment Jobs Parameters

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fetch Model information
# MAGIC
# MAGIC We will fetch the model information for the latest model version from Unity Catalog.

# COMMAND ----------

import mlflow
from mlflow.tracking.client import MlflowClient

# COMMAND ----------

# Fully qualified model name
model_name = dbutils.widgets.get("model_name")
model_version = dbutils.widgets.get("model_version")
model_alias = "Challenger" # Label to test/validate

client = MlflowClient()
model_details = client.get_model_version(model_name, model_version)

run_info = client.get_run(run_id=model_details.run_id)

print(f"Validating {model_alias} model for {model_name} on model version {model_version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Model checks

# COMMAND ----------

# MAGIC %md
# MAGIC #### Description check
# MAGIC
# MAGIC Has the data scientist provided a description of the model being submitted?

# COMMAND ----------

# If there's no description or an insufficient number of characters, tag accordingly
if not model_details.description:
  has_description = False
  print("Please add model description")

elif not len(model_details.description) > 20:
  has_description = False
  print("Please add detailed model description (40 char min).")
  
else:
  has_description = True

print(f'Model {model_name} version {model_details.version} has description: {has_description}')
client.set_model_version_tag(name=model_name, version=str(model_details.version), key="has_description", value=has_description)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Validate prediction
# MAGIC
# MAGIC We want to test to see that the model can predict on production data.  So, we will load the model and the latest from the feature store and test by making some predictions.
# MAGIC
# MAGIC **NOTE: This may take ~7mins since we're creating a new virtual environment (env_manager=`virtualenv`) to run the inference and not the serverless cluster's default environment itself.**

# COMMAND ----------

from databricks.feature_engineering import FeatureEngineeringClient
from pyspark.sql.types import StructType
import pandas as pd


fe = FeatureEngineeringClient()

# Load model as a Spark UDF
model_uri = f"models:/{model_name}/{model_version}"
label_col = "churn"

# Predict on a Spark DataFrame
try:
  # Read labels and IDs
  labelsDF = spark.read.table("advanced_churn_label_table").filter("split='test'").limit(10)

  # Batch score
  features_w_preds = fe.score_batch(
    df=labelsDF,
    model_uri=model_uri,
    result_type=labelsDF.schema[label_col].dataType,
    env_manager="virtualenv")
  
  display(features_w_preds)
  predicts_check = True

except Exception as e:
  print(e)
  features_w_preds = spark.createDataFrame([], StructType([]))
  print("Unable to predict on features.")
  predicts_check = False

  pass

client.set_model_version_tag(name=model_name, version=str(model_version), key="predicts", value=predicts_check)

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

# Download artifacts from tracking server
local_path = mlflow.artifacts.download_artifacts(run_id=run_info.info.run_id, dst_path=local_dir)

# Tag model version as possessing artifacts or not
if not os.listdir(local_path):
  has_artifacts = False
  print("There are no artifacts associated with this model.  Please include some data/metrics visualization or data profiling.  MLflow supports HTML, .png, and more.")

else:
  has_artifacts = True
  print("Artifacts downloaded in: {}".format(local_path))
  print("Artifacts: {}".format(os.listdir(local_path)))

client.set_model_version_tag(name=model_name, version=model_version, key="has_artifacts", value=has_artifacts)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Model performance metric
# MAGIC
# MAGIC We want to validate the model performance metric. Typically, we want to compare this metric obtained for the `@Challenger` model against that of the `@Champion` model. Since we have yet to register a `@Champion` model, we will only retrieve the metric for the `@Challenger` model without doing a comparison.
# MAGIC
# MAGIC The registered model captures information about the MLflow experiment run, where the model metrics were logged during training. This gives traceability from the deployed model back to the initial training runs.
# MAGIC
# MAGIC Here, we will use the F1 score for the out-of-sample test data that was set aside at training time.

# COMMAND ----------

model_run_id = model_details.run_id
f1_score = mlflow.get_run(model_run_id).data.metrics['test_f1_score']

try:
    # Compare the challenger f1 score to the existing champion if it exists
    champion_model = client.get_model_version_by_alias(model_name, "Champion")
    champion_f1 = mlflow.get_run(champion_model.run_id).data.metrics['test_f1_score']
    print(f'Champion f1 score: {champion_f1}. Challenger f1 score: {f1_score}.')
    metric_f1_passed = f1_score >= champion_f1

except:
    print(f"No Champion found. Accept the model as it's the first one.")
    metric_f1_passed = True

print(f'Model {model_name} version {model_details.version} metric_f1_passed: {metric_f1_passed}')

# Tag that F1 metric check has passed
client.set_model_version_tag(name=model_name, version=model_details.version, key="metric_f1_passed", value=metric_f1_passed)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Benchmark or business metrics on the eval dataset
# MAGIC
# MAGIC Let's use our validation dataset to check the potential new model impact.
# MAGIC
# MAGIC ***Note: This is just to evaluate our models, not to be confused with A/B testing**. A/B testing is done online, splitting the traffic between 2 models. It requires a feedback loop to evaluate the effect of the prediction (e.g., after a prediction, did the discount we offered to the customer prevent the churn?). We will cover A/B testing later in the real-time model serving notebook.*

# COMMAND ----------

import pyspark.sql.functions as F


# Get our validation dataset:
validation_df = spark.table('advanced_churn_label_table').filter("split='validate'")

# Call the model with the given alias and return the prediction
def predict_churn(validation_df, model_alias):
    features_w_preds = fe.score_batch(df=validation_df, model_uri=f"models:/{model_name}@{model_alias}", 
                                      result_type=validation_df.schema[label_col].dataType)

    return features_w_preds

# COMMAND ----------

import pandas as pd
import plotly.express as px
from sklearn.metrics import confusion_matrix


# Note: This is over-simplified and depends on the use-case, but the idea is to evaluate our model against business metrics
cost_of_customer_churn = 2000 #in dollar
cost_of_discount = 500 #in dollar

cost_true_negative = 0 #did not churn, we did not give him the discount
cost_false_negative = cost_of_customer_churn #did churn, we lost the customer
cost_true_positive = cost_of_customer_churn -cost_of_discount #We avoided churn with the discount
cost_false_positive = -cost_of_discount #doesn't churn, we gave the discount for free

def get_model_value_in_dollar(model_alias):
    # Convert preds_df to Pandas DataFrame
    model_predictions = predict_churn(validation_df, model_alias).toPandas()
    # Calculate the confusion matrix
    tn, fp, fn, tp = confusion_matrix(model_predictions['churn'], model_predictions['prediction']).ravel()
    return tn * cost_true_negative+ fp * cost_false_positive + fn * cost_false_negative + tp * cost_true_positive

try:
    champion_model = client.get_model_version_by_alias(model_name, "Champion")
    champion_potential_revenue_gain = get_model_value_in_dollar("Champion")
    challenger_potential_revenue_gain = get_model_value_in_dollar("Challenger")

    data = {'Model Alias': ['Challenger', 'Champion'],
            'Potential Revenue Gain': [challenger_potential_revenue_gain, champion_potential_revenue_gain]}
    
    business_metric_passed = challenger_potential_revenue_gain >= champion_potential_revenue_gain

except:
    print("No Champion found. Skipping business metrics evaluation.")
    print("You can return to re-run this cell after promoting the Challenger model as Champion in the rest of this notebook.")

    data = {'Model Alias': ['Challenger', 'Champion'],
            'Potential Revenue Gain': [0, 0]}
    
    business_metric_passed = True

# Create a bar plot using plotly express
px.bar(data, x='Model Alias', y='Potential Revenue Gain', color='Model Alias',
    labels={'Potential Revenue Gain': 'Revenue Impacted'},
    title='Business Metrics - Revenue Impacted')

# Tag that business metric check has passed
client.set_model_version_tag(name=model_name, version=model_details.version, key="business_metric_passed", value=business_metric_passed)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation results
# MAGIC
# MAGIC That's it! We have demonstrated some simple checks on the model. Let's take a look at the validation results.

# COMMAND ----------

results = client.get_model_version(model_name, model_version)
results.tags

# COMMAND ----------

# MAGIC %md
# MAGIC ## Promoting the new version/Challenger to Champion
# MAGIC
# MAGIC When we are satisfied with the results of the model, we can promote it to Champion. This is done by setting its alias to `@Champion`. Inference pipelines that load the model using the `@Champion` alias will load this new model. If there is one, the alias on the older Champion model will be automatically unset. The model retains its `@Challenger` alias until a newer Challenger model is deployed with the alias to replace it.

# COMMAND ----------

if metric_f1_passed and has_artifacts and has_description and predicts_check and business_metric_passed:
  print(f"Registering model {model_name} Version {model_version} as Champion!")
  client.set_registered_model_alias(
    name=model_name,
    alias="Champion",
    version=model_version
  )

  # Update Approval Tag
  client.set_model_version_tag(name=model_name, version=model_details.version, key="Approval_Check", value="Approved")

else:
  client.set_model_version_tag(name=model_name, version=model_details.version, key="Approval_Check", value="Failed")

# COMMAND ----------

# MAGIC %md
# MAGIC Note that we are promoting the model while keeping it in one catalog and schema in this demo. We do this for simplicity so the demo can be self-contained to a catalog and schema.
# MAGIC
# MAGIC In actual practice, it is recommended to maintain separate catalogs for Dev, QA, and Prod data and AI assets. This applies to models as well. In that case, we would register the production model to a production catalog, with an appropriate `@alias` set. This can be done programmatically and triggered when the model is ready to be promoted to production.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Congratulations, our model is now validated and promoted accordingly
# MAGIC
# MAGIC We now know that our model is ready to be used in inference pipelines and real-time serving endpoints, as it matches our validation standards.
# MAGIC
# MAGIC
# MAGIC Next: [Run batch inference from our newly promoted Champion model]($./05_batch_inference)
