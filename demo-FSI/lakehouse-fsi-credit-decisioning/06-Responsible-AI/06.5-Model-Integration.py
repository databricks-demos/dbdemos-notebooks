# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Model Integration
# MAGIC
# MAGIC In this notebook, we implement integration tests to ensure that the model works well with other components in the system before deploying it to Production. By doing so, we ensure that misstion critical systems will not be unexpectedly disrupted when we introduce a new model version.
# MAGIC
# MAGIC While the model has been validated for fairness and tested for comparable performance against the existing model in production, having this structured integration process ensures that system robustness and business continuity are taken into account in Responsible AI considerations.
# MAGIC
# MAGIC <img src="https://github.com/manganganath/dbdemos-notebooks/blob/main/demo-FSI/lakehouse-fsi-credit-decisioning/06-Responsible-AI/images/architecture_5.png?raw=true" 
# MAGIC      style="width: 100%; height: auto; display: block; margin: 0;" />

# COMMAND ----------

# MAGIC %run ../_resources/00-setup $reset_all_data=false

# COMMAND ----------

# MAGIC %md
# MAGIC ## Retrieve the `Staging` model
# MAGIC
# MAGIC We will first retrieve the `Staging` model registered in Unity Catalog. As we run integration tests against it, we will update the model with status information from the tests.

# COMMAND ----------

import mlflow

# Retrieve model info run by alias
client = mlflow.tracking.MlflowClient()

# Fetch the Staging model
try:
  model_info = client.get_model_version_by_alias(name=f"{catalog}.{db}.{model_name}", alias="staging")
except:
  model_info = None

assert model_info is not None, 'No Staging model. Deploy one to Staging first.'

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## Testing batch inference
# MAGIC
# MAGIC Since the model will be integrated into a batch inference pipeline, we will illustrate integration testing by running a batch inference test in this demo.
# MAGIC
# MAGIC We can easily load the model as a Spark UDF scoring function by indicating its `Staging` alias. This is the same way as how we load models for batch scoring in Production. The only difference is the the alias we use, as we will see in the next notebook.

# COMMAND ----------

import mlflow

mlflow.set_registry_uri('databricks-uc')

# Load model as a Spark UDF.
loaded_model = mlflow.pyfunc.spark_udf(spark, 
                                       model_uri=f"models:/{catalog}.{db}.{model_name}@staging", result_type='double', 
                                       env_manager="virtualenv")

# COMMAND ----------

# MAGIC %md
# MAGIC For now, we will test the batch inference pipeline by loading the features in the Staging environment, and apply the model to perform inference. Performing integration tests can help us detect issues arising from missing features in upstream tables, empty prediction values due to feature values not used in training, just to name a couple of examples.

# COMMAND ----------

from pyspark.sql import functions as F

features = loaded_model.metadata.get_input_schema().input_names()

feature_df = spark.table("credit_decisioning_features_labels").fillna(0)

try:
  prediction_df = feature_df.withColumn("prediction", loaded_model(F.struct(*features)).cast("integer")) \
                   .withColumn("model_id", F.lit(1)).cache()
except:
  prediction_df = None

display(prediction_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Perform tests
# MAGIC
# MAGIC Next, we perform a number of tests on the inference results to ensure that the systems will behave properly after integrating the model. We use a few simple examples:
# MAGIC
# MAGIC - Check that a object is returned for the resulting DataFrame
# MAGIC   - The inference code may not return a result if there are missing features in the upstream table.
# MAGIC   - Furthermore, a `None` return object can cause issues to downstream code.
# MAGIC - Check that the resulting DataFrame has rows in it
# MAGIC   - An empty DataFrame may cause issues to downstream code.
# MAGIC - Check that there are no null values in the prediction column
# MAGIC   - The team needs to determine if null values are expected, and how the system should handle them.

# COMMAND ----------

# These tests should all return True to pass
result_df_not_null = (
  prediction_df != None
)
result_df_not_empty = (
  prediction_df.count() > 0
)
no_null_pred_values = (
    prediction_df.filter(prediction_df.prediction.isNull()).count() == 0
)

print(f"An object is returned for the result DataFrame: {result_df_not_null}")
print(f"Result DataFrame contains rows: {result_df_not_empty}")
print(f"No null prediction values: {no_null_pred_values}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Update integration testing status
# MAGIC
# MAGIC Having performed the tests, we will now update the model's integration testing status. Only models that have passed all tests are allowed to be promoted to production.
# MAGIC
# MAGIC For auditability, we also apply tags on the version of the model in Unity Catalog to record the status of the integration tests.

# COMMAND ----------

# Indicate if result_df_not_null test has passed
pass_fail = "failed"
if result_df_not_null:
  pass_fail = "passed"
client.set_model_version_tag(f"{catalog}.{db}.{model_name}", model_info.version, f"inttest_result_df_not_null", pass_fail)

# Indicate if result_df_not_empty test has passed
pass_fail = "failed"
if result_df_not_empty:
  pass_fail = "passed"
client.set_model_version_tag(f"{catalog}.{db}.{model_name}", model_info.version, f"inttest_result_df_not_empty", pass_fail)

# Indicate if no_null_pred_values test has passed
pass_fail = "failed"
if no_null_pred_values:
  pass_fail = "passed"
client.set_model_version_tag(f"{catalog}.{db}.{model_name}", model_info.version, f"inttest_no_null_pred_values", pass_fail)

# Model Validation Status is 'approved' only if both compliance checks and champion-challenger test have passed
# Otherwise Model Validation Status is 'rejected'
integration_test_status = "Not tested"
if result_df_not_null & result_df_not_empty & no_null_pred_values:
  integration_test_status = "passed"
else:
  integration_test_status = "failed"
client.set_model_version_tag(f"{catalog}.{db}.{model_name}", model_info.version, "integration_test_status", integration_test_status)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Promote model to Production
# MAGIC
# MAGIC Our model is now ready to be promoted to `Production`. As we do so, we also archive the existing production model by setting its alias to `Archived`.
# MAGIC
# MAGIC If our model did not pass the integration tests, we simply transition it to `Archived`, while leaving the existing model in `Production` untouched.

# COMMAND ----------

# Fetch the Production model (if any)
try:
    prod_model_info = client.get_model_version_by_alias(name=f"{catalog}.{db}.{model_name}", alias="production")
except:
    prod_model_info = None

if integration_test_status == "passed":
  # This model has passed integration testing. Check if there's an existing model in Production
  if prod_model_info is not None:
    # Existing model in production. Archive the existing prod model.
    client.set_registered_model_alias(name=f"{catalog}.{db}.{model_name}", alias="archived", version=prod_model_info.version)
    client.set_model_version_tag(f"{catalog}.{db}.{model_name}", prod_model_info.version, f"archived", "true")
    print(f'Version {prod_model_info.version} of {catalog}.{db}.{model_name} is now archived.')
  else:
    # No model in production.
    print(f'{catalog}.{db}.{model_name} does not have a Production model.')
  # Promote this model to Production
  client.delete_registered_model_alias(name=f"{catalog}.{db}.{model_name}", alias="staging")
  client.set_registered_model_alias(name=f"{catalog}.{db}.{model_name}", alias="production", version=model_info.version)
  print(f'Version {model_info.version} of {catalog}.{db}.{model_name} is now promoted to Production.')
else:
  # This model has failed integration testing. Set it to Archived
  client.delete_registered_model_alias(name=f"{catalog}.{db}.{model_name}", alias="staging")
  client.set_registered_model_alias(name=f"{catalog}.{db}.{model_name}", alias="archived", version=model_info.version)
  client.set_model_version_tag(f"{catalog}.{db}.{model_name}", model_info.version, f"archived", "true")
  print(f'Version {model_info.version} of {catalog}.{db}.{model_name} is transitioned to Archived. No model promoted to Production.')

# COMMAND ----------

# MAGIC %md
# MAGIC We just moved our automl model as production ready! 
# MAGIC
# MAGIC Open the model in Unity Catalog to explore its artifact and analyze the parameters used, including traceability to the notebook used for its creation.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Next Steps
# MAGIC
# MAGIC
# MAGIC Now that the model is in Production, we proceed to [06.6-Model-Inference]($./06-Responsible-AI/06.6-Model-Inference), where the deployed model is used for batch or real-time inference and predictions are logged for explainability and transparency. This ensures that model outputs are reliable, ethical, and aligned with regulatory standards, closing the loop on Responsible AI implementation.
