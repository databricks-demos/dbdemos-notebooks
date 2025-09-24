# Databricks notebook source
# MAGIC %md
# MAGIC # Churn Prediction Model Inference
# MAGIC
# MAGIC ## Inference with the Champion model
# MAGIC
# MAGIC Models in Unity Catalog can be loaded for use in batch inference pipelines. Generated predictions would be used to advise on customer retention strategies or be used for analytics. The model in use is the __@Champion__ model, and we will load it for use in our pipeline.
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/mlops/advanced/banners/mlflow-uc-end-to-end-advanced-5-v2.png?raw=true" width="1200">
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection or disable tracker during installation. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=lakehouse&notebook=05_batch_inference&demo_name=mlops-end2end&event=VIEW">

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

# MAGIC %run ../_resources/00-setup $adv_mlops=true $setup_adv_inference_data=true

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Consume/Use the model for batch inferences
# MAGIC
# MAGIC <!--img style="float: right; margin-left: 20px" width="600" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/churn_batch_inference.gif" /-->
# MAGIC
# MAGIC Now that our model is available in the Unity Catalog Model Registry, we can load it to compute our inferences and save them in a table to start building dashboards.
# MAGIC
# MAGIC We will use the feature engineering client's `score_batch` method, automatically loading a PySpark UDF and distributing the inference on the entire cluster. If the data is small, we can load the model in plain Python and use a Pandas Dataframe.
# MAGIC
# MAGIC If you don't know how to start, you can get sample code from the __"Artifacts"__ page of the model's experiment run.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Batch inference on the Champion model
# MAGIC
# MAGIC We are ready to run inference on the Champion model. We will leverage the feature engineering client's `score_batch` method and generate predictions for our customer records.
# MAGIC
# MAGIC For simplicity, we assume that features have been pre-computed for all new customer records and already stored in a feature table. These are typically done by separate feature engineering pipelines.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reproduce inference env in notebook _(OPTIONNAL)_
# MAGIC ONLY if you plan on executing the batch inference in the default Serverless environment defined here (`env_manager="local"`), otherwise no need as inference can run on a virtual environment (`env_manager="virtual_env" or "uv"`) pulled from the model requirements artifacts or if the same MLR version was used for training this model version.

# COMMAND ----------

# MAGIC %md
# MAGIC ```python
# MAGIC from mlflow.store.artifact.models_artifact_repo import ModelsArtifactRepository
# MAGIC
# MAGIC
# MAGIC requirements_path = ModelsArtifactRepository(f"models:/{catalog}.{db}.advanced_mlops_churn@Champion").download_artifacts(artifact_path="requirements.txt") # download model from remote registry
# MAGIC ```
# MAGIC -----------------------------------------------------------------
# MAGIC ```bash
# MAGIC %pip install --quiet -r $requirements_path
# MAGIC
# MAGIC
# MAGIC %restart_python
# MAGIC ```
# MAGIC ----------------------------------------------------------------
# MAGIC ```bash
# MAGIC %run ../_resources/00-setup $adv_mlops=true $setup_adv_inference_data=true
# MAGIC ```

# COMMAND ----------

env_manager = "virtualenv" # For fe.score_batch() function - set to "local" if NOT running on Serverless AND/OR pip installing all model artifacts

# COMMAND ----------

# MAGIC %md
# MAGIC ### Use built-in `fe.score_batch()` on model uri
# MAGIC Pull new labels/customer_ids to score and run inference locally using spark()

# COMMAND ----------

# DBTITLE 1,Set model alias to use for batch inference
model_alias = "Champion" # "Challenger"

# Fully qualified model name
model_name = f"{catalog}.{db}.advanced_mlops_churn"

# Model URI
model_uri = f"models:/{model_name}@{model_alias}"
# model_uri = f"models:/{model_name}/{model_version}"

# COMMAND ----------

from databricks.feature_engineering import FeatureEngineeringClient


fe = FeatureEngineeringClient()

# Load customer features to be scored
inference_df = spark.read.table("advanced_churn_cust_ids")
# Reduce the amount of inferences for the demo to run faster
inference_df = inference_df.limit(100)

# Batch score
preds_df = fe.score_batch(df=inference_df, model_uri=model_uri, result_type="string", env_manager=env_manager)
display(preds_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC That's it! Our data can now be saved as a table and reused by the Data Analyst / Marketing team to take special action and reduce Churn risk on these customers!
# MAGIC
# MAGIC Your data will also be available within Genie to answer any churn-related question using plain text English!

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save the predictions for monitoring
# MAGIC
# MAGIC Since we want to monitor the model and its predictions over time, we will save the predictions, along with information on the model used to produce them. This table can then be monitored for feature drift and prediction drift.
# MAGIC
# MAGIC Note that this table does not have the ground truth labels. These are usually collected and made available over time, and in many cases, may not even be available! However, this does not stop us from monitoring the data for drift, as that alone may be a sign that the model has to be retrained.
# MAGIC
# MAGIC The table displayed below is saved into `advanced_churn_offline_inference`. It includes the model version used for scoring, the model alias, the predictions, and the timestamp when the inference was made. It does not contain any labels.
# MAGIC

# COMMAND ----------

from mlflow import MlflowClient


client = MlflowClient()

model = client.get_registered_model(name=model_name)
model_version = client.get_model_version_by_alias(name=model_name, alias=model_alias).version

# COMMAND ----------

from datetime import datetime, timedelta
from pyspark.sql import functions as F


offline_inference_df = preds_df.drop("split") \
                              .withColumn("model_version", F.lit(model_version)) \
                              .withColumn("inference_timestamp", F.lit(datetime.now())) # - timedelta(days=1)))

offline_inference_df.write.mode("append") \
                    .option("overwriteSchema", True) \
                    .saveAsTable("advanced_churn_offline_inference")

# display(offline_inference_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Congratulations! You have successfully used the model for batch inference.
# MAGIC
# MAGIC Let's look at how we can deploy this model as a REST API endpoint for real-time inference.
# MAGIC
# MAGIC Next:  [Serve the features and model in real-time]($./06_serve_features_and_model)
