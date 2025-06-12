# Databricks notebook source
# MAGIC %md
# MAGIC # Churn Prediction Model Inference
# MAGIC
# MAGIC ## Inference with the Champion model
# MAGIC
# MAGIC Models in Unity Catalog can be loaded for use in batch inference pipelines. Generated predictions would be used to advise on customer retention strategies or be used for analytics. The model in use is the __@Champion__ model, and we will load it for use in our pipeline.
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/mlops/advanced/banners/mlflow-uc-end-to-end-advanced-5.png?raw=true" width="1200">
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection or disable tracker during installation. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=lakehouse&notebook=05_batch_inference&demo_name=mlops-end2end&event=VIEW">

# COMMAND ----------

# DBTITLE 1,Install MLflow version for model lineage in UC [for MLR < 15.2]
# MAGIC %pip install --quiet mlflow==2.22.0 databricks-feature-engineering==0.12.1
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../_resources/00-setup $adv_mlops=true $setup_adv_inference_data=true

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##Deploying the model for batch inferences
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
# MAGIC ## Run inferences

# COMMAND ----------

# MAGIC %md
# MAGIC ### Batch inference on the Champion model
# MAGIC
# MAGIC We are ready to run inference on the Champion model. We will leverage the feature engineering client's `score_batch` method and generate predictions for our customer records.
# MAGIC
# MAGIC For simplicity, we assume that features have been pre-computed for all new customer records and already stored in a feature table. These are typically done by separate feature engineering pipelines.

# COMMAND ----------

from mlflow.store.artifact.models_artifact_repo import ModelsArtifactRepository
requirements_path = ModelsArtifactRepository(f"models:/{catalog}.{db}.advanced_mlops_churn@Challenger").download_artifacts(artifact_path="requirements.txt") # download model from remote registry

# COMMAND ----------

# MAGIC %pip install --quiet -r $requirements_path
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../_resources/00-setup $adv_mlops=true $setup_adv_inference_data=true

# COMMAND ----------

# DBTITLE 1,In a python notebook
from databricks.feature_engineering import FeatureEngineeringClient
import pyspark.sql.functions as F

# Load customer features to be scored
inference_df = spark.read.table("advanced_churn_cust_ids")

fe = FeatureEngineeringClient()

# Fully qualified model name
model_name = f"{catalog}.{db}.advanced_mlops_churn"

# Model URI
model_uri = f"models:/{model_name}@Champion"

# Batch score
preds_df = fe.score_batch(df=inference_df, model_uri=model_uri, result_type="string")
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
from datetime import datetime
client = MlflowClient()

model = client.get_registered_model(name=model_name)
model_version = int(client.get_model_version_by_alias(name=model_name, alias="Champion").version)

# COMMAND ----------

import pyspark.sql.functions as F
from datetime import datetime, timedelta

offline_inference_df = preds_df.withColumn("model_name", F.lit(model_name)) \
                              .withColumn("model_version", F.lit(model_version)) \
                              .withColumn("model_alias", F.lit("Champion")) \
                              .withColumn("inference_timestamp", F.lit(datetime.now()- timedelta(days=2)))

offline_inference_df.write.mode("overwrite") \
                    .saveAsTable("advanced_churn_offline_inference")

display(offline_inference_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Congratulations! You have successfully used the model for batch inference.
# MAGIC
# MAGIC Let's look at how we can deploy this model as a REST API endpoint for real-time inference.
# MAGIC
# MAGIC Next:  [Serve the features and model in real-time]($./06_serve_features_and_model)
