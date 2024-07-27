# Databricks notebook source
# MAGIC %md
# MAGIC # Churn Prediction Model Inference
# MAGIC
# MAGIC ## Inference with the Champion model
# MAGIC
# MAGIC With Models in Unity Catalog, they can be loaded for use in batch inference pipelines. The generated predictions can used to devise customer retention strategies, or be used for analytics. The model in use is the __Champion__ model, and we will load this for use in our pipeline.
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/mlops/mlops-uc-end2end-5.png?raw=true" width="1200">
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection or disable tracker during installation. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=lakehouse&notebook=05_batch_inference&demo_name=mlops-end2end&event=VIEW">

# COMMAND ----------

# DBTITLE 1,Install MLflow version for model lineage in UC [for MLR < 15.2]
# MAGIC %pip install --quiet mlflow==2.14.3
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../_resources/00-setup $setup_adv_inference_data=true

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##Deploying the model for batch inferences
# MAGIC
# MAGIC <!--img style="float: right; margin-left: 20px" width="600" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/churn_batch_inference.gif" /-->
# MAGIC
# MAGIC Now that our model is available in the Unity Catalog Model Registry, we can load it to compute our inferences and save them in a table to start building dashboards.
# MAGIC
# MAGIC We will use MLFlow function to load a pyspark UDF and distribute our inference in the entire cluster. If the data is small, we can also load the model with plain python and use a pandas Dataframe.
# MAGIC
# MAGIC If you don't know how to start, you can get sample code from the __"Artifacts"__ page of the model's experiment run.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run inferences

# COMMAND ----------

# MAGIC %md
# MAGIC ### Batch inference on the Champion model
# MAGIC
# MAGIC We are ready to run inference on the Champion model. We will load the model as a Spark UDF and generate predictions for our customer records.
# MAGIC
# MAGIC For simplicity, we assume that features have been extracted for the new customer records and these are already stored in the feature table. These are typically done by separate feature engineering pipelines.

# COMMAND ----------

model_alias = "Champion"
model_name = f"{catalog}.{dbName}.mlops_advanced_churn"

# COMMAND ----------

# DBTITLE 1,In a python notebook
from databricks.feature_engineering import FeatureEngineeringClient
import pyspark.sql.functions as F

# Load customer features to be scored
inference_df = spark.read.table(f"mlops_churn_advanced_cust_ids")

fe = FeatureEngineeringClient()

# Model URI
model_uri = f"models:/{model_name}@{model_alias}"

# Batch score
preds_df = fe.score_batch(df=inference_df, model_uri=model_uri, result_type="string")

display(preds_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC That's it! Our data can now be saved as a table and re-used by the Data Analyst / Marketing team to take special action and reduce Churn risk on these customers!
# MAGIC
# MAGIC Your data will also be available within Genie to answer any churn-related question using plain text english!

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save the predictions for monitoring
# MAGIC
# MAGIC Since we want to monitor the model and its predictions over time, we will save the predictions, along with information on the model used to produce them. This table can then be monitored for feature drift and prediction drift.
# MAGIC
# MAGIC Note that this table does not have the ground truth labels. These are usually collected and made available over time, and in many cases, may not even be available! However, this does not stop us from monitoring the data for drift, as that alone may be a sign that the model has to be retrained.
# MAGIC
# MAGIC This is from the table saved in the inference notebooks
# MAGIC Includes model version, alias, predictions and timestamp
# MAGIC No labels
# MAGIC

# COMMAND ----------

from mlflow import MlflowClient
from datetime import datetime

client = MlflowClient()

model = client.get_registered_model(name=model_name)
model_version = int(
    client.get_model_version_by_alias(name=model_name, alias=model_alias).version
)

# COMMAND ----------

import pyspark.sql.functions as F

offline_inference_df = preds_df.withColumn("model_name", F.lit(model_name)) \
                              .withColumn("model_version", F.lit(model_version)) \
                              .withColumn("model_alias", F.lit(model_alias)) \
                              .withColumn("inference_timestamp", F.current_timestamp())

offline_inference_df.write.mode("overwrite") \
                    .saveAsTable("mlops_churn_advanced_offline_inference")

display(offline_inference_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Conclusion
# MAGIC
# MAGIC TODO: Add text for advanced demo.
# MAGIC
# MAGIC This is all for the quickstart demo! We have looked at basic concepts of MLOps and how Databricks helps you achieve them. They include:
# MAGIC
# MAGIC - Feature engineering and storing feature tables with labels in Databricks
# MAGIC - AutoML, model training and experiment tracking in MLflow
# MAGIC - Registering models as Models in Unity Catalog for governed usage
# MAGIC - Model validation, Champion-Challenger testing, and model promotion
# MAGIC - Batch inference by loading the model as a pySpark UDF
# MAGIC
# MAGIC We hope you've enjoyed this demo. As the next step, look out for our Advanced End-to-end MLOps demo, which will include more in-depth walkthroughs on the following aspects of MLOps:
# MAGIC
# MAGIC - Feature serving and Feature Store
# MAGIC - Data and model monitoring
# MAGIC - Deployment for real-time inference
