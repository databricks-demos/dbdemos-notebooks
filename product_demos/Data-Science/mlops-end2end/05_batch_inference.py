# Databricks notebook source
# MAGIC %md
# MAGIC # Churn Prediction Model Inference
# MAGIC
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/mlops-end2end-flow-6.png" width="1200">
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Ffeatures%2Fmlops%2F06_staging_inference&dt=MLOPS">
# MAGIC <!-- [metadata={"description":"MLOps end2end workflow: Load the model from MLFLow and run inferences, in batch or realtime.",
# MAGIC  "authors":["quentin.ambard@databricks.com"],
# MAGIC  "db_resources":{},
# MAGIC   "search_tags":{"vertical": "retail", "step": "Model testing", "components": ["mlflow"]},
# MAGIC                  "canonicalUrl": {"AWS": "", "Azure": "", "GCP": ""}}] -->

# COMMAND ----------

# MAGIC %run ./_resources/00-setup $reset_all_data=false $catalog="dbdemos"

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##Deploying the model for batch inferences
# MAGIC
# MAGIC <img style="float: right; margin-left: 20px" width="600" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/churn_batch_inference.gif" />
# MAGIC
# MAGIC Now that our model is available in the Registry, we can load it to compute our inferences and save them in a table to start building dashboards.
# MAGIC
# MAGIC We will use MLFlow function to load a pyspark UDF and distribute our inference in the entire cluster. If the data is small, we can also load the model with plain python and use a pandas Dataframe.
# MAGIC
# MAGIC If you don't know how to start, Databricks can generate a batch inference notebook in just one click from the model registry !

# COMMAND ----------

dbutils.widgets.dropdown("mode","False",["True", "False"], "Overwrite inference table (for monitoring)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run inferences

# COMMAND ----------

model_version = client.get_model_version_by_alias(name=model_name, alias="Challenger").version # Get challenger version
print(f"Running Inference using {model_name} version: {model_version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pyspark

# COMMAND ----------

# DBTITLE 1,In a python notebook
from databricks.feature_engineering import FeatureEngineeringClient
import pandas as pd


fe = FeatureEngineeringClient()

# Get list of new observations to score
labelsDF = spark.read.table(labels_table_name)
model_uri = f"models:/{model_name}/{model_version}"

predictions = fe.score_batch(
  df=labelsDF, 
  model_uri=model_uri,
  result_type=labelsDF.schema[label_col].dataType
)

display(predictions)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Materialize/Write Inference table to Delta Lake for ad-hoc consumption and monitoring
# MAGIC That's it! Our data can now be saved as a table and re-used by the Data Analyst / Marketing team to take special action and reduce Churn risk on these customers!

# COMMAND ----------

if dbutils.widgets.get("mode") == "True":
  mode="overwrite"
else:
  mode="append"

# COMMAND ----------

# MAGIC %md 
# MAGIC ### OPTIONAL
# MAGIC **For Demo purposes:** 
# MAGIC * Simulate first batch with baseline model/version 1
# MAGIC * Simulate second batch with champion model/version 2

# COMMAND ----------

from datetime import datetime, timedelta

# COMMAND ----------

# Simulate first batch with baseline model/version "1"
this_timestamp = (datetime.now() + timedelta(days=-2)).timestamp()

predictions.sample(fraction=np.random.random()) \
           .withColumn(timestamp_col, F.lit(this_timestamp).cast("timestamp")) \
           .withColumn("Model_Version", F.lit("1")) \
           .write.format("delta").mode(mode).option("overwriteSchema", True) \
           .option("delta.enableChangeDataFeed", True) \
           .saveAsTable(f"{catalog}.{dbName}.{inference_table_name}")

# COMMAND ----------

# Simulate second batch with champion model/version "2"
this_timestamp = (datetime.now() + timedelta(days=-1)).timestamp()

predictions.sample(fraction=np.random.random()) \
           .withColumn(timestamp_col, F.lit(this_timestamp).cast("timestamp")) \
           .withColumn("Model_Version", F.lit("2")) \
           .write.format("delta").mode(mode).option("overwriteSchema", True) \
           .option("delta.enableChangeDataFeed", True) \
           .saveAsTable(f"{catalog}.{dbName}.{inference_table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run current batch inference with latest model version

# COMMAND ----------

# DBTITLE 1,Write random sample to a delta inference table
this_timestamp = (datetime.now()).timestamp()

predictions.sample(fraction=np.random.random()) \
           .withColumn(timestamp_col, F.lit(this_timestamp).cast("timestamp")) \
           .withColumn("Model_Version", F.lit(model_version)) \
           .write.format("delta").mode(mode).option("overwriteSchema", True) \
           .option("delta.enableChangeDataFeed", True) \
           .saveAsTable(f"{catalog}.{dbName}.{inference_table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run inference on baseline/test table 
# MAGIC _Usually should be done once after model (re)training but put here for demo purposes_

# COMMAND ----------

# DBTITLE 1,Read baseline table
# Read baseline table without prediction and model version columns and select distinct to avoid dupes
baseline_df = spark.table(baseline_table_name).drop("prediction", "Model_Version").distinct()

# COMMAND ----------

# DBTITLE 1,Batch score using provided feature values from baseline table
# Features included in baseline_df will be used rather than those stored in feature tables
baseline_predictions_df = fe.score_batch(
  df=baseline_df.withColumn(timestamp_col, F.lit(this_timestamp).cast("timestamp")), # Add dummy timestamp
  model_uri=model_uri,
  result_type=baseline_df.schema[label_col].dataType
)

# COMMAND ----------

# DBTITLE 1,Append/Materialize to baseline table
baseline_predictions_df.drop(timestamp_col).withColumn("Model_Version", F.lit(model_version)) \
                        .write.format("delta").mode("append").option("overwriteSchema", True) \
                        .saveAsTable(baseline_table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Next: Serve model as REST API endpoint [OPTIONAL]
# MAGIC
# MAGIC With new data coming in and features being refreshs, we can use the autoML API to automate model retraining and pushing through the staging validation process.
# MAGIC
# MAGIC Next steps:
# MAGIC * [Deploy and Serve model as REST API]($./06_serve_model)
# MAGIC * [Create monitor for model performance]($./07_model_monitoring)
# MAGIC * [Automate model re-training]($./08_retrain_churn_automl)
