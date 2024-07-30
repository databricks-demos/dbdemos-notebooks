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

# MAGIC %pip install dbldatagen -qU
# MAGIC
# MAGIC
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../_resources/00-setup $reset_all_data=false

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

from datetime import datetime, timedelta
from pyspark.sql.functions import lit

# COMMAND ----------

# MAGIC %md 
# MAGIC ### OPTIONAL
# MAGIC **For Demo purposes:** 
# MAGIC * Simulate first batch with baseline model/version 1
# MAGIC * Simulate second batch with champion model/version 2

# COMMAND ----------

# Simulate first batch with baseline model/version "1"
this_timestamp = (datetime.now() + timedelta(days=-1)).timestamp()

predictions.sample(fraction=np.random.random()) \
           .withColumn(timestamp_col, lit(this_timestamp).cast("timestamp")) \
           .withColumn("Model_Version", lit("1")) \
           .write.format("delta").mode(mode).option("overwriteSchema", True) \
           .option("delta.enableChangeDataFeed", True) \
           .saveAsTable(f"{catalog}.{dbName}.{inference_table_name}")

# COMMAND ----------

# Simulate second batch with champion model/version "2"
this_timestamp = (datetime.now() + timedelta(days=-1)).timestamp()

predictions.sample(fraction=np.random.random()) \
          .withColumn(timestamp_col, lit(this_timestamp).cast("timestamp")) \
          .withColumn("Model_Version", lit("2")) \
          .write.format("delta").mode(mode).option("overwriteSchema", True) \
          .option("delta.enableChangeDataFeed", True) \
          .saveAsTable(f"{catalog}.{dbName}.{inference_table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run current batch inference with latest model version

# COMMAND ----------

# DBTITLE 1,Write random sample to a delta inference table
this_timestamp = (datetime.now()).timestamp()

predictions.withColumn(timestamp_col, lit(this_timestamp).cast("timestamp")) \
           .withColumn("Model_Version", lit(model_version)) \
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
baseline_df = spark.table(f"{catalog}.{db}.{inference_table_name}_baseline").drop("prediction", "Model_Version").distinct()

# COMMAND ----------

# DBTITLE 1,Batch score using provided feature values from baseline table
# Features included in baseline_df will be used rather than those stored in feature tables
baseline_predictions_df = fe.score_batch(
  df=baseline_df.withColumn(timestamp_col, lit(this_timestamp).cast("timestamp")), # Add dummy timestamp
  model_uri=model_uri,
  result_type=baseline_df.schema[label_col].dataType
)

# COMMAND ----------

# DBTITLE 1,Append/Materialize to baseline table
baseline_predictions_df.withColumn("Model_Version", lit("model_version")) \
                        .write.format("delta").mode("append").option("overwriteSchema", True) \
                        .option("mergeSchema", "true") \
                        .saveAsTable(f"{catalog}.{db}.{inference_table_name}_baseline")

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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate synthetic data _[Work-In-Progress]_
# MAGIC **TO-DO:**
# MAGIC - Move this to a seperate notebook under `_resources`
# MAGIC - Tweak distribution parameters to create significant `label`, `prediction` and (custom metric) `expected_loss` drifts (i.e. high `monthly_charges` with `churn`==Yes and `prediction`==No)

# COMMAND ----------

import dbldatagen as dg


dfSource = spark.read.table("dbdemos.retail_amine_elhelou.mlops_churn_inference_log")
analyzer = dg.DataAnalyzer(sparkSession=spark, df=dfSource)

display(analyzer.summarizeToDF())

# COMMAND ----------

# DBTITLE 1,Generates code - COPY the output
code =  dg.DataAnalyzer.scriptDataGeneratorFromSchema(dfSource.schema)

# COMMAND ----------

# DBTITLE 1,Paste here and change the params
import pyspark.sql.types


# Column definitions are stubs only - modify to generate correct data  
#
generation_spec = (
    dg.DataGenerator(sparkSession=spark, 
                     name='synthetic_data', 
                     rows=5000,
                     random=True,
                     )
    .withColumn('customer_id', 'string', template=r'dddd-AAAA')
    .withColumn('scoring_timestamp', 'timestamp', begin=(datetime.now() + timedelta(days=-30)), end=(datetime.now() + timedelta(days=-1)), interval="1 hour")
    .withColumn('churn', 'string', values=['No', 'Yes'], random=True, weights=[0.5, 0.5])
    .withColumn('gender', 'string', values=['Female', 'Male'], random=True, weights=[0.5, 0.5])
    .withColumn('senior_citizen', 'string', values=['No', 'Yes'], random=True, weights=[0.5, 0.5])
    .withColumn('partner', 'string', values=['No', 'Yes'], random=True, weights=[0.5, 0.5])
    .withColumn('dependents', 'string', values=['No', 'Yes'], random=True, weights=[0.5, 0.5])
    .withColumn('tenure', 'double', minValue=0.0, maxValue=72.0, step=1.0)
    .withColumn('phone_service', values=['No', 'Yes'], random=True, weights=[0.5, 0.5])
    .withColumn('multiple_lines', 'string', values=['No', 'Yes'], random=True, weights=[0.5, 0.5])
    .withColumn('internet_service', 'string', values=['No', 'Yes'], random=True, weights=[0.5, 0.5])
    .withColumn('online_security', 'string', values=['No', 'Yes'], random=True, weights=[0.5, 0.5])
    .withColumn('online_backup', 'string', values=['No', 'Yes'], random=True, weights=[0.5, 0.5])
    .withColumn('device_protection', 'string', values=['No', 'Yes'], random=True, weights=[0.5, 0.5])
    .withColumn('tech_support', 'string', values=['No', 'Yes'], random=True, weights=[0.5, 0.5])
    .withColumn('streaming_tv', 'string', values=['No', 'Yes'], random=True, weights=[0.5, 0.5])
    .withColumn('streaming_movies', 'string', values=['No', 'Yes'], random=True, weights=[0.5, 0.5])
    .withColumn('contract', 'string', values=['Month-to-month', 'One year','Two year'], random=True, weights=[0.3, 0.3, 0.4])
    .withColumn('paperless_billing', 'string', values=['No', 'Yes'], random=True, weights=[0.5, 0.5])
    .withColumn('payment_method', 'string', values=['Credit card (automatic)', 'Mailed check',
'Bank transfer (automatic)', 'Electronic check'], weights=[0.25, 0.25, 0.25, 0.25])
    .withColumn('monthly_charges', 'double', minValue=18.0, maxValue=118.0, step=0.5)
    .withColumn('total_charges', 'double', minValue=0.0, maxValue=8684.0, step=20)
    .withColumn('num_optional_services', 'double', minValue=0.0, maxValue=6.0, step=1)
    .withColumn('avg_price_increase', 'float', minValue=-19.0, maxValue=130.0, step=20)
    .withColumn('prediction', 'string', values=['No', 'Yes'], random=True, weights=[0.5, 0.5])
    .withColumn('Model_Version', 'string', values=['1', '2'], random=True, weights=[0.2, 0.8])
    )

# COMMAND ----------

df_synthetic_data = generation_spec.build()

# COMMAND ----------

display(df_synthetic_data)

# COMMAND ----------

df_synthetic_data.write.mode("append").saveAsTable(f"{catalog}.{db}.{inference_table_name}")
