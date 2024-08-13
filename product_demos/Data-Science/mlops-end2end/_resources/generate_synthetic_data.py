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
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../_resources/00-setup $reset_all_data=false

# COMMAND ----------

dbutils.widgets.dropdown("mode","False",["True", "False"], "Overwrite inference table (for monitoring)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate synthetic data _[Work-In-Progress]_
# MAGIC **TO-DO:**
# MAGIC - Move this to a seperate notebook under `_resources`
# MAGIC - Tweak distribution parameters to create significant `label`, `prediction` and (custom metric) `expected_loss` drifts (i.e. high `monthly_charges` with `churn`==Yes and `prediction`==No)

# COMMAND ----------

inference_table_name = "mlops_churn_advanced_inference"
baseline_table_name = "mlops_churn_advanced_baseline"
model_alias = "Champion"
model_name = f"{catalog}.{db}.mlops_advanced_churn"

# COMMAND ----------

import dbldatagen as dg


dfSource = spark.read.table(f"{inference_table_name}")
analyzer = dg.DataAnalyzer(sparkSession=spark, df=dfSource)

display(analyzer.summarizeToDF())

# COMMAND ----------

display(dfSource)

# COMMAND ----------

# DBTITLE 1,Generates code - COPY the output
code =  dg.DataAnalyzer.scriptDataGeneratorFromSchema(dfSource.schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Same distribution as the real data - No drift is expected

# COMMAND ----------

# DBTITLE 1,Original distribution
import dbldatagen as dg
import pyspark.sql.types
from datetime import timedelta
# Column definitions are stubs only - modify to generate correct data  
#
generation_spec = (
    dg.DataGenerator(sparkSession=spark, 
                     name='synthetic_data', 
                     rows=5000,
                     random=True,
                     )
    .withColumn('customer_id', 'string', template=r'dddd-AAAA')
    .withColumn('transaction_ts', 'timestamp', begin=(datetime.now() + timedelta(days=-30)), end=(datetime.now() + timedelta(days=-1)), interval="1 hour")
    .withColumn('gender', 'string', values=['Female', 'Male'], random=True, weights=[0.5, 0.5])
    .withColumn('senior_citizen', 'string', values=['No', 'Yes'], random=True, weights=[0.85, 0.15])
    .withColumn('partner', 'string', values=['No', 'Yes'], random=True, weights=[0.5, 0.5])
    .withColumn('dependents', 'string', values=['No', 'Yes'], random=True, weights=[0.7, 0.3])
    .withColumn('tenure', 'double', minValue=0.0, maxValue=72.0, step=1.0)
    .withColumn('phone_service', values=['No', 'Yes'], random=True, weights=[0.9, 0.1])
    .withColumn('multiple_lines', 'string', values=['No', 'Yes'], random=True, weights=[0.5, 0.5])
    .withColumn('internet_service', 'string', values=['Fiber optic', 'DSL', 'No'], random=True, weights=[0.5, 0.3, 0.2])
    .withColumn('online_security', 'string', values=['No', 'Yes'], random=True, weights=[0.5, 0.5])
    .withColumn('online_backup', 'string', values=['No', 'Yes'], random=True, weights=[0.5, 0.5])
    .withColumn('device_protection', 'string', values=['No', 'Yes'], random=True, weights=[0.5, 0.5])
    .withColumn('tech_support', 'string', values=['No', 'Yes'], random=True, weights=[0.5, 0.5])
    .withColumn('streaming_tv', 'string', values=['No', 'Yes', 'No internet service'], random=True, weights=[0.4, 0.4, 0.2])
    .withColumn('streaming_movies', 'string', values=['No', 'Yes', 'No internet service'], random=True, weights=[0.4, 0.4, 0.2])
    .withColumn('contract', 'string', values=['Month-to-month', 'One year','Two year'], random=True, weights=[0.5, 0.25, 0.25])
    .withColumn('paperless_billing', 'string', values=['No', 'Yes'], random=True, weights=[0.6, 0.4])
    .withColumn('payment_method', 'string', values=['Credit card (automatic)', 'Mailed check',
'Bank transfer (automatic)', 'Electronic check'], weights=[0.2, 0.2, 0.2, 0.4])
    .withColumn('monthly_charges', 'double', minValue=18.0, maxValue=118.0, step=0.5)
    .withColumn('total_charges', 'double', minValue=0.0, maxValue=8684.0, step=20)
    .withColumn('num_optional_services', 'double', minValue=0.0, maxValue=6.0, step=1)
    .withColumn('avg_price_increase', 'float', minValue=-19.0, maxValue=130.0, step=20)
    )

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
display(df_synthetic_data)

# COMMAND ----------

from databricks.feature_engineering import FeatureEngineeringClient
import pyspark.sql.functions as F


fe = FeatureEngineeringClient()

# Model URI
model_uri = f"models:/{model_name}@{model_alias}"

# Batch score
preds_df = fe.score_batch(df=df_synthetic_data, model_uri=model_uri, result_type="string")
preds_df = preds_df \
  .withColumn('model_name', F.lit("aminen_catalog.advanced_mlops.mlops_advanced_churn")) \
  .withColumn('model_version', F.lit(2)) \
  .withColumn('model_alias', F.lit("Champion")) \
  .withColumn('inference_timestamp', F.lit(datetime.now())) \
  .withColumn('churn', preds_df['prediction'])
display(preds_df)

# COMMAND ----------

preds_df.write.mode("append").saveAsTable(f"{catalog}.{db}.{inference_table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Same distribution as the real data & bad model performance - Label drift is expected

# COMMAND ----------

import dbldatagen as dg
import pyspark.sql.types
from datetime import timedelta
# Column definitions are stubs only - modify to generate correct data  
#
generation_spec = (
    dg.DataGenerator(sparkSession=spark, 
                     name='synthetic_data', 
                     rows=5000,
                     random=True,
                     )
    .withColumn('customer_id', 'string', template=r'dddd-AAAA')
    .withColumn('transaction_ts', 'timestamp', begin=(datetime.now() + timedelta(days=-30)), end=(datetime.now() + timedelta(days=-1)), interval="1 hour")
    .withColumn('gender', 'string', values=['Female', 'Male'], random=True, weights=[0.5, 0.5])
    .withColumn('senior_citizen', 'string', values=['No', 'Yes'], random=True, weights=[0.85, 0.15])
    .withColumn('partner', 'string', values=['No', 'Yes'], random=True, weights=[0.5, 0.5])
    .withColumn('dependents', 'string', values=['No', 'Yes'], random=True, weights=[0.7, 0.3])
    .withColumn('tenure', 'double', minValue=0.0, maxValue=72.0, step=1.0)
    .withColumn('phone_service', values=['No', 'Yes'], random=True, weights=[0.9, 0.1])
    .withColumn('multiple_lines', 'string', values=['No', 'Yes'], random=True, weights=[0.5, 0.5])
    .withColumn('internet_service', 'string', values=['Fiber optic', 'DSL', 'No'], random=True, weights=[0.5, 0.3, 0.2])
    .withColumn('online_security', 'string', values=['No', 'Yes'], random=True, weights=[0.5, 0.5])
    .withColumn('online_backup', 'string', values=['No', 'Yes'], random=True, weights=[0.5, 0.5])
    .withColumn('device_protection', 'string', values=['No', 'Yes'], random=True, weights=[0.5, 0.5])
    .withColumn('tech_support', 'string', values=['No', 'Yes'], random=True, weights=[0.5, 0.5])
    .withColumn('streaming_tv', 'string', values=['No', 'Yes', 'No internet service'], random=True, weights=[0.4, 0.4, 0.2])
    .withColumn('streaming_movies', 'string', values=['No', 'Yes', 'No internet service'], random=True, weights=[0.4, 0.4, 0.2])
    .withColumn('contract', 'string', values=['Month-to-month', 'One year','Two year'], random=True, weights=[0.5, 0.25, 0.25])
    .withColumn('paperless_billing', 'string', values=['No', 'Yes'], random=True, weights=[0.6, 0.4])
    .withColumn('payment_method', 'string', values=['Credit card (automatic)', 'Mailed check',
'Bank transfer (automatic)', 'Electronic check'], weights=[0.2, 0.2, 0.2, 0.4])
    .withColumn('monthly_charges', 'double', minValue=18.0, maxValue=118.0, step=0.5)
    .withColumn('total_charges', 'double', minValue=0.0, maxValue=8684.0, step=20)
    .withColumn('num_optional_services', 'double', minValue=0.0, maxValue=6.0, step=1)
    .withColumn('avg_price_increase', 'float', minValue=-19.0, maxValue=130.0, step=20)
    .withColumn('churn', 'string', values=[ 'Yes'], random=True)
    )

# COMMAND ----------

df_synthetic_data = generation_spec.build()
display(df_synthetic_data)

# COMMAND ----------

from databricks.feature_engineering import FeatureEngineeringClient
import pyspark.sql.functions as F


fe = FeatureEngineeringClient()

# Model URI
model_uri = f"models:/{model_name}@{model_alias}"

# Batch score
preds_df = fe.score_batch(df=df_synthetic_data, model_uri=model_uri, result_type="string")
preds_df = preds_df \
  .withColumn('model_name', F.lit("aminen_catalog.advanced_mlops.mlops_advanced_churn")) \
  .withColumn('model_version', F.lit(2)) \
  .withColumn('model_alias', F.lit("Champion")) \
  .withColumn('inference_timestamp', F.lit(datetime.now())) 

# COMMAND ----------

preds_df.write.mode("append").saveAsTable(f"{catalog}.{db}.{inference_table_name}")
