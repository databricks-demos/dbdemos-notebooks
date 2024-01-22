# Databricks notebook source
dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")

# COMMAND ----------

# MAGIC %run ../../../../_resources/00-global-setup $reset_all_data=$reset_all_data $db_prefix=retail $min_dbr_version=11

# COMMAND ----------

import mlflow
if "evaluate" not in dir(mlflow):
    raise Exception("ERROR - YOU NEED MLFLOW 2.0 for this demo. Select DBRML 12+")
    
from databricks.feature_store import FeatureStoreClient
from mlflow import MlflowClient
import requests
from io import StringIO
#Dataset under apache license: https://github.com/IBM/telco-customer-churn-on-icp4d/blob/master/LICENSE
csv = requests.get("https://raw.githubusercontent.com/IBM/telco-customer-churn-on-icp4d/master/data/Telco-Customer-Churn.csv").text
df = pd.read_csv(StringIO(csv), sep=",")
def cleanup_column(pdf):
  # Clean up column names
  pdf.columns = [re.sub(r'(?<!^)(?=[A-Z])', '_', name).lower().replace("__", "_") for name in pdf.columns]
  pdf.columns = [re.sub(r'[\(\)]', '', name).lower() for name in pdf.columns]
  pdf.columns = [re.sub(r'[ -]', '_', name).lower() for name in pdf.columns]
  return pdf.rename(columns = {'streaming_t_v': 'streaming_tv', 'customer_i_d': 'customer_id'})
  

df = cleanup_column(df)
spark.createDataFrame(df).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("churn_bronze_customers")

# COMMAND ----------

def display_automl_churn_link(table_name, force_refresh = False): 
  if force_refresh:
    reset_automl_run("churn_auto_ml")
  display_automl_link("churn_auto_ml", "dbdemos_mlops_churn", spark.table(table_name), "churn", 5)

def get_automl_churn_run(table_name = "dbdemos_mlops_churn_features", force_refresh = False):
  if force_refresh:
    reset_automl_run("churn_auto_ml")
  from_cache, r = get_automl_run_or_start("churn_auto_ml", "dbdemos_mlops_churn", spark.table(table_name), "churn", 5)
  return r

# COMMAND ----------

# Replace this with your Slack webhook
slack_webhook = ""


# COMMAND ----------

# MAGIC %run ./API_Helpers
