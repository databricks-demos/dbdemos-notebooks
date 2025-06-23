# Databricks notebook source
# MAGIC %pip install -qqqq lightgbm databricks-feature-store bayesian-optimization mlflow hyperopt shap databricks-automl-runtime
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

def drop_fs_table(table_name):
  from databricks.feature_store import FeatureStoreClient
  fs = FeatureStoreClient()
  try:
    fs.drop_table(table_name)  
  except Exception as e:
    print(f"Can't drop the fs table, probably doesn't exist? {e}")
  try:
    spark.sql(f"DROP TABLE IF EXISTS `{table_name}`")
  except Exception as e:
    print(f"Can't drop the delta table, probably doesn't exist? {e}")

# COMMAND ----------

# MAGIC %run ../config

# COMMAND ----------

DEFAULT_CATALOG = catalog_name
DEFAULT_SCHEMA = schema_name