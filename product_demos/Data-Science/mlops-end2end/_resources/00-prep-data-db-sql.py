# Databricks notebook source
# MAGIC %md 
# MAGIC # Save the data for DBSQL dashboards
# MAGIC
# MAGIC This reuse the files from the lakehouse demo. Please see bundle for more details.

# COMMAND ----------

dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")
reset_all_data = dbutils.widgets.get("reset_all_data") == "true"

# COMMAND ----------

import json
import time
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, sha1, col, initcap, to_timestamp

folder = "/demos/retail/churn"
catalog = "hive_metastore"

if reset_all_data:
  #data generation on another notebook to avoid installing libraries (takes a few seconds to setup pip env)
  print(f"Generating data under {folder} , please wait a few sec...")
  path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
  root_folder = "mlops-end2end"
  parent_count = path[path.rfind(root_folder):].count('/') - 1
  prefix = "./" if parent_count == 0 else parent_count*"../"
  prefix = f'{prefix}_resources/'
  dbutils.notebook.run(prefix+"02-create-churn-tables", 600, {"root_folder": root_folder, "catalog": catalog, "db": "dbdemos_c360", "reset_all_data": reset_all_data, "cloud_storage_path": "/demos/"})
else:
  print("data already existing. Run with reset_all_data=true to force a data cleanup for your local demo.")
