# Databricks notebook source
dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")
reset_all_data = dbutils.widgets.get("reset_all_data") == "true"

# COMMAND ----------

catalog = "main__build"
schema = dbName = db = "dbdemos_cdc_pipeline"

volume_name = "raw_data"

# COMMAND ----------

# MAGIC %run ../../../_resources/00-global-setup-v2

# COMMAND ----------

DBDemos.setup_schema(catalog, db, reset_all_data, volume_name)
volume_folder =  f"/Volumes/{catalog}/{db}/{volume_name}"

# COMMAND ----------

import json
import time
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
import pyspark.sql.functions as F

raw_data_location = volume_folder 

if reset_all_data or DBDemos.is_folder_empty(raw_data_location+"/user_csv"):
  spark.sql("""DROP TABLE if exists clients_cdc""")
  spark.sql("""DROP TABLE if exists retail_client_silver""")
  #data generation on another notebook to avoid installing libraries (takes a few seconds to setup pip env)
  print(f"Generating data under {raw_data_location} , please wait a few sec...")
  path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
  parent_count = path[path.rfind("Delta-Lake-CDC-CDF"):].count('/') - 1
  prefix = "./" if parent_count == 0 else parent_count*"../"
  prefix = f'{prefix}_resources/'
  dbutils.notebook.run(prefix+"01-load-data", 120, {"raw_data_location": raw_data_location})
else:
  print("data already existing. Run with reset_all_data=true to force a data cleanup for your local demo.")
