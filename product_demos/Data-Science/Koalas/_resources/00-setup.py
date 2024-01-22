# Databricks notebook source
# MAGIC %md
# MAGIC # Data initialization

# COMMAND ----------

dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")

# COMMAND ----------

# MAGIC %run ../../../../_resources/00-global-setup $reset_all_data=$reset_all_data $db_prefix=features

# COMMAND ----------

# DBTITLE 1,Loading data...
reset_all_data = dbutils.widgets.get("reset_all_data") == "true"
raw_data_path = cloud_storage_path+"/koalas"
if reset_all_data or is_folder_empty(raw_data_path+"/users"):
  #data generation on another notebook to avoid installing libraries (takes a few seconds to setup pip env)
  print(f"Generating data under {raw_data_path} , please wait a few sec...")
  path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
  parent_count = path[path.rfind("lakehouse-retail-churn"):].count('/') - 1
  prefix = "./" if parent_count == 0 else parent_count*"../"
  prefix = f'{prefix}_resources/'
  dbutils.notebook.run(prefix+"01-load-data", 600, {"raw_data_path": raw_data_path})
else:
  print("data already existing. Run with reset_all_data=true to force a data cleanup for your local demo.")
