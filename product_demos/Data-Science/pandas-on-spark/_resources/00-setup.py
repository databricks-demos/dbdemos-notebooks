# Databricks notebook source
# MAGIC %md
# MAGIC # Data initialization

# COMMAND ----------

dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")
reset_all_data = dbutils.widgets.get("reset_all_data") == "true"

# COMMAND ----------

catalog = "main__build"
schema = dbName = db = "dbdemos_pandas_on_spark"
volume_name = "my_volume"
volume_path = f"/Volumes/{catalog}/{schema}/{volume_name}"

# COMMAND ----------

# MAGIC %run ../../../../_resources/00-global-setup-v2

# COMMAND ----------

DBDemos.setup_schema(catalog, db, reset_all_data, volume_name)

# COMMAND ----------

# DBTITLE 1,Loading data...
reset_all_data = dbutils.widgets.get("reset_all_data") == "true"
if reset_all_data or DBDemos.is_folder_empty(volume_path+"/users"):
  #data generation on another notebook to avoid installing libraries (takes a few seconds to setup pip env)
  print(f"Generating data under {volume_path} , please wait a few sec...")
  path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
  parent_count = path[path.rfind("pandas-on-spark"):].count('/') - 1
  prefix = "./" if parent_count == 0 else parent_count*"../"
  prefix = f'{prefix}_resources/'
  dbutils.notebook.run(prefix+"01-load-data", 600, {"raw_data_path": volume_path})
else:
  print("data already existing. Run with reset_all_data=true to force a data cleanup for your local demo.")
