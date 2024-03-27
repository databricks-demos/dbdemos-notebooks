# Databricks notebook source
dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")

# COMMAND ----------

# MAGIC %run ../../../_resources/00-global-setup-v2 $reset_all_data=$reset_all_data

# COMMAND ----------

reset_all_data = dbutils.widgets.get("reset_all_data") == "true"

current_user = DBDemos.get_current_user()
catalog = "dbdemos"
database = DBDemos.get_current_user(remove_symbols=True)

# WE NEED THIS BECAUSE OF AN ENCAPSULATION ISSUE (BUG?) linked below. 
# https://github.com/databricks-demos/dbdemos-notebooks/blob/main/_resources/00-global-setup-v2.py#L44
dbName = database

cloud_storage_path = f"/Users/{current_user}/demos/{catalog}_{database}"
raw_data_location = cloud_storage_path+"/delta_cdf"

if reset_all_data or DBDemos.is_folder_empty(raw_data_location+"/user_csv"):
  DBDemos.setup_schema(catalog=catalog, db=database, reset_all_data=reset_all_data)

  spark.sql(f"USE {catalog}.{database}")
  spark.sql("""DROP TABLE if exists clients_cdc""")
  spark.sql("""DROP TABLE if exists retail_client_silver""")
  spark.sql("""DROP TABLE if exists retail_client_gold""")

  #data generation on another notebook to avoid installing libraries (takes a few seconds to setup pip env)
  print(f"Generating data under {raw_data_location} , please wait a few sec...")
  path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
  parent_count = path[path.rfind("Delta-Lake-CDC-CDF"):].count('/') - 1
  prefix = "./" if parent_count == 0 else parent_count*"../"
  prefix = f'{prefix}_resources/'
  dbutils.notebook.run(prefix+"01-load-data", 120, {"raw_data_location": raw_data_location})
else:
  print("data already existing. Run with reset_all_data=true to force a data cleanup for your local demo.")
