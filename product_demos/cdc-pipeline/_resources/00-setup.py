# Databricks notebook source
dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")

# COMMAND ----------

# MAGIC %run ../../../_resources/00-global-setup-v2

# COMMAND ----------

# Note: End users should not modify this code. 
# Instead, use dbdemos.install("cdc-pipeline", catalog="..", schema="...")

catalog = "dbdemos"
db = "cdc_pipeline"
volume_name = "cdc_pipeline"
reset_all_data = dbutils.widgets.get("reset_all_data") == "true"

DBDemos.setup_schema(catalog, db, reset_all_data, volume_name)
volume_folder =  f"/Volumes/{catalog}/{db}/{volume_name}"

# COMMAND ----------

total_databases = spark.sql(f"show databases in {catalog} like '{db}'").count()
assert (total_databases == 1), f"There should be exactly one database [{db}] within catalog [{catalog}]"

total_volumes = spark.sql(f"show volumes in `{catalog}`.`{db}`").count()
assert (total_volumes == 1), f"There should be exactly one volume [{volume_name}] within {catalog}.{db}"

# COMMAND ----------

if reset_all_data or DBDemos.is_folder_empty(volume_folder+"/user_csv"):
  # delete all data
  spark.sql(f"USE `{catalog}`.`{db}`")
  spark.sql("""DROP TABLE if exists clients_cdc""")
  spark.sql("""DROP TABLE if exists retail_client_silver""")
  spark.sql("""DROP TABLE if exists retail_client_gold""")

  #data generation on another notebook to avoid installing libraries (takes a few seconds to setup pip env)
  print(f"Generating data under {volume_folder} , please wait a few sec...")
  path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
  parent_count = path[path.rfind("Delta-Lake-CDC-CDF"):].count('/') - 1
  prefix = "./" if parent_count == 0 else parent_count*"../"
  prefix = f'{prefix}_resources/'
  dbutils.notebook.run(prefix+"01-load-data", 120, {"raw_data_location": volume_folder})
else:
  print("data already existing. Run with reset_all_data=true to force a data cleanup for your local demo.")
