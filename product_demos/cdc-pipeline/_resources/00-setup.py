# Databricks notebook source
dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")

# COMMAND ----------

# MAGIC %run ../../../_resources/00-global-setup-v2 $reset_all_data=$reset_all_data

# COMMAND ----------

# Note: We do not recommend to change the catalog here as it won't impact all the demo resources such as DLT pipeline and Dashboards. Instead, please re-install the demo with a specific catalog and schema using 
# dbdemos.install("lakehouse-retail-c360", catalog="..", schema="...")

catalog = "dbdemos"
db = "cdc_pipeline"
volume_name = "data"
reset_all_data = dbutils.widgets.get("reset_all_data") == "true"


DBDemos.setup_schema(catalog, db, reset_all_data, volume_name)
volume_folder =  f"/Volumes/{catalog}/{db}/{volume_name}"

# COMMAND ----------

if reset_all_data or DBDemos.is_folder_empty(volume_folder+"/user_csv"):
  DBDemos.setup_schema(catalog=catalog, db=database, reset_all_data=reset_all_data)

  spark.sql(f"USE {catalog}.{database}")
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
