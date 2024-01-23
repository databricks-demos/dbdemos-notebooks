# Databricks notebook source
dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")

# COMMAND ----------

# MAGIC %run ../../../_resources/00-global-setup $reset_all_data=$reset_all_data $db_prefix=retail

# COMMAND ----------

from time import sleep
raw_data_location = cloud_storage_path+"/auto_loader"

reset_all_data = dbutils.widgets.get("reset_all_data") == "true"

raw_data_location = cloud_storage_path+"/auto_loader"

if reset_all_data or is_folder_empty(raw_data_location+"/user_json"):
  #data generation on another notebook to avoid installing libraries (takes a few seconds to setup pip env)
  print(f"Generating data under {raw_data_location} , please wait a few sec...")
  path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
  folder_path = path[:path.rfind("/")]
  prefix = ''
  if not folder_path.endswith('_resources'):
    prefix = './_resources/'
  dbutils.notebook.run(prefix+"./01-load-data", 120, {"raw_data_location": raw_data_location})
else:
  print("data already existing. Run with reset_all_data=true to force a data cleanup for your local demo.")
  
  
#cleanup schema in all cases
dbutils.fs.rm(raw_data_location+'/inferred_schema', True)

# COMMAND ----------

# DBTITLE 1,Helper functions to wait between streams to have a nice execution & results clicking on "run all"
#Wait to have data to be available in the _rescued_data column.
def wait_for_rescued_data():
  i = 0
  while is_folder_empty(raw_data_location+'/_wait_rescued/data/_delta_log/') or spark.read.load(raw_data_location+'/_wait_rescued/data').count() == 0:
    get_stream().filter("_rescued_data is not null") \
               .writeStream.option("checkpointLocation", raw_data_location+'/_wait_rescued/ckpt') \
               .trigger(once=True).start(raw_data_location+'/_wait_rescued/data').awaitTermination()
    i+=1
    sleep(1)
    if i > 30:
      raise Exception("Can't capture the new column. Please make sure the stream on the previous cell is running.")

