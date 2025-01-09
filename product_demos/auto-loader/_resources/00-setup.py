# Databricks notebook source
dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")
reset_all_data = dbutils.widgets.get("reset_all_data") == "true"

# COMMAND ----------

catalog = "main__build"
schema = dbName = db = "dbdemos_autoloader"

volume_name = "raw_data"

# COMMAND ----------

# MAGIC %run ../../../_resources/00-global-setup-v2

# COMMAND ----------

DBDemos.setup_schema(catalog, db, reset_all_data, volume_name)
volume_folder =  f"/Volumes/{catalog}/{db}/{volume_name}"

# COMMAND ----------

import time
def get_chkp_folder():
    import random
    import string
    randomCar = ''.join(random.choices(string.ascii_letters + string.digits, k=8))  # 
    return volume_folder+'/checkpoint/streams/'+randomCar

# COMMAND ----------

from time import sleep

if reset_all_data or DBDemos.is_folder_empty(volume_folder+"/user_json"):
  #data generation on another notebook to avoid installing libraries (takes a few seconds to setup pip env)
  print(f"Generating data under {volume_folder} , please wait a few sec...")
  path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
  folder_path = path[:path.rfind("/")]
  prefix = ''
  if not folder_path.endswith('_resources'):
    prefix = './_resources/'
  dbutils.notebook.run(prefix+"./01-load-data", 120, {"volume_folder": volume_folder})
else:
  print("data already existing. Run with reset_all_data=true to force a data cleanup for your local demo.")
  
  
#cleanup schema in all cases
dbutils.fs.rm(volume_folder+'/inferred_schema', True)

# COMMAND ----------

# DBTITLE 1,Helper functions to wait between streams to have a nice execution & results clicking on "run all"
#Wait to have data to be available in the _rescued_data column.
def wait_for_rescued_data():
  i = 0
  while DBDemos.is_folder_empty(volume_folder+'/_wait_rescued/data/_delta_log/') or spark.read.load(volume_folder+'/_wait_rescued/data').count() == 0:
    get_stream().filter("_rescued_data is not null") \
               .writeStream.option("checkpointLocation", volume_folder+'/_wait_rescued/ckpt') \
               .trigger(once=True).start(volume_folder+'/_wait_rescued/data').awaitTermination()
    i+=1
    sleep(1)
    if i > 30:
      raise Exception("Can't capture the new column. Please make sure the stream on the previous cell is running.")

