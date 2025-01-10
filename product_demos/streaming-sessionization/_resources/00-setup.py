# Databricks notebook source
dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")
reset_all_data = dbutils.widgets.get("reset_all_data") == "true"

# COMMAND ----------

catalog = "main__build"
schema = dbName = db = "dbdemos_streaming_sessionization"

volume_name = "raw_data"

# COMMAND ----------

# MAGIC %run ../../../_resources/00-global-setup-v2

# COMMAND ----------

DBDemos.setup_schema(catalog, db, reset_all_data, volume_name)
volume_folder =  f"/Volumes/{catalog}/{db}/{volume_name}"

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.functions import col
import sys
import time
import pandas as pd 

#Reduce parallelism as we have just a few messages being produced

try:
  spark.conf.set("spark.default.parallelism", "12")
  spark.conf.set("spark.sql.shuffle.partitions", "12")
except Exception as e:
  print(f"Error {e}: Unable to set parallelism, conf not available in serverless")


def get_chkp_folder():
  import random
  import string
  randomCar = ''.join(random.choices(string.ascii_letters + string.digits, k=8))  # 
  return volume_folder+'/checkpoint/streams/'+randomCar
