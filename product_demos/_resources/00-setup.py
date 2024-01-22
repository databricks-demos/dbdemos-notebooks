# Databricks notebook source
dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")

# COMMAND ----------

# MAGIC %run ../../_resources/00-global-setup $reset_all_data=$reset_all_data $db_prefix=features

# COMMAND ----------

reset_all_data = dbutils.widgets.get("reset_all_data") == "true"
if reset_all_data or not test_not_empty_folder(cloud_storage_path+"/auto_loader/user_json"):
  print("setting up data, please wait a few sec...")
  spark.conf.set("spark.databricks.service.dbutils.fs.parallel.enabled", "true")
  dbutils.fs.cp("/mnt/field-demos/retail/users_json", cloud_storage_path+"/auto_loader/user_json", True)
  spark.conf.set("spark.databricks.service.dbutils.fs.parallel.enabled", "false")
