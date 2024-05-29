# Databricks notebook source
# MAGIC %md
# MAGIC Demo configuration. 
# MAGIC
# MAGIC If you wish to install the demo using another schema and database, it's best to do it with dbdemos:
# MAGIC
# MAGIC `dbdemos.install('xxx', catalog='xx', schema='xx')`

# COMMAND ----------

catalog = "main__build"
dbName = db = "dbdemos_llm_fine_tuning"
volume_name = "raw_training_data"
