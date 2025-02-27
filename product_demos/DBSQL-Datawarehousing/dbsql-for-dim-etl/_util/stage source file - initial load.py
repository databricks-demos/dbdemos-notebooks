# Databricks notebook source
# DBTITLE 1,initialize config for sgc
# MAGIC %run "./initialize-staging"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stage the source file for initial load
# MAGIC
# MAGIC Download and copy _patients50_init.csv_ to volume staging path (/Volumes/\<catalog_name\>/\<schema_name\>/staging/patient)

# COMMAND ----------

# DBTITLE 1,staging directory
# clear staging directory
dbutils.fs.rm(stg_path + "/patient", True)

# if not present
dbutils.fs.mkdirs(stg_path + "/patient")

# COMMAND ----------

# DBTITLE 1,initial file name
file_name = "patients50_init.csv"

# COMMAND ----------

# DBTITLE 1,stage the file for initial load
# download to volume staging path
get_file(file_name, "patient")