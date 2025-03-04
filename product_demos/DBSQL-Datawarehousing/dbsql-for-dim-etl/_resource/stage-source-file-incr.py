# Databricks notebook source
# DBTITLE 1,initialize config for sgc
# MAGIC %run "../01-Setup/01.1-initialize"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stage the source file for incremental load
# MAGIC
# MAGIC Download and copy _patients_incr1.csv, patients_incr2.csv_ to volume staging path (/Volumes/\<catalog_name\>/\<schema_name\>/staging/patient)

# COMMAND ----------

vol_path = spark.sql("select staging_path").first()["staging_path"]
file_path = vol_path + "/patient"

# COMMAND ----------

# DBTITLE 1,initial file name
cloud_loc = "s3://one-env-uc-external-location/shyam.rao/demo_data/"

# COMMAND ----------

# DBTITLE 1,stage the file for initial load
dbutils.fs.cp(cloud_loc + "patients_incr1.csv", file_path)
dbutils.fs.cp(cloud_loc + "patients_incr2.csv", file_path)
