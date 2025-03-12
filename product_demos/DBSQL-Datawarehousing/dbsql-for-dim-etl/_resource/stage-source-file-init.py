# Databricks notebook source
# MAGIC %md
# MAGIC ## Stage the source file for initial load
# MAGIC
# MAGIC This notebook simulates the uploading of source data file(s) to a data staging location, to be ingested into the Data Warehouse.

# COMMAND ----------

# DBTITLE 1,initialize config for sgc
# MAGIC %run "../01-Setup/01.1-initialize"

# COMMAND ----------

# MAGIC %md
# MAGIC Copy / upload _patients50_init.csv_ to volume staging path (/Volumes/\<catalog_name\>/\<schema_name\>/staging/patient)

# COMMAND ----------

vol_path = spark.sql("select staging_path").first()["staging_path"]

# COMMAND ----------

# clear source files (if existing)
dbutils.fs.rm(vol_path + "/patient" + "/patients50_init.csv")
dbutils.fs.rm(vol_path + "/patient" + "/patients_incr1.csv")
dbutils.fs.rm(vol_path + "/patient" + "/patients_incr2.csv")

# COMMAND ----------

# create staging folder (if not existing)
dbutils.fs.mkdirs(vol_path + "/patient")

# COMMAND ----------

# DBTITLE 1,stage the file for initial load
cloud_loc = "s3://dbdemos-dataset/dbsql/sql-etl-hls-patient"
dbutils.fs.cp(cloud_loc + "/patients50_init.csv", vol_path + "/patient")
