# Databricks notebook source
# MAGIC %md
# MAGIC ## Stage the source file for incremental load
# MAGIC
# MAGIC This notebook simulates the uploading of new and incremental source data extracts to a data staging location for, to be ingested into the Data Warehouse.

# COMMAND ----------

# DBTITLE 1,initialize config for sgc
# MAGIC %run "../01-Setup/01.1-initialize"

# COMMAND ----------

# MAGIC %md
# MAGIC Copy / upload _patients_incr1.csv, patients_incr2.csv_ to volume staging path (/Volumes/\<catalog_name\>/\<schema_name\>/staging/patient)

# COMMAND ----------

vol_path = spark.sql("select staging_path").first()["staging_path"]

# COMMAND ----------

# DBTITLE 1,initial file name
cloud_loc = "s3://dbdemos-dataset/dbsql/sql-etl-hls-patient"

# COMMAND ----------

# DBTITLE 1,stage the file for initial load
dbutils.fs.cp(cloud_loc + "/patients_incr1.csv", vol_path + "/patient")
dbutils.fs.cp(cloud_loc + "/patients_incr2.csv", vol_path + "/patient")
