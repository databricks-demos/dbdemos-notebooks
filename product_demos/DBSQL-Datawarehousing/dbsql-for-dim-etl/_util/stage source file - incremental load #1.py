# Databricks notebook source
# DBTITLE 1,initialize config for sgc
# MAGIC %run "./initialize-staging"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stage the source file for first incremental load

# COMMAND ----------

# DBTITLE 1,incremental file name (1)
file_name = "patients_incr1.csv"

# COMMAND ----------

# DBTITLE 1,stage the file for initial load
# download to volume staging path
get_file(file_name, "patient")

# COMMAND ----------

file_name = "patients_incr2.csv"

# COMMAND ----------

# download to volume staging path
get_file(file_name, "patient")