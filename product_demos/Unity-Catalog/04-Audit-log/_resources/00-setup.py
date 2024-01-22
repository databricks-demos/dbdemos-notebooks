# Databricks notebook source
# MAGIC %md 
# MAGIC # Setup notebook. 
# MAGIC 
# MAGIC ## Require CREATE CATALOG permission in the metastore.
# MAGIC Create the required database, don't run this notebook independantly, it's called by the main notebook.

# COMMAND ----------

dbutils.widgets.text("catalog", "dbdemos", "UC Catalog")

# COMMAND ----------

import pyspark.sql.functions as F
from concurrent.futures import ThreadPoolExecutor
import collections

catalog = dbutils.widgets.get("catalog")

catalog_exists = False
for r in spark.sql("SHOW CATALOGS").collect():
    if r['catalog'] == catalog:
        catalog_exists = True

#As non-admin users don't have permission by default, let's do that only if the catalog doesn't exist (an admin need to run it first)     
if not catalog_exists:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
    spark.sql(f"GRANT CREATE, USAGE on CATALOG {catalog} TO `account users`")
spark.sql(f"USE CATALOG {catalog}")

db_name = "audit_log"
db_not_exist = len([db for db in spark.sql("SHOW DATABASES").collect() if db.databaseName == db_name]) == 0
if db_not_exist:
  spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.{db_name} ")
  spark.sql(f"GRANT CREATE, USAGE on DATABASE {catalog}.{db_name} TO `account users`")
  
spark.conf.set("spark.databricks.cloudFiles.schemaInference.sampleSize.numFiles", "100000000")
