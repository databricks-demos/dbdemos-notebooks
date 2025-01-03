# Databricks notebook source
catalog = "main__build"
schema = dbName = db = "dbdemos_uc_lineage"


# COMMAND ----------

# MAGIC %run ../../../../_resources/00-global-setup-v2

# COMMAND ----------

import pyspark.sql.functions as F
DBDemos.setup_schema(catalog, db, False, None)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS dinner ( recipe_id INT, full_menu STRING);
# MAGIC CREATE TABLE IF NOT EXISTS dinner_price ( recipe_id INT, full_menu STRING, price DOUBLE);
# MAGIC CREATE TABLE IF NOT EXISTS menu ( recipe_id INT, app STRING, main STRING, desert STRING);
# MAGIC CREATE TABLE IF NOT EXISTS price ( recipe_id BIGINT, price DOUBLE) ;
