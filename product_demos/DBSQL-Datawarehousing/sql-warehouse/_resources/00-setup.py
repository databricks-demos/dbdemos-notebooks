# Databricks notebook source
# MAGIC %md 
# MAGIC #### Setup for PK/FK demo notebook
# MAGIC
# MAGIC Hide this notebook result.

# COMMAND ----------

# MAGIC %run ../config

# COMMAND ----------

# MAGIC %run ../../../../_resources/00-global-setup-v2

# COMMAND ----------

DBDemos.setup_schema(catalog, db, False)
