# Databricks notebook source
# MAGIC %md 
# MAGIC ## Configuration file
# MAGIC
# MAGIC Please change your catalog and schema here to run the demo on a different catalog.
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection or disable tracker during installation. View README for more details.  -->
# MAGIC
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=governance&notebook=config&demo_name=uc-01-acl&event=VIEW">

# COMMAND ----------

#Note: we do not recommend to change the catalog here as it won't impact all the demo resources such as DLT pipeline and Dashboards.
#Instead, please re-install the demo with a specific catalog and schema using dbdemos.install("lakehouse-retail-c360", catalog="..", schema="...")

catalog = "main_build"
schema = dbName = db = "dbdemos_uc_01_acl"

# COMMAND ----------

# MAGIC %md
# MAGIC
