# Databricks notebook source
# MAGIC %md 
# MAGIC ## Configuration file
# MAGIC
# MAGIC Please change your catalog and schema here to run the demo on a different catalog.
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection or disable tracker during installation. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=lakehouse&notebook=config&demo_name=lakehouse-retail-c360&event=VIEW">

# COMMAND ----------

#Note: we do not recommend to change the catalog here as it won't impact all the demo resources such as DLT pipeline and Dashboards.
#Instead, please re-install the demo with a specific catalog and schema using dbdemos.install("...", catalog="..", schema="...")

catalog = "nuwan"
schema = dbName = db = "sc_rai"

#catalog = "main__build"
#schema = dbName = db = "dbdemos_fsi_credit_decisioning"

volume_name = "credit_raw_data"
model_name = "rai_credit_decisioning"
endpoint_name = "rai_credit_decisioning_endpoint_1"
