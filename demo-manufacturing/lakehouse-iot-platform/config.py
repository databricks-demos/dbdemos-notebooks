# Databricks notebook source
# MAGIC %md 
# MAGIC ## Configuration file
# MAGIC
# MAGIC Please change your catalog and schema here to run the demo on a different catalog.
# MAGIC
# MAGIC  
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=lakehouse&notebook=config&demo_name=lakehouse-patient-readmission&event=VIEW">

# COMMAND ----------

#Note: we do not recommend to change the catalog here as it won't impact all the demo resources such as DLT pipeline and Dashboards.
#Instead, please re-install the demo with a specific catalog and schema using dbdemos.install("lakehouse-retail-c360", catalog="..", schema="...")

catalog = "main_build"
schema = dbName = db = "dbdemos_iot_platform"

secret_scope_name = "dbdemos"
secret_key_name = "ai_agent_sp_token"

MODEL_SERVING_ENDPOINT_NAME   = "dbdemos_vs_endpoint"
VECTOR_SEARCH_ENDPOINT_NAME   = "dbdemos_iot_turbine_vector_endpoint"
FEATURE_SERVING_ENDPOINT_NAME = "dbdemos_iot_turbine_feature_endpoint"

volume_name = "turbine_raw_landing"
model_name = "dbdemos_turbine_maintenance"
agent_name = "dbdemos_agent_prescriptive_maintenance"
