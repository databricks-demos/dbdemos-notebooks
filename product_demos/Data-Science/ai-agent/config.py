# Databricks notebook source
# MAGIC %md 
# MAGIC ## Configuration file
# MAGIC
# MAGIC Please change your catalog and schema here to run the demo on a different catalog.
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection or disable tracker during installation. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=lakehouse&notebook=config&demo_name=lakehouse-retail-c360&event=VIEW">

# COMMAND ----------

#Note: we do not recommend to change the catalog here as it won't impact all the demo resources such as SDP pipeline and Dashboards.
#Instead, please re-install the demo with a specific catalog and schema using dbdemos.install("lakehouse-retail-c360", catalog="..", schema="...")

catalog = "main_build"
schema = dbName = db = "dbdemos_ai_agent"

volume_name = "raw_data"
VECTOR_SEARCH_ENDPOINT_NAME="dbdemos_vs_endpoint"

MODEL_NAME = "dbdemos_ai_agent_demo"
ENDPOINT_NAME = f'{MODEL_NAME}_{catalog}_{db}'[:60]

# This must be a tool-enabled model
LLM_ENDPOINT_NAME = 'databricks-claude-3-7-sonnet'
