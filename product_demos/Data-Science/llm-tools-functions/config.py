# Databricks notebook source
# MAGIC %md 
# MAGIC ## Configuration file
# MAGIC
# MAGIC Please change your catalog and schema here to run the demo on a different catalog.
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection or disable tracker during installation. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=data-science&org_id=1444828305810485&notebook=config&demo_name=chatbot-rag-llm&event=VIEW">

# COMMAND ----------

VECTOR_SEARCH_ENDPOINT_NAME="dbdemos_vs_endpoint"


#catalog = "main__build"
catalog = "main_dbdemo_agent"
dbName = db = "dbdemos_agent_tools"
volume_name = "dbdemos_agent_volume"
