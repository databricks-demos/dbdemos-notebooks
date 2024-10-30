# Databricks notebook source
# MAGIC %md 
# MAGIC # init notebook setting up the backend. 
# MAGIC
# MAGIC Do not edit the notebook, it contains import and helpers for the demo
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection or disable tracker during installation. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=data-science&org_id=1444828305810485&notebook=00-init&demo_name=chatbot-rag-llm&event=VIEW">

# COMMAND ----------

# MAGIC %run ../config

# COMMAND ----------

# MAGIC %run ../../../../_resources/00-global-setup-v2

# COMMAND ----------

dbutils.widgets.text("reset_all_data", "false", "Reset Data")
reset_all_data = dbutils.widgets.get("reset_all_data") == "true"
DBDemos.setup_schema(catalog, db, reset_all_data, volume_name)

# COMMAND ----------

from databricks.sdk import WorkspaceClient

def get_shared_warehouse(name=None):
    w = WorkspaceClient()
    warehouses = w.warehouses.list()
    for wh in warehouses:
        if wh.name == name:
            return wh
    for wh in warehouses:
        if wh.name.lower() == "shared endpoint":
            return wh
    for wh in warehouses:
        if wh.name.lower() == "dbdemos-shared-endpoint":
            return wh
    #Try to fallback to an existing shared endpoint.
    for wh in warehouses:
        if "shared" in wh.name.lower():
            return wh
    for wh in warehouses:
        if "dbdemos" in wh.name.lower():
            return wh
    for wh in warehouses:
        if wh.num_clusters > 0:
            return wh
    raise Exception("Couldn't find any Warehouse to use. Please create a wh first to run the demo and add the id here, or pass a name as parameter to the get_shared_warehouse(name='xxx') function")


def display_tools(tools):
    display(pd.DataFrame([{k: str(v) for k, v in vars(tool).items()} for tool in tools]))
