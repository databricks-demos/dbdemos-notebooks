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

    # Check for warehouse by exact name (if provided)
    if name:
        for wh in warehouses:
            if wh.name == name:
                return wh

    # Define fallback priorities
    fallback_priorities = [
        lambda wh: wh.name.lower() == "serverless starter warehouse",
        lambda wh: wh.name.lower() == "shared endpoint",
        lambda wh: wh.name.lower() == "dbdemos-shared-endpoint",
        lambda wh: "shared" in wh.name.lower(),
        lambda wh: "dbdemos" in wh.name.lower(),
        lambda wh: wh.num_clusters > 0,
    ]

    # Try each fallback condition in order
    for condition in fallback_priorities:
        for wh in warehouses:
            if condition(wh):
                return wh

    # Raise an exception if no warehouse is found
    raise Exception(
        "Couldn't find any Warehouse to use. Please create one first or pass "
        "a specific name as a parameter to the get_shared_warehouse(name='xxx') function."
    )


def display_tools(tools):
    display(pd.DataFrame([{k: str(v) for k, v in vars(tool).items()} for tool in tools]))
