# Databricks notebook source
# MAGIC %md
# MAGIC #Programatically enabling system tables for Databricks
# MAGIC Since system tables are governed by Unity Catalog, you need at least one Unity Catalog-governed workspace in your account to enable system tables. That way you can map your system tables to the Unity Catalog metastore. System tables must be enabled by an **account admin**. You can enable system tables in your account using either the Databricks CLI or by calling the Unity Catalog API in a notebook.
# MAGIC <br>
# MAGIC <br>
# MAGIC You can enable system tables using API calls or directly in a Databricks notebook (such as this example). Please refer to the documentation for your cloud for further details ([AWS](https://docs.databricks.com/administration-guide/system-tables/index.html) | [Azure](https://learn.microsoft.com/en-us/azure/databricks/administration-guide/system-tables/))
# MAGIC
# MAGIC
# MAGIC **Running this notebook will turn the system table and you will see them in your system catalog.**
# MAGIC
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection or disable tracker during installation. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=governance&notebook=_enable_system_tables&demo_name=04-system-tables&event=VIEW">

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating the widget to be used as input for the metastore ID
# MAGIC The metastore ID can be found by clicking on the metastore details icon present in the `Data` tab of your Databricks workspace.
# MAGIC ![metastore_id_image](./images/metastore_id.png)

# COMMAND ----------

# MAGIC %pip install databricks-sdk==0.20.0
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

metastore_id = spark.sql("SELECT current_metastore() as metastore_id").collect()[0]["metastore_id"]
metastore_id = metastore_id[metastore_id.rfind(':')+1:]
dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.text("metastore_id", metastore_id, "Metastore ID")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setting up authentication
# MAGIC We are leveraging the fact that we are already using a Databricks notebook and so we don't need to handle tokens and environment variables, we can import what we need using dbutils

# COMMAND ----------

import requests
from time import sleep
metastore_id = dbutils.widgets.get("metastore_id")
host = "https://"+dbutils.notebook.entry_point.getDbutils().notebook().getContext().browserHostName().get()
from databricks.sdk import WorkspaceClient
headers = WorkspaceClient().config.authenticate()

# COMMAND ----------

# MAGIC %md
# MAGIC Check which schemas are available in order to go through the list of schemas that we need to enable

# COMMAND ----------

import json
r = requests.get(f"{host}/api/2.0/unity-catalog/metastores/{metastore_id}/systemschemas", headers=headers).json()
print(json.dumps(r, indent=1))

# COMMAND ----------

schemas_to_enable = []
already_enabled = []
others = []
for schema in r['schemas']:
    if schema['state'].lower() == "available":
        schemas_to_enable.append(schema["schema"])
    elif schema['state'].lower() == "enable_completed":
        already_enabled.append(schema["schema"])
    else:
        others.append(schema["schema"])
print(f"Schemas that will be enabled: {schemas_to_enable}")
print(f"Schemas that are already enabled: {already_enabled}")
print(f"Unavailable schemas: {others}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Enabling schemas

# COMMAND ----------

for schema in schemas_to_enable:
    host = "https://"+dbutils.notebook.entry_point.getDbutils().notebook().getContext().browserHostName().get()
    headers = {"Authorization": "Bearer "+dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()}
    r = requests.put(f"{host}/api/2.0/unity-catalog/metastores/{metastore_id}/systemschemas/{schema}", headers=headers)
    if r.status_code == 200:
        print(f"Schema {schema} enabled successfully")
    else:
        print(f"""Error enabling the schema `{schema}`: {r.json()["error_code"]} | Description: {r.json()["message"]}""")
    sleep(1)
