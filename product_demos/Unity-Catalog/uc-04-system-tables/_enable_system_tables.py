# Databricks notebook source
# MAGIC %md
# MAGIC #Programatically enabling system tables for Databricks
# MAGIC Since system tables are governed by Unity Catalog, you need at least one Unity Catalog-governed workspace in your account to enable system tables. That way you can map your system tables to the Unity Catalog metastore. System tables must be enabled by an **account admin**. You can enable system tables in your account using the Databricks CLI, the Databricks SDK for Python, or by calling the Unity Catalog API in a notebook.
# MAGIC <br>
# MAGIC <br>
# MAGIC You can enable system tables using API calls or directly in a Databricks notebook (such as this example). Please refer to the documentation for your cloud for further details ([AWS](https://docs.databricks.com/administration-guide/system-tables/index.html) | [Azure](https://learn.microsoft.com/en-us/azure/databricks/administration-guide/system-tables/))
# MAGIC
# MAGIC
# MAGIC **Running this notebook will enable the system tables and you will see them in your system catalog.**
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

# MAGIC %pip install databricks-sdk==0.30.0
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Get current Metastore ID
metastore_id = spark.sql("SELECT current_metastore() as metastore_id").collect()[0]["metastore_id"]
metastore_id = metastore_id[metastore_id.rfind(':')+1:]
dbutils.widgets.removeAll()

# COMMAND ----------

# DBTITLE 1,Create widget and assign default Metastore ID
dbutils.widgets.text("metastore_id", metastore_id, "Metastore ID")
metastore_id = dbutils.widgets.get("metastore_id")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Enabling schemas
# MAGIC Use Python SDK and the workspace client to authenticate, identify available schemas, and enable those schemas

# COMMAND ----------

# DBTITLE 1,Enable schemas
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

system_schemas = w.system_schemas.list(metastore_id=metastore_id)

not_enabled = [system_schema for system_schema in system_schemas if system_schema.state.value == 'AVAILABLE']
print(f"Schemas that are not enabled: {not_enabled}")

for system_schema in not_enabled:
    print(f'Enabling system schema "{system_schema.schema}"')
    w.system_schemas.enable(metastore_id=metastore_id, schema_name=system_schema.schema)
