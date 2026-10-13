# Databricks notebook source
# MAGIC %md
# MAGIC # Deploy the Personalization Console
# MAGIC
# MAGIC
# MAGIC The Console is a Databricks App that serves a next-best action from each user's live session. It reads the sessions table in Lakebase as its own service principal.

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/product/realtime-clickstream-serving/realtime-personalization-architecture.png" width="100%" alt="Real-time personalization on live session state"/>

# COMMAND ----------

# MAGIC %pip install --quiet --upgrade "databricks-sdk>=0.85.0" "psycopg[binary]>=3.0"

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./_resources/00-setup

# COMMAND ----------

# DBTITLE 1,Demo Configuration
dbutils.widgets.text("app_name", "dbdemos-rtm-clickstream-app", "App name")
app_name = dbutils.widgets.get("app_name")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Application Provisioning
# MAGIC
# MAGIC The `database` resource binds the target Lakebase instance with `CAN_CONNECT_AND_CREATE` privileges, automatically provisioning a dedicated PostgreSQL role for the application's Service Principal.
# MAGIC
# MAGIC * **Deployment Source:** Source code is evaluated from the adjacent `app/` directory.

# COMMAND ----------

import os
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.apps import (
    App,
    AppResource,
    AppResourceDatabase,
    AppResourceDatabaseDatabasePermission,
    AppDeployment,
)

w = WorkspaceClient()
app_source_path = os.path.join(os.getcwd(), "app")

database_resource = AppResource(
    name="database",
    database=AppResourceDatabase(
        instance_name=lakebase_instance,
        database_name=lakebase_db,
        permission=AppResourceDatabaseDatabasePermission.CAN_CONNECT_AND_CREATE,
    ),
)

console_app = App(
    name=app_name,
    description="Personalization Console - live session state from Lakebase",
    default_source_code_path=app_source_path,
    resources=[database_resource],
)

try:
    w.apps.create_and_wait(app=console_app)
except Exception as e:
    if "already exists" in str(e):
        print(f"App '{app_name}' already exists, reusing it.")
    else:
        raise

app_sp = w.apps.get(name=app_name).service_principal_client_id
print(f"App '{app_name}' ready. Service principal client id: {app_sp}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deploy the App Code

# COMMAND ----------

w.apps.deploy_and_wait(app_name=app_name, app_deployment=AppDeployment(source_code_path=app_source_path))
print("Deployed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Service Principal Authorization
# MAGIC
# MAGIC The Personalization Console application queries the `live.sessions` table using its assigned Service Principal. This step provisions the required role-based privileges.
# MAGIC
# MAGIC * **Permissions:** Assigns `USAGE` on the schema and `SELECT` on the target table.
# MAGIC * **Implementation:** Executes shared access-control logic encapsulated in `DBDemos.grant_app_sp_read`.

# COMMAND ----------

DBDemos.grant_app_sp_read(w, lakebase_instance, lakebase_db, lakebase_schema, lakebase_table, app_sp)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Open the Console

# COMMAND ----------

print(w.apps.get(name=app_name).url)