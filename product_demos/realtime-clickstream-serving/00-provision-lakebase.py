# Databricks notebook source
# MAGIC %md
# MAGIC ## Live Session Serving with Lakebase
# MAGIC
# MAGIC Traditional architectures rely on a separate operational data store synchronized with the lakehouse to serve live session state. **Lakebase consolidates this stack** by enabling the streaming pipeline to write session data directly into a managed PostgreSQL instance, which downstream applications consume via standard SQL.
# MAGIC
# MAGIC > **Provisioning Note:** This notebook performs a one-time initialization; subsequent executions automatically target and reuse the existing instance.

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/product/realtime-clickstream-serving/realtime-personalization-architecture.png" width="100%" alt="Real-time personalization on live session state"/>

# COMMAND ----------

# DBTITLE 1,Demo Configuration
dbutils.widgets.text("lakebase_instance", "clickstream-sessions",         "Lakebase instance name")
dbutils.widgets.text("lakebase_db",       "databricks_postgres",          "Lakebase database name")
dbutils.widgets.text("lakebase_schema",   "live",                         "Lakebase schema name")
dbutils.widgets.text("lakebase_table",    "sessions",                     "Lakebase table name")
dbutils.widgets.text("app_service_principal", "",                         "App SP id for read grant (set after deploying the app)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sizing and Cost
# MAGIC
# MAGIC We use the smallest tier (**CU_1**) - plenty for demo loads. For production, scale up or switch to Lakebase Autoscaling (scale-to-zero).
# MAGIC
# MAGIC | Tier  | RAM   | When to use |
# MAGIC |-------|-------|-------------|
# MAGIC | CU_1  | 16 GB | Demo, development, small applications (this notebook's default) |
# MAGIC | CU_2  | 32 GB | Light production |
# MAGIC | CU_4  | 64 GB | Production with mid-size working sets |
# MAGIC | CU_8  | 128 GB | High concurrency or large state |
# MAGIC
# MAGIC The instance persists indefinitely until explicitly stopped. Execute `w.database.update_database_instance(stopped=True)` post-demo to prevent unnecessary compute charges.

# COMMAND ----------

# MAGIC %run ./_resources/00-setup

# COMMAND ----------

# MAGIC %pip install --quiet --upgrade "databricks-sdk>=0.85.0" "protobuf==5.29.5" "psycopg[binary]>=3.0"

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./_resources/00-setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1/ Create (or Reuse) the Lakebase Instance

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.database import DatabaseInstance
from databricks.sdk.errors.platform import NotFound, ResourceConflict, ResourceAlreadyExists
import time

w = WorkspaceClient()

try:
  inst = w.database.get_database_instance(name=lakebase_instance)
  print(f"Reusing existing Lakebase instance: {inst.name} (state={inst.state})")
  # If a previous run stopped the instance, resume it so the wait loop below can reach AVAILABLE.
  if getattr(inst, "effective_stopped", False) or str(inst.state).upper().endswith("STOPPED"):
    print("Instance is stopped, resuming it...")
    w.database.update_database_instance(
      name=lakebase_instance,
      database_instance=DatabaseInstance(name=lakebase_instance, stopped=False),
      update_mask="stopped",
    )
except NotFound:
  print(f"Creating new Lakebase instance: {lakebase_instance}")
  # create_database_instance hands back a long-running-operation handle, not the instance itself,
  # so we do not read .state off it here. The wait loop just below re-fetches the instance and polls.
  w.database.create_database_instance(
    database_instance=DatabaseInstance(
      name=lakebase_instance,
      capacity="CU_1",
      stopped=False,
    )
  )
  print("Submitted. Waiting for it to become available...")

# Wait until ready
for _ in range(60):
  inst = w.database.get_database_instance(name=lakebase_instance)
  if str(inst.state).upper().endswith("AVAILABLE") or str(inst.state).upper() == "AVAILABLE":
    break
  print(f"  state={inst.state} - waiting...")
  time.sleep(15)

print(f"\nReady: {inst.name}")
print(f"  DNS:      {inst.read_write_dns}")
print(f"  Capacity: {inst.capacity}")
print(f"  State:    {inst.state}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2/ Provision the Target State Table
# MAGIC
# MAGIC The streaming sink executes low-latency upserts into this target table, maintaining a single mutable record per active user that updates continuously as the session evolves. This table serves as the real-time feature store queried by the personalization layer during user interactions.
# MAGIC
# MAGIC ### Materialized State Attributes:
# MAGIC * **Location context:** Current application surface.
# MAGIC * **Funnel progression:** Derived user intent state.
# MAGIC * **Engagement score:** Quantified real-time activity metric.
# MAGIC * **Assistance status:** Active flag for support surface interactions.

# COMMAND ----------

import psycopg, uuid

def _credential():
  cred = w.database.generate_database_credential(
    request_id=str(uuid.uuid4()),
    instance_names=[lakebase_instance],
  )
  return cred.token

def _connect(dbname: str):
  return psycopg.connect(
    f"host={inst.read_write_dns} "
    f"dbname={dbname} "
    f"user={w.current_user.me().user_name} "
    f"password={_credential()} "
    f"sslmode=require"
  )

# Targets the default 'databricks_postgres' database; no CREATE DATABASE statement required.
# The 'AVAILABLE' state status slightly precedes network socket readiness. 
# Implement connection retries to prevent premature initialization failures.
for _attempt in range(40):
  try:
    _probe = _connect(lakebase_db); _probe.close(); break
  except Exception as _e:
    print(f"  waiting for the Postgres endpoint to accept connections... ({str(_e)[:70]})")
    time.sleep(15)

with _connect(lakebase_db) as app_conn:
  with app_conn.cursor() as cur:
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {lakebase_schema}")
    cur.execute(f"""
      CREATE TABLE IF NOT EXISTS {lakebase_schema}.{lakebase_table} (
        user_id          text PRIMARY KEY,
        click_count      bigint,        -- events seen this session
        start_time       bigint,
        end_time         bigint,
        status           text,          -- online / offline
        current_surface  text,          -- the app surface the user is on right now
        funnel_stage     text,          -- browsing / engaged / converting
        engagement_score bigint,        -- 0-100, derived from activity and funnel progress
        needs_help       boolean,       -- has the user hit a support surface
        last_updated     timestamptz NOT NULL DEFAULT now()
      )
    """)
  app_conn.commit()
print(f"  schema + table ready: {lakebase_db}.{lakebase_schema}.{lakebase_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3/ Initial State Verification

# COMMAND ----------

with _connect(lakebase_db) as app_conn:
  with app_conn.cursor() as cur:
    cur.execute(f"SELECT COUNT(*) FROM {lakebase_schema}.{lakebase_table}")
    n = cur.fetchone()[0]
    cur.execute(f"""SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE table_schema=%s AND table_name=%s
                    ORDER BY ordinal_position""", (lakebase_schema, lakebase_table))
    cols = cur.fetchall()
print(f"Rows in {lakebase_schema}.{lakebase_table}: {n}")
print("Schema:")
for c, t in cols:
  print(f"  {c}: {t}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4/ Grant Service Principal Read Access
# MAGIC
# MAGIC The downstream Personalization Console application authenticates via a dedicated Service Principal (SP). This cell configures the required role-based access control (RBAC).
# MAGIC
# MAGIC ### Infrastructure Dependencies
# MAGIC * **Execution Order:** This step must be run **after** application deployment. The SP's PostgreSQL role is only provisioned when the application's `database` resource attaches.
# MAGIC * **Required Privileges:** Grants `USAGE` on the target schema and `SELECT` on the table.
# MAGIC
# MAGIC ### Execution Instructions
# MAGIC 1. Input the application SP's Client ID into the `app_service_principal` widget.
# MAGIC 2. Execute the cell.
# MAGIC
# MAGIC > **Note:** This operation is idempotent and can be safely re-executed to reassert or remediate access control states.

# COMMAND ----------

# Provisions read privileges for the application Service Principal using DBDemos.grant_app_sp_read.
# Automatically skips if the widget is unpopulated. Post-deployment, populate with the SP client ID and re-run.
app_sp = dbutils.widgets.get("app_service_principal").strip()
if not app_sp:
  print("app_service_principal widget is empty, skipping the grant. Set it to your app SP's client id "
        "and re-run this cell after deploying the Personalization Console app.")
else:
  DBDemos.grant_app_sp_read(w, lakebase_instance, lakebase_db, lakebase_schema, lakebase_table, app_sp)

# COMMAND ----------

# MAGIC %md
# MAGIC **Connection info recorded for the streaming notebook:**
# MAGIC - Instance name: `{lakebase_instance}` (widgets carry this through)
# MAGIC - Target table:  `{lakebase_db}.{lakebase_schema}.{lakebase_table}`
# MAGIC
# MAGIC ### Next: [01-realtime-sessionize]($./01-realtime-sessionize) - the rate source feeds Real-Time Mode sessionization, which writes to Lakebase.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Production Optimization: Lakebase Autoscaling
# MAGIC
# MAGIC For production workloads, leverage **Lakebase Autoscaling** to enable scale-to-zero cost efficiencies. This mechanism automatically suspends the PostgreSQL compute instance during idle periods and wakes it transparently upon the first incoming query.
# MAGIC
# MAGIC * **Code Impact:** The core streaming pipeline logic remains completely unchanged.
# MAGIC * **Provisioning Adjustment:** Update the initialization notebook to invoke `w.postgres.create_project()` instead of `w.database.create_database_instance()`.
# MAGIC
# MAGIC For comprehensive implementation details, reference the [Lakebase Autoscaling Documentation](https://docs.databricks.com/aws/en/oltp/projects/autoscaling).