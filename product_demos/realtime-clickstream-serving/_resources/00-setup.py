# Databricks notebook source
# MAGIC %md
# MAGIC ## Setup
# MAGIC Catalog/schema for tracking demo state. The actual session data lives in **Lakebase**
# MAGIC (Postgres), not the Lakehouse - that's the whole point of this demo. We use a Unity Catalog schema
# MAGIC only as a place to record the Lakebase instance name + a status log.

# COMMAND ----------

dbutils.widgets.text("lakebase_instance", "clickstream-sessions", "Lakebase Provisioned instance name")
dbutils.widgets.text("lakebase_db",       "databricks_postgres", "Lakebase database name (default Lakebase DB)")
dbutils.widgets.text("lakebase_schema",   "live", "Lakebase schema name")
dbutils.widgets.text("lakebase_table",    "sessions", "Lakebase table name")

# COMMAND ----------

catalog = "main__build"
schema = db = "dbdemos_realtime_clickstream"
lakebase_instance = dbutils.widgets.get("lakebase_instance")
lakebase_db      = dbutils.widgets.get("lakebase_db")
lakebase_schema  = dbutils.widgets.get("lakebase_schema")
lakebase_table   = dbutils.widgets.get("lakebase_table")

# COMMAND ----------

# Inlined DBDemos helpers (same as the micro-batch sibling clickstream-direct-to-lakehouse).
import time

class DBDemos:
  @staticmethod
  def setup_schema(catalog, db):
    assert catalog not in ("hive_metastore", "spark_catalog"), "Demo requires Unity Catalog."
    current = spark.sql("SELECT current_catalog() c").collect()[0]["c"]
    if current != catalog:
      catalogs = [r["catalog"] for r in spark.sql("SHOW CATALOGS").collect()]
      if catalog not in catalogs:
        try:
          spark.sql(f"CREATE CATALOG IF NOT EXISTS `{catalog}`")
        except Exception as e:
          raise RuntimeError(
            f"Could not create catalog `{catalog}`: {str(e).splitlines()[0]}\n"
            f"This workspace may not allow creating catalogs (eg governed prod, or Default Storage). "
            f"Set the `catalog` variable in this setup notebook to an existing Unity Catalog catalog you can create schemas in, "
            f"and do not point this demo at a production catalog."
          ) from None
      spark.sql(f"USE CATALOG `{catalog}`")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS `{db}`")
    spark.sql(f"USE `{catalog}`.`{db}`")
    print(f"Using {catalog}.{db}")

  @staticmethod
  def stop_all_streams(sleep_time=0):
    if sleep_time: time.sleep(sleep_time)
    for q in spark.streams.active:
      try: q.stop()
      except Exception as e: print(f"  failed to stop stream {q.id}: {e}")

  @staticmethod
  def grant_app_sp_read(w, lakebase_instance, lakebase_db, lakebase_schema, lakebase_table, app_sp):
    # Grant the app service principal USAGE on the schema and SELECT on the sessions table. The SP's
    # Postgres role appears once the app's database resource attaches, so poll for it before granting.
    import time, uuid, psycopg
    inst = w.database.get_database_instance(name=lakebase_instance)
    def _connect():
      cred = w.database.generate_database_credential(
        request_id=str(uuid.uuid4()), instance_names=[lakebase_instance])
      return psycopg.connect(
        f"host={inst.read_write_dns} dbname={lakebase_db} "
        f"user={w.current_user.me().user_name} password={cred.token} sslmode=require")
    with _connect() as conn:
      conn.autocommit = True
      with conn.cursor() as cur:
        for _ in range(20):
          cur.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (app_sp,))
          if cur.fetchone() is not None:
            break
          print(f"  waiting for the app service principal role '{app_sp}' to appear...")
          time.sleep(15)
        else:
          raise RuntimeError(
            f"Service principal role '{app_sp}' did not appear on {lakebase_instance}. "
            f"Confirm the app deployed, then re-run this step.")
        cur.execute(f'GRANT USAGE ON SCHEMA {lakebase_schema} TO "{app_sp}"')
        cur.execute(f'GRANT SELECT ON {lakebase_schema}.{lakebase_table} TO "{app_sp}"')
        cur.execute(
          """SELECT privilege_type FROM information_schema.role_table_grants
             WHERE table_schema=%s AND table_name=%s AND grantee=%s""",
          (lakebase_schema, lakebase_table, app_sp))
        privs = sorted(r[0] for r in cur.fetchall())
        cur.execute("SELECT has_schema_privilege(%s, %s, 'USAGE')", (app_sp, lakebase_schema))
        usage_ok = cur.fetchone()[0]
    print(f"Granted. '{app_sp}' now holds {privs} on {lakebase_schema}.{lakebase_table} (schema USAGE={usage_ok}).")

# COMMAND ----------

DBDemos.setup_schema(catalog, db)

# Managed volume for streaming checkpoints
spark.sql(f"CREATE VOLUME IF NOT EXISTS `{catalog}`.`{db}`.`_checkpoints`")

import pyspark.sql.functions as F
from pyspark.sql.functions import col
