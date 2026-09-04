# Databricks notebook source
# MAGIC %md
# MAGIC ## Setup
# MAGIC Creates the catalog, schema, and volume used by this demo.
# MAGIC Set `catalog` and `schema` below if you don't want the defaults.

# COMMAND ----------

dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")

reset_all_data = dbutils.widgets.get("reset_all_data") == "true"

# COMMAND ----------

catalog = "main__build"
schema = dbName = db = "dbdemos_clickstream_zerobus"
volume_name = "checkpoints"

# COMMAND ----------

# MAGIC %md
# MAGIC ### DBDemos Helpers (Inlined So This Demo Is Self-Contained)
# MAGIC
# MAGIC When packaged via dbdemos, this gets replaced by `%run ../../../_resources/00-global-setup-v2`.

# COMMAND ----------

import time

class DBDemos:
  @staticmethod
  def setup_schema(catalog, db, reset_all_data, volume_name=None):
    if reset_all_data:
      print(f"Resetting demo data: dropping schema `{catalog}`.`{db}` and volume `{volume_name}`")
      try:
        if volume_name:
          spark.sql(f"DROP VOLUME IF EXISTS `{catalog}`.`{db}`.`{volume_name}`")
        spark.sql(f"DROP SCHEMA IF EXISTS `{catalog}`.`{db}` CASCADE")
      except Exception as e:
        print(f"  catalog or schema didn't exist, skipping reset ({e})")

    assert catalog not in ("hive_metastore", "spark_catalog"), \
      "This demo requires Unity Catalog. Pick a Unity Catalog catalog name."

    current = spark.sql("SELECT current_catalog() AS c").collect()[0]["c"]
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

    if volume_name:
      spark.sql(f"CREATE VOLUME IF NOT EXISTS `{volume_name}`")

  @staticmethod
  def wait_for_table(table_name, timeout_seconds=180):
    """Block until the table exists and has at least one row."""
    start = time.time()
    while not spark.catalog.tableExists(table_name) or spark.table(table_name).count() == 0:
      if time.time() - start > timeout_seconds:
        raise TimeoutError(f"Timed out waiting for `{table_name}` to populate.")
      time.sleep(2)
    print(f"  table `{table_name}` is ready")

# COMMAND ----------

DBDemos.setup_schema(catalog, db, reset_all_data, volume_name)
volume_folder = f"/Volumes/{catalog}/{db}/{volume_name}"
print(f"Volume folder: {volume_folder}")

# COMMAND ----------

try:
  spark.conf.set("spark.default.parallelism", "12")
  spark.conf.set("spark.sql.shuffle.partitions", "12")
except Exception as e:
  print(f"Skipping parallelism conf (likely on serverless): {e}")
