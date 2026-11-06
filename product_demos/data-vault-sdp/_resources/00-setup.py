# Databricks notebook source
# MAGIC %md
# MAGIC # Setup: Create Bronze Tables from samples.tpch
# MAGIC
# MAGIC This notebook:
# MAGIC - Creates the catalog and schema if needed
# MAGIC - Seeds bronze tables from `samples.tpch` with `_ingested_at` timestamp
# MAGIC - Provides a helper function to mutate data for SCD2 demo in notebook 04

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit, col
from datetime import datetime, timedelta

# dbdemos rewrites these two lines to the catalog and schema chosen at install time
catalog = "main__build"
schema = dbName = db = "dbdemos_data_vault_sdp"
volume_path = f"/Volumes/{catalog}/{schema}"

print(f"Catalog: {catalog}, Schema: {schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Catalog and Schema

# COMMAND ----------

# Only create the catalog when it is missing: on workspaces with Default Storage,
# CREATE CATALOG IF NOT EXISTS fails even for an existing catalog.
if spark.sql(f"SHOW CATALOGS LIKE '{catalog}'").count() == 0:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
spark.sql(f"USE SCHEMA {schema}")

print(f"Catalog and schema created/verified: {catalog}.{schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Seed Bronze Tables from samples.tpch
# MAGIC
# MAGIC We read from `samples.tpch` and write to `bronze_<table>` with an `_ingested_at` timestamp.
# MAGIC This allows the vault to stream from bronze as a CDC source.

# COMMAND ----------

def seed_bronze_table(source_table: str, target_table: str):
    """Read from samples.tpch.<source_table> and write to <target_table> with _ingested_at."""
    df = spark.table(f"samples.tpch.{source_table}").withColumn(
        "_ingested_at", current_timestamp()
    )

    # Write as managed Delta table
    (
        df.coalesce(1)
        .write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{catalog}.{schema}.{target_table}")
    )

    count = spark.table(f"{catalog}.{schema}.{target_table}").count()
    print(f"Seeded {target_table}: {count} rows")

# Seed all tpch tables into bronze
seed_bronze_table("customer", "bronze_customer")
seed_bronze_table("orders", "bronze_orders")
seed_bronze_table("lineitem", "bronze_lineitem")
seed_bronze_table("part", "bronze_part")
seed_bronze_table("supplier", "bronze_supplier")
seed_bronze_table("partsupp", "bronze_partsupp")
seed_bronze_table("nation", "bronze_nation")
seed_bronze_table("region", "bronze_region")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper: mutate_for_scd2_demo()
# MAGIC
# MAGIC For notebook 04 (schema evolution demo), we need a way to simulate source updates.
# MAGIC This function applies small mutations to the bronze tables so notebook 04 can re-run the pipeline.

# COMMAND ----------

def mutate_for_scd2_demo():
    """
    Apply mutations to bronze_customer and bronze_orders for the SCD2 demo.
    - Change a customer's account balance (c_acctbal)
    - Change an order's status (o_orderstatus)
    - Insert a new customer
    """
    print("Applying mutations for SCD2 demo...")

    # Mutate bronze_customer: update c_acctbal for customer 1, insert new customer
    spark.sql(f"""
      UPDATE {catalog}.{schema}.bronze_customer
      SET c_acctbal = c_acctbal + 1000.00
      WHERE c_custkey = 1
    """)

    # Add a new customer (max c_custkey + 1)
    max_cust = spark.sql(f"SELECT MAX(c_custkey) FROM {catalog}.{schema}.bronze_customer").collect()[0][0]
    new_cust_key = int(max_cust) + 1

    spark.sql(f"""
      INSERT INTO {catalog}.{schema}.bronze_customer
      VALUES ({new_cust_key}, 'New Demo Customer', '123 Demo St', 10, '555-0000', 5000.00, 'BUILDING', 'Demo customer added for SCD2 test', current_timestamp())
    """)

    # Mutate bronze_orders: update o_orderstatus
    spark.sql(f"""
      UPDATE {catalog}.{schema}.bronze_orders
      SET o_orderstatus = 'S'
      WHERE o_orderkey = 1
    """)

    print("SCD2 demo mutations applied.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Tables Ready
# MAGIC
# MAGIC The following bronze tables are now ready to stream into the Data Vault:
# MAGIC - bronze_customer
# MAGIC - bronze_orders
# MAGIC - bronze_lineitem
# MAGIC - bronze_part
# MAGIC - bronze_supplier
# MAGIC - bronze_partsupp
# MAGIC - bronze_nation
# MAGIC - bronze_region

# COMMAND ----------

spark.sql(f"SHOW TABLES IN {catalog}.{schema}").display()
