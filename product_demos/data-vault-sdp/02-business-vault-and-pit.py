# Databricks notebook source
# MAGIC %md
# MAGIC # Business Vault and Point-in-Time Tables
# MAGIC
# MAGIC The business vault adds derived or computed tables to make querying the raw vault faster.
# MAGIC Two key patterns:
# MAGIC 1. **Computed Satellites:** Aggregations (revenue, counts) that are expensive to compute on every query.
# MAGIC 2. **Point-in-Time (PIT) Tables:** Pre-assembled snapshots for fast satellite reassembly.

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col, lit, current_timestamp, max as spark_max, sum as spark_sum, count
)

catalog = spark.conf.get("catalog", "main")
schema = spark.conf.get("schema", "dbdemos_data_vault_sdp")

# COMMAND ----------

# MAGIC %md
# MAGIC ## WARNING: Filtering Satellites for Current Rows
# MAGIC
# MAGIC Satellites track history. When you query a satellite, always filter for the current version:
# MAGIC `WHERE __END_AT IS NULL`
# MAGIC
# MAGIC Without this filter, a dimension built from a 3-year-old satellite will silently have 3x
# MAGIC more rows than it should, multiplying all downstream fact counts by accident.
# MAGIC
# MAGIC This is the most common Data Vault bug and causes subtle analytics failures.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Computed Satellite: Order Metrics
# MAGIC
# MAGIC The raw vault requires many joins to compute order metrics: total lines, net revenue, etc.
# MAGIC The computed satellite `sat_order_metrics` materializes these once per load.
# MAGIC
# MAGIC This is a one-to-one with hub_order (one row per order), derived from raw lineitem.

# COMMAND ----------

@dp.table
def sat_order_metrics():
    """
    Computed satellite: order-level metrics from lineitem.
    Groups by order, computes line_count, total_extended_price, total_discount, total_tax, net_revenue.
    """
    return spark.sql(f"""
      SELECT
        l_orderkey AS o_orderkey,
        COUNT(*) as line_count,
        SUM(l_extendedprice) as total_extended_price,
        SUM(l_discount) as total_discount,
        SUM(l_tax) as total_tax,
        SUM(l_extendedprice * (1 - l_discount)) as net_revenue,
        current_timestamp() as load_ts,
        'computed_from_lineitem' as record_source
      FROM {catalog}.{schema}.bronze_lineitem
      GROUP BY l_orderkey
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Point-in-Time (PIT) Table
# MAGIC
# MAGIC A PIT table is a denormalized snapshot that, for each hub key and load_ts, holds
# MAGIC the hash of the current (as of that load_ts) satellite row for each satellite.
# MAGIC
# MAGIC **Why PIT?** Querying the raw vault means joining hub + sat_customer + sat_order + ... = many joins.
# MAGIC A PIT table pre-computes "for order 123 as of 2024-01-15, which versions of
# MAGIC sat_customer, sat_order, sat_part were current?" in a single row.
# MAGIC
# MAGIC **The cost-benefit trade-off:**
# MAGIC - Without PIT: Raw vault queries are fast to write (few lines) but slow to run (many joins, many satellite scans).
# MAGIC - With PIT: Raw vault queries are slow to write (navigate PIT + lookup sats) but fast to run (one join to PIT).
# MAGIC - **Solution:** Build an information mart (gold layer) on top that does the lookups for you.

# COMMAND ----------

@dp.table
def pit_order():
    """
    Simplified PIT for hub_order: one row per order, with current satellite hashes.
    Use this to check if a customer's address changed between loads without joining all satellites.

    WARNING: Filter satellites for current rows (__END_AT IS NULL) to avoid row count multiplication.
    """
    return spark.sql(f"""
      SELECT
        ho.order_hk,
        MAX(ho.load_ts) as pit_load_ts,
        MAX(CASE WHEN so.__END_AT IS NULL THEN so.hash_diff ELSE NULL END) as sat_order_hash_current,
        MAX(CASE WHEN sc.__END_AT IS NULL THEN sc.hash_diff ELSE NULL END) as sat_customer_hash_current,
        'computed' as record_source
      FROM {catalog}.{schema}.hub_order ho
      LEFT JOIN {catalog}.{schema}.sat_order so
        ON ho.order_hk = so.order_hk
      LEFT JOIN {catalog}.{schema}.lnk_order_customer loc
        ON ho.order_hk = loc.order_hk
      LEFT JOIN {catalog}.{schema}.sat_customer sc
        ON loc.customer_hk = sc.customer_hk
        AND sc.__END_AT IS NULL
      GROUP BY ho.order_hk
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cost Analysis: Raw Vault vs. PIT vs. Mart
# MAGIC
# MAGIC Query: "For each order, show order date, customer name, total revenue, and shipping country."
# MAGIC
# MAGIC ### Option 1: Query raw vault directly (slow)
# MAGIC ```sql
# MAGIC SELECT
# MAGIC   ho.order_hk, so.o_orderdate, sc.c_name, sm.net_revenue, sn.n_name
# MAGIC FROM hub_order ho
# MAGIC JOIN sat_order so ON ho.order_hk = so.order_hk AND so.__END_AT IS NULL
# MAGIC JOIN lnk_order_customer loc ON ho.order_hk = loc.order_hk
# MAGIC JOIN sat_customer sc ON loc.customer_hk = sc.customer_hk AND sc.__END_AT IS NULL
# MAGIC JOIN lnk_customer_nation lcn ON sc.customer_hk = lcn.customer_hk
# MAGIC JOIN sat_nation sn ON lcn.nation_hk = sn.nation_hk AND sn.__END_AT IS NULL
# MAGIC JOIN sat_order_metrics sm ON ho.order_hk = sm.order_hk;
# MAGIC ```
# MAGIC **Cost:** 6 joins, all tables scanned, filtering for current versions (__END_AT IS NULL), expensive.
# MAGIC **Speed:** Slow for repeated queries.
# MAGIC
# MAGIC ### Option 2: Query through PIT (fast, but complex)
# MAGIC ```sql
# MAGIC SELECT
# MAGIC   p.order_hk, p.sat_order_hash, p.customer_hk, p.record_source
# MAGIC FROM pit_order p;
# MAGIC -- Then look up sat_order, sat_customer, etc., using the hashes if needed.
# MAGIC ```
# MAGIC **Cost:** 1 join (just PIT), but requires post-lookup for full data.
# MAGIC **Speed:** Fast, but adds application-level logic.
# MAGIC
# MAGIC ### Option 3: Query through information mart (fast and simple)
# MAGIC ```sql
# MAGIC SELECT
# MAGIC   order_id, order_date, customer_name, net_revenue, nation_name
# MAGIC FROM fact_lineitem f
# MAGIC JOIN dim_customer c ON f.customer_id = c.customer_id
# MAGIC JOIN dim_nation n ON c.nation_id = n.nation_id;
# MAGIC ```
# MAGIC **Cost:** 2 joins, pre-aggregated, pre-denormalized.
# MAGIC **Speed:** Fast.
# MAGIC **Downside:** Mart must be rebuilt for every new query pattern.
# MAGIC
# MAGIC **Conclusion:** Build the vault for flexibility (change-proof). Build the mart for speed.
# MAGIC PIT tables bridge the two: they reduce join count without full denormalization.

# COMMAND ----------

# MAGIC %md
# MAGIC ## What's Next?
# MAGIC
# MAGIC Notebook 03 builds the information mart (star schema) over the vault.
# MAGIC This is where analytics queries become both fast and simple.
