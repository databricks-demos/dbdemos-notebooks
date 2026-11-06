# Databricks notebook source
# MAGIC %md
# MAGIC # Information Mart: Star Schema
# MAGIC
# MAGIC Transform the raw vault into a queryable star schema.
# MAGIC This is the "gold" layer: fast, simple, denormalized for BI tools.

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql.functions import col, lit, current_timestamp, year, month, dayofmonth, dayofweek

catalog = spark.conf.get("catalog", "main")
schema = spark.conf.get("schema", "dbdemos_data_vault_sdp")

# COMMAND ----------

# MAGIC %md
# MAGIC ## WARNING: Always Filter Satellites for Current Version
# MAGIC
# MAGIC Satellites track history. When you query a satellite, filter for the current version:
# MAGIC `WHERE __END_AT IS NULL`
# MAGIC
# MAGIC Without this, a fact table will silently multiply its row count by the number of historical versions,
# MAGIC breaking all downstream analytics.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimension Tables
# MAGIC
# MAGIC Dimensions are denormalized lookups built from satellites.
# MAGIC Each dimension row = current version of a hub (where __END_AT IS NULL).

# COMMAND ----------

@dp.table
def dim_customer():
    """Customer dimension from hub_customer + sat_customer (current version)."""
    return spark.sql(f"""
      SELECT
        hc.customer_hk as customer_key,
        hc.c_custkey as customer_id,
        COALESCE(sc.c_name, 'Unknown') as customer_name,
        COALESCE(sc.c_address, 'Unknown') as address,
        COALESCE(sc.c_phone, 'Unknown') as phone,
        COALESCE(sc.c_acctbal, 0.0) as account_balance,
        COALESCE(sc.c_mktsegment, 'Unknown') as market_segment,
        COALESCE(sc.c_comment, '') as comment,
        sc.load_ts as last_updated
      FROM {catalog}.{schema}.hub_customer hc
      LEFT JOIN {catalog}.{schema}.sat_customer sc
        ON hc.customer_hk = sc.customer_hk
        AND sc.__END_AT IS NULL
    """)

@dp.table
def dim_part():
    """Part dimension from hub_part + sat_part (current version)."""
    return spark.sql(f"""
      SELECT
        hp.part_hk as part_key,
        hp.p_partkey as part_id,
        COALESCE(sp.p_name, 'Unknown') as part_name,
        COALESCE(sp.p_mfgr, 'Unknown') as manufacturer,
        COALESCE(sp.p_brand, 'Unknown') as brand,
        COALESCE(sp.p_retailprice, 0.0) as retail_price,
        COALESCE(sp.load_ts, current_timestamp()) as last_updated
      FROM {catalog}.{schema}.hub_part hp
      LEFT JOIN {catalog}.{schema}.sat_part sp
        ON hp.part_hk = sp.part_hk
        AND sp.__END_AT IS NULL
    """)

@dp.table
def dim_supplier():
    """Supplier dimension from hub_supplier + sat_supplier (current version)."""
    return spark.sql(f"""
      SELECT
        hs.supplier_hk as supplier_key,
        hs.s_suppkey as supplier_id,
        COALESCE(ss.s_name, 'Unknown') as supplier_name,
        COALESCE(ss.s_address, 'Unknown') as address,
        COALESCE(ss.s_phone, 'Unknown') as phone,
        COALESCE(ss.s_acctbal, 0.0) as account_balance,
        COALESCE(ss.load_ts, current_timestamp()) as last_updated
      FROM {catalog}.{schema}.hub_supplier hs
      LEFT JOIN {catalog}.{schema}.sat_supplier ss
        ON hs.supplier_hk = ss.supplier_hk
        AND ss.__END_AT IS NULL
    """)

@dp.table
def dim_nation():
    """Nation dimension from hub_nation + sat_nation (current version)."""
    return spark.sql(f"""
      SELECT
        hn.nation_hk as nation_key,
        hn.n_nationkey as nation_id,
        COALESCE(sn.n_name, 'Unknown') as nation_name,
        COALESCE(sn.n_comment, '') as comment,
        COALESCE(sn.load_ts, current_timestamp()) as last_updated
      FROM {catalog}.{schema}.hub_nation hn
      LEFT JOIN {catalog}.{schema}.sat_nation sn
        ON hn.nation_hk = sn.nation_hk
        AND sn.__END_AT IS NULL
    """)

@dp.table
def dim_region():
    """Region dimension from hub_region + sat_region (current version)."""
    return spark.sql(f"""
      SELECT
        hr.region_hk as region_key,
        hr.r_regionkey as region_id,
        COALESCE(sr.r_name, 'Unknown') as region_name,
        COALESCE(sr.r_comment, '') as comment,
        COALESCE(sr.load_ts, current_timestamp()) as last_updated
      FROM {catalog}.{schema}.hub_region hr
      LEFT JOIN {catalog}.{schema}.sat_region sr
        ON hr.region_hk = sr.region_hk
        AND sr.__END_AT IS NULL
    """)

@dp.table
def dim_date():
    """Date dimension. Simple version with year, month, day."""
    return spark.sql(f"""
      SELECT DISTINCT
        l.l_shipdate as date_key,
        YEAR(l.l_shipdate) as year,
        MONTH(l.l_shipdate) as month,
        DAYOFMONTH(l.l_shipdate) as day,
        DAYOFWEEK(l.l_shipdate) as day_of_week
      FROM {catalog}.{schema}.bronze_lineitem l
      WHERE l.l_shipdate IS NOT NULL
      UNION ALL
      SELECT DISTINCT
        o.o_orderdate,
        YEAR(o.o_orderdate),
        MONTH(o.o_orderdate),
        DAYOFMONTH(o.o_orderdate),
        DAYOFWEEK(o.o_orderdate)
      FROM {catalog}.{schema}.bronze_orders o
      WHERE o.o_orderdate IS NOT NULL
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fact Table: Fact Lineitem
# MAGIC
# MAGIC Grain: Order x Part x Supplier x LineNumber.
# MAGIC One row per order line. Measures: quantity, extended price, discount, tax, net revenue.
# MAGIC Denormalized with current dimension attributes.

# COMMAND ----------

@dp.table
def fact_lineitem():
    """
    Fact table: one row per lineitem.
    Grain: l_orderkey, l_partkey, l_suppkey, l_linenumber.
    Measures: quantity, extended_price, discount, tax, net_revenue.
    Dimensions: order, part, supplier, customer, nation, region, date.
    """
    return spark.sql(f"""
      SELECT
        -- Grain keys
        bl.l_orderkey as order_id,
        bl.l_linenumber as line_number,
        bl.l_partkey as part_id,
        bl.l_suppkey as supplier_id,

        -- Dimension keys (for joining to slowly changing dimensions)
        ho.order_hk as order_key,
        hp.part_hk as part_key,
        hs.supplier_hk as supplier_key,
        hc.customer_hk as customer_key,
        hn.nation_hk as nation_key,
        hr.region_hk as region_key,

        -- Degenerate dimensions (directly from fact source)
        bl.l_orderkey,
        bo.o_orderdate as order_date,
        bl.l_shipdate as ship_date,
        bo.o_orderstatus as order_status,

        -- Measures
        bl.l_quantity as quantity,
        bl.l_extendedprice as extended_price,
        bl.l_discount as discount,
        bl.l_tax as tax,
        bl.l_extendedprice * (1 - bl.l_discount) as net_revenue,
        bl.l_extendedprice * (1 - bl.l_discount) * (1 + bl.l_tax) as total_with_tax,

        -- Metadata
        bl._ingested_at as load_ts
      FROM {catalog}.{schema}.bronze_lineitem bl
      JOIN {catalog}.{schema}.bronze_orders bo ON bl.l_orderkey = bo.o_orderkey
      JOIN {catalog}.{schema}.hub_order ho ON bo.o_orderkey = ho.o_orderkey
      JOIN {catalog}.{schema}.hub_part hp ON bl.l_partkey = hp.p_partkey
      JOIN {catalog}.{schema}.hub_supplier hs ON bl.l_suppkey = hs.s_suppkey
      JOIN {catalog}.{schema}.lnk_order_customer loc ON ho.order_hk = loc.order_hk
      JOIN {catalog}.{schema}.hub_customer hc ON loc.customer_hk = hc.customer_hk
      JOIN {catalog}.{schema}.lnk_customer_nation lcn ON hc.customer_hk = lcn.customer_hk
      JOIN {catalog}.{schema}.hub_nation hn ON lcn.nation_hk = hn.nation_hk
      JOIN {catalog}.{schema}.lnk_nation_region lnr ON hn.nation_hk = lnr.nation_hk
      JOIN {catalog}.{schema}.hub_region hr ON lnr.region_hk = hr.region_hk
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aggregated Fact: Order Summary
# MAGIC
# MAGIC For faster queries, pre-aggregate to order level (grain: order).

# COMMAND ----------

@dp.table
def fact_order_summary():
    """Aggregated fact table: one row per order with totals."""
    return spark.sql(f"""
      SELECT
        fl.order_id,
        fl.order_date,
        fl.customer_key,
        fl.order_status,
        COUNT(*) as line_count,
        SUM(fl.quantity) as total_quantity,
        SUM(fl.extended_price) as total_extended_price,
        SUM(fl.discount) as total_discount,
        SUM(fl.tax) as total_tax,
        SUM(fl.net_revenue) as net_revenue,
        SUM(fl.total_with_tax) as total_with_tax
      FROM {catalog}.{schema}.fact_lineitem fl
      GROUP BY fl.order_id, fl.order_date, fl.customer_key, fl.order_status
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Comparison: Raw Vault Query vs. Mart Query
# MAGIC
# MAGIC Same question: "Revenue by nation, last 100 orders, sorted by revenue desc"
# MAGIC
# MAGIC ### Query on Raw Vault (many joins, slow)
# MAGIC ```sql
# MAGIC SELECT
# MAGIC   sn.n_name as nation,
# MAGIC   SUM(bl.l_extendedprice * (1 - bl.l_discount)) as net_revenue
# MAGIC FROM bronze_lineitem bl
# MAGIC JOIN hub_order ho ON ... (construct hash key)
# MAGIC JOIN lnk_order_customer loc ON ...
# MAGIC JOIN hub_customer hc ON ...
# MAGIC JOIN sat_customer sc ON hc.customer_hk = sc.customer_hk AND sc.__END_AT IS NULL
# MAGIC JOIN lnk_customer_nation lcn ON ...
# MAGIC JOIN hub_nation hn ON ...
# MAGIC JOIN sat_nation sn ON hn.nation_hk = sn.nation_hk AND sn.__END_AT IS NULL
# MAGIC WHERE ro.load_ts = (SELECT MAX(load_ts) FROM hub_order ORDER BY load_ts DESC LIMIT 100)
# MAGIC GROUP BY sn.n_name
# MAGIC ORDER BY net_revenue DESC;
# MAGIC ```
# MAGIC **Cost:** Multiple joins, filter by satellite end date, many scans.
# MAGIC
# MAGIC ### Query on Mart (simple, fast)
# MAGIC ```sql
# MAGIC SELECT
# MAGIC   dn.nation_name as nation,
# MAGIC   SUM(fl.net_revenue) as net_revenue
# MAGIC FROM fact_order_summary fo
# MAGIC JOIN dim_customer dc ON fo.customer_key = dc.customer_key
# MAGIC JOIN dim_nation dn ON dc.nation_key = dn.nation_key
# MAGIC WHERE fo.order_id IN (SELECT DISTINCT order_id FROM fact_lineitem ORDER BY order_date DESC LIMIT 100)
# MAGIC GROUP BY dn.nation_name
# MAGIC ORDER BY net_revenue DESC;
# MAGIC ```
# MAGIC **Cost:** 2-3 joins, pre-denormalized, pre-aggregated.
# MAGIC **Benefit:** Clear for BI tools, Tableau can point at it directly.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Information Mart Complete
# MAGIC
# MAGIC The star schema is now ready for analytics and BI tools.
# MAGIC - **Dimensions:** Customer, Part, Supplier, Nation, Region, Date
# MAGIC - **Facts:** fact_lineitem (grain: line), fact_order_summary (grain: order)
# MAGIC
# MAGIC Next: Notebook 04 demonstrates schema evolution and SCD2 history tracking.
