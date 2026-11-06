# Databricks notebook source
# MAGIC %md
# MAGIC # Raw Vault: Hubs, Links, and Satellites
# MAGIC
# MAGIC Build the foundational Data Vault layers using Spark Declarative Pipelines.
# MAGIC Every hub, link, and satellite is explicit Python you can read top to bottom.

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql.functions import (
    sha2, concat_ws, upper, trim, coalesce, current_timestamp,
    lit, col, unix_micros
)

catalog = spark.conf.get("catalog", "main")
schema = spark.conf.get("schema", "dbdemos_data_vault_sdp")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Hash Key Generation
# MAGIC
# MAGIC All Data Vault surrogate keys (hub_hk, link_hk, etc.) are hashes of the business keys.
# MAGIC We use this formula to ensure consistency:
# MAGIC
# MAGIC ```
# MAGIC sha2(concat_ws('||',
# MAGIC   upper(trim(coalesce(cast(col1 as string), '^^'))),
# MAGIC   upper(trim(coalesce(cast(col2 as string), '^^'))),
# MAGIC   ...
# MAGIC ), 256)
# MAGIC ```
# MAGIC
# MAGIC **Why the null placeholder '^^'?**
# MAGIC
# MAGIC In SQL, NULL is not a value. If we hash ('A', NULL) directly, we get the same hash as
# MAGIC ('A') with one fewer element, which breaks many-to-many relationships.
# MAGIC The placeholder '^^' ensures ('A', NULL) and ('A', '') hash differently, so the
# MAGIC business key domain stays clean: you can distinguish "unknown" from "empty string".
# MAGIC
# MAGIC This is critical for links: if a supplier is unknown, we still need a distinct key.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions for Hash Keys
# MAGIC
# MAGIC These functions reduce repetition and ensure consistency.

# COMMAND ----------

def hash_key(*cols):
    """Hash a composite key."""
    return sha2(
        concat_ws(
            "||",
            *[upper(trim(coalesce(c.cast("string"), lit("^^")))) for c in cols]
        ),
        256
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Streaming Bronze Tables
# MAGIC
# MAGIC Read from bronze_* as streaming sources. SDP streams them through the vault.

# COMMAND ----------

@dp.table(
    name="stream_bronze_customer",
    comment="Stream customer data from bronze for hub_customer and sat_customer."
)
def stream_bronze_customer():
    return spark.readStream.table(f"{catalog}.{schema}.bronze_customer")

@dp.table(
    name="stream_bronze_orders",
    comment="Stream order data for hub_order and sat_order."
)
def stream_bronze_orders():
    return spark.readStream.table(f"{catalog}.{schema}.bronze_orders")

@dp.table(
    name="stream_bronze_lineitem",
    comment="Stream lineitem for lnk_lineitem and sat_lineitem (multi-active)."
)
def stream_bronze_lineitem():
    return spark.readStream.table(f"{catalog}.{schema}.bronze_lineitem")

@dp.table(
    name="stream_bronze_part",
    comment="Stream part data for hub_part and sat_part."
)
def stream_bronze_part():
    return spark.readStream.table(f"{catalog}.{schema}.bronze_part")

@dp.table(
    name="stream_bronze_supplier",
    comment="Stream supplier data for hub_supplier and sat_supplier."
)
def stream_bronze_supplier():
    return spark.readStream.table(f"{catalog}.{schema}.bronze_supplier")

@dp.table(
    name="stream_bronze_partsupp",
    comment="Stream partsupp for lnk_part_supplier and sat_part_supplier."
)
def stream_bronze_partsupp():
    return spark.readStream.table(f"{catalog}.{schema}.bronze_partsupp")

@dp.table(
    name="stream_bronze_nation",
    comment="Stream nation data for hub_nation and sat_nation."
)
def stream_bronze_nation():
    return spark.readStream.table(f"{catalog}.{schema}.bronze_nation")

@dp.table(
    name="stream_bronze_region",
    comment="Stream region data for hub_region and sat_region."
)
def stream_bronze_region():
    return spark.readStream.table(f"{catalog}.{schema}.bronze_region")

# COMMAND ----------

# MAGIC %md
# MAGIC ## HUBS: Immutable Business Keys
# MAGIC
# MAGIC Each hub holds a unique business key (SCD Type 1: insert-only).
# MAGIC We use AUTO CDC with `stored_as_scd_type=1` and negate the timestamp.
# MAGIC Why negate? Earliest sighting must win: hub load_ts records when the key first arrived.
# MAGIC AUTO CDC keeps the HIGHEST sequence value. By negating, earliest (highest negated value) wins.

# COMMAND ----------

# Hub: Customer
@dp.temporary_view(name="hub_customer_changes")
def hub_customer_changes():
    """Change feed for hub_customer. Negated load_ts ensures earliest sighting wins."""
    return (
        spark.readStream.table(f"{catalog}.{schema}.stream_bronze_customer")
        .select(
            hash_key(col("c_custkey")).alias("customer_hk"),
            col("c_custkey"),
            col("_ingested_at").alias("load_ts"),
            lit("bronze_customer").alias("record_source"),
            (-unix_micros(col("_ingested_at"))).alias("_dv_seq")
        )
    )

dp.create_streaming_table(
    name="hub_customer",
    comment="Customer hub. One row per unique c_custkey. Earliest sighting wins."
)

dp.create_auto_cdc_flow(
    target="hub_customer",
    source="hub_customer_changes",
    keys=["customer_hk"],
    sequence_by="_dv_seq",
    stored_as_scd_type=1
)

# COMMAND ----------

@dp.temporary_view(name="hub_order_changes")
def hub_order_changes():
    return (
        spark.readStream.table(f"{catalog}.{schema}.stream_bronze_orders")
        .select(
            hash_key(col("o_orderkey")).alias("order_hk"),
            col("o_orderkey"),
            col("_ingested_at").alias("load_ts"),
            lit("bronze_orders").alias("record_source"),
            (-unix_micros(col("_ingested_at"))).alias("_dv_seq")
        )
    )

dp.create_streaming_table(
    name="hub_order",
    comment="Order hub. One row per unique o_orderkey."
)

dp.create_auto_cdc_flow(
    target="hub_order",
    source="hub_order_changes",
    keys=["order_hk"],
    sequence_by="_dv_seq",
    stored_as_scd_type=1
)

# COMMAND ----------

@dp.temporary_view(name="hub_part_changes")
def hub_part_changes():
    return (
        spark.readStream.table(f"{catalog}.{schema}.stream_bronze_part")
        .select(
            hash_key(col("p_partkey")).alias("part_hk"),
            col("p_partkey"),
            col("_ingested_at").alias("load_ts"),
            lit("bronze_part").alias("record_source"),
            (-unix_micros(col("_ingested_at"))).alias("_dv_seq")
        )
    )

dp.create_streaming_table(
    name="hub_part",
    comment="Part hub. One row per unique p_partkey."
)

dp.create_auto_cdc_flow(
    target="hub_part",
    source="hub_part_changes",
    keys=["part_hk"],
    sequence_by="_dv_seq",
    stored_as_scd_type=1
)

# COMMAND ----------

@dp.temporary_view(name="hub_supplier_changes")
def hub_supplier_changes():
    return (
        spark.readStream.table(f"{catalog}.{schema}.stream_bronze_supplier")
        .select(
            hash_key(col("s_suppkey")).alias("supplier_hk"),
            col("s_suppkey"),
            col("_ingested_at").alias("load_ts"),
            lit("bronze_supplier").alias("record_source"),
            (-unix_micros(col("_ingested_at"))).alias("_dv_seq")
        )
    )

dp.create_streaming_table(
    name="hub_supplier",
    comment="Supplier hub. One row per unique s_suppkey."
)

dp.create_auto_cdc_flow(
    target="hub_supplier",
    source="hub_supplier_changes",
    keys=["supplier_hk"],
    sequence_by="_dv_seq",
    stored_as_scd_type=1
)

# COMMAND ----------

@dp.temporary_view(name="hub_nation_changes")
def hub_nation_changes():
    return (
        spark.readStream.table(f"{catalog}.{schema}.stream_bronze_nation")
        .select(
            hash_key(col("n_nationkey")).alias("nation_hk"),
            col("n_nationkey"),
            col("_ingested_at").alias("load_ts"),
            lit("bronze_nation").alias("record_source"),
            (-unix_micros(col("_ingested_at"))).alias("_dv_seq")
        )
    )

dp.create_streaming_table(
    name="hub_nation",
    comment="Nation hub. One row per unique n_nationkey."
)

dp.create_auto_cdc_flow(
    target="hub_nation",
    source="hub_nation_changes",
    keys=["nation_hk"],
    sequence_by="_dv_seq",
    stored_as_scd_type=1
)

# COMMAND ----------

@dp.temporary_view(name="hub_region_changes")
def hub_region_changes():
    return (
        spark.readStream.table(f"{catalog}.{schema}.stream_bronze_region")
        .select(
            hash_key(col("r_regionkey")).alias("region_hk"),
            col("r_regionkey"),
            col("_ingested_at").alias("load_ts"),
            lit("bronze_region").alias("record_source"),
            (-unix_micros(col("_ingested_at"))).alias("_dv_seq")
        )
    )

dp.create_streaming_table(
    name="hub_region",
    comment="Region hub. One row per unique r_regionkey."
)

dp.create_auto_cdc_flow(
    target="hub_region",
    source="hub_region_changes",
    keys=["region_hk"],
    sequence_by="_dv_seq",
    stored_as_scd_type=1
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## LINKS: Immutable Many-to-Many Relationships
# MAGIC
# MAGIC Links capture relationships between hubs at a point in time (SCD Type 1: insert-only).
# MAGIC Each row represents "I knew about this relationship on this load_ts".
# MAGIC For links, normal sequence order: later sightings may update the row, and that is acceptable.
# MAGIC A link with __END_AT not null is a different statement (relationship ending), not a weakness.

# COMMAND ----------

# Link: Order placed by Customer
@dp.temporary_view(name="lnk_order_customer_changes")
def lnk_order_customer_changes():
    return (
        spark.readStream.table(f"{catalog}.{schema}.stream_bronze_orders")
        .select(
            hash_key(col("o_orderkey"), col("o_custkey")).alias("link_hk"),
            hash_key(col("o_orderkey")).alias("order_hk"),
            hash_key(col("o_custkey")).alias("customer_hk"),
            col("_ingested_at").alias("load_ts"),
            lit("bronze_orders").alias("record_source")
        )
    )

dp.create_streaming_table(
    name="lnk_order_customer",
    comment="Link: Order placed by Customer."
)

dp.create_auto_cdc_flow(
    target="lnk_order_customer",
    source="lnk_order_customer_changes",
    keys=["link_hk"],
    sequence_by="load_ts",
    stored_as_scd_type=1
)

# COMMAND ----------

# Link: LineItem ties Order, Part, and Supplier
@dp.temporary_view(name="lnk_lineitem_changes")
def lnk_lineitem_changes():
    return (
        spark.readStream.table(f"{catalog}.{schema}.stream_bronze_lineitem")
        .select(
            hash_key(col("l_orderkey"), col("l_partkey"), col("l_suppkey")).alias("link_hk"),
            hash_key(col("l_orderkey")).alias("order_hk"),
            hash_key(col("l_partkey")).alias("part_hk"),
            hash_key(col("l_suppkey")).alias("supplier_hk"),
            col("_ingested_at").alias("load_ts"),
            lit("bronze_lineitem").alias("record_source")
        )
    )

dp.create_streaming_table(
    name="lnk_lineitem",
    comment="Link: LineItem ties Order, Part, and Supplier."
)

dp.create_auto_cdc_flow(
    target="lnk_lineitem",
    source="lnk_lineitem_changes",
    keys=["link_hk"],
    sequence_by="load_ts",
    stored_as_scd_type=1
)

# COMMAND ----------

@dp.temporary_view(name="lnk_customer_nation_changes")
def lnk_customer_nation_changes():
    return (
        spark.readStream.table(f"{catalog}.{schema}.stream_bronze_customer")
        .select(
            hash_key(col("c_custkey"), col("c_nationkey")).alias("link_hk"),
            hash_key(col("c_custkey")).alias("customer_hk"),
            hash_key(col("c_nationkey")).alias("nation_hk"),
            col("_ingested_at").alias("load_ts"),
            lit("bronze_customer").alias("record_source")
        )
    )

dp.create_streaming_table(
    name="lnk_customer_nation",
    comment="Link: Customer belongs to Nation."
)

dp.create_auto_cdc_flow(
    target="lnk_customer_nation",
    source="lnk_customer_nation_changes",
    keys=["link_hk"],
    sequence_by="load_ts",
    stored_as_scd_type=1
)

# COMMAND ----------

@dp.temporary_view(name="lnk_supplier_nation_changes")
def lnk_supplier_nation_changes():
    return (
        spark.readStream.table(f"{catalog}.{schema}.stream_bronze_supplier")
        .select(
            hash_key(col("s_suppkey"), col("s_nationkey")).alias("link_hk"),
            hash_key(col("s_suppkey")).alias("supplier_hk"),
            hash_key(col("s_nationkey")).alias("nation_hk"),
            col("_ingested_at").alias("load_ts"),
            lit("bronze_supplier").alias("record_source")
        )
    )

dp.create_streaming_table(
    name="lnk_supplier_nation",
    comment="Link: Supplier based in Nation."
)

dp.create_auto_cdc_flow(
    target="lnk_supplier_nation",
    source="lnk_supplier_nation_changes",
    keys=["link_hk"],
    sequence_by="load_ts",
    stored_as_scd_type=1
)

# COMMAND ----------

@dp.temporary_view(name="lnk_nation_region_changes")
def lnk_nation_region_changes():
    return (
        spark.readStream.table(f"{catalog}.{schema}.stream_bronze_nation")
        .select(
            hash_key(col("n_nationkey"), col("n_regionkey")).alias("link_hk"),
            hash_key(col("n_nationkey")).alias("nation_hk"),
            hash_key(col("n_regionkey")).alias("region_hk"),
            col("_ingested_at").alias("load_ts"),
            lit("bronze_nation").alias("record_source")
        )
    )

dp.create_streaming_table(
    name="lnk_nation_region",
    comment="Link: Nation part of Region."
)

dp.create_auto_cdc_flow(
    target="lnk_nation_region",
    source="lnk_nation_region_changes",
    keys=["link_hk"],
    sequence_by="load_ts",
    stored_as_scd_type=1
)

# COMMAND ----------

@dp.temporary_view(name="lnk_part_supplier_changes")
def lnk_part_supplier_changes():
    return (
        spark.readStream.table(f"{catalog}.{schema}.stream_bronze_partsupp")
        .select(
            hash_key(col("ps_partkey"), col("ps_suppkey")).alias("link_hk"),
            hash_key(col("ps_partkey")).alias("part_hk"),
            hash_key(col("ps_suppkey")).alias("supplier_hk"),
            col("_ingested_at").alias("load_ts"),
            lit("bronze_partsupp").alias("record_source")
        )
    )

dp.create_streaming_table(
    name="lnk_part_supplier",
    comment="Link: Part supplied by Supplier."
)

dp.create_auto_cdc_flow(
    target="lnk_part_supplier",
    source="lnk_part_supplier_changes",
    keys=["link_hk"],
    sequence_by="load_ts",
    stored_as_scd_type=1
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## SATELLITES: Slowly Changing Dimensions (SCD2)
# MAGIC
# MAGIC Satellites track attribute changes over time (SCD Type 2).
# MAGIC When payload columns change (detected via hash_diff), AUTO CDC closes the old version
# MAGIC (__END_AT is set) and opens a new one (__START_AT).
# MAGIC
# MAGIC Key points:
# MAGIC - `track_history_column_list=["hash_diff"]` is critical. The default tracks all columns,
# MAGIC   including load_ts. Since load_ts changes on every load, a reloaded unchanged row would
# MAGIC   wrongly open a new version. By tracking only hash_diff, unchanged rows stay in one version.
# MAGIC - Do not manually manage load_end_ts: AUTO CDC owns the end-dating via __END_AT.
# MAGIC - Current row filter: `__END_AT IS NULL`
# MAGIC - As-of query: `__START_AT <= ts AND (__END_AT IS NULL OR __END_AT > ts)`

# COMMAND ----------

# Satellite: Customer attributes (SCD2)
@dp.temporary_view(name="sat_customer_changes")
def sat_customer_changes():
    return (
        spark.readStream.table(f"{catalog}.{schema}.stream_bronze_customer")
        .select(
            hash_key(col("c_custkey")).alias("customer_hk"),
            col("c_name"), col("c_address"), col("c_phone"), col("c_acctbal"),
            col("c_mktsegment"), col("c_comment"),
            hash_key(
                col("c_name"), col("c_address"), col("c_phone"),
                col("c_acctbal"), col("c_mktsegment"), col("c_comment")
            ).alias("hash_diff"),
            col("_ingested_at").alias("load_ts"),
            lit("bronze_customer").alias("record_source")
        )
    )

dp.create_streaming_table(
    name="sat_customer",
    comment="Customer attributes, SCD2. A new version per change to the payload."
)

dp.create_auto_cdc_flow(
    target="sat_customer",
    source="sat_customer_changes",
    keys=["customer_hk"],
    sequence_by="load_ts",
    stored_as_scd_type=2,
    track_history_column_list=["hash_diff"]
)

# COMMAND ----------

# Satellite: Order attributes (SCD2)
@dp.temporary_view(name="sat_order_changes")
def sat_order_changes():
    return (
        spark.readStream.table(f"{catalog}.{schema}.stream_bronze_orders")
        .select(
            hash_key(col("o_orderkey")).alias("order_hk"),
            col("o_orderstatus"), col("o_totalprice"), col("o_orderdate"),
            col("o_orderpriority"), col("o_clerk"), col("o_shippriority"),
            hash_key(
                col("o_orderstatus"), col("o_totalprice"), col("o_orderdate"),
                col("o_orderpriority"), col("o_clerk"), col("o_shippriority")
            ).alias("hash_diff"),
            col("_ingested_at").alias("load_ts"),
            lit("bronze_orders").alias("record_source")
        )
    )

dp.create_streaming_table(
    name="sat_order",
    comment="Order attributes, SCD2."
)

dp.create_auto_cdc_flow(
    target="sat_order",
    source="sat_order_changes",
    keys=["order_hk"],
    sequence_by="load_ts",
    stored_as_scd_type=2,
    track_history_column_list=["hash_diff"]
)

# COMMAND ----------

# Satellite: Part attributes (SCD2)
@dp.temporary_view(name="sat_part_changes")
def sat_part_changes():
    return (
        spark.readStream.table(f"{catalog}.{schema}.stream_bronze_part")
        .select(
            hash_key(col("p_partkey")).alias("part_hk"),
            col("p_name"), col("p_mfgr"), col("p_brand"), col("p_retailprice"),
            hash_key(
                col("p_name"), col("p_mfgr"), col("p_brand"), col("p_retailprice")
            ).alias("hash_diff"),
            col("_ingested_at").alias("load_ts"),
            lit("bronze_part").alias("record_source")
        )
    )

dp.create_streaming_table(
    name="sat_part",
    comment="Part attributes, SCD2."
)

dp.create_auto_cdc_flow(
    target="sat_part",
    source="sat_part_changes",
    keys=["part_hk"],
    sequence_by="load_ts",
    stored_as_scd_type=2,
    track_history_column_list=["hash_diff"]
)

# COMMAND ----------

# Satellite: Supplier attributes (SCD2)
@dp.temporary_view(name="sat_supplier_changes")
def sat_supplier_changes():
    return (
        spark.readStream.table(f"{catalog}.{schema}.stream_bronze_supplier")
        .select(
            hash_key(col("s_suppkey")).alias("supplier_hk"),
            col("s_name"), col("s_address"), col("s_phone"), col("s_acctbal"),
            hash_key(
                col("s_name"), col("s_address"), col("s_phone"), col("s_acctbal")
            ).alias("hash_diff"),
            col("_ingested_at").alias("load_ts"),
            lit("bronze_supplier").alias("record_source")
        )
    )

dp.create_streaming_table(
    name="sat_supplier",
    comment="Supplier attributes, SCD2."
)

dp.create_auto_cdc_flow(
    target="sat_supplier",
    source="sat_supplier_changes",
    keys=["supplier_hk"],
    sequence_by="load_ts",
    stored_as_scd_type=2,
    track_history_column_list=["hash_diff"]
)

# COMMAND ----------

# Satellite: Nation attributes (SCD2)
@dp.temporary_view(name="sat_nation_changes")
def sat_nation_changes():
    return (
        spark.readStream.table(f"{catalog}.{schema}.stream_bronze_nation")
        .select(
            hash_key(col("n_nationkey")).alias("nation_hk"),
            col("n_name"), col("n_comment"),
            hash_key(col("n_name"), col("n_comment")).alias("hash_diff"),
            col("_ingested_at").alias("load_ts"),
            lit("bronze_nation").alias("record_source")
        )
    )

dp.create_streaming_table(
    name="sat_nation",
    comment="Nation attributes, SCD2."
)

dp.create_auto_cdc_flow(
    target="sat_nation",
    source="sat_nation_changes",
    keys=["nation_hk"],
    sequence_by="load_ts",
    stored_as_scd_type=2,
    track_history_column_list=["hash_diff"]
)

# COMMAND ----------

# Satellite: Region attributes (SCD2)
@dp.temporary_view(name="sat_region_changes")
def sat_region_changes():
    return (
        spark.readStream.table(f"{catalog}.{schema}.stream_bronze_region")
        .select(
            hash_key(col("r_regionkey")).alias("region_hk"),
            col("r_name"), col("r_comment"),
            hash_key(col("r_name"), col("r_comment")).alias("hash_diff"),
            col("_ingested_at").alias("load_ts"),
            lit("bronze_region").alias("record_source")
        )
    )

dp.create_streaming_table(
    name="sat_region",
    comment="Region attributes, SCD2."
)

dp.create_auto_cdc_flow(
    target="sat_region",
    source="sat_region_changes",
    keys=["region_hk"],
    sequence_by="load_ts",
    stored_as_scd_type=2,
    track_history_column_list=["hash_diff"]
)

# COMMAND ----------

# Satellite: Part-Supplier relationship attributes (SCD2)
@dp.temporary_view(name="sat_part_supplier_changes")
def sat_part_supplier_changes():
    return (
        spark.readStream.table(f"{catalog}.{schema}.stream_bronze_partsupp")
        .select(
            hash_key(col("ps_partkey"), col("ps_suppkey")).alias("link_hk"),
            col("ps_availqty"), col("ps_supplycost"), col("ps_comment"),
            hash_key(
                col("ps_availqty"), col("ps_supplycost"), col("ps_comment")
            ).alias("hash_diff"),
            col("_ingested_at").alias("load_ts"),
            lit("bronze_partsupp").alias("record_source")
        )
    )

dp.create_streaming_table(
    name="sat_part_supplier",
    comment="Part-Supplier relationship attributes, SCD2."
)

dp.create_auto_cdc_flow(
    target="sat_part_supplier",
    source="sat_part_supplier_changes",
    keys=["link_hk"],
    sequence_by="load_ts",
    stored_as_scd_type=2,
    track_history_column_list=["hash_diff"]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## SATELLITE: Multi-Active Lineitem
# MAGIC
# MAGIC The lineitem satellite is special: one order can have multiple line items.
# MAGIC We track l_linenumber as a secondary key so each line has its own history.
# MAGIC This is a multi-active satellite: multiple rows can be current (__END_AT IS NULL)
# MAGIC for the same parent (link_hk), distinguished by l_linenumber.
# MAGIC
# MAGIC Critical: l_linenumber must NOT be in hash_diff. It is the grain (part of the key),
# MAGIC and putting the grain in the digest makes a new line and a changed line indistinguishable.
# MAGIC Only the measure columns go in hash_diff: quantity, extended price, discount, tax, shipdate, returnflag.

# COMMAND ----------

# Satellite: LineItem attributes (SCD2, multi-active on l_linenumber)
@dp.temporary_view(name="sat_lineitem_changes")
def sat_lineitem_changes():
    return (
        spark.readStream.table(f"{catalog}.{schema}.stream_bronze_lineitem")
        .select(
            hash_key(col("l_orderkey"), col("l_partkey"), col("l_suppkey")).alias("link_hk"),
            col("l_linenumber"),  # Secondary key: NOT in hash_diff
            col("l_quantity"), col("l_extendedprice"), col("l_discount"),
            col("l_tax"), col("l_shipdate"), col("l_returnflag"),
            hash_key(
                col("l_quantity"), col("l_extendedprice"), col("l_discount"),
                col("l_tax"), col("l_shipdate"), col("l_returnflag")
            ).alias("hash_diff"),
            col("_ingested_at").alias("load_ts"),
            lit("bronze_lineitem").alias("record_source")
        )
    )

dp.create_streaming_table(
    name="sat_lineitem",
    comment="LineItem attributes, SCD2, multi-active on l_linenumber. Multiple rows can be current for the same link_hk."
)

dp.create_auto_cdc_flow(
    target="sat_lineitem",
    source="sat_lineitem_changes",
    keys=["link_hk", "l_linenumber"],
    sequence_by="load_ts",
    stored_as_scd_type=2,
    track_history_column_list=["hash_diff"]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Raw Vault Complete
# MAGIC
# MAGIC The raw vault now contains 20 tables:
# MAGIC
# MAGIC **Hubs (6):** hub_customer, hub_order, hub_part, hub_supplier, hub_nation, hub_region
# MAGIC - SCD1 (insert-only), earliest sighting wins
# MAGIC - Columns: *_hk, business_key, load_ts, record_source
# MAGIC
# MAGIC **Links (6):** lnk_order_customer, lnk_lineitem, lnk_customer_nation, lnk_supplier_nation, lnk_nation_region, lnk_part_supplier
# MAGIC - SCD1 (insert-only)
# MAGIC - Columns: link_hk, hub_hk (foreign keys), load_ts, record_source
# MAGIC
# MAGIC **Satellites (8):** sat_customer, sat_order, sat_part, sat_supplier, sat_nation, sat_region, sat_part_supplier, sat_lineitem
# MAGIC - SCD2 (track history), use __START_AT / __END_AT (not load_ts / load_end_ts)
# MAGIC - Columns: hub/link_hk, load_ts, hash_diff, __START_AT, __END_AT, record_source, payload
# MAGIC - sat_lineitem is multi-active: keyed on (link_hk, l_linenumber)
# MAGIC
# MAGIC Every table is built by a change feed view, declared target, and AUTO CDC flow.
# MAGIC This keeps the transformation transparent and the SCD2 logic automatic and correct.
# MAGIC
# MAGIC Next: notebook 02 builds computed satellites and business logic.
