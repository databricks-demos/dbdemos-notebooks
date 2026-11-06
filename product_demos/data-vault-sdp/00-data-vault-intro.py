# Databricks notebook source
# MAGIC %md
# MAGIC # Data Vault 2.0 Introduction
# MAGIC
# MAGIC ## What is Data Vault?
# MAGIC
# MAGIC Data Vault is a dimensional modeling technique designed to be both historical and agile.
# MAGIC Unlike a star schema (optimized for specific queries) or 3NF (optimized for normalization),
# MAGIC Data Vault optimizes for change: it assumes your source systems, metrics, and business
# MAGIC questions will evolve, sometimes rapidly.
# MAGIC
# MAGIC The core idea: separate the model into three layers with distinct responsibilities.

# COMMAND ----------

# MAGIC %md
# MAGIC ## The Three Components
# MAGIC
# MAGIC ### 1. Hubs
# MAGIC A hub represents a unique business entity (a business key). Every hub row is immutable: once
# MAGIC we know about a customer, that row never changes. A hub carries a surrogate key (hash of the
# MAGIC business key) and metadata like load_ts and record_source.
# MAGIC
# MAGIC **Example: hub_customer**
# MAGIC ```
# MAGIC customer_hk (hash of c_custkey)
# MAGIC c_custkey (business key, immutable)
# MAGIC load_ts
# MAGIC record_source
# MAGIC ```
# MAGIC
# MAGIC ### 2. Links
# MAGIC A link captures a many-to-many relationship between hubs at a point in time. A link between
# MAGIC hub_order and hub_customer records that relationship the moment it occurred.
# MAGIC Links are immutable: once a relationship is recorded, it is never deleted.
# MAGIC
# MAGIC **Example: lnk_order_customer**
# MAGIC ```
# MAGIC link_hk (hash of all foreign keys)
# MAGIC order_hk (foreign key to hub_order)
# MAGIC customer_hk (foreign key to hub_customer)
# MAGIC load_ts
# MAGIC record_source
# MAGIC ```
# MAGIC
# MAGIC ### 3. Satellites
# MAGIC A satellite holds the changing attributes of a hub or link. When a customer's address or
# MAGIC phone changes, we add a new satellite row, not update an old one. Satellites use Slowly
# MAGIC Changing Dimension (SCD) Type 2: each row has valid-from and valid-to dates, so you can
# MAGIC query a customer's address "as of" a specific date.
# MAGIC
# MAGIC **Example: sat_customer (SCD2)**
# MAGIC ```
# MAGIC customer_hk (foreign key to hub_customer)
# MAGIC __START_AT (when this version started)
# MAGIC hash_diff (hash of all non-key attributes to detect change)
# MAGIC __END_AT (when this version ended, null if current)
# MAGIC record_source
# MAGIC -- payload columns: c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_comment
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## When to Choose Data Vault
# MAGIC
# MAGIC ### Choose Data Vault when:
# MAGIC - **Source systems change frequently.** New attributes arrive, existing attributes disappear,
# MAGIC   relationships shift. Your model does not break because every table only depends on one hub
# MAGIC   or link.
# MAGIC - **You must track full history.** Every change is recorded; you can query "when did we first
# MAGIC   know about this customer?" or "what was their address on Jan 1?"
# MAGIC - **Multiple teams own different parts of the warehouse.** Hubs and satellites can be owned
# MAGIC   independently. One team owns sat_customer address, another owns sat_customer financials.
# MAGIC - **You have complex many-to-many relationships.** Links let you record relationships without
# MAGIC   adding columns to a fact table.
# MAGIC
# MAGIC ### Do NOT choose Data Vault when:
# MAGIC - **Your queries are simple and fixed.** A star schema is faster to query and easier to
# MAGIC   understand. Data Vault adds query cost (many joins) unless you build a mart.
# MAGIC - **You want a minimal warehouse.** Data Vault stores full history, so storage grows quickly.
# MAGIC - **Your business is stable.** If requirements never change, 3NF is simpler and more efficient.

# COMMAND ----------

# MAGIC %md
# MAGIC ## The Trade-offs
# MAGIC
# MAGIC | Dimension | Data Vault | Star Schema | 3NF |
# MAGIC |-----------|-----------|-----------|-----|
# MAGIC | Query speed (raw) | Slow (many joins) | Fast | Moderate |
# MAGIC | History tracking | Native (SCD2) | Requires manual table design | Requires audit tables |
# MAGIC | Agility to change | High (add new satellite) | Low (alter fact/dim) | Moderate |
# MAGIC | Model understandability | Steeper learning curve | Intuitive | Complex |
# MAGIC | Storage | High (full history) | Moderate | Moderate |
# MAGIC | Satellite ownership | Independent | Shared | Single owner |
# MAGIC
# MAGIC **Why we build an Information Mart (gold layer):** Raw vault queries are join-heavy.
# MAGIC We compute the most common queries once and materialize them into a star schema.
# MAGIC Analysts query the mart, not the vault, so they get both speed and simplicity.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Architecture Overview
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/data-vault-sdp/data-vault-sdp-architecture.png?raw=true" width="1000px">
# MAGIC
# MAGIC | Layer | Notebook | Tables |
# MAGIC |---|---|---|
# MAGIC | Bronze | `_resources/00-setup` | 8 `bronze_*` tables seeded from `samples.tpch`, append-only |
# MAGIC | Raw vault | `01-raw-vault-hubs-links-sats` | 6 hubs: `hub_customer`, `hub_order`, `hub_part`, `hub_supplier`, `hub_nation`, `hub_region` |
# MAGIC | | | 6 links: `lnk_order_customer`, `lnk_lineitem` (order, part, supplier), `lnk_customer_nation`, `lnk_supplier_nation`, `lnk_nation_region`, `lnk_part_supplier` |
# MAGIC | | | 8 SCD2 satellites, one per hub plus `sat_part_supplier` and `sat_lineitem` (multi-active on line number) |
# MAGIC | Business vault | `02-business-vault-and-pit` | `sat_order_metrics` (computed satellite), `pit_order` (point-in-time) |
# MAGIC | Information mart | `03-information-mart-star-schema` | 6 dimensions: `dim_customer`, `dim_part`, `dim_supplier`, `dim_nation`, `dim_region`, `dim_date`; facts: `fact_lineitem`, `fact_order_summary` |
# MAGIC | History | `04-schema-evolution-and-history` | Appends changed rows to bronze and re-runs the pipeline |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Vault in Spark Declarative Pipelines
# MAGIC
# MAGIC We use Spark Declarative Pipelines (formerly Delta Live Tables) to build this model.
# MAGIC SDP provides:
# MAGIC - **@dp.table:** Define a transformation in pure Python/SQL. Output is a Delta table.
# MAGIC - **@dp.temporary_view:** Define an intermediate view (change feed).
# MAGIC - **dp.create_streaming_table():** Declare a streaming table target.
# MAGIC - **dp.create_auto_cdc_flow():** Auto-generate CDC logic (MERGE, SCD1/SCD2 end-dating).
# MAGIC
# MAGIC ### Key SDP Pattern: Three-Part CDC (change feed + target + AUTO flow)
# MAGIC
# MAGIC For each vault table, three parts:
# MAGIC 1. A temporary view with projected columns, hash keys, hash_diff, load_ts
# MAGIC 2. A declared streaming table (the target)
# MAGIC 3. An AUTO CDC flow linking them
# MAGIC
# MAGIC For a hub (SCD1, earliest sighting wins):
# MAGIC
# MAGIC ```python
# MAGIC @dp.temporary_view(name="hub_customer_changes")
# MAGIC def hub_customer_changes():
# MAGIC     return (spark.readStream.table("bronze_customer")
# MAGIC         .select(hash_key(...), c_custkey, load_ts, record_source,
# MAGIC                  (-unix_micros(col("load_ts"))).alias("_dv_seq")))
# MAGIC
# MAGIC dp.create_streaming_table(name="hub_customer")
# MAGIC dp.create_auto_cdc_flow(
# MAGIC     target="hub_customer", source="hub_customer_changes",
# MAGIC     keys=["customer_hk"], sequence_by="_dv_seq",
# MAGIC     stored_as_scd_type=1)
# MAGIC ```
# MAGIC
# MAGIC For a satellite (SCD2, track history):
# MAGIC
# MAGIC ```python
# MAGIC @dp.temporary_view(name="sat_customer_changes")
# MAGIC def sat_customer_changes():
# MAGIC     return (spark.readStream.table("bronze_customer")
# MAGIC         .select(hash_key(...), c_name, c_address, ...,
# MAGIC                  hash_diff(...).alias("hash_diff"), load_ts, record_source))
# MAGIC
# MAGIC dp.create_streaming_table(name="sat_customer")
# MAGIC dp.create_auto_cdc_flow(
# MAGIC     target="sat_customer", source="sat_customer_changes",
# MAGIC     keys=["customer_hk"], sequence_by="load_ts",
# MAGIC     stored_as_scd_type=2,
# MAGIC     track_history_column_list=["hash_diff"])
# MAGIC ```
# MAGIC
# MAGIC AUTO CDC adds __START_AT and __END_AT automatically. It closes old rows when hash_diff changes.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notebook Flow
# MAGIC
# MAGIC 1. **This notebook (00-intro)**: Understand the model.
# MAGIC 2. **01-raw-vault-hubs-links-sats.py**: Build hubs, links, and satellites with explicit code.
# MAGIC    See every hash key, every MERGE statement, every SCD2 logic.
# MAGIC 3. **02-business-vault-and-pit.py**: Add computed satellites (metrics) and PIT tables
# MAGIC    for fast reassembly.
# MAGIC 4. **03-information-mart-star-schema.py**: Build a star schema over the vault. Compare
# MAGIC    query cost (vault vs. mart).
# MAGIC 5. **04-schema-evolution-and-history.py**: Simulate source changes, re-run, and verify
# MAGIC    SCD2 history.
# MAGIC
# MAGIC Each notebook builds on the last. Together, they show you:
# MAGIC - How to encode business logic (hash keys, surrogate key generation)
# MAGIC - How to track history (SCD2) and multi-active dimensions
# MAGIC - How to build a vault that is both queryable (through the mart) and resilient to change

# COMMAND ----------

# MAGIC %md
# MAGIC Let's start. Move to notebook 01.
