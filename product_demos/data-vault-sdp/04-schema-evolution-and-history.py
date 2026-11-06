# Databricks notebook source
# MAGIC %md
# MAGIC # Schema Evolution and SCD2 History
# MAGIC
# MAGIC This notebook demonstrates why Data Vault excels at handling change:
# MAGIC 1. Simulate source data mutations (schema "evolution").
# MAGIC 2. Re-run the SDP pipeline.
# MAGIC 3. Verify that satellites track history with valid-from/to dates.
# MAGIC 4. Query data "as of" a specific timestamp.
# MAGIC
# MAGIC This is the payoff: your warehouse remains queryable across schema changes.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scenario: Source Change
# MAGIC
# MAGIC Two mutations happen in the source:
# MAGIC 1. A customer's account balance changes (c_acctbal for customer 1).
# MAGIC 2. An order's status changes (o_orderstatus for order 1).
# MAGIC 3. A new customer is inserted.
# MAGIC
# MAGIC In a traditional star schema, you would:
# MAGIC - Overwrite the old dimensions (losing history).
# MAGIC - Run complex ETL to detect and handle Type 2 changes.
# MAGIC
# MAGIC In Data Vault, the satellite layer automatically tracks this.

# COMMAND ----------

# dbdemos rewrites these two lines to the catalog and schema chosen at install time
catalog = "main__build"
schema = dbName = db = "dbdemos_data_vault_sdp"
fq = f"{catalog}.{schema}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Before: Capture the Current State
# MAGIC
# MAGIC The business key lives in the hub, the attributes in the satellite, so every lookup joins the two on the hash key.

# COMMAND ----------

def customer_history(custkey):
    return spark.sql(f"""
      SELECT h.c_custkey, s.c_acctbal, s.__START_AT, s.__END_AT, s.hash_diff
      FROM {fq}.sat_customer s
      JOIN {fq}.hub_customer h USING (customer_hk)
      WHERE h.c_custkey = {custkey}
      ORDER BY s.__START_AT
    """)

def order_history(orderkey):
    return spark.sql(f"""
      SELECT h.o_orderkey, s.o_orderstatus, s.__START_AT, s.__END_AT, s.hash_diff
      FROM {fq}.sat_order s
      JOIN {fq}.hub_order h USING (order_hk)
      WHERE h.o_orderkey = {orderkey}
      ORDER BY s.__START_AT
    """)

customer_history(1).display()
order_history(1).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mutate Source Data
# MAGIC
# MAGIC Bronze is append-only, the way a vault source should be: a change arrives as a new row with a later `_ingested_at`,
# MAGIC never as an `UPDATE`. (An `UPDATE` would also stop the pipeline, because a streaming read cannot replay updated files.)

# COMMAND ----------

# Customer 1: a new version with a higher account balance
spark.sql(f"""
  INSERT INTO {fq}.bronze_customer
  SELECT c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal + 1000.00, c_mktsegment, c_comment, current_timestamp()
  FROM {fq}.bronze_customer WHERE c_custkey = 1
  QUALIFY ROW_NUMBER() OVER (ORDER BY _ingested_at DESC) = 1
""")

# Order 1: a new version with a changed status
spark.sql(f"""
  INSERT INTO {fq}.bronze_orders
  SELECT o_orderkey, o_custkey, CASE WHEN o_orderstatus = 'S' THEN 'O' ELSE 'S' END, o_totalprice, o_orderdate,
         o_orderpriority, o_clerk, o_shippriority, o_comment, current_timestamp()
  FROM {fq}.bronze_orders WHERE o_orderkey = 1
  QUALIFY ROW_NUMBER() OVER (ORDER BY _ingested_at DESC) = 1
""")

# A brand-new customer
new_cust_key = spark.sql(f"SELECT MAX(c_custkey) + 1 FROM {fq}.bronze_customer").collect()[0][0]
spark.sql(f"""
  INSERT INTO {fq}.bronze_customer
  VALUES ({new_cust_key}, 'Demo Customer for SCD2', '999 Demo Lane', 10, '555-0001', 5000.00, 'MACHINERY', 'Added for demo', current_timestamp())
""")
print(f"Appended a new version of customer 1 and order 1, and a new customer {new_cust_key}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Re-run the Pipeline
# MAGIC
# MAGIC Trigger an incremental update of the demo pipeline and wait for it. Only the appended rows are processed.

# COMMAND ----------

import time
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
pipeline_name = f"dbdemos_data_vault_{catalog}_{schema}"
pipeline = next(iter(w.pipelines.list_pipelines(filter=f"name LIKE '{pipeline_name}'")), None)
assert pipeline is not None, f"Pipeline {pipeline_name} not found. Create it from the bundle (or with dbdemos.install) first."
update_id = w.pipelines.start_update(pipeline.pipeline_id).update_id
while True:
    state = w.pipelines.get_update(pipeline.pipeline_id, update_id).update.state.value
    if state in ("COMPLETED", "FAILED", "CANCELED"):
        break
    time.sleep(15)
print(f"Pipeline update {update_id}: {state}")
assert state == "COMPLETED", "The pipeline update did not complete. Open the pipeline to see why."

# COMMAND ----------

# MAGIC %md
# MAGIC ## After: Query SCD2 History
# MAGIC
# MAGIC The satellite now has TWO versions of customer 1's record:
# MAGIC - Old version: `__START_AT` = first load, `__END_AT` = second load
# MAGIC - New version: `__START_AT` = second load, `__END_AT` = NULL
# MAGIC
# MAGIC The hub still has ONE row for customer 1, with its original `load_ts`: a hub records when a key first arrived.

# COMMAND ----------

customer_history(1).display()
order_history(1).display()
spark.sql(f"SELECT c_custkey, load_ts FROM {fq}.hub_customer WHERE c_custkey IN (1, {new_cust_key})").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query "As Of" a Timestamp
# MAGIC
# MAGIC One of the most powerful features of SCD2 in Data Vault:
# MAGIC you can query any attribute as of any point in the past. Here, the moment just before the change.

# COMMAND ----------

as_of = spark.sql(f"""
  SELECT MAX(s.__START_AT) - INTERVAL 1 SECOND
  FROM {fq}.sat_customer s JOIN {fq}.hub_customer h USING (customer_hk)
  WHERE h.c_custkey = 1
""").collect()[0][0]

spark.sql(f"""
  SELECT h.c_custkey, s.c_acctbal, s.__START_AT AS valid_from, s.__END_AT AS valid_to
  FROM {fq}.sat_customer s JOIN {fq}.hub_customer h USING (customer_hk)
  WHERE h.c_custkey = 1
    AND s.__START_AT <= '{as_of}' AND (s.__END_AT > '{as_of}' OR s.__END_AT IS NULL)
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Why This Matters
# MAGIC
# MAGIC ### Compliance and Audit
# MAGIC "What was a customer's account balance on 2024-01-15?"
# MAGIC ```sql
# MAGIC SELECT s.c_acctbal
# MAGIC FROM sat_customer s JOIN hub_customer h USING (customer_hk)
# MAGIC WHERE h.c_custkey = ? AND s.__START_AT <= '2024-01-15' AND (s.__END_AT > '2024-01-15' OR s.__END_AT IS NULL)
# MAGIC ```
# MAGIC
# MAGIC ### Schema Evolution Without Breaking Queries
# MAGIC If the source adds a new column tomorrow (e.g., c_loyalty_tier), you:
# MAGIC 1. Add it to bronze_customer (no schema issues, Delta auto-detects).
# MAGIC 2. Add it to sat_customer payload (one line of code).
# MAGIC 3. Existing queries still work (they just won't see the new column).
# MAGIC 4. New queries can access c_loyalty_tier from sat_customer immediately.
# MAGIC
# MAGIC In a star schema, you would have to:
# MAGIC 1. Alter dim_customer to add the column.
# MAGIC 2. Backfill historical values (or set to NULL).
# MAGIC 3. Update ETL logic.
# MAGIC 4. Hope no queries are currently running against dim_customer.
# MAGIC
# MAGIC ### Many-to-One Relationships with History
# MAGIC Multi-active satellites (like sat_lineitem with l_linenumber) let you track
# MAGIC multiple versions of the same parent in the same table without losing history.
# MAGIC This is clean in Data Vault but messy in a traditional warehouse.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Vault Principles Summary
# MAGIC
# MAGIC | Principle | Benefit |
# MAGIC |-----------|---------|
# MAGIC | Immutable hubs | Unique entities are recorded once. No duplicates. Business key domain is clean. |
# MAGIC | Immutable links | Many-to-many relationships are recorded as they occur. Adds expressiveness. |
# MAGIC | SCD2 satellites | Every attribute change is tracked. You can query history. |
# MAGIC | Hash keys as surrogates | Distributed-friendly. No sequence generators. No single point of contention. |
# MAGIC | Separate ownership | Hub team owns entity uniqueness. Satellite teams own attribute definitions. Reduces coupling. |
# MAGIC | Agile to source changes | New attributes = new satellite. Schema changes = new columns in satellite. No impact on existing queries. |
# MAGIC
# MAGIC ## What We Built
# MAGIC
# MAGIC - **Raw Vault (Notebook 01):** 6 hubs, 6 links, 8 satellites. Immutable, append-only. Sits on bronze data.
# MAGIC - **Business Vault (Notebook 02):** Computed satellites (metrics) and PIT tables for fast reassembly. Bridges raw vault to marts.
# MAGIC - **Information Mart (Notebook 03):** Star schema (6 dims, 2 facts). Optimized for analytics. Denormalized for speed.
# MAGIC - **Schema Evolution (This notebook):** SCD2 history and "as of" queries. Handles change gracefully.
# MAGIC
# MAGIC ## Trade-offs Recap
# MAGIC
# MAGIC **Data Vault wins when:**
# MAGIC - Requirements change frequently.
# MAGIC - You must track full history.
# MAGIC - Multiple teams own different parts.
# MAGIC - Relationships are complex.
# MAGIC
# MAGIC **Use a star schema when:**
# MAGIC - Requirements are stable.
# MAGIC - Query speed is critical and you have limited resources for a mart.
# MAGIC - You are willing to sacrifice history for simplicity.
# MAGIC
# MAGIC **Next steps:**
# MAGIC - Deploy the SDP pipeline to a workspace.
# MAGIC - Connect Databricks SQL / Lakeview to the information mart.
# MAGIC - Build dashboards on fact_lineitem and fact_order_summary.
# MAGIC - For exploratory analysis, query the vault directly (slower but flexible).

# COMMAND ----------

# MAGIC %md
# MAGIC You have now walked through a complete Data Vault 2.0 implementation on Spark Declarative Pipelines.
# MAGIC The warehouse is change-proof, queryable, and auditable. Well done.
