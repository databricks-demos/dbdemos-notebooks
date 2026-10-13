# Databricks notebook source
# MAGIC %md
# MAGIC # Define Your Event Schema Once: The Unity Catalog Table
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/product/clickstream-direct-to-lakehouse/zerobus-delta-architecture.png" width="100%" alt="Clickstream events flow through Zerobus into a Delta events table, and Structured Streaming sessionizes them into a sessions table"/>
# MAGIC
# MAGIC With Zerobus, you create the table in Unity Catalog and write to it directly over the API, and every write is governed by Unity Catalog.
# MAGIC
# MAGIC This notebook defines the `events` managed Delta table. Re-run it only when the schema changes.

# COMMAND ----------

# MAGIC %run ./_resources/00-setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define the Events Table

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.{db}.events (
  user_id        STRING  NOT NULL,
  event_id       STRING  NOT NULL,
  event_date     BIGINT  NOT NULL,    -- unix epoch seconds
  platform       STRING,
  action         STRING,
  uri            STRING
)
USING DELTA
""")

display(spark.sql(f"DESCRIBE TABLE {catalog}.{db}.events"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Schema Evolution
# MAGIC Add a column by altering the DDL above. As long as you only add columns and leave existing ones stable, in-flight producers keep working - Zerobus validates each record against the current schema.
# MAGIC
# MAGIC ### Next: [01-zerobus-producer]($./01-zerobus-producer) - Stream Synthetic Clickstream Events Straight into This Table
