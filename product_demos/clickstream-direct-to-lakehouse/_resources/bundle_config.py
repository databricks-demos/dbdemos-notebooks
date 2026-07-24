# Databricks notebook source
# MAGIC %md
# MAGIC ## Demo Bundle Configuration
# MAGIC Please ignore / do not delete, only used to prep and bundle the demo.

# COMMAND ----------

{
  "name": "clickstream-direct-to-lakehouse",
  "category": "data-engineering",
  "title": "Clickstream Direct to Lakehouse (Zerobus)",
  "serverless_supported": False,  # needs a workspace-managed service principal with MODIFY,SELECT on the table, on serverless the service principal is account-managed and we hit permission issues. Validated on classic.
  "custom_schema_supported": True,
  "default_catalog": "main",
  "default_schema": "dbdemos_clickstream_zerobus",
  "description": "Send application clickstream events directly into a Unity Catalog Delta table via Zerobus, then sessionize with transformWithState.",
  "fullDescription": "Zerobus enables direct ingestion of application events into a Unity Catalog Delta table, eliminating the need for an intermediate message bus or separate Spark ingestion job. This demo streams synthetic clickstream events to the events table via the Zerobus REST API, then runs real-time sessionization using Structured Streaming's <code>transformWithState</code> (TWS) API.<br/><br/>You'll see:<ul><li>How to define the event schema as a Unity Catalog managed Delta table, where every write is governed by Unity Catalog</li><li>A producer notebook that simulates an application client streaming events via the Zerobus REST API</li><li>A sessionization pipeline that filters at-least-once delivery duplicates and terminates sessions after a configurable inactivity gap using <code>transformWithState</code></li></ul><br/><b>Target use cases:</b> real-time telemetry (application events, clickstream, IoT/device telemetry requiring sub-minute query availability), brokerless high-volume ingestion, pipeline consolidation (replacing staged S3 plus COPY INTO), and greenfield architecture.",
  "usecase": "Data Engineering",
  "products": ["Zerobus", "Delta Lake", "Streaming", "Unity Catalog"],
  "related_links": [
      {"title": "Zerobus overview", "url": "https://docs.databricks.com/aws/en/ingestion/zerobus-overview"},
      {"title": "transformWithState", "url": "https://docs.databricks.com/aws/en/stateful-applications/"}],
  "demo_assets": [],
  "bundle": True,
  "tags": [{"zerobus": "Zerobus"}, {"delta": "Delta Lake"}, {"streaming": "Streaming"}],
  "notebooks": [
    {
      "path": "_resources/00-setup",
      "pre_run": False,
      "publish_on_website": False,
      "add_cluster_setup_cell": False,
      "title": "Setup",
      "description": "Create catalog, schema, and volume for the demo",
      "depends_on_previous": False
    },
    {
      "path": "00-setup-table",
      "pre_run": True,
      "publish_on_website": True,
      "add_cluster_setup_cell": True,
      "title": "Define the Event Schema (Unity Catalog Table)",
      "description": "Create the events Unity Catalog managed Delta table - the table is the schema the producer writes against.",
      "depends_on_previous": False
    },
    {
      "path": "01-zerobus-producer",
      "pre_run": False,
      "publish_on_website": True,
      "add_cluster_setup_cell": True,
      "title": "Stream Events Directly into Your Lakehouse with Zerobus",
      "description": "Generate synthetic clickstream events and stream them into a Unity Catalog Delta table via Zerobus. Stands in for your application's emit path.",
      "depends_on_previous": True
    },
    {
      "path": "02-sessionize",
      "pre_run": False,
      "publish_on_website": True,
      "add_cluster_setup_cell": True,
      "title": "Sessionize with transformWithState",
      "description": "Read the Zerobus-fed events table, deduplicate, compute per-user sessions with TWS, MERGE into a sessions table.",
      "depends_on_previous": True
    }
  ],
  "cluster": {
      "spark_version": "17.3.x-scala2.13",
      "spark_conf": {
        "spark.master": "local[*]",
        "spark.databricks.cluster.profile": "singleNode"
    },
    "custom_tags": {
        "ResourceClass": "SingleNode"
    },
    "num_workers": 0,
    "single_user_name": "{{CURRENT_USER}}",
    "data_security_mode": "SINGLE_USER"
  }
}
