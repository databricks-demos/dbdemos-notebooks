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
  "fullDescription": "This demo shows how to land application events directly into a Unity Catalog managed Delta table via Zerobus ingestion, then run sessionization on top using Spark Structured Streaming's <code>transformWithState</code> operator.<br/><br/>You'll see:<ul><li>How to define your event schema as a Unity Catalog Delta table - the table is the schema contract</li><li>A producer notebook that stands in for your application or a sidecar service</li><li>A sessionization pipeline that deduplicates (Zerobus is at-least-once) and computes per-user sessions with <code>transformWithState</code></li></ul><br/><b>When to use this pattern:</b> production application events, clickstream, and IoT or device telemetry that need to reach the lakehouse continuously, replacing custom S3-batch pipelines, and greenfield ingestion where latency in seconds is enough.<br/><b>When to use something else:</b> if you already run Kafka for fan-out to other consumers, keep using it.",
  "usecase": "Data Engineering",
  "products": ["Zerobus", "Delta Lake", "Streaming", "Unity Catalog"],
  "related_links": [
      {"title": "Zerobus overview", "url": "https://docs.databricks.com/aws/en/ingestion/zerobus-overview"},
      {"title": "transformWithState", "url": "https://docs.databricks.com/aws/en/stateful-applications/"}],
  "recommended_items": ["streaming-sessionization"],
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
