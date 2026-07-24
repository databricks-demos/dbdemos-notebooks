# Databricks notebook source
# MAGIC %md
# MAGIC ## Demo bundle configuration
# MAGIC Please ignore / do not delete, only used to prep and bundle the demo.

# COMMAND ----------

{
  "name": "realtime-clickstream-serving",
  "category": "data-engineering",
  "title": "Real-Time Clickstream Sessions Served from Lakebase (RTM + Postgres)",
  "serverless_supported": False,  # Spark Real-Time Mode requires classic DBR 16.2+
  "custom_schema_supported": True,
  "default_catalog": "main",
  "default_schema": "dbdemos_realtime_clickstream",
  "description": "Sub-second sessionization with Spark Real-Time Mode, output to Lakebase (managed Postgres) so your application reads live session state via SQL.",
  "custom_message": "This demo provisions a Lakebase Postgres instance and deploys a Databricks App - make sure you have permission to create both in your workspace. The streaming pipeline requires classic compute on DBR 18.3 or above.",
  "fullDescription": "<p>Some teams need sessions in under a second - fraud holds, real-time personalization, cart-abandonment alerts, anti-cheat. The usual answer is to stand up a second streaming framework (Flink) and a separate KV store (Redis or Cassandra), and operate two pipelines.</p><p>This demo shows the Databricks-native alternative: Spark <b>Real-Time Mode</b> with <code>transformWithState</code> sessionizes events with sub-second latency, and the streaming sink writes per-user state directly into <b>Lakebase</b> (Databricks-managed Postgres). Your application reads sessions via a normal <code>SELECT</code> on a Postgres table.</p><p>You'll see:</p><ul><li>Lakebase instance provisioning + schema setup</li><li>Synthetic clickstream from Spark's <code>rate</code> source (swap to Kafka for production with two lines)</li><li>Row-based <code>transformWithState</code> processor with processing-time timers (RTM doesn't support event-time timers)</li><li>Streaming upsert sink that writes per-user state into Lakebase, no JDBC boilerplate</li><li>How your application would query the live session state</li></ul><p><b>When to use this:</b> hot-path features that need sub-second latency and are read-heavy on the serving side.<br/><b>When to use something else:</b> if seconds-class latency is fine, see <i>clickstream-direct-to-lakehouse</i> (Zerobus + micro-batch).</p>",
  "usecase": "Data Engineering",
  "products": ["Real-Time Mode", "Lakebase", "Spark", "Unity Catalog"],
  "related_links": [
      {"title": "Real-time mode in Structured Streaming", "url": "https://docs.databricks.com/aws/en/structured-streaming/real-time/"},
      {"title": "Lakebase Provisioned",                    "url": "https://docs.databricks.com/aws/en/oltp/instances/"}],
  "recommended_items": ["streaming-sessionization", "clickstream-direct-to-lakehouse"],
  "demo_assets": [],
  "bundle": True,
  "tags": [{"rtm": "Real-Time Mode"}, {"lakebase": "Lakebase"}, {"streaming": "Streaming"}],
  "notebooks": [
    {
      "path": "_resources/00-setup",
      "pre_run": False,
      "publish_on_website": False,
      "add_cluster_setup_cell": False,
      "title": "Setup",
      "description": "Catalog + schema for demo state tracking",
      "depends_on_previous": False
    },
    {
      "path": "00-provision-lakebase",
      "pre_run": False,
      "publish_on_website": True,
      "add_cluster_setup_cell": True,
      "title": "Provision Lakebase (Postgres)",
      "description": "Stand up the Lakebase instance + sessions table that the streaming pipeline writes into.",
      "depends_on_previous": False
    },
    {
      "path": "01-realtime-sessionize",
      "pre_run": False,
      "publish_on_website": True,
      "add_cluster_setup_cell": True,
      "title": "Real-Time Sessions in Lakebase",
      "description": "Rate source feeding Real-Time Mode transformWithState (Row API, processing-time timers), with the native Postgres streaming sink that upserts per-user sessions into Lakebase. Reads back from Postgres at the end.",
      "depends_on_previous": True
    },
    {
      "path": "02-deploy-personalization-console",
      "pre_run": False,
      "publish_on_website": True,
      "add_cluster_setup_cell": True,
      "title": "Deploy the Personalization Console",
      "description": "Create and deploy the Personalization Console app with a Lakebase database resource, then grant its service principal read access to the sessions table.",
      "depends_on_previous": True
    },
    {
      "path": "app",
      "pre_run": False,
      "publish_on_website": False,
      "add_cluster_setup_cell": False,
      "title": "Personalization Console App",
      "description": "Streamlit application folder deployed by the 02 notebook.",
      "depends_on_previous": False
    }
  ],
  "cluster": {
      "spark_version": "18.x-scala2.13",
      "spark_conf": {
        "spark.databricks.streaming.realTimeMode.enabled": "true",
        "spark.sql.shuffle.partitions": "4"
      },
      "num_workers": 2,
      "single_user_name": "{{CURRENT_USER}}",
      "data_security_mode": "SINGLE_USER"
  }
}
