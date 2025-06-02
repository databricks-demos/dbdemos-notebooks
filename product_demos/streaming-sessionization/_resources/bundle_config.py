# Databricks notebook source
# MAGIC %md 
# MAGIC ## Demo bundle configuration
# MAGIC Please ignore / do not delete, only used to prep and bundle the demo

# COMMAND ----------

{
  "name": "streaming-sessionization",
  "category": "data-engineering",
  "title": "Spark Streaming - Advanced",
  "serverless_supported": True,
  "custom_schema_supported": True,
  "default_catalog": "main",
  "default_schema": "dbdemos_streaming_sessionization",
  "description": "Deep dive on Spark Streaming with Delta to build webapp user sessions from clicks, with custom aggregation state management.",
  "fullDescription": "The Databricks Lakehouse Platform dramatically simplifies data streaming to deliver real-time analytics, machine learning and applications on one platform. In this demo, we'll present how the Databricks Lakehouse provide streaming capabilities to ingest and analyze click-stream data (typically from message queues such as Kafka). <br/>Sessionization is the process of finding time-bounded user sessions from a flow of events, grouping all events happening around the same time (e.g., number of clicks, pages most viewed, etc)<br/>Understanding sessions is critical for a lot of use cases: <ul><li>Detect cart abandonment in your online shot, and automatically trigger marketing actions as follow-up to increase your sales</li><li>Build better attribution models for your affiliation, based on the user actions during each session</li><li>Understand the user journey in your website, and provide a better experience to increase your user retention</li></ul><br/><br/>In this demo, we will :<ul><li>Ingest data from Kafka</li><li>Save the data as Delta tables, ensuring quality and performance at scale</li><li>Compute user sessions based on activity</li></ul>",
  "usecase": "Data Engineering",
  "products": ["Streaming", "Delta Lake", "Spark"],
  "related_links": [
      {"title": "View all Product demos", "url": "<TBD: LINK TO A FILTER WITH ALL DBDEMOS CONTENT>"},
      {"title": "Databricks Streaming Project Lightspeed", "url": "https://www.databricks.com/blog/2022/06/28/project-lightspeed-faster-and-simpler-stream-processing-with-apache-spark.html"}],
  "recommended_items": ["dlt-loans", "dlt-cdc", "delta-lake"],
  "demo_assets": [],
  "bundle": True,
  "tags": [{"delta": "Delta Lake"}],
  "notebooks": [
    {
      "path": "_resources/00-setup", 
      "pre_run": False, 
      "publish_on_website": False, 
      "add_cluster_setup_cell": False, 
      "title":  "Load data", 
      "description": "Init load data",
      "depends_on_previous": False
    },
    {
      "path": "_resources/00-setup-scala", 
      "pre_run": False, 
      "publish_on_website": False, 
      "add_cluster_setup_cell": False, 
      "title":  "Load data", 
      "description": "Init load data",
      "depends_on_previous": False
    },
    {
      "path": "01-Delta-session-BRONZE", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True, 
      "title":  "Consume data from Kafka - Bronze", 
      "description": "Save kafka events in a Delta Lake table and start analysis.",
      "depends_on_previous": False
    },
    {
      "path": "02-Delta-session-SILVER", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True, 
      "title":  "Delta streaming, drop duplicate", 
      "description": "Clean events and remove duplicate in the Silver layer.",
      "depends_on_previous": False,
      "parameters": {"reset_all_data": "false"}
    },
    {
      "path": "03-Delta-session-GOLD", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True, 
      "title":  "Streaming State / Aggregation", 
      "description": "Compute sessions with applyInPandasWithState and upsert (MERGE) output to sessions table.",
      "depends_on_previous": False,
      "parameters": {"reset_all_data": "false"}
    },
    {
      "path": "scala/01-Delta-session-BRONZE-scala", 
      "pre_run": False, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True, 
      "title":  "Consume data from Kafka - Bronze - scala", 
      "description": "Save kafka events in a Delta Lake table and start analysis.",
      "depends_on_previous": False
    },
    {
      "path": "scala/02-Delta-session-SILVER-scala", 
      "pre_run": False, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True, 
      "title":  "Delta streaming, drop duplicate - scala", 
      "description": "Clean events and remove duplicate in the Silver layer.",
      "depends_on_previous": False,
      "parameters": {"reset_all_data": "false"}
    },
    {
      "path": "scala/03-Delta-session-GOLD-scala", 
      "pre_run": False, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True, 
      "title":  "Streaming State / Aggregation - scala", 
      "description": "Compute sessions in scala using flatMapGroupsWithState.",
      "depends_on_previous": False,
      "parameters": {"reset_all_data": "false"}
    },
    {
      "path": "_00-Delta-session-PRODUCER", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True, 
      "title":  "Data Injector", 
      "description": "Simulate user events on website",
      "depends_on_previous": False,
      "parameters": {"reset_all_data": "false", "produce_time_sec": "300"}
    }
  ],
  "cluster": {
      "spark_version": "16.4.x-scala2.12",
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
