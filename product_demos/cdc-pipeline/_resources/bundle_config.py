# Databricks notebook source
# MAGIC %md 
# MAGIC ## Demo bundle configuration
# MAGIC Please ignore / do not delete, only used to prep and bundle the demo

# COMMAND ----------

{
  "name": "cdc-pipeline",
  "category": "data-engineering",
  "title": "CDC Pipeline with Delta",
  "description": "Process CDC data to build an entire pipeline and materialize your operational tables in your lakehouse.",
  "fullDescription": "This demo highlight how to implement a CDC flow (Change Data Capture) with Spark API and Delta Lake.<br/> CDC is typically done ingesting changes from external system (ERP, SQL databases) with tools like fivetran, debezium etc. <br/> In this demo, we'll show you how to re-create your table consuming CDC information. <br/><br/>Ultimately, we'll show you how to programatically scan multiple incoming folder and trigger N stream (1 for each CDC table).<br/>Note that CDC is made easier with Delta Live Table (CDC). We recommend you to try the DLT CDC demo!",
  "usecase": "Data Engineering",
  "products": ["Delta Lake", "Spark", "CDC"],
  "related_links": [
      {"title": "View all Product demos", "url": "<TBD: LINK TO A FILTER WITH ALL DBDEMOS CONTENT>"}, 
      {"title": "Databricks Delta Lake CDC", "url": "https://www.databricks.com/blog/2021/06/09/how-to-simplify-cdc-with-delta-lakes-change-data-feed.html"}],
  "recommended_items": ["dlt-cdc", "dlt-loans", "delta-lake"],
  "demo_assets": [],    
  "bundle": True,
  "tags": [{"delta": "Delta Lake"}],
  "notebooks": [
    {
      "path": "_resources/00-setup", 
      "pre_run": False, 
      "publish_on_website": False, 
      "add_cluster_setup_cell": False, 
      "title":  "Setup", 
      "description": "Setup."
    },
    {
      "path": "_resources/01-load-data", 
      "pre_run": False, 
      "publish_on_website": False, 
      "add_cluster_setup_cell": False, 
      "title":  "Data initialization", 
      "description": "Data initialization"
    },
    {
      "path": "01-CDC-CDF-simple-pipeline", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True, 
      "title":  "Implement CDC flow with Delta Lake", 
      "description": "Ingest CDC data and materialize your tables and propagate changes downstream."
    },
    {
      "path": "02-CDC-CDF-full-multi-tables", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True, 
      "title":  "Delta Lake Performance & operation", 
      "description": "Programatically ingest multiple CDC flows to synch all your database."
    }
  ],
  "cluster": {
      "spark_version": "14.3.x-scala2.12",
    "spark_conf": {
        "spark.master": "local[*, 4]",
        "spark.databricks.cluster.profile": "singleNode"
    },
    "custom_tags": {
        "ResourceClass": "SingleNode"
    },
    "num_workers": 0,
    "single_user_name": "{{CURRENT_USER}}",
    "data_security_mode": "SINGLE_USER",
    "node_type_id": "m5.large",
    "driver_node_type_id": "m5.large",
  }
}
