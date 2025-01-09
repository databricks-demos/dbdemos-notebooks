# Databricks notebook source
# MAGIC %md 
# MAGIC ## Demo bundle configuration
# MAGIC Please ignore / do not delete, only used to prep and bundle the demo

# COMMAND ----------

{
  "name": "auto-loader",
  "category": "data-engineering",
  "title": "Databricks Autoloader (cloudfile)",
  "serverless_supported": True,
  "custom_schema_supported": True,
  "default_catalog": "main",
  "default_schema": "dbdemos_autoloader",
  "description": "Incremental ingestion on your cloud storage folder.",
  "fullDescription": "Auto Loader incrementally and efficiently processes new data files as they arrive in cloud storage without any additional setup. In this demo, we'll show you how the autoloader is working and cover its main capabilities: <ul><li>Incremental & cost-efficient ingestion (removes unnecessary listing or state handling)</li><li>Simple and resilient operation: no tuning or manual code required</li><li>Scalable to billions of files</li><li>Schema inference and schema evolution are handled out of the box for most formats (csv, json, avro, images...)</li></ul>",
  "usecase": "Data Engineering",
  "products": ["Auto Loader", "Delta Lake"],
  "related_links": [
      {"title": "View all Product demos", "url": "<TBD: LINK TO A FILTER WITH ALL DBDEMOS CONTENT>"}, 
      {"title": "Databricks Auto Loader", "url": "https://www.databricks.com/blog/2020/02/24/introducing-databricks-ingest-easy-data-ingestion-into-delta-lake.html"}],
  "recommended_items": ["delta-lake", "dlt-loans", "dlt-cdc"],
  "demo_assets": [],     
  "bundle": True,
  "tags": [{"autoloader": "Auto Loader"}],
  "notebooks": [
    {
      "path": "_resources/00-setup", 
      "pre_run": False, 
      "publish_on_website": False, 
      "add_cluster_setup_cell": False, 
      "title":  "Init data", 
      "description": "load data."
    },
    {
      "path": "_resources/01-load-data", 
      "pre_run": False, 
      "publish_on_website": False, 
      "add_cluster_setup_cell": False, 
      "title":  "Init data", 
      "description": "load data."
    }, 
    {
      "path": "01-Auto-loader-schema-evolution-Ingestion", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True, 
      "title":  "Databricks Autoloader", 
      "description": "Simplify incremental ingestion with Databricks Autoloader (cloud_file)."
    }
  ],
  "cluster": {
      "spark_version": "15.4.x-scala2.12",
      "spark_conf": {
        "spark.master": "local[*]",
        "spark.databricks.cluster.profile": "singleNode"
    },
    "custom_tags": {
        "ResourceClass": "SingleNode"
    },
    "single_user_name": "{{CURRENT_USER}}",
    "data_security_mode": "SINGLE_USER",
    "num_workers": 0
  }
}
