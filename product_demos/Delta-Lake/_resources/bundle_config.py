# Databricks notebook source
# MAGIC %md 
# MAGIC ## Demo bundle configuration
# MAGIC Please ignore / do not delete, only used to prep and bundle the demo

# COMMAND ----------

{
  "name": "delta-lake",
  "category": "data-engineering",
  "title": "Delta Lake",
  "serverless_supported": True,
  "custom_schema_supported": True,
  "default_catalog": "main",
  "default_schema": "dbdemos_delta_lake",
  "description": "Store your table with Delta Lake & discover how Delta Lake can simplify your Data Pipelines.",
  "fullDescription": "Delta Lake is an open format storage layer that delivers reliability, security and performance on your data lake — for both streaming and batch operations. By replacing data silos with a single home for structured, semi-structured and unstructured data, Delta Lake is the foundation of a cost-effective, highly scalable lakehouse.<br /> In this demo, we'll show you how Delta Lake is working and its main capabilities: <ul><li>ACID transactions</li><li>Support for DELETE/UPDATE/MERGE</li><li>Unify batch & streaming</li><li>Time Travel</li><li>Clone zero copy</li><li>Generated partitions</li><li>CDF - Change Data Flow (DBR runtime)</li><li>Blazing-fast queries</li></ul>",
  "usecase": "Data Engineering",
  "products": ["Delta Lake", "Spark"],
  "related_links": [
      {"title": "View all Product demos", "url": "<TBD: LINK TO A FILTER WITH ALL DBDEMOS CONTENT>"}, 
      {"title": "Databricks Delta Lake", "url": "https://www.databricks.com/blog/2022/05/20/five-simple-steps-for-implementing-a-star-schema-in-databricks-with-delta-lake.html"}],
  "recommended_items": ["identity-pk-fk", "dlt-loans", "dlt-cdc"],
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
      "description": "Init load data"
    },
    {
      "path": "config", 
      "pre_run": False, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": False, 
      "title":  "Config", 
      "description": "Setup schema and catalog name"
    },
    {
      "path": "00-Delta-Lake-Introduction", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True, 
      "title":  "Introduction to Delta Lake", 
      "description": "Start here to discover Delta Lake"
    },
    {
      "path": "01-Getting-Started-With-Delta-Lake", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True, 
      "title":  "Get started with Delta Lake", 
      "description": "Create your first table, DML operation, time travel, RESTORE, CLONE and more.",
      "parameters": {"raw_data_location": "/Users/quentin.ambard@databricks.com/demos/retail_dbdemos/delta"}    
    },
    {
      "path": "02-Delta-Lake-Performance", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True, 
      "title":  "Delta Lake Performance & operation", 
      "description": "Faster queries with LIQUID CLUSTERING."
    },
    {
      "path": "03-Delta-Lake-Uniform", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True, 
      "title":  "Universal Format with Delta Lake", 
      "description": "Access your Delta table with Iceberg and Hudi."
    },
    {
      "path": "04-Delta-Lake-CDF", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True, 
      "title":  "Change Data Flow (CDF)", 
      "description": "Capture and propagate table changes."
    },
    {
      "path": "05-Advanced-Delta-Lake-Internal", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True, 
      "title":  "Delta Lake Internals", 
      "description": "Deep dive in Delta Lake file format."
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
    "num_workers": 0,
    "single_user_name": "{{CURRENT_USER}}",
    "data_security_mode": "SINGLE_USER"
  }
}
