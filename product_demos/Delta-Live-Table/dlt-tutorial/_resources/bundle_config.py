# Databricks notebook source
# MAGIC %md 
# MAGIC ## Demo bundle configuration
# MAGIC Please ignore / do not delete, only used to prep and bundle the demo

# COMMAND ----------

{
  "name": "dlt-tutorial",
  "category": "data-engineering",
  "title": "CDC Data Generator for DLT Tutorial",
  "serverless_supported": True,
  "custom_schema_supported": False,
  "default_catalog": "main",
  "default_schema": "dbdemos_dlt_cdc",
  "description": "Generate CDC data for the DLT tutorial. <br/> This demo is part of the Delta Live Table tutorial. <br/> It shows how to use Delta Live Table to implement a CDC pipeline with SCD2 (Slowly Changing Dimension type 2).<br/> The demo also includes a data quality dashboard and a monitoring notebook.",
  "fullDescription": "This demo highlight how Delta Live Table simplify CDC (Change Data Capture).<br/> CDC is typically done ingesting changes from external system (ERP, SQL databases) with tools like fivetran, debezium etc. <br/> In this demo, we'll show you how to re-create your table consuming CDC information. <br/>We'll also implement a SCD2 (Slowly Changing Dimention table of type 2). While this can be really tricky to implement when data arrives out of order, DLT makes this super simple with one simple keyword.<br/><br/>Ultimately, we'll show you how to programatically scan multiple incoming folder and trigger N stream (1 for each CDC table), leveraging DLT with python.",
    "usecase": "Data Engineering",
  "products": ["DLT", "Delta Lake", "Spark", "CDC"],
  "related_links": [
      {"title": "View all Product demos", "url": "<TBD: LINK TO A FILTER WITH ALL DBDEMOS CONTENT>"}, 
      {"title": "Databricks Delta Lake CDC", "url": "https://www.databricks.com/blog/2022/04/25/simplifying-change-data-capture-with-databricks-delta-live-tables.html"}],
  "recommended_items": ["dlt-unit-test", "dlt-loans", "cdc-pipeline"],
  "demo_assets": [
      {"title": "Delta Live Table pipeline", "url": "https://www.dbdemos.ai/assets/img/dbdemos/dlt-cdc-dlt-0.png"}],
  "bundle": True,
  "tags": [{"dlt": "Delta Live Table"}],
  "notebooks": [
    {
      "path": "_resources/00-Data_CDC_Generator", 
      "pre_run": True, 
      "publish_on_website": False, 
      "add_cluster_setup_cell": False,
      "title":  "CDC data generator", 
      "description": "Generate data for the pipeline."
    },
  ],
}
