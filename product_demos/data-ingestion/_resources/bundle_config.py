# Databricks notebook source
# MAGIC %md 
# MAGIC ## Demo bundle configuration
# MAGIC Please ignore / do not delete, only used to prep and bundle the demo

# COMMAND ----------

{
  "name": "data-ingestion",
  "category": "data-engineering",
  "title": "Data Ingestion with Databricks",
  "serverless_supported": True,
  "custom_schema_supported": True,
  "default_catalog": "main",
  "default_schema": "dbdemos_data_ingestion",
  "description": "Data Ingestion with Databricks including introduction to Lakeflow Connect, SQL's read_files, and Auto Loader",
  "fullDescription": "Tired of data silos and complex ingestion pipelines? Join us for a dynamic demo showcasing the unparalleled flexibility and power of the Databricks Data Intelligence Platform to bring ALL your data into the Lakehouse, no matter the source or format. In this demo, we’ll unveil how Databricks empowers you to effortlessly unify your entire data estate with a seamless, end-to-end ingestion strategy: <ul><li>Lakeflow Connect: Witness the simplicity of managed ingestion from diverse enterprise sources like SaaS applications and databases. See how you can click-and-connect to critical business data with minimal effort, bringing structured and semi-structured data directly into your Lakehouse.</li><li>SQL read_files: Discover the instant gratification of direct SQL access to raw files. Learn how to query and explore data from any cloud storage location (CSV, JSON, Parquet, and more) on-the-fly, empowering ad-hoc analysis and accelerating data preparation.</li><li>Auto Loader: Experience the robust automation of incremental file ingestion. See how Auto Loader intelligently detects and loads new data as it lands in your cloud storage, handling schema evolution and ensuring exactly-once processing for real-time streaming analytics and AI-ready data.</li></ul>",
  "usecase": "Data Engineering",
  "products": ["Auto Loader", "Delta Lake", "Lakeflow Connect", "DBSQL"],
  "related_links": [
      {"title": "View all Product demos", "url": "<TBD: LINK TO A FILTER WITH ALL DBDEMOS CONTENT>"}, 
      {"title": "Databricks Auto Loader", "url": "https://www.databricks.com/blog/2020/02/24/introducing-databricks-ingest-easy-data-ingestion-into-delta-lake.html"}],
  "recommended_items": ["delta-lake", "sdp-loans", "sdp-cdc"],
  "demo_assets": [],     
  "bundle": True,
  "tags": [{"data-ingestion": "Data Ingestion"}],
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
      "path": "00-Ingestion-data-introduction",
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True,
      "title": "Data Ingestion with Databricks",
      "description": "Introduction to data ingestion with Databricks"
    },
    {
      "path": "01-ingestion-with-sql-read_files",
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True, 
      "title":  "Databricks SQL read_files",
      "description": "Ingest cloud files in native SQL with read_files"
    }, 
    {
      "path": "02-Auto-loader-schema-evolution-Ingestion", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True, 
      "title":  "Databricks Autoloader", 
      "description": "Simplify incremental ingestion with Databricks Autoloader (cloud_file)."
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
    "single_user_name": "{{CURRENT_USER}}",
    "data_security_mode": "SINGLE_USER",
    "num_workers": 0
  }
}
