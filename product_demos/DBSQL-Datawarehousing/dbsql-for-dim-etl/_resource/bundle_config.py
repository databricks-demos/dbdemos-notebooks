# Databricks notebook source
# MAGIC %md 
# MAGIC ## Demo bundle configuration
# MAGIC Please ignore / do not delete, only used to prep and bundle the demo

# COMMAND ----------

{
    "name": "dbsql-for-dim-etl",
    "category": "DBSQL",
    "title": "DBSQL: Create and Populate Patient Dimension",
    "custom_schema_supported": True,
    "default_catalog": "main",
    "default_schema": "dbdemos_sql_etl",
    "description": "The demo will illustrate the data architecture and data workflow that creates and populates a dimension in a Star Schema using Databricks SQL. This will utilize a Patient dimension in the Healthcare domain. The demo will illustrate all facets of an end-to-end ETL to transform, validate, and load an SCD2 dimension.",
    "bundle": True,
    "tags": [{"dbsql": "ETL/DW/DBSQL"}],
    "notebooks": [
      {
        "path": "00-patient-dimension-ETL-introduction", 
        "pre_run": False, 
        "publish_on_website": True, 
        "add_cluster_setup_cell": False,
        "title":  "Patient Dimension ETL Introduction", 
        "description": "Start here to explore the demo."
      },
      {
        "path": "01-Setup/01.1-initialize", 
        "pre_run": False, 
        "publish_on_website": True, 
        "add_cluster_setup_cell": False,
        "title":  "Configure and Initialize", 
        "description": "Configure demo catalog, schema, and initialize global variables."
      },
      {
        "path": "01-Setup/01.2-setup", 
        "pre_run": False, 
        "publish_on_website": True, 
        "add_cluster_setup_cell": False,
        "title":  "Create demo Catalog/Schema/Volume", 
        "description": "Create demo Catalog/Schema/Volume."
      },
      {
        "path": "02-Create/02.1-create-code-table", 
        "pre_run": False, 
        "publish_on_website": True, 
        "add_cluster_setup_cell": False,
        "title":  "Create Code Table", 
        "description": "Create the code master table and initialize with sample data."
      },
      {
        "path": "02-Create/02.2-ETL-log-table", 
        "pre_run": False, 
        "publish_on_website": True, 
        "add_cluster_setup_cell": False,
        "title":  "Create ETL Log Table", 
        "description": "Create the ETL log table to log metadata on each ETL run."
      },
      {
        "path": "02-Create/02.3-create-patient-tables", 
        "pre_run": False, 
        "publish_on_website": True, 
        "add_cluster_setup_cell": False,
        "title":  "Create Patient Tables", 
        "description": "Create the patient staging, patient integration, and patient dimension tables."
      },
      {
        "path": "03-Populate/03.1-patient-dimension-ETL", 
        "pre_run": False, 
        "publish_on_website": True, 
        "add_cluster_setup_cell": False,
        "title":  "Populate Patient Dimension", 
        "description": "Populate the patient staging, patient integration, and patient dimension tables."
      },
      {
        "path": "03-Populate/03.2-log-ETL-run-utility", 
        "pre_run": False, 
        "publish_on_website": True, 
        "add_cluster_setup_cell": False,
        "title":  "Log ETL Run Utility", 
        "description": "Utility to log the metadata from each ETL run by table."
      },
      {
        "path": "_resource/browse-load", 
        "pre_run": False, 
        "publish_on_website": True, 
        "add_cluster_setup_cell": False,
        "title":  "Browse Load", 
        "description": "Browse the data populated in the patient tables and log table."
      },
      {
        "path": "_resource/stage-source-file-init", 
        "pre_run": False, 
        "publish_on_website": True, 
        "add_cluster_setup_cell": False,
        "title":  "Stage Source Data File - Initial Load", 
        "description": "Stage the source CSV file for initial load onto the staging volume and folder."
      },
      {
        "path": "_resource/stage-source-file-init", 
        "pre_run": False, 
        "publish_on_website": True, 
        "add_cluster_setup_cell": False,
        "title":  "Stage Source Data File - Incremental Load", 
        "description": "Stage the source CSV file for incremental load onto the staging volume and folder."
      }
    ],
    "init_job": {},
    "serverless_supported": True,
    "cluster": {},
    "pipelines": [],
  }
