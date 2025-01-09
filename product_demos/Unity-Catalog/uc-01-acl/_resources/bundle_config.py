# Databricks notebook source
# MAGIC %md 
# MAGIC ## Demo bundle configuration
# MAGIC Please ignore / do not delete, only used to prep and bundle the demo

# COMMAND ----------

{
  "name": "uc-01-acl",
  "category": "governance",
  "serverless_supported": True,
  "custom_schema_supported": True,
  "default_schema": "uc_acl",
  "default_catalog": "main",
  "title": "Table ACL & Row + Column Level security with UC",
  "description": "Discover how to GRANT permission on your table with Unity Catalog and implement fined grained control such as data masking at column level or filter rows based on each user.",
  "fullDescription": "Unity Catalog is a unified governance solution for all data and AI assets including files, tables, machine learning models and dashboards in your lakehouse on any cloud.<br/>In this demo, we’ll show how Unity Catalog can be used to secure your table and grant ACL on tables. We’ll also see how Unity Catalog can provide dynamic data masking on columns leveraging SQL functions, and filter rows based on the current user. This will allow you to hide or anonymize  data based on each user permissions, from a simple condition based on GROUP or more advanced control.",
  "usecase": "Data Governance",
  "products": ["Unity Catalog"],
  "related_links": [
      {"title": "View all Product demos", "url": "<TBD: LINK TO A FILTER WITH ALL DBDEMOS CONTENT>"},
      {"title": "Databricks Unity Catalog: Fine grained Governance", "url": "https://www.databricks.com/blog/2021/05/26/introducing-databricks-unity-catalog-fine-grained-governance-for-data-and-ai-on-the-lakehouse.html"}],
  "recommended_items": ["uc-02-external-location", "uc-03-data-lineage", "uc-04-audit-log"],
  "demo_assets": [],
  "bundle": True,
  "tags": [{"uc": "Unity Catalog"}],
  "notebooks": [
    {
      "path": "_resources/00-setup",
      "pre_run": False,
      "publish_on_website": False,
      "add_cluster_setup_cell": False,
      "title":  "Setup",
      "description": "Init data for demo."
    },
    {
      "path": "00-UC-Table-ACL", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True, 
      "title":  "Table ACL with UC", 
      "description": "Secure table access with GRANT command.",
      "parameters": {"catalog": "dbdemos"}
    },
    {
      "path": "01-Row-Column-access-control", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True, 
      "title":  "Row-level & and Column level masking", 
      "description": "Use SQL functions to filter your data on row and column level.",
      "parameters": {"catalog": "dbdemos"}
    },
    {
      "path": "02-[legacy]-UC-Dynamic-view", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True, 
      "title":  "Legacy Dynamic views", 
      "description": "Create dynamic views with UC for finer access control (legacy).",
      "parameters": {"catalog": "dbdemos"}
    }
  ],
  "cluster": {
    "autoscale": {
        "min_workers": 1,
        "max_workers": 1
    },
    "spark_version": "13.1.x-scala2.12",
    "data_security_mode": "USER_ISOLATION"
  }
}
