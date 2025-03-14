# Databricks notebook source
# MAGIC %md 
# MAGIC ## Demo bundle configuration
# MAGIC Please ignore / do not delete, only used to prep and bundle the demo

# COMMAND ----------

{
  "name": "uc-04-audit-log",
  "category": "governance",
  "title": "Audit-log with Databricks",
  "description": "[DEPRECATED - prefer uc-04-system-tables] Track usage with Audit-log.",
  "fullDescription": "<strong>Databricks Unity Catalog now provides complete system tables including out of the box Audit Log tables. This demo will be deprecated soon. Install uc-04-system-tables instead.</strong>Unity Catalog is a unified governance solution for all data and AI assets including files, tables, machine learning models and dashboards in your lakehouse on any cloud.<br/>In this demo, we'll show how Unity Catalog can be used to track all operation on top of your dataset. You'll be able to analyze who is accessing which table, find pattern and monitor your entire data assets for governance and security purpose.",
    "usecase": "Data Governance",
  "products": ["Unity Catalog"],
  "related_links": [
      {"title": "View all Product demos", "url": "<TBD: LINK TO A FILTER WITH ALL DBDEMOS CONTENT>"},
      {"title": "Databricks Unity Catalog: Fine grained Governance", "url": "https://www.databricks.com/blog/2021/05/26/introducing-databricks-unity-catalog-fine-grained-governance-for-data-and-ai-on-the-lakehouse.html"}],
  "recommended_items": ["uc-01-acl", "uc-02-external-location", "uc-03-data-lineage"],
  "demo_assets": [],
  "bundle": False,
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
      "path": "00-auditlog-activation", 
      "pre_run": False, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True, 
      "title":  "Enable audit log", 
      "description": "Use APIs to create enable Audit Log (run only once for setup)."
    },
    {
      "path": "01-AWS-Audit-log-ingestion", 
      "pre_run": False,
      "publish_on_website": True, 
      "add_cluster_setup_cell": True, 
      "title":  "Audit log ingestion", 
      "description": "Create an ingestion pipeline to ingest and analyse your logs."
    },
    {
      "path": "02-log-analysis-query", 
      "pre_run": False, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True, 
      "title":  "Log Analysis example", 
      "description": "SQL queries example to analyze your logs."
    }
  ],
  "cluster": {
    "num_workers": 4,
    "single_user_name": "{{CURRENT_USER}}",
    "data_security_mode": "SINGLE_USER"
  }
}
