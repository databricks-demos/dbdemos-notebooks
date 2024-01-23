# Databricks notebook source
# MAGIC %md 
# MAGIC ## Demo bundle configuration
# MAGIC Please ignore / do not delete, only used to prep and bundle the demo

# COMMAND ----------

{
  "name": "uc-03-data-lineage",
  "category": "governance",
  "title": "Data Lineage with Unity Catalog",
  "description": "Discover data lineage with Unity Catalog: table to table and column to column",
  "fullDescription": "Unity Catalog is a unified governance solution for all data and AI assets including files, tables, machine learning models and dashboards in your lakehouse on any cloud.<br/>In this demo, we'll show how Unity Catalog provides Lineage on any data that you read & write.<br/>Not only Unity Catalog provides lineage at a table level, but also at a row level, allowing you to track which application is using which data, ideal for PII/GDPR data analysis and governance.",
    "usecase": "Data Governance",
  "products": ["Unity Catalog"],
  "related_links": [
      {"title": "View all Product demos", "url": "<TBD: LINK TO A FILTER WITH ALL DBDEMOS CONTENT>"},
      {"title": "Databricks Unity Catalog: Fine grained Governance", "url": "https://www.databricks.com/blog/2021/05/26/introducing-databricks-unity-catalog-fine-grained-governance-for-data-and-ai-on-the-lakehouse.html"}],
  "recommended_items": ["uc-01-acl", "uc-02-external-location", "uc-04-audit-log"],
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
      "path": "00-UC-lineage", 
      "pre_run": False, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True, 
      "title":  "UC data lineage", 
      "description": "Data lineage from Table to Table & Column 2 column."
    }
  ],
  "cluster": {
      "spark_conf": {
        "spark.master": "local[*]",
        "spark.databricks.cluster.profile": "singleNode",
        "spark.databricks.dataLineage.enabled": "true"
    },
    "custom_tags": {
        "ResourceClass": "SingleNode"
    },
    "num_workers": 0,
    "single_user_name": "{{CURRENT_USER}}",
    "data_security_mode": "SINGLE_USER"
  }
}
