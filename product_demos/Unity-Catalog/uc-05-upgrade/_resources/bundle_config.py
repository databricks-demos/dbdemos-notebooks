# Databricks notebook source
# MAGIC %md 
# MAGIC ## Demo bundle configuration
# MAGIC Please ignore / do not delete, only used to prep and bundle the demo

# COMMAND ----------

{
  "name": "uc-05-upgrade",
  "category": "governance",
  "title": "Upgrade table to Unity Catalog",
  "custom_schema_supported": True,
  "default_catalog": "main",
  "default_schema": "dbdemos_upgraded_on_uc",
  "description": "Discover how to upgrade your hive_metastore tables to Unity Catalog to benefit from UC capabilities: Security/ACL/Row-level/Lineage/Audit...",
  "fullDescription": "Unity Catalog is a unified governance solution for all data and AI assets including files, tables, machine learning models and dashboards in your lakehouse on any cloud.<br/>In this demo, we'll show how to upgrade your existing tables from the legacy Hive Metastore catalog to Unity Catalog. You'll then be able to leverage all UC capabilities (Security/ACL/Row-level/Lineage/Audit...)<br/> This demo contains example using the UI, but also show example script to upgrade an entire database, handling external location but also managed tables.",
    "usecase": "Data Governance",
  "products": ["Unity Catalog"],
  "related_links": [
      {"title": "View all Product demos", "url": "<TBD: LINK TO A FILTER WITH ALL DBDEMOS CONTENT>"},
      {"title": "Databricks Unity Catalog: Fine grained Governance", "url": "https://www.databricks.com/blog/2021/05/26/introducing-databricks-unity-catalog-fine-grained-governance-for-data-and-ai-on-the-lakehouse.html"}],
  "recommended_items": ["uc-01-acl", "uc-02-external-location", "uc-03-data-lineage"],
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
      "path": "00-Upgrade-database-to-UC", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True, 
      "title":  "Upgrade database to UC", 
      "description": "Migration example, from one table to multiple databases."
    }
  ],
  "cluster": {
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
