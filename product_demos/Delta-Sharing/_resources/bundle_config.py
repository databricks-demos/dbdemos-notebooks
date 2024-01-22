# Databricks notebook source
# MAGIC %md 
# MAGIC ## Demo bundle configuration
# MAGIC Please ignore / do not delete, only used to prep and bundle the demo

# COMMAND ----------

{
  "name": "delta-sharing-airlines",
  "category": "governance",
  "title": "Delta Sharing - Airlines",
  "description": "Share your data to external organization using Delta Sharing.",
  "fullDescription": "Databricks Delta Sharing provides an open solution to securely share live data from your lakehouse to any computing platform.<br/>In this demo, we'll cover Delta Sharing main capabilities:<ul><li>Open cross-platform sharing: Avoid vendor lock-in, and easily share existing data in Delta Lake and Apache Parquet formats to any data platform.</li><li>Share live data with no replication: Share live data across data platforms, clouds or regions without replicating or copying it to another system.</li><li>Centralized governance: Centrally manage, govern, audit and track usage of the shared data on one platform.</li></ul>",
  "usecase": "Data Governance",
  "products": ["Delta Sharing", "Unity Catalog"],
  "related_links": [
      {"title": "View all Product demos", "url": "<TBD: LINK TO A FILTER WITH ALL DBDEMOS CONTENT>"},
      {"title": "Announcing GA of Delta Sharing", "url": "https://www.databricks.com/blog/2022/08/26/announcing-general-availability-delta-sharing.html"}],
  "recommended_items": ["uc-01-acl", "uc-02-external-location", "uc-03-data-lineage"],
  "demo_assets": [],
  "bundle": True,
  "tags": [{"delta-sharing": "Delta Sharing"}],
  "notebooks": [
    {
      "path": "_resources/00-setup", 
      "pre_run": False, 
      "publish_on_website": False, 
      "add_cluster_setup_cell": False, 
      "title":  "Setup data", 
      "description": "Create the catalog for the demo."
    },
    {
      "path": "01-Delta-Sharing-presentation", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": False, 
      "title":  "Delta Sharing - Introduction", 
      "description": "Discover Delta-Sharing and explore your sharing capabilities."
    },
    {
      "path": "02-provider-delta-sharing-demo", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True, 
      "title":  "Share data as Provider", 
      "description": "Discover how to create SHARE and RECIPIENT to share data with external organization."
    },
    {
      "path": "03-receiver-delta-sharing-demo", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True, 
      "title":  "Consume data (external system)", 
      "description": "Read data shared from Delta Sharing using any external system."
    },
    {
      "path": "04-share-data-within-databricks", 
      "pre_run": False, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True, 
      "title":  "Consume data with Databricks UC", 
      "description": "Simplify data access from another Databricks Workspace with Unity Catalog."
    },
    {
      "path": "05-extra-delta-sharing-rest-api",
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True, 
      "title":  "Delta Sharing Internals - REST API", 
      "description": "Extra: Deep dive in Delta Sharing REST API."
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
