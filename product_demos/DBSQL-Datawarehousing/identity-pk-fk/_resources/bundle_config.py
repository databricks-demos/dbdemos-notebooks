# Databricks notebook source
# MAGIC %md 
# MAGIC ## Demo bundle configuration
# MAGIC Please ignore / do not delete, only used to prep and bundle the demo

# COMMAND ----------

{
  "name": "identity-pk-fk",
  "category": "DBSQL",
  "serverless_supported": True,
  "custom_schema_supported": True,
  "default_catalog": "main",
  "default_schema": "dbdemos_pk_fk",
  "title": "Data Warehousing with Identity, Primary Key & Foreign Key",
  "description": "Define your schema with auto incremental column and Primary + Foreign Key. Ideal for Data Warehouse & BI support!",
  "fullDescription": "To simplify SQL operations and support migrations from on-prem and alternative warehouse, Databricks Lakehouse now provide convenient ways to build Entity Relationship Diagrams that are simple to maintain and evolve. In this demo, we'll cover:<ul><li>The ability to automatically generate auto-incrementing identify columns. Just insert data and the engine will automatically increment the ID.</li><li>Support for defining primary key</li><li>Support for defining foreign key constraints.</li></ul>Databricks Lakehouse is the best Datawarehouse!",
  "usecase": "Data Warehousing & BI",
  "products": ["Databricks SQL", "Dashboard", "Delta Lake"],
  "related_links": [
      {"title": "View all Product demos", "url": "<TBD: LINK TO A FILTER WITH ALL DBDEMOS CONTENT>"}, 
      {"title": "Generate Identity columns and surrogate keys", "url": "https://www.databricks.com/blog/2022/08/08/identity-columns-to-generate-surrogate-keys-are-now-available-in-a-lakehouse-near-you.html"}],
  "recommended_items": ["sql-ai-functions", "feature-store", "llm-dolly-chatbot"],
  "demo_assets": [],
  "bundle": True,
  "tags": [{"dbsql": "BI/DW/DBSQL"}],
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
      "path": "00-Identity_PK_FK", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True, 
      "title":  "Identify column, PK & FK", 
      "description": "Define your schema with auto incremental column and Primary + Foreign Key."
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
