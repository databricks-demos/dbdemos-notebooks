# Databricks notebook source
# MAGIC %md 
# MAGIC ## Demo bundle configuration
# MAGIC Please ignore / do not delete, only used to prep and bundle the demo

# COMMAND ----------

{
  "name": "uc-02-external-location",
  "category": "governance",
  "serverless_supported": True,
  "title": "Access data on External Location",
  "description": "Discover how you can secure files/table in external location (cloud storage like S3/ADLS/GCS) with simple GRANT command.",
    "fullDescription": "Unity Catalog is a unified governance solution for all data and AI assets including files, tables, machine learning models and dashboards in your lakehouse on any cloud.<br/>In this demo, we'll show how Unity Catalog can be used to secure External Locations. <br/>External locations are cloud blob storages (S3,GCS,ADLS) that required to be accessed in a secured fashion.<br/>Unity catalog let you create CREDENTIAL objects to secure such access. You can then define who has access to the EXTERNAL LOCATION leveraging these CREDENTIALS.<br/>Once setup, your Analyst will be able to process and analyze any files stored in cloud storages.",
    "usecase": "Data Governance",
  "products": ["Unity Catalog"],
  "related_links": [
      {"title": "View all Product demos", "url": "<TBD: LINK TO A FILTER WITH ALL DBDEMOS CONTENT>"},
      {"title": "Databricks Unity Catalog: Fine grained Governance", "url": "https://www.databricks.com/blog/2021/05/26/introducing-databricks-unity-catalog-fine-grained-governance-for-data-and-ai-on-the-lakehouse.html"}],
  "recommended_items": ["uc-01-acl", "uc-03-data-lineage", "uc-04-audit-log"],
  "demo_assets": [],
  "bundle": True,
  "tags": [{"uc": "Unity Catalog"}],
  "notebooks": [
    {
      "path": "AWS-Securing-data-on-external-locations", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True, 
      "title":  "External location on AWS", 
      "description": "Secure file & table access with External location.",
      "parameters": {"external_bucket_url": "s3a://databricks-e2demofieldengwest"}
    },
    {
      "path": "Azure-Securing-data-on-external-locations", 
      "pre_run": False, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True, 
      "title":  "External location on Azure", 
      "description": "Secure file & table access with External location."
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
