# Databricks notebook source
# MAGIC %md 
# MAGIC ## Demo bundle configuration
# MAGIC Please ignore / do not delete, only used to prep and bundle the demo

# COMMAND ----------

{
  "name": "feature-store",
  "custom_schema_supported": True,
  "default_catalog": "main",
  "default_schema": "dbdemos_fs_travel",
  "category": "data-science",
  "serverless_supported": True,
  "title": "Feature Store and Online Inference",
  "description": "Leverage Databricks Feature Store with streaming and online store.",
  "bundle": True,
  "notebooks": [
    {
      "path": "_resources/00-init-basic-new",
      "pre_run": False,
      "publish_on_website": False,
      "add_cluster_setup_cell": False,
      "title":  "Setup",
      "description": "Init data for demo."
    },
    {
      "path": "01_Feature_store_introduction", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True, 
      "title":  "Feature Store - intro", 
      "description": "Introduction to Databricks FS"
    },
    {
      "path": "02_Feature_store_advanced", 
      "pre_run": False, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True, 
      "title":  "Feature Store - advanced", 
      "description": "Point in time & online feature store"
    },
    {
      "path": "03_Feature_store_pipeline", 
      "pre_run": False, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True, 
      "title":  "Feature Store - full", 
      "description": "Feature Store creation in pipeline"
    }
  ],
  "cluster": {
    "num_workers": 0,
    "spark_conf": {
        "spark.master": "local[*, 4]"
    },
    "spark_version": "16.4.x-cpu-ml-scala2.12",
    "single_user_name": "{{CURRENT_USER}}",
    "data_security_mode": "SINGLE_USER"
  }  
}
