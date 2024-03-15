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
  "title": "Feature Store and Online Inference",
  "description": "Leverage Databricks Feature Store with streaming and online store.",
  "fullDescription": "Databricks Feature Store provide centralized repository that enables data scientists to find and share features and also ensures that the same code used to compute the feature values is used for model training and inference.<br/><br/><br/>Databricks Feature store solves the complexity of handling both big dataset at scale for training and small data for realtime inference, accelerating your Data Science team with best practices.<br/><br/>In this demo, we will cover the full Feature Store capabilities in a set of 3 notebooks. Each notebook will introduce new capabilities.<br/><br/><ul><li>Feature store lookup tables</li><li>Leverage Databricks Automl to programatically build a model</li><li>Use point in time lookups to prevent from data leackage</li><li>Add Streaming table to refresh your features in realtime</li><li>Deploy Online store for real time inference</li><li>Deploy our model as s serverless Serving Endpoint</li></ul>",
  "usecase": "Data Science & AI",
  "products": ["Feature Store","MLFLow", "Auto ML"],
  "demo_assets": [],
  "bundle": True,
  "tags": [{"ds": "Data Science"}],
  "notebooks": [
    {
      "path": "_resources/00-init-basic",
      "pre_run": False,
      "publish_on_website": False,
      "add_cluster_setup_cell": False,
      "title":  "Setup",
      "description": "Init data for demo."
    },
    {
      "path": "_resources/00-init-expert",
      "pre_run": False,
      "publish_on_website": False,
      "add_cluster_setup_cell": False,
      "title":  "Setup",
      "description": "Init data for expert demo."
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
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True, 
      "title":  "Feature Store - advanced", 
      "description": "Point in time & automl"
    },
    {
      "path": "03_Feature_store_expert", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True, 
      "title":  "Feature Store - full", 
      "description": "Streaming, online backed, model serving"
    }
  ],
  "cluster": {
    "num_workers": 0,
    "spark_conf": {
        "spark.master": "local[*, 4]"
    },
    "spark_version": "14.3.x-cpu-ml-scala2.12",
    "single_user_name": "{{CURRENT_USER}}",
    "data_security_mode": "SINGLE_USER"
  }  
}
