# Databricks notebook source
# MAGIC %md 
# MAGIC ## Demo bundle configuration
# MAGIC Please ignore / do not delete, only used to prep and bundle the demo

# COMMAND ----------

{
  "name": "lakehouse-monitoring",
  "category": "DBSQL",
  "serverless_supported": True,
  "custom_schema_supported": True,
  "default_catalog": "main",
  "default_schema": "dbdemos_lhm",
  "serverless_supported": True,
  "title": "Lakehouse Monitoring for Retail Transactions",
  "description": "Monitor your data quality with Lakehouse Monitoring",
  "fullDescription": "Databricks Lakehouse Monitoring allows you to monitor all your data pipelines and ML models – without additional tools and complexity. Integrated into Unity Catalog, teams can track quality alongside governance, building towards the self-serve data platform dream. By continuously assessing the profile of your data, Lakehouse Monitoring allows you to stay ahead of potential issues, ensuring that pipelines run smoothly and ML models remain effective over time.",
  "bundle": True,
  "tags": [{"dlt": "Spark Declarative Pipelines"},  {"ds": "Data Science"}, {"dbsql": "BI/DW/DBSQL"}],
  "notebooks": [
    {
      "path": "_resources/01-DataGeneration", 
      "pre_run": False, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": False,
      "title":  "Prep data", 
      "description": "Data generation."
    },
    {
      "path": "01-Timeseries-monitor", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": False,
      "title":  "Create your first monitor", 
      "description": "Discover Lakehouse Montoring."
    },
    {
      "path": "02-Inference-monitor", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": False,
      "title":  "Monitor your Inference table", 
      "description": "Leverage Lakehouse Montoring to track your Model performance."
    },
    {
      "path": "config", 
      "pre_run": False, 
      "publish_on_website": False, 
      "add_cluster_setup_cell": False,
      "title":  "Configuration", 
      "description": "Setup."
    }
  ],
  "init_job": {
  },
  "cluster": {
        "autoscale": {
            "min_workers": 1,
            "max_workers": 1
        },
        "spark_version": "16.4.x-cpu-ml-scala2.12",
        "single_user_name": "{{CURRENT_USER}}",
        "data_security_mode": "SINGLE_USER",
        "runtime_engine": "STANDARD"
  }, 
  "pipelines": [
  ],
  "workflows": []  

}
