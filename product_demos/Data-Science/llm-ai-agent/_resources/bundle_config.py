# Databricks notebook source
# MAGIC %md 
# MAGIC ## Demo bundle configuration
# MAGIC Please ignore / do not delete, only used to prep and bundle the demo

# COMMAND ----------

{
  "name": "llm-ai-agent",
  "category": "data-science",
  "custom_schema_supported": True,
  "default_catalog": "main",
  "default_schema": "dbdemos_ai_agent",
  "serverless_supported": True,
  "title": "GenAI Agent with Databricks",
  "description": "Deploy your Agents on Databricks Mosaic AI and perform evaluation",
  "bundle": True,
  "notebooks": [
    {
      "path": "_resources/00-init",
      "pre_run": False,
      "publish_on_website": False,
      "add_cluster_setup_cell": False,
      "title":  "Setup",
      "description": "Init data for demo."
    },
    {
      "path": "_resources/01-data-generation",
      "pre_run": False,
      "publish_on_website": False,
      "add_cluster_setup_cell": False,
      "title":  "Setup",
      "description": "Generate dataset."
    },
    {
      "path": "01-create-tools/01-first-tools",
      "pre_run": False,
      "publish_on_website": True,
      "add_cluster_setup_cell": False,
      "title":  "Setup",
      "description": "Introduction notebook."
    },
    {
      "path": "02-evaluate-and-deploy/agent", 
      "pre_run": False, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": False, 
      "title":  "First Steps: RAG on Databricks", 
      "description": "Quickstart: deploy your RAG in 10 min. Start here!"
    },
    {
      "path": "05-full-application/requirements.txt", 
      "pre_run": False, 
      "publish_on_website": False, 
      "add_cluster_setup_cell": False, 
      "title":  "App requirement", 
      "description": "App dependencies"
    },
    {
      "path": "05-full-application/05.1-deploy-app", 
      "pre_run": False, 
      "publish_on_website": False, 
      "add_cluster_setup_cell": False, 
      "title":  "Deploy your Databricks Application", 
      "description": "Run this notebook to deploy the chatbot application"
    },
    {
      "path": "05-full-application/app.yaml", 
      "pre_run": False, 
      "publish_on_website": False, 
      "add_cluster_setup_cell": False, 
      "title":  "App conf", 
      "description": "Setup"
    },
    {
      "path": "05-full-application/static", 
      "pre_run": False, 
      "publish_on_website": False, 
      "add_cluster_setup_cell": False, 
      "title":  "Frontend static", 
      "description": "Static resouces compiled by npm"
    },
    {
      "path": "05-full-application/app", 
      "pre_run": False, 
      "publish_on_website": False, 
      "add_cluster_setup_cell": False, 
      "title":  "Application backend", 
      "description": "Python Application server backend"
    },
    {
      "path": "config", 
      "pre_run": False, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": False, 
      "title":  "Configuration file (define your endpoint and schema name)", 
      "description": "Setup your database and model endpoint."
    }
  ],
  "cluster": {
    "num_workers": 0,
    "spark_conf": {
        "spark.master": "local[*, 4]"
    },
    "spark_version": "14.3.x-scala2.12",
    "single_user_name": "{{CURRENT_USER}}",
    "data_security_mode": "SINGLE_USER"
  }  
}
