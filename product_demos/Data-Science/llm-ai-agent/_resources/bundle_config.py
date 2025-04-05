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
      "path": "_resources/00-init-advanced",
      "pre_run": False,
      "publish_on_website": False,
      "add_cluster_setup_cell": False,
      "title":  "Setup",
      "description": "setup for the advanced demo (pdf + ocr setup)."
    },
    {
      "path": "00-RAG-LLM-RAG-Introduction",
      "pre_run": False,
      "publish_on_website": True,
      "add_cluster_setup_cell": False,
      "title":  "Setup",
      "description": "Introduction notebook."
    },
    {
      "path": "01-first-step/01-First-Step-RAG-On-Databricks", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": False, 
      "title":  "First Steps: RAG on Databricks", 
      "description": "Quickstart: deploy your RAG in 10 min. Start here!"
    },
    {
      "path": "01-first-step/chain", 
      "pre_run": False, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": False, 
      "title":  "Langchain chain", 
      "description": "Your full chain used to build your chatbot."
    },
    {
      "path": "02-simple-app/01-Data-Preparation-and-Index", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": False, 
      "title":  "Data preparation for chatbot", 
      "description": "Prepare doc chunks and build your Vector Search Index"
    },
    {
      "path": "02-simple-app/02-Deploy-RAG-Chatbot-Model", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": False, 
      "title":  "Build your RAG chatbot service", 
      "description": "Leverage Foundation Model to perform RAG and answer customer questions."
    },
    {
      "path": "02-simple-app/03-Deploy-Frontend-Lakehouse-App", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": False, 
      "title":  "Deploy your chatbot frontend app", 
      "description": "Leverage Lakehouse App to deploy your front app."
    },
    {
      "path": "03-advanced-app/01-PDF-Advanced-Data-Preparation", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": False, 
      "depends_on_previous": False,
      "title":  "Create PDF chunks and vector search index", 
      "description": "Ingestion unstructured data and create a self-managed vector search index"
    },
    {
      "path": "03-advanced-app/02-Advanced-Chatbot-Chain", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": False, 
      "title":  "Build your langchain bot", 
      "description": "Advanced langchain chain, working with chat history."
    },
    {
      "path": "03-advanced-app/03-Offline-Evaluation", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": False, 
      "title":  "Offline evaluation with LLM as a Judge", 
      "description": "Evaluate your chatbot with an offline dataset."
    },
    {
      "path": "03-advanced-app/04-Online-Evaluation", 
      "pre_run": False, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": False, 
      "title":  "Deploy your endpoint with Inference tables", 
      "description": "Log your endpoint payload as a Delta table."
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
