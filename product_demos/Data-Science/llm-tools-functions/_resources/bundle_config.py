# Databricks notebook source
# MAGIC %md 
# MAGIC ## Demo bundle configuration
# MAGIC Please ignore / do not delete, only used to prep and bundle the demo

# COMMAND ----------

{
  "name": "llm-tools-functions",
  "category": "data-science",
  "serverless_supported": True,
  "custom_schema_supported": True,
  "default_catalog": "main",
  "default_schema": "dbdemos_ai_function_calling",
  "title": "Compound AI System and Agent Tools",
  "description": "Build and deploy tools to support your AI Systems with Unity Catalog",
  "fullDescription": "In this tutorial, you’ll discover how Databricks AI accelerates your LLM use case.<br/>We will cover how Databricks is uniquely positioned to help you build your own chatbot and deploy a real-time chatbot using Databricks serverless capabilities.<br/>Retrieval Augmented Generation (RAG) is a powerful technique where we enrich the LLM prompt with additional context specific to your domain so that the model can provide better answers. <br/>This technique provides excellent results using public models without having to deploy and fine-tune your own LLMs.<br/>You will learn how to:<br/><ul><li>Prepare clean documents to build your internal knowledge base and specialize your chatbot</li><li>Leverage Databricks Vector Search with Foundation Models to create and store document embeddings</li><li>Search similar documents from our knowledge database with Vector Search</li><li>Deploy a real-time model using RAG and providing the context to a hosted LLM through the Foundation Models</li></ul>",
  "usecase": "Data Science & AI",
  "products": ["LLM", "Vector Search", "AI"],
  "related_links": [
      {"title": "View all Product demos", "url": "<TBD: LINK TO A FILTER WITH ALL DBDEMOS CONTENT>"}, 
      {"title": "Free Dolly", "url": "https://www.databricks.com/blog/2023/04/12/dolly-first-open-commercially-viable-instruction-tuned-llm"}],
  "recommended_items": ["sql-ai-functions", "feature-store", "mlops-end2end"],
  "demo_assets": [],
  "bundle": True,
  "tags": [{"ds": "Data Science"}],
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
      "path": "_resources/00-init-cookie",
      "pre_run": False,
      "publish_on_website": False,
      "add_cluster_setup_cell": False,
      "title":  "Setup",
      "description": "Init data for cookie demo."
    },
    {
      "path": "_resources/00-init-stylist",
      "pre_run": False,
      "publish_on_website": False,
      "add_cluster_setup_cell": False,
      "title":  "Setup",
      "description": "Init data for stylist demo."
    },
    {
      "path": "config",
      "pre_run": False,
      "publish_on_website": True,
      "add_cluster_setup_cell": False,
      "title":  "Config",
      "description": "Setup schema and database."
    },
    {
      "path": "00-stylist-AI-function-tools-introduction",
      "pre_run": True,
      "publish_on_website": True,
      "add_cluster_setup_cell": True,
      "title":  "AI Function tools",
      "description": "Build your first AI functions & agent system."
    },
    {
      "path": "01-agent-cookie-demo",
      "pre_run": True,
      "publish_on_website": True,
      "add_cluster_setup_cell": True,
      "title":  "AI Function tools",
      "description": "Generate instagram cookie campaign based on your data."
    }
  ],
  "cluster": {
    "num_workers": 0,
    "spark_conf": {
        "spark.master": "local[*, 4]"
    },
    "spark_version": "15.4.x-scala2.12",
    "single_user_name": "{{CURRENT_USER}}",
    "data_security_mode": "SINGLE_USER"
  }  
}
