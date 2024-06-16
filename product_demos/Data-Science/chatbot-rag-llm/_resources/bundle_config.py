# Databricks notebook source
# MAGIC %md 
# MAGIC ## Demo bundle configuration
# MAGIC Please ignore / do not delete, only used to prep and bundle the demo

# COMMAND ----------

{
  "name": "llm-rag-chatbot",
  "category": "data-science",
  "custom_schema_supported": True,
  "default_schema": "rag_chatbot",
  "default_catalog": "main",
  "title": "LLM Chatbot With Retrieval Augmented Generation (RAG) and DBRX",
  "description": "Deploy your Chatbot on Databricks AI with RAG, DBRX, Vector Search & Mosaic AI Agent Evaluation",
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
      "path": "_resources/00-init-advanced",
      "pre_run": False,
      "publish_on_website": False,
      "add_cluster_setup_cell": False,
      "title":  "Setup",
      "description": "setup for the advanced demo (pdf + ocr setup)."
    },
    {
      "path": "_resources/02-lakehouse-app-helpers",
      "pre_run": False,
      "publish_on_website": False,
      "add_cluster_setup_cell": False,
      "title":  "Lakehouse app Setup",
      "description": "Helper for lakehouse apps."
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
      "pre_run": False, 
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
      "pre_run": False, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": False, 
      "title":  "Data preparation for chatbot", 
      "description": "Prepare doc chunks and build your Vector Search Index"
    },
    {
      "path": "02-simple-app/02-Deploy-RAG-Chatbot-Model", 
      "pre_run": False, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": False, 
      "title":  "Build your RAG chatbot service", 
      "description": "Leverage Foundation Model to perform RAG and answer customer questions."
    },
    {
      "path": "02-simple-app/03-Deploy-Frontend-Lakehouse-App", 
      "pre_run": False, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": False, 
      "title":  "Deploy your chatbot frontend app", 
      "description": "Leverage Lakehouse App to deploy your front app."
    },
    {
      "path": "03-advanced-app/01-PDF-Advanced-Data-Preparation", 
      "pre_run": False, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": False, 
      "title":  "Create PDF chunks and vector search index", 
      "description": "Ingestion unstructured data and create a self-managed vector search index"
    },
    {
      "path": "03-advanced-app/02-Advanced-Chatbot-Chain", 
      "pre_run": False, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": False, 
      "title":  "Build your langchain bot", 
      "description": "Advanced langchain chain, working with chat history."
    },
    {
      "path": "03-advanced-app/03-Offline-Evaluation", 
      "pre_run": False, 
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
