# Databricks notebook source
# MAGIC %md 
# MAGIC ## Demo bundle configuration
# MAGIC Please ignore / do not delete, only used to prep and bundle the demo

# COMMAND ----------

{
  "name": "llm-rag-chatbot",
  "category": "data-science",
  "title": "LLM Chatbot With Retrieval Augmented Generation (RAG) and MosaicML",
  "description": "Deploy your Chatbot on the Lakehouse AI with RAG, Mosaic ML, Vector Search & AI gateway",
  "fullDescription": "In this tutorial, you’ll discover how the Lakehouse AI accelerates your LLM use case.<br/>We will cover how Databricks is uniquely positioned to help you build your own chatbot and deploy a real-time chatbot using Databricks serverless capabilities.<br/>Retrieval Augmented Generation (RAG) is a powerful technique where we enrich the LLM prompt with additional context specific to your domain so that the model can provide better answers. <br/>This technique provides excellent results using public models without having to deploy and fine-tune your own LLMs.<br/>You will learn how to:<br/><ul><li>Prepare clean documents to build your internal knowledge base and specialize your chatbot</li><li>Leverage Databricks Vector Search with AI Gateway to create and store document embeddings</li><li>Search similar documents from our knowledge database with Vector Search</li><li>Deploy a real-time model using RAG and providing the context to a hosted LLM through the AI Gateway</li></ul>",
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
      "path": "_resources/01-Data-Preparation-full",
      "pre_run": False,
      "publish_on_website": False,
      "add_cluster_setup_cell": False,
      "title":  "Setup",
      "description": "full dataset init for fine tuning."
    },
    {
      "path": "00-RAG-chatbot-Introduction", 
      "pre_run": False, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": False, 
      "title":  "RAG chatbot Intro with Mosaic", 
      "description": "Introduction to Lakehouse AI and GenAI"
    },
    {
      "path": "01-Data-Preparation", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True, 
      "title":  "Data preparation for chatbot", 
      "description": "build your knowledge database and prepare doc chunks"
    },
    {
      "path": "02-Creating-Vector-Index", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True, 
      "title":  "Create your Vector Search index", 
      "description": "Deploy an embedding endpoint and synchronize your index"
    },
    {
      "path": "03-Deploy-RAG-Chatbot-Model", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True, 
      "title":  "Build your RAG chatbot service", 
      "description": "Deploy an AI gateway for MosaicML endpoint and perform RAG to answer user queries."
    }
  ],
  "cluster": {
    "autoscale": {
        "min_workers": 1,
        "max_workers": 1
    },
    "spark_version": "13.3.x-cpu-ml-scala2.12",
    "single_user_name": "{{CURRENT_USER}}",
    "data_security_mode": "SINGLE_USER"
  }
}
