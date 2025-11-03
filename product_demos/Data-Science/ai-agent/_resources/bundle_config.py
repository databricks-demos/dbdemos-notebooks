# Databricks notebook source
# MAGIC %md 
# MAGIC ## Demo bundle configuration
# MAGIC Please ignore / do not delete, only used to prep and bundle the demo

# COMMAND ----------

{
  "name": "ai-agent",
  "category": "data-science",
  "custom_schema_supported": True,
  "default_catalog": "main",
  "default_schema": "dbdemos_ai_agent",
  "serverless_supported": True,
  "title": "AI Agent System and Evaluation with Databricks AI",
  "description": "Deploy your AI Agent system on Databricks AI with foundation LLM, Langchain, PDF extraction and Vector Search & Mosaic AI Agent Evaluation",
  "bundle": True,
  "notebooks": [
    {
      "path": "_resources/01-setup",
      "pre_run": False,
      "publish_on_website": False,
      "add_cluster_setup_cell": False,
      "title":  "Setup",
      "description": "Demo setup"
    },
    {
      "path": "_resources/02-data-generation",
      "pre_run": False,
      "publish_on_website": False,
      "add_cluster_setup_cell": False,
      "title":  "Data Generation",
      "description": "Generate data"
    },
    {
      "path": "_resources/03-doc-pdf-documentation",
      "pre_run": False,
      "publish_on_website": False,
      "add_cluster_setup_cell": False,
      "title":  "Generate pdf",
      "description": "Generate pdf for demo"
    },
    {
      "path": "_resources/04-eval-dataset-generation",
      "pre_run": False,
      "publish_on_website": False,
      "add_cluster_setup_cell": False,
      "title":  "Eval dataset gen",
      "description": "Generate evaluation dataset"
    },
    {
      "path": "01-create-tools/01_create_first_billing_agent",
      "pre_run": True,
      "publish_on_website": True,
      "add_cluster_setup_cell": False,
      "title":  "Create your first Agent",
      "description": "Deploy your first agent using UC functions."
    },
    {
      "path": "02-agent-eval/02.1_agent_evaluation",
      "pre_run": True,
      "publish_on_website": True,
      "add_cluster_setup_cell": False,
      "title":  "Agent Evaluation",
      "description": "Evaluate our agent with MLFlow."
    },
    {
      "path": "02-agent-eval/agent.py",
      "pre_run": False,
      "publish_on_website": True,
      "add_cluster_setup_cell": False,
      "title":  "Your Langchain Agent",
      "description": "Custom MLFlow flavor model for our langchain agent."
    },
    {
      "path": "03-knowledge-base-rag/03.1-pdf-rag-tool",
      "pre_run": True,
      "publish_on_website": True,
      "add_cluster_setup_cell": False,
      "title":  "Build a PDF knowledge base",
      "description": "Add a RAG to your agent using Vector Search."
    },
    {
      "path": "04-deploy-app/04-Deploy-Frontend-Lakehouse-App",
      "pre_run": True,
      "publish_on_website": True,
      "add_cluster_setup_cell": False,
      "title":  "Deploy your app",
      "description": "Create the chatbot frontend and capture feedback."
    },
    {
      "path": "04-deploy-app/chatbot_app",
      "pre_run": False,
      "publish_on_website": False,
      "add_cluster_setup_cell": False,
      "title":  "Chatbot app",
      "description": "Gradio application folder."
    },
    {
      "path": "05-production-monitoring/05.production-monitoring",
      "pre_run": True,
      "publish_on_website": True,
      "add_cluster_setup_cell": False,
      "title":  "Monitor your application",
      "description": "Leverage Databricks monitoring to evaluate your agent live."
    },
    {
      "path": "06-improving-business-kpis/06-business-dashboard",
      "pre_run": False,
      "publish_on_website": True,
      "add_cluster_setup_cell": False,
      "title":  "Track your agent business impact",
      "description": "Use Databricks AI/BI with Genie to measure your AI App impact on your business."
    },
    {
      "path": "07-adding-mcp/07-agent_mcp",
      "pre_run": True,
      "publish_on_website": True,
      "add_cluster_setup_cell": False,
      "title":  "Add MCP to your agents",
      "description": "Use Databricks managed MCP to add more capabilities to your agent."
    },
    {
      "path": "01-ai-agent-introduction",
      "pre_run": False,
      "publish_on_website": True,
      "add_cluster_setup_cell": False,
      "title":  "Introduction",
      "description": "Start here."
    },
    {
      "path": "config",
      "pre_run": False,
      "publish_on_website": True,
      "add_cluster_setup_cell": False,
      "title":  "Config",
      "description": "Configuration file"
    }
  ],
  "cluster": {
    "num_workers": 0,
    "spark_conf": {
        "spark.master": "local[*, 4]"
    },
    "single_user_name": "{{CURRENT_USER}}",
    "data_security_mode": "SINGLE_USER"
  }  
}
