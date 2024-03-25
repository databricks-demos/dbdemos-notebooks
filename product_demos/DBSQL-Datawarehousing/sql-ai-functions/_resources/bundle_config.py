# Databricks notebook source
# MAGIC %md 
# MAGIC ## Demo bundle configuration
# MAGIC Please ignore / do not delete, only used to prep and bundle the demo

# COMMAND ----------

{
  "name": "sql-ai-functions",
  "custom_schema_supported": True,
  "default_catalog": "main",
  "default_schema": "dbdemos_ai_query",
  "category": "DBSQL",
  "title": "AI Functions: query LLM with DBSQL",
  "description": "Call Azure OpenAI's model from your Lakehouse data using AI_GENERATE_TEXT()",
  "fullDescription": "This walkthrough shows how to use Azure OpenAI's GPT models with Databricks SQL AI Functions to process unstructured data, identify topics, sentiment, and generate responses. <br/><br/>AI Functions simplify deriving meaning from unstructured data and make it easy for analysts to interact with LLMs using SQL<br/><br/>In this demo, we'll show how to automatically classify and answer customer reviews, asking OpenAI's LLM to detect negative reviews and preparing an answer. <br/><br/>We'll also explore how LLM can be used to generate fake data.",
  "usecase": "Data Warehousing & BI",
  "products": ["Databricks SQL", "AI Functions", "AI_GENERATE_TEXT()"],
  "related_links": [
      {"title": "View all Product demos", "url": "<TBD: LINK TO A FILTER WITH ALL DBDEMOS CONTENT>"}, 
      {"title": "Databricks AI functions", "url": "https://www.databricks.com/blog/2023/04/18/introducing-ai-functions-integrating-large-language-models-databricks-sql.html"}],
  "recommended_items": ["llm-dolly-chatbot", "identity-pk-fk", "feature-store"],
  "demo_assets": [],
  "bundle": True,
  "tags": [{"dbsql": "BI/DW/DBSQL"}, {"ds": "Data Science"}],
  "notebooks": [
    {
      "path": "00-SQL-AI-Functions-Introduction", 
      "pre_run": False,
      "publish_on_website": True,
      "add_cluster_setup_cell": False,
      "title":  "Introduction to AI Functions", 
      "description": "Introduction to SQL AI Functions, start here."
    },
    {
      "path": "01-Builtin-SQL-AI-Functions", 
      "pre_run": True,
      "publish_on_website": True,
      "add_cluster_setup_cell": False,
      "title":  "Discover Databricks AI function", 
      "description": "Builtin AI functions to solve all common tasks.",
      "warehouse_id": "9be8079d873b30a6"
    },
    {
      "path": "02-Generate-fake-data-with-AI-functions", 
      "pre_run": True,
      "publish_on_website": True,
      "add_cluster_setup_cell": False,
      "title":  "Discover AI QUERY function", 
      "description": "Call custom Model Serving Endpoint within SQL and generate fake dataset.",
      "warehouse_id": "9be8079d873b30a6"
    },
    {
      "path": "03-automated-product-review-and-answer", 
      "pre_run": True,
      "publish_on_website": True,
      "add_cluster_setup_cell": False,
      "title":  "Leverage AI QUERY in a complete pipeline.", 
      "description": "Implement a flow to analyse and answer customer reviews.",
      "warehouse_id": "9be8079d873b30a6"
    },
    {
      "path": "04-Extra-setup-external-model-OpenAI", 
      "pre_run": False,
      "publish_on_website": True,
      "add_cluster_setup_cell": False,
      "title":  "Analyze and answer customer reviews", 
      "description": "Build a pipeline to ingest customer reviews, detect the negative one and generate answers.",
      "warehouse_id": "9be8079d873b30a6"
    }
  ],
  "create_cluster": False
}
