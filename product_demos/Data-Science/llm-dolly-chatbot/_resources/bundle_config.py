# Databricks notebook source
# MAGIC %md 
# MAGIC ## Demo bundle configuration
# MAGIC Please ignore / do not delete, only used to prep and bundle the demo

# COMMAND ----------

{
  "name": "llm-dolly-chatbot",
  "category": "data-science",
  "title": "Build your Chat Bot with Dolly",
  "description": "Democratizing the magic of ChatGPT with open models and Databricks Lakehouse (starts GPU)",
  "fullDescription": "Large Language Models produce some amazing results, chatting and answering questions with seeming intelligence.<br/>But how can you get LLMs to answer questions about your specific datasets? Imagine answering questions based on your company's knowledge base, docs or Slack chats.<br/>The good news is that this is easy to build on Databricks, leveraging open-source tooling and open LLMs.<br/>Databricks released Dolly, Dolly the first truly open LLM. Because Dolly was fine tuned using databricks-dolly-15k (15,000 high-quality human-generated prompt / response pairs specifically designed for instruction tuning large language models), it can be used as starting point to create your own commercial model.<br/>In this demo, we'll show you how to leverage Dolly to build your own chat bot:<br/><br/><ul><li>Data ingestion & preparation</li><li>Vector database for similarity search</li><li>Prompt engineering using langchain and hugging face transformers</li><li>Q&A bot to answer our customers</li><li>More advance bot with memory to chain answers</li></ul><br/><br/>Note: this cluster will start a 64GB RAM GPU",
  "usecase": "Data Science & AI",
  "products": ["LLM", "Dolly", "AI"],
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
      "path": "01-Dolly-Introduction", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": False, 
      "title":  "Dolly Intro", 
      "description": "Introduction to Databricks Dolly"
    },
    {
      "path": "02-Data-preparation", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True, 
      "title":  "Data preparation for chatbot", 
      "description": "Ingest data and save them as vector",
      "libraries": [{"maven": {"coordinates": "com.databricks:spark-xml_2.12:0.16.0"}}]
    },
    {
      "path": "03-Q&A-prompt-engineering-for-dolly", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True, 
      "title":  "Prompt engineering for Q&A bot", 
      "description": "Build your first bot with langchain and dolly"
    },
    {
      "path": "04-chat-bot-prompt-engineering-dolly", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True, 
      "title":  "Adding memory to our bot", 
      "description": "Improve our bot to chain multiple answers keeping context"
    }
  ],
  "cluster": {
    "num_workers": 0,
    "spark_version": "13.0.x-gpu-ml-scala2.12",
    "spark_conf": {
        "spark.databricks.cluster.profile": "singleNode",
        "spark.master": "local[*, 4]"
    },
    "node_type_id": {"AWS": "g5.4xlarge", "AZURE": "Standard_NC8as_T4_v3", "GCP": "a2-highgpu-1g"},
    "driver_node_type_id": {"AWS": "g5.4xlarge", "AZURE": "Standard_NC8as_T4_v3", "GCP": "a2-highgpu-1g"},
    "custom_tags": {
        "ResourceClass": "SingleNode"
    }
  },
  "cluster_libraries": [{"maven": {"coordinates": "com.databricks:spark-xml_2.12:0.16.0"}}]
}
