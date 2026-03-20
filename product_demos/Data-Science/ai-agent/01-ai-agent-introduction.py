# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC # Build and Evaluate your AI Agent Systems with Databricks and MLFlow
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/ai-agent/agent-demo-0.png?raw=true" width="800px" style="float: right">
# MAGIC
# MAGIC Databricks makes it easy to create and register your own Agents and tools to manipulate your data and to connect to any external system (from the OpenAI Agent SDK and Claude Agents to MCP servers).
# MAGIC
# MAGIC But building your agent is only the first step toward deploying to production and driving business impact!
# MAGIC
# MAGIC In this demo, we will be building an agent for your customer support team. It'll be able to process your customer support request and provide guidance; ultimately **accelerating your time to resolution, increasing satisfaction, and reducing costs!**
# MAGIC
# MAGIC You will need the following steps, and Databricks makes it easier than ever:
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection or disable tracker during installation. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=data-science&org_id=1444828305810485&notebook=01-ai-agent-introduction&demo_name=ai-agent&event=VIEW">
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## 1/ Create your tools
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/ai-agent/agent-demo-1.png?raw=true" width="500px" style="float: right">
# MAGIC
# MAGIC Let's start by creating our first tools. We'll create a few functions to help your customer support team to find customer information, and register them in your catalog.schema. 
# MAGIC
# MAGIC Remember, this could be any functions!

# COMMAND ----------

# MAGIC %md
# MAGIC Open [01-create-tools/01_create_first_billing_agent]($./01-create-tools/01_create_first_billing_agent) to get started

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## 2/ Deploy and Evaluate your agent
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/ai-agent/mlflow-evaluate-0.png?raw=true?raw=true" width="500px" style="float: right">
# MAGIC
# MAGIC Now that our tools are ready, we'll create an Agent using LangChain and equip it with the tools we previously created!
# MAGIC
# MAGIC Once our agent is created, we'll start running a first Evaluation round against an existing evaluation dataset, and improve our prompt to make it better and have better evaluation metrics!

# COMMAND ----------

# MAGIC %md
# MAGIC Open [02-agent-eval/02.1_agent_evaluation]($./02-agent-eval/02.1_agent_evaluation) to deploy and evaluate your agent.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3/ Add a Knowledge base
# MAGIC
# MAGIC Our agent is working well, but cannot answer questions about our internal product and procedures.
# MAGIC
# MAGIC It's time to add a knowledge base, extract information from PDFs, and add Vector Search to enrich our context with custom information in our prompt!
# MAGIC
# MAGIC And of course, we'll close the loop by re-evaluating our agent, making sure we keep improving it.
# MAGIC
# MAGIC
# MAGIC Open [03-knowledge-base-rag/03.1-pdf-rag-tool]($./03-knowledge-base-rag/03.1-pdf-rag-tool) to add your Knowledge base.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4/ Deploy your agent using a Databricks Application, capturing Customer Feedback in MLflow
# MAGIC
# MAGIC Now that our agent is ready, we can deploy it using a Databricks Application.
# MAGIC
# MAGIC For our demo, we'll release a simple GradIO chatbot application to show you how to capture user feedback in MLflow and use it to improve your eval set and model performance over time!
# MAGIC
# MAGIC Open [04-deploy-app/04-Deploy-Frontend-Lakehouse-App]($./04-deploy-app/04-Deploy-Frontend-Lakehouse-App) to get started.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5/ Add live, production monitoring to your agent
# MAGIC
# MAGIC Our frontend application is now live! Our next step is to add a monitoring process to evaluate all incoming user requests and ensure they align with our evaluation metrics over time.
# MAGIC
# MAGIC Open [05-production-monitoring/05.production-monitoring]($./05-production-monitoring/05.production-monitoring) to add Production Monitoring to your agents!

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6/ Track how your agent improves your business metrics
# MAGIC
# MAGIC Our last step is to measure how your agent impacts your business!
# MAGIC
# MAGIC Thankfully, Databricks lets you manage your entire data stack. You can, for example, synchronize your CRM or Ticket support data and track how the CSAT score increases over time, benchmarking a team with and without an agent!
# MAGIC
# MAGIC
# MAGIC Open [06-improving-business-kpis/06-improving-business-kpis]($./06-improving-business-kpis/06-improving-business-kpis) to track your Busines KPI with a ready to use dashboard!

# COMMAND ----------

# MAGIC %md
# MAGIC Congratulations!
# MAGIC You're now ready to build and deploy your AI agent with Databricks and MLflow 3!
# MAGIC
# MAGIC ## What's next: Discover ready-to-use agents with Agent Bricks
# MAGIC
# MAGIC Many AI tasks are common, and Databricks provides out-of-the-box agents that you can use and stitch together:
# MAGIC
# MAGIC - **Information Extraction**: Extract key information, classify content, or summarize text from documents into a structured JSON.
# MAGIC - **Custom LLM**: Specialize an LLM endpoint to perform custom text tasks (e.g., content generation, chat) aligned within your domain-specific guidelines.
# MAGIC - **Multi-Agent Supervisor**: Design an AI system that brings yours agents together. You can even include Genie Spaces.
# MAGIC - **Knowledge Assistant**: Turn your docs into an expert AI chatbot.
# MAGIC - ...
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC Open [Databricks Agent Bricks](/ml/agents) and set up these agents in a few clicks!