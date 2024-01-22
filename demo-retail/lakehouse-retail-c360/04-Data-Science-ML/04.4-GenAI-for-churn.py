# Databricks notebook source
# MAGIC %md
# MAGIC # Leveraging GenAI to reduce churn with the Data Intelligence Platform
# MAGIC
# MAGIC Now that we can target which customer is the most likely to churn, we can leverage Databricks GenAI capabilities to prevent churn and provide ultra-personalized interactions.
# MAGIC
# MAGIC
# MAGIC GenAI applications could let you:
# MAGIC
# MAGIC - Analyze previous customer interaction to better understand their issue
# MAGIC - Improve your model score to better target customers at risk of churn by extracting sentiments or entities from previous discussion
# MAGIC - Build hyper personalized chatbot, adding the customer experience details when asking a question to improve their experience (ex: which item did you buy previously)
# MAGIC - ...
# MAGIC
# MAGIC All of these use-cases are made easy with the Data Intelligence Platform
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection or disable tracker during installation. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=lakehouse&notebook=04.4-GenAI-for-churn&demo_name=lakehouse-retail-c360&event=VIEW">

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## Customize answers or extract informations from reviews/discussion with Databricks AI Functions
# MAGIC
# MAGIC Databricks AI Functions let you query your AI Endpoints in plain SQL. This makes it super easy to create function with custom prompt to extract information from any text or provide answer with specific content.
# MAGIC
# MAGIC For more details on AI Functions, run `dbdemos.install('sql-ai-functions')`

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## Build your GenAI applications leveraging Databricks Intelligence Data Platform and Foundation LLMs (llama2, Mistral...)
# MAGIC
# MAGIC Databricks provides builtin GenAI capabilities, making it easy to build and deploy your own GenAI application, based on state of the art foundation models. 
# MAGIC
# MAGIC You can easily deploy your own chatbot with Retrieval Augmented Generation (RAG), providing personalized context for your customers at a lower cost.
# MAGIC
# MAGIC For more details on Databricks LLM and RAG applications,  run `dbdemos.install('llm-rag-chatbot')`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next: Orchestrate your pipeline to deploy production-grade data apps
# MAGIC
# MAGIC Open the [Workflows notebook]($../05-Workflow-orchestration/05-Workflow-orchestration-churn) to discover how Databricks can orchestrate all these tasks, or [go back to the introduction]($../00-churn-introduction-lakehouse)
