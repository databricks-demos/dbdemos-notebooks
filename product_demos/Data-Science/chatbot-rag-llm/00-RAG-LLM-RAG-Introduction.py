# Databricks notebook source
# MAGIC %md
# MAGIC # Deploy Your LLM Chatbots With the Data Intelligence Platform and DBRX Instruct
# MAGIC
# MAGIC In this tutorial, you will learn how to build your own Chatbot Assisstant to help your customers answer questions about Databricks, using Retrieval Augmented Generation (RAG), Databricks State of The Art LLM DBRX Instruct Foundation Model and Vector Search.
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection or disable tracker during installation. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=data-science&notebook=00-RAG-LLM-RAG-Introduction&demo_name=chatbot-rag-llm&event=VIEW">

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Quickstart: Getting started
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/chatbot-rag/llm-rag-self-managed-flow-0.png?raw=true" style="float: right"  width="700px;">
# MAGIC Start here if this is your first time implementing a GenAI application leveraging Databricks DBRX, our State Of the Art LLM, open and Available as a model serving endpoint.
# MAGIC
# MAGIC You will learn:
# MAGIC
# MAGIC - How to prepare your document dataset, creating text chunk from documentation pages
# MAGIC - Create your Vector Search index and send queries to find similar documents
# MAGIC - Build your langchain model leveraging Databricks Foundation Model (DBRX Instruct)
# MAGIC - Deploy the chatbot model as Model Serving Endpoint 

# COMMAND ----------

# MAGIC %md 
# MAGIC Get started: open the [01-quickstart/00-RAG-chatbot-Introduction notebook]($./01-quickstart/00-RAG-chatbot-Introduction).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Advanced: Going further
# MAGIC
# MAGIC Explore this content to discover how to leverage all the Databricks Data Intelligence Platform capabilities for your GenAI Apps.
# MAGIC
# MAGIC You will learn:
# MAGIC
# MAGIC - How to extract information from unstructured documents (pdfs) and create custom chunks
# MAGIC - Leverage Databricks Embedding Foundation Model to compute the chunks embeddings
# MAGIC - Create a Self Managed Vector Search index and send queries to find similar documents
# MAGIC - Build an advanecd langchain model leveraging Databricks Foundation Model (DBRX Instruct)
# MAGIC - Evaluate your model chatbot model correctness with MLflow
# MAGIC - Deploy your Model Serving Endpoint with Table Inferences to automatically log your model traffic
# MAGIC - Run online llm evaluation and track your metrics with Databricks Monitoring

# COMMAND ----------

# MAGIC %md 
# MAGIC Learn more adavanced GenAI concepts: [open the 02-advanced/01-PDF-Advanced-Data-Preparation]($./02-advanced/01-PDF-Advanced-Data-Preparation).
