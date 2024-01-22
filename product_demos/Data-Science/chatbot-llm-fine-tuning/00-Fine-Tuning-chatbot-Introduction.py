# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC # Scaling your business with a GenAI-Powered Assistant
# MAGIC
# MAGIC LLMs are disrupting the way we interact with information, from internal knowledge bases to external, customer-facing documentation or support.
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/chatbot-rag/moisaic-logo.png?raw=true" width="200px" style="float: right" />
# MAGIC
# MAGIC While ChatGPT democratized the use of LLM-based chatbot for retail, companies need to deploy personalized models that answer their needs:
# MAGIC
# MAGIC - Privacy requirements on sensitive information
# MAGIC - Preventing hallucination
# MAGIC - Specialized content, not available on the Internet
# MAGIC - Specific behavior for customer tasks
# MAGIC - Control over speed and cost
# MAGIC - Deploy models on private infrastructure for security reasons
# MAGIC - ...
# MAGIC
# MAGIC ## Introducing Databricks Lakehouse AI
# MAGIC
# MAGIC To solve these challenges, custom knowledge bases and models need to be deployed. However, doing so at scale isn't simple and requires:
# MAGIC
# MAGIC - Ingesting and transforming massive amounts of data 
# MAGIC - Ensuring privacy and security across your data pipeline
# MAGIC - Deploying systems such as Vector Search Index 
# MAGIC - Having access to GPUs and deploying efficient models
# MAGIC - Training and deploying custom models
# MAGIC
# MAGIC This is where the Databricks Lakehouse AI comes in. Databricks simplify all these steps so that you can focus on building your final model, with the best prompt and performances.
# MAGIC
# MAGIC
# MAGIC ## GenAI & Maturity curve
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/chatbot-rag/llm-rag-maturity.png?raw=true" width="600px" style="float:right"/>
# MAGIC
# MAGIC Deploying GenAI can be done in multiple ways:
# MAGIC
# MAGIC - **Prompt engineering on public APIs (e.g. LLama 2 + MosaicML)**: answer from public information, retail (think ChatGPT)
# MAGIC - **Retrieval Augmented Generation (RAG)**: specialize your model with additional content. *This is what we'll focus in this demo*
# MAGIC - **OSS model Fine tuning**: when you have a large corpus of custom data and need specific model behavior (execute a task)
# MAGIC - **Train your own LLM**: for full control on the underlying data sources of the model (biomedical, Code, Finance...)

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC ## What is Retrieval Augmented Generation (RAG) for LLMs?
# MAGIC
# MAGIC RAG is a powerful and efficient GenAI technique that allows you to improve model performance by leveraging your own data (e.g., documentation specific to your business), without the need to fine-tune the model.
# MAGIC
# MAGIC This is done by providing your custom information as context to the LLM. This reduces hallucination and allows the LLM to produce results that provide company-specific data, without making any changes to the original LLM.
# MAGIC
# MAGIC RAG has shown success in chatbots and Q&A systems that need to maintain up-to-date information or access domain-specific knowledge.
# MAGIC
# MAGIC ### RAG and Vector Search
# MAGIC
# MAGIC To be able to provide additional context to our LLM, we need to search for documents/articles where the answer to our user question might be.
# MAGIC To do so,  a common solution is to deploy a vector database. This will create document embeddings (vectors of fixed size, computed by a model).<br/>
# MAGIC The vectors will then be used to perform realtime similarity search during inference.
# MAGIC
# MAGIC ### Implementing RAG with Databricks Lakehouse AI and MosaicML endpoint
# MAGIC
# MAGIC In this demo, we will show you how to build and deploy your custom chatbot, answering questions on any custom or private information.
# MAGIC
# MAGIC As example, we will specialize this chatbot to answer questions over Databricks, feeding databricks.com documentation articles to the model for accurate answers.
# MAGIC
# MAGIC Here is the flow we will implement:
# MAGIC
# MAGIC - Download databricks.com documentation articles
# MAGIC - Prepare the articles for our model (split into chunks)
# MAGIC - Create a Vector Search Index using an Embedding endpoint
# MAGIC - Deploy an AI gateway as a Proxy to MosaicML
# MAGIC - Build and deploy our RAG chatbot
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/chatbot-rag/llm-rag-full.png?raw=true" style="float: right; margin-left: 10px"  width="1100px;">

# COMMAND ----------

# MAGIC %md
# MAGIC ## Disclaimer
# MAGIC
# MAGIC **This demo contains features in Private Preview: Databricks AI Gateway and Databricks Vector Search.**
# MAGIC
# MAGIC By running this demo, you accept the following:
# MAGIC ```
# MAGIC * Operability and support channels might be insufficient for production use. 
# MAGIC   Support is best effort and informal—please reach out to your account or external Slack channel for best effort support.
# MAGIC * This private preview will be free of charge at this time. We may charge for it in the future.
# MAGIC * You will still incur charges for DBUs.
# MAGIC * This product may change or may never be released.
# MAGIC * We may terminate the preview or your access to it with 2 weeks of notice.
# MAGIC * There may be API changes before Public Preview or General Availability. 
# MAGIC   We will give you at least 2 weeks notice before any significant changes so that you have time to update your projects.
# MAGIC * Non-public information about the preview is confidential.
# MAGIC ```
# MAGIC
# MAGIC *Accessing the private preview can be done by requesting access to your Account team. Please keep in mind that private previews are limited and Databricks might not be able to onboard all customers.*
# MAGIC
# MAGIC
# MAGIC *The demo team will do its best to keep this demo up to date. For any feedback, please open a github ticket: https://github.com/databricks-demos/dbdemos*

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## 1/ Data preparation
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/chatbot-rag/llm-rag-data-prep.png?raw=true" style="float: right; width: 800px; margin-left: 10px">
# MAGIC
# MAGIC The first step is to ingest and prepare the data to create of Vector Search Index.
# MAGIC
# MAGIC We'll use the Data Engineering Lakehouse capabilities to do Ingest our Documentation pages, split them into smaller chunks and save them as a Delta Lake table.

# COMMAND ----------

# MAGIC  %md
# MAGIC Start your data ingestion: open the [01-Data-Preparation]($./01-Data-Preparation) notebook

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## 2/ Creating the Vector Search Index
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/chatbot-rag/llm-rag-data-prep-4.png?raw=true" style="float: right; width: 500px; margin-left: 10px">
# MAGIC
# MAGIC Now that our data is ready, let's discover how you can add Vector Search to your Delta Lake table.
# MAGIC
# MAGIC Vector Search powers semantic similarity search queries, finding documents similar to your question within milliseconds.

# COMMAND ----------

# MAGIC  %md
# MAGIC Create your Vector Search Index: open the [02-Creating-Vector-Index]($./02-Creating-Vector-Index) notebook.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## 3/ Last step: deploying a RAG chatbot endpoint with MosaicML Endpoint
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/chatbot-rag/llm-rag-inference.png?raw=true" style="float: right; width: 800px; margin-left: 10px">
# MAGIC
# MAGIC Our data is ready and our Vector Search Index can answer similarity queries.
# MAGIC
# MAGIC We can now create an AI Gateway to access the LLama2-70B model via MosaicML and build our realtime RAG chatbot, answering advanced Databricks questions.

# COMMAND ----------

# MAGIC  %md
# MAGIC Build & Deploy your RAG chatbot : open the [03-Deploy-RAG-Chatbot-Model]($./03-Deploy-RAG-Chatbot-Model) notebook.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conclusion
# MAGIC
# MAGIC We've seen how Lakehouse AI is uniquely positioned to help you solve your GenAI challenge:
# MAGIC
# MAGIC - Simplify Data Ingestion and preparation with Databricks Data Engineering capabilities
# MAGIC - Accelerate Vector Search Index deployment with fully managed indexes
# MAGIC - Simplify, secure and control your LLM access with AI gateway
# MAGIC - Access a MosaicML LLama2-70B endpoint
# MAGIC - Deploy realtime model endpoint to generate answers which leverage your custom data
# MAGIC
# MAGIC Lakehouse AI is uniquely positioned to accelerate your GenAI deployment of RAG applications.
# MAGIC
# MAGIC Interested in deploying your own models? Reach out to your account team!
