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
# MAGIC
# MAGIC # Scaling your business with a GenAI-Powered Assistant and DBRX Instruct
# MAGIC
# MAGIC LLMs are disrupting the way we interact with information, from internal knowledge bases to external, customer-facing documentation or support.
# MAGIC  
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/chatbot-rag/moisaic-logo.png?raw=true" width="100px" style="float: right" />
# MAGIC
# MAGIC While ChatGPT democratized LLM-based chatbots for consumer use, companies need to deploy personalized models that answer their needs:
# MAGIC
# MAGIC - Privacy requirements on sensitive information
# MAGIC - Preventing hallucination
# MAGIC - Specialized content, not available on the Internet
# MAGIC - Specific behavior for customer tasks
# MAGIC - Control over speed and cost
# MAGIC - Deploy models on private infrastructure for security reasons
# MAGIC
# MAGIC ## Introducing Moasic AI Quality Labs
# MAGIC
# MAGIC To solve these challenges, custom knowledge bases and models need to be deployed. However, doing so at scale isn't simple and requires:
# MAGIC
# MAGIC - Ingesting and transforming massive amounts of data 
# MAGIC - Ensuring privacy and security across your data pipeline
# MAGIC - Deploying systems such as Vector Search Index 
# MAGIC - Having access to GPUs and deploying efficient LLMs for inference serving
# MAGIC - Training and deploying custom models
# MAGIC - Evaluating your RAG application
# MAGIC
# MAGIC This is where the Databricks AI comes in. Databricks simplifies all these steps so that you can focus on building your final model, with the best prompts and performance.
# MAGIC
# MAGIC
# MAGIC ## GenAI & Maturity curve
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/chatbot-rag/llm-rag-maturity.png?raw=true" width="600px" style="float:right"/>
# MAGIC
# MAGIC Deploying GenAI can be done in multiple ways:
# MAGIC
# MAGIC - **Prompt engineering on public APIs (e.g. Databricks DBRX Instruct, LLama 2, openAI)**: answer from public information, retail (think ChatGPT)
# MAGIC - **Retrieval Augmented Generation (RAG)**: specialize your model with additional content. *This is what we'll focus on in this demo*
# MAGIC - **OSS model Fine tuning**: when you have a large corpus of custom data and need specific model behavior (execute a task)
# MAGIC - **Train your own LLM**: for full control on the underlying data sources of the model (biomedical, Code, Finance...)
# MAGIC
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection or disable tracker during installation. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=data-science&org_id=1444828305810485&notebook=00-RAG-chatbot-Introduction&demo_name=chatbot-rag-llm&event=VIEW">

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## What is Retrieval Augmented Generation (RAG) for LLMs?
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/chatbot-rag/rag-marchitecture.png?raw=true" width="700px" style="float: right" />
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
# MAGIC To do so,  a common solution is to deploy a vector database. This involves the creation of document embeddings, vectors of fixed size representing your document.<br/>
# MAGIC The vectors will then be used to perform real-time similarity search during inference.
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## 00-First Step: Deploy and test your first RAG application in 10minutes
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/chatbot-rag/rag-basic.png?raw=true" style="float: right"  width="700px;">
# MAGIC New to RAG and Mosaic AI Quality Labs? Start here if this is your first time implementing a GenAI application leveraging Databricks DBRX.
# MAGIC
# MAGIC You will learn:
# MAGIC
# MAGIC - Create your Vector Search index and send queries to find similar documents
# MAGIC - Build your langchain model leveraging Databricks Foundation Model (DBRX Instruct)
# MAGIC - Deploy and test your Chatbot with Databricks review app

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC Get started: open the [00-first-step/01-First-Step-RAG-On-Databricks]($./00-first-step/01-First-Step-RAG-On-Databricks).

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## 01-standard: Build a production-grade RAG application
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/chatbot-rag/llm-rag-managed-flow-0.png?raw=true" style="float: right"  width="700px;">
# MAGIC Start here if this is your first time implementing a GenAI application leveraging Databricks DBRX, our State Of the Art LLM, open and Available as a model serving endpoint.
# MAGIC
# MAGIC You will learn:
# MAGIC
# MAGIC - How to prepare your document dataset, creating text chunk from documentation pages
# MAGIC - Create your Vector Search index and send queries to find similar documents
# MAGIC - Build a complete langchain model leveraging Databricks Foundation Model (DBRX Instruct)
# MAGIC - Deploy and test your Chatbot with Databricks review app
# MAGIC - Ask external expert to test and review your chatbot
# MAGIC - Deploy a front end application using Databricks Lakehouse app

# COMMAND ----------

# MAGIC %md 
# MAGIC Get started: open the [01-standard-rag/01-Data-Preparation-and-Index notebook]($./01-standard-rag/01-Data-Preparation-and-Index).

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Advanced: Going further, build and manage your Evaluation Dataset with Mosaic AI Quality Labs
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/chatbot-rag/quality-lab-flow-eval.png?raw=true" style="float: right"  width="800px;">
# MAGIC Explore this content to discover how to leverage all the Databricks Data Intelligence Platform capabilities for your GenAI Apps.
# MAGIC
# MAGIC You will learn:
# MAGIC
# MAGIC - How to extract information from unstructured documents (pdfs) and create custom chunks
# MAGIC - Leverage Databricks Embedding Foundation Model to compute the chunks embeddings
# MAGIC - Create a Self Managed Vector Search index and send queries to find similar documents
# MAGIC - Build an advanced langchain model with chat history, leveraging Databricks Foundation Model (DBRX Instruct)
# MAGIC - Ask external expert to test and review your chatbot
# MAGIC - Run online llm evaluation and track your metrics with Databricks Monitoring
# MAGIC - Deploy a front end application using Databricks Lakehouse app

# COMMAND ----------

# MAGIC %md 
# MAGIC Learn more adavanced GenAI concepts: [open the 02-advanced/01-PDF-Advanced-Data-Preparation]($./02-advanced/01-PDF-Advanced-Data-Preparation).

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## What's next: LLM Fine Tuning
# MAGIC
# MAGIC Discover how to fine-tune your LLMs for your RAG applications: `dbdemos.install('llm-fine-tuning)`
