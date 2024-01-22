# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC # 3/ Creating the Chat bot with Retrieval Augmented Generation (RAG)
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/chatbot-rag/llm-rag-inference.png?raw=true" style="float: right; margin-left: 10px"  width="900px;">
# MAGIC
# MAGIC
# MAGIC Our Vector Search Index is now ready!
# MAGIC
# MAGIC Let's now create and deploy a new Model Serving Endpoint to perform RAG.
# MAGIC
# MAGIC The flow will be the following:
# MAGIC
# MAGIC - A user asks a question
# MAGIC - The question is sent to our serverless Chatbot RAG endpoint
# MAGIC - The endpoint searches for docs similar to the question, leveraging Vector Search on our Documentation table
# MAGIC - The endpoint creates a prompt enriched with the doc
# MAGIC - The prompt is sent to the AI Gateway, ensuring security, stability and governance
# MAGIC - The gateway sends the prompt to a MosaicML LLM Endpoint (currently LLama 2 70B)
# MAGIC - Mosaic returns the result
# MAGIC - We display the output to our customer!

# COMMAND ----------

# MAGIC %pip install databricks-vectorsearch-preview mlflow[gateway] databricks-sdk==0.8.0
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./_resources/00-init $catalog=dbdemos $db=chatbot $reset_all_data=false

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## model fine tuning

# COMMAND ----------

TODO: fine tune llama2 with question/answer dataset
TODO: deploy the model as endpoint

# COMMAND ----------

# MAGIC %md
# MAGIC ### Let's try ourLLM model:
# MAGIC
# MAGIC AI Gateway accepts Databricks tokens as its authentication mechanism. 
# MAGIC
# MAGIC Let's send a simple REST call to our gateway. Note that we don't specify the LLM key nor the model details, only the gateway route.

# COMMAND ----------

TODO: call the model
