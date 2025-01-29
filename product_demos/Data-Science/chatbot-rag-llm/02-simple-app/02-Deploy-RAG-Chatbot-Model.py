# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC # 2/ Creating the chatbot with Retrieval Augmented Generation (RAG) and DBRX Instruct
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/chatbot-rag/llm-rag-managed-flow-2.png?raw=true" style="float: right; margin-left: 10px"  width="900px;">
# MAGIC
# MAGIC Our Vector Search Index is now ready!
# MAGIC
# MAGIC Let's now create and deploy a new Model Serving Endpoint to perform RAG.
# MAGIC
# MAGIC The flow will be the following:
# MAGIC
# MAGIC - A user asks a question
# MAGIC - The question is sent to our serverless Chatbot RAG endpoint
# MAGIC - The endpoint compute the embeddings and searches for docs similar to the question, leveraging the Vector Search Index
# MAGIC - The endpoint creates a prompt enriched with the doc
# MAGIC - The prompt is sent to the DBRX Instruct Foundation Model Serving Endpoint
# MAGIC - We display the output to our users!
# MAGIC
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection or disable tracker during installation. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=data-science&org_id=1444828305810485&notebook=02-Deploy-RAG-Chatbot-Model&demo_name=chatbot-rag-llm&event=VIEW">

# COMMAND ----------

# MAGIC %md 
# MAGIC *Note: RAG performs document searches using Databricks Vector Search. In this notebook, we assume that the search index is ready for use. Make sure you run the previous [01-Data-Preparation-and-Index]($./01-Data-Preparation-and-Index [DO NOT EDIT]) notebook.*
# MAGIC

# COMMAND ----------

# DBTITLE 1,Install the required libraries
# MAGIC %pip install --quiet -U databricks-agents==0.15.0 mlflow-skinny==2.20.0 mlflow==2.20.0 mlflow[gateway]==2.20.0 langchain==0.2.1 langchain_core==0.2.5 langchain_community==0.2.4 databricks-vectorsearch databricks-sdk==0.40.0
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../_resources/00-init $reset_all_data=false

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Building our Chain
# MAGIC
# MAGIC In this example, we'll assume you already have a basic understanding of langchain. Check our [previous notebook](../00-first-step/01-First-Step-RAG-On-Databricks) to take it one step at a time!

# COMMAND ----------

rag_chain_config = {
    "databricks_resources": {
        "llm_endpoint_name": "databricks-meta-llama-3-1-70b-instruct",
        "vector_search_endpoint_name": VECTOR_SEARCH_ENDPOINT_NAME,
    },
    "input_example": {
        "messages": [{"content": "Sample user question", "role": "user"}]
    },
    "llm_config": {
        "llm_parameters": {"max_tokens": 1500, "temperature": 0.01},
        "llm_prompt_template": "You are a trusted AI assistant that helps answer questions based only on the provided information. If you do not know the answer to a question, you truthfully say you do not know. Here is the history of the current conversation you are having with your user: {chat_history}. And here is some context which may or may not help you answer the following question: {context}.  Answer directly, do not repeat the question, do not start with something like: the answer to the question, do not add AI in front of your answer, do not say: here is the answer, do not mention the context or the question. Based on this context, answer this question: {question}",
        "llm_prompt_template_variables": ["context", "chat_history", "question"],
    },
    "retriever_config": {
        "chunk_template": "Passage: {chunk_text}\n",
        "data_pipeline_tag": "poc",
        "parameters": {"k": 5, "query_type": "ann"},
        "schema": {"chunk_text": "content", "document_uri": "url", "primary_key": "id"},
        "vector_search_index": f"{catalog}.{db}.databricks_documentation_vs_index",
    },
}
try:
    with open('rag_chain_config.yaml', 'w') as f:
        yaml.dump(rag_chain_config, f)
except:
    print('pass to work on build job')
model_config = mlflow.models.ModelConfig(development_config='rag_chain_config.yaml')

# COMMAND ----------

# DBTITLE 1,Write the chain to a companion file to avoid serialization issues
# MAGIC %%writefile chain.py
# MAGIC import os
# MAGIC import mlflow
# MAGIC from operator import itemgetter
# MAGIC from databricks.vector_search.client import VectorSearchClient
# MAGIC from langchain_community.chat_models import ChatDatabricks
# MAGIC from langchain_community.vectorstores import DatabricksVectorSearch
# MAGIC from langchain_core.runnables import RunnableLambda
# MAGIC from langchain_core.output_parsers import StrOutputParser
# MAGIC from langchain_core.prompts import PromptTemplate
# MAGIC from langchain_core.runnables import RunnablePassthrough
# MAGIC
# MAGIC ## Enable MLflow Tracing
# MAGIC mlflow.langchain.autolog()
# MAGIC
# MAGIC # Return the string contents of the most recent message from the user
# MAGIC def extract_user_query_string(chat_messages_array):
# MAGIC     return chat_messages_array[-1]["content"]
# MAGIC
# MAGIC def extract_previous_messages(chat_messages_array):
# MAGIC     messages = "\n"
# MAGIC     for msg in chat_messages_array[:-1]:
# MAGIC         messages += (msg["role"] + ": " + msg["content"] + "\n")
# MAGIC     return messages
# MAGIC
# MAGIC def combine_all_messages_for_vector_search(chat_messages_array):
# MAGIC     return extract_previous_messages(chat_messages_array) + extract_user_query_string(chat_messages_array)
# MAGIC
# MAGIC #Get the conf from the local conf file
# MAGIC model_config = mlflow.models.ModelConfig(development_config='rag_chain_config.yaml')
# MAGIC
# MAGIC databricks_resources = model_config.get("databricks_resources")
# MAGIC retriever_config = model_config.get("retriever_config")
# MAGIC llm_config = model_config.get("llm_config")
# MAGIC
# MAGIC # Connect to the Vector Search Index
# MAGIC vs_client = VectorSearchClient(disable_notice=True)
# MAGIC vs_index = vs_client.get_index(
# MAGIC     endpoint_name=databricks_resources.get("vector_search_endpoint_name"),
# MAGIC     index_name=retriever_config.get("vector_search_index"),
# MAGIC )
# MAGIC vector_search_schema = retriever_config.get("schema")
# MAGIC
# MAGIC # Turn the Vector Search index into a LangChain retriever
# MAGIC vector_search_as_retriever = DatabricksVectorSearch(
# MAGIC     vs_index,
# MAGIC     text_column=vector_search_schema.get("chunk_text"),
# MAGIC     columns=[
# MAGIC         vector_search_schema.get("primary_key"),
# MAGIC         vector_search_schema.get("chunk_text"),
# MAGIC         vector_search_schema.get("document_uri"),
# MAGIC     ],
# MAGIC ).as_retriever(search_kwargs=retriever_config.get("parameters"))
# MAGIC
# MAGIC # Required to:
# MAGIC # 1. Enable the RAG Studio Review App to properly display retrieved chunks
# MAGIC # 2. Enable evaluation suite to measure the retriever
# MAGIC mlflow.models.set_retriever_schema(
# MAGIC     primary_key=vector_search_schema.get("primary_key"),
# MAGIC     text_column=vector_search_schema.get("chunk_text"),
# MAGIC     doc_uri=vector_search_schema.get("document_uri")
# MAGIC )
# MAGIC
# MAGIC # Method to format the docs returned by the retriever into the prompt
# MAGIC def format_context(docs):
# MAGIC     chunk_template = retriever_config.get("chunk_template")
# MAGIC     chunk_contents = [
# MAGIC         chunk_template.format(
# MAGIC             chunk_text=d.page_content,
# MAGIC         )
# MAGIC         for d in docs
# MAGIC     ]
# MAGIC     return "".join(chunk_contents)
# MAGIC
# MAGIC # Prompt Template for generation
# MAGIC prompt = PromptTemplate(
# MAGIC     template=llm_config.get("llm_prompt_template"),
# MAGIC     input_variables=llm_config.get("llm_prompt_template_variables"),
# MAGIC )
# MAGIC
# MAGIC # FM for generation
# MAGIC model = ChatDatabricks(
# MAGIC     endpoint=databricks_resources.get("llm_endpoint_name"),
# MAGIC     extra_params=llm_config.get("llm_parameters"),
# MAGIC )
# MAGIC
# MAGIC # RAG Chain
# MAGIC chain = (
# MAGIC     {
# MAGIC         "question": itemgetter("messages") | RunnableLambda(extract_user_query_string),
# MAGIC         "context": itemgetter("messages")
# MAGIC         | RunnableLambda(combine_all_messages_for_vector_search)
# MAGIC         | vector_search_as_retriever
# MAGIC         | RunnableLambda(format_context),
# MAGIC         "chat_history": itemgetter("messages") | RunnableLambda(extract_previous_messages)
# MAGIC     }
# MAGIC     | prompt
# MAGIC     | model
# MAGIC     | StrOutputParser()
# MAGIC )
# MAGIC
# MAGIC # Tell MLflow logging where to find your chain.
# MAGIC mlflow.models.set_model(model=chain)
# MAGIC
# MAGIC # COMMAND ----------
# MAGIC
# MAGIC # chain.invoke(model_config.get("input_example"))

# COMMAND ----------

#dbdemos__delete_this_cell
#force the experiment to the field demos one. Required to launch as a batch
init_experiment_for_batch("chatbot-rag-llm-standard", "simple")

# COMMAND ----------

# Log the model to MLflow
with mlflow.start_run(run_name=f"dbdemos_rag_quickstart"):
    logged_chain_info = mlflow.langchain.log_model(
        lc_model=os.path.join(os.getcwd(), 'chain.py'),  # Chain code file e.g., /path/to/the/chain.py 
        model_config='rag_chain_config.yaml',  # Chain configuration 
        artifact_path="chain",  # Required by MLflow
        input_example=model_config.get("input_example"),  # Save the chain's input schema.  MLflow will execute the chain before logging & capture it's output schema.
    )

# Test the chain locally
chain = mlflow.langchain.load_model(logged_chain_info.model_uri)
chain.invoke(model_config.get("input_example"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Let's deploy our RAG application and open it for external expert users

# COMMAND ----------

from databricks import agents
MODEL_NAME = "dbdemos_rag_demo"
MODEL_NAME_FQN = f"{catalog}.{db}.{MODEL_NAME}"

# COMMAND ----------

instructions_to_reviewer = f"""### Instructions for Testing the our Databricks Documentation Chatbot assistant

Your inputs are invaluable for the development team. By providing detailed feedback and corrections, you help us fix issues and improve the overall quality of the application. We rely on your expertise to identify any gaps or areas needing enhancement.

1. **Variety of Questions**:
   - Please try a wide range of questions that you anticipate the end users of the application will ask. This helps us ensure the application can handle the expected queries effectively.

2. **Feedback on Answers**:
   - After asking each question, use the feedback widgets provided to review the answer given by the application.
   - If you think the answer is incorrect or could be improved, please use "Edit Answer" to correct it. Your corrections will enable our team to refine the application's accuracy.

3. **Review of Returned Documents**:
   - Carefully review each document that the system returns in response to your question.
   - Use the thumbs up/down feature to indicate whether the document was relevant to the question asked. A thumbs up signifies relevance, while a thumbs down indicates the document was not useful.

Thank you for your time and effort in testing our assistant. Your contributions are essential to delivering a high-quality product to our end users."""

# Register the chain to UC
uc_registered_model_info = mlflow.register_model(model_uri=logged_chain_info.model_uri, name=MODEL_NAME_FQN)

# Deploy to enable the Review APP and create an API endpoint
deployment_info = agents.deploy(model_name=MODEL_NAME_FQN, model_version=uc_registered_model_info.version, scale_to_zero=True)

# Add the user-facing instructions to the Review App
agents.set_review_instructions(MODEL_NAME_FQN, instructions_to_reviewer)

wait_for_model_serving_endpoint_to_be_ready(deployment_info.endpoint_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Grant stakeholders access to the Mosaic AI Agent Evaluation App
# MAGIC
# MAGIC Now, grant your stakeholders permissions to use the Review App. To simplify access, stakeholders do not require to have Databricks accounts.

# COMMAND ----------

user_list = ["quentin.ambard@databricks.com"]
# Set the permissions.
agents.set_permissions(model_name=MODEL_NAME_FQN, users=user_list, permission_level=agents.PermissionLevel.CAN_QUERY)

print(f"Share this URL with your stakeholders: {deployment_info.review_app_url}")

# COMMAND ----------

# MAGIC %md ## Find review app name
# MAGIC
# MAGIC If you lose this notebook's state and need to find the URL to your Review App, you can list the chatbot deployed:

# COMMAND ----------

for deployment in agents.list_deployments():
  if deployment.model_name == MODEL_NAME_FQN:
    print(f"Review App URL: {deployment.review_app_url}")    

# COMMAND ----------

# MAGIC %md
# MAGIC ## Congratulations! You have deployed your first GenAI RAG model!
# MAGIC
# MAGIC You're now ready to deploy the same logic for your internal knowledge base leveraging Lakehouse AI.
# MAGIC
# MAGIC We've seen how the Lakehouse AI is uniquely positioned to help you solve your GenAI challenge:
# MAGIC
# MAGIC - Simplify Data Ingestion and preparation with Databricks Engineering Capabilities
# MAGIC - Accelerate Vector Search  deployment with fully managed indexes
# MAGIC - Leverage Databricks DBRX Instruct foundation model endpoint
# MAGIC - Deploy realtime model endpoint to perform RAG and provide Q&A capabilities
# MAGIC
# MAGIC Lakehouse AI is uniquely positioned to accelerate your GenAI deployment.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next: Deploying our GenAI Assistant application to end users with Databricks Lakehouse Application
# MAGIC
# MAGIC We are now ready to build a front end application so that our users can ask questions to the chatbot. 
# MAGIC
# MAGIC Open the [03-Deploy-Frontend-Lakehouse-App]($./03-Deploy-Frontend-Lakehouse-App) how to deploy your first Lakehouse Application.

# COMMAND ----------

# MAGIC %md # Cleanup
# MAGIC
# MAGIC To free up resources, please delete uncomment and run the below cell.

# COMMAND ----------

# /!\ THIS WILL DROP YOUR DEMO SCHEMA ENTIRELY /!\ 
# cleanup_demo(catalog, db, serving_endpoint_name, f"{catalog}.{db}.databricks_documentation_vs_index")
