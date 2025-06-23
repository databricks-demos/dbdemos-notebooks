# Databricks notebook source
# MAGIC %md
# MAGIC # Hands-On Lab:  Building a knowledge base from PDF using Databricks Vector search
# MAGIC
# MAGIC As we saw before, our agent isn't working well when it comes to answer specific, technical questions.
# MAGIC
# MAGIC That's because it doesn't have any knowledge about our internal systems and product. 
# MAGIC
# MAGIC Thanksfully, all this information is available to us as PDF. These pdf are stored in our volume. We'll parse them and save them in our Vector Search!

# COMMAND ----------

# DBTITLE 1,Library Installs
# MAGIC %pip install databricks-agents mlflow>=3.1.0 databricks-sdk==0.55.0
# MAGIC # Restart to load the packages into the Python environment
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../_resources/01-setup

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 1. Extracting the PDF information
# MAGIC - **Action**: Identify and retrieve the most recent return request from the ticketing or returns system.  
# MAGIC - **Why**: Ensures you’re working on the most urgent or next-in-line customer issue.
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %sql
# MAGIC LIST '/Volumes/main_build/dbdemos_ai_agent/raw_data/pdf_documentation/'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT ai_parse_document(content) AS parsed_document
# MAGIC   FROM READ_FILES("/Volumes/main_build/dbdemos_ai_agent/raw_data/pdf_documentation/", format => 'binaryFile') limit 2

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS knowledge_base (
# MAGIC   id BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC   product_name STRING,
# MAGIC   title STRING,
# MAGIC   full_guide STRING,
# MAGIC   file_path STRING)
# MAGIC   TBLPROPERTIES (delta.enableChangeDataFeed = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO TABLE knowledge_base (product_name, title, full_guide, file_path)
# MAGIC SELECT ai_extract.*, full_guide FROM (
# MAGIC   SELECT 
# MAGIC     ai_extract(full_guide, array('product_name', 'title')) AS ai_extract,
# MAGIC     full_guide
# MAGIC   FROM (
# MAGIC     -- TODO: review why parsed_document:document.pages[*].content isn't working
# MAGIC     SELECT array_join(
# MAGIC             transform(parsed_document:document.pages::ARRAY<STRUCT<content:STRING>>, x -> x.content), '\n') AS full_guide,
# MAGIC             doc_path as file_path
# MAGIC     FROM (
# MAGIC       SELECT ai_parse_document(content) AS parsed_document
# MAGIC       FROM READ_FILES("/Volumes/main_build/dbdemos_ai_agent/raw_data/pdf_documentation/", format => 'binaryFile')
# MAGIC     )
# MAGIC   ));
# MAGIC
# MAGIC SELECT * FROM knowledge_base;

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## 1.2/ Vector search Endpoints
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/chatbot-rag/rag-basic-prep-2.png?raw=true" style="float: right; margin-left: 10px" width="400px">
# MAGIC
# MAGIC Vector search endpoints are entities where your indexes will live. Think about them as entry point to handle your search request. 
# MAGIC
# MAGIC Let's start by creating our first Vector Search endpoint. Once created, you can view it in the [Vector Search Endpoints UI](#/setting/clusters/vector-search). Click on the endpoint name to see all indexes that are served by the endpoint.

# COMMAND ----------

from databricks.vector_search.client import VectorSearchClient
vsc = VectorSearchClient(disable_notice=True)

if not endpoint_exists(vsc, VECTOR_SEARCH_ENDPOINT_NAME):
    vsc.create_endpoint(name=VECTOR_SEARCH_ENDPOINT_NAME, endpoint_type="STANDARD")

wait_for_vs_endpoint_to_be_ready(vsc, VECTOR_SEARCH_ENDPOINT_NAME)
print(f"Endpoint named {VECTOR_SEARCH_ENDPOINT_NAME} is ready.")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/chatbot-rag/rag-basic-prep-3.png?raw=true" style="float: right; margin-left: 10px" width="400px">
# MAGIC
# MAGIC
# MAGIC ## 1.3/ Creating the Vector Search Index
# MAGIC
# MAGIC Once the endpoint is created, all we now have to do is to as Databricks to create the index on top of the existing table. 
# MAGIC
# MAGIC You just need to specify the text column and our embedding foundation model (`GTE`).  Databricks will build and synchronize the index automatically for us.
# MAGIC
# MAGIC This can be done using the API, or in a few clicks within the Unity Catalog Explorer menu:
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/index_creation.gif?raw=true" width="600px">
# MAGIC

# COMMAND ----------

# DBTITLE 1,Get the Latest Return in the Processing Queue
from databricks.sdk import WorkspaceClient
import databricks.sdk.service.catalog as c

#The table we'd like to index
source_table_fullname = f"{catalog}.{db}.knowledge_base"
# Where we want to store our index
vs_index_fullname = f"{catalog}.{db}.knowledge_base_vs_index"

if not index_exists(vsc, VECTOR_SEARCH_ENDPOINT_NAME, vs_index_fullname):
  print(f"Creating index {vs_index_fullname} on endpoint {VECTOR_SEARCH_ENDPOINT_NAME}...")
  vsc.create_delta_sync_index(
    endpoint_name=VECTOR_SEARCH_ENDPOINT_NAME,
    index_name=vs_index_fullname,
    source_table_name=source_table_fullname,
    pipeline_type="TRIGGERED",
    primary_key="id",
    embedding_source_column='content', #The column containing our text
    embedding_model_endpoint_name='databricks-gte-large-en' #The embedding endpoint used to create the embeddings
  )
  #Let's wait for the index to be ready and all our embeddings to be created and indexed
  wait_for_index_to_be_ready(vsc, VECTOR_SEARCH_ENDPOINT_NAME, vs_index_fullname)
else:
  #Trigger a sync to update our vs content with the new data saved in the table
  wait_for_index_to_be_ready(vsc, VECTOR_SEARCH_ENDPOINT_NAME, vs_index_fullname)
  vsc.get_index(VECTOR_SEARCH_ENDPOINT_NAME, vs_index_fullname).sync()

print(f"index {vs_index_fullname} on table {source_table_fullname} is ready")

# COMMAND ----------

# MAGIC %md 
# MAGIC ## 1.4/ Searching for relevant content
# MAGIC
# MAGIC That's all we have to do. Databricks will automatically capture and synchronize new entries in your table with the index.
# MAGIC
# MAGIC Note that depending on your dataset size and model size, index creation can take a few seconds to start and index your embeddings.
# MAGIC
# MAGIC Let's give it a try and search for similar content.
# MAGIC
# MAGIC *Note: `similarity_search` also support a filters parameter. This is useful to add a security layer to your RAG system: you can filter out some sensitive content based on who is doing the call (for example filter on a specific department based on the user preference).*

# COMMAND ----------

# DBTITLE 1,Create a function registered to Unity Catalog
question = "My wifi router gives me error 01, what should I do?"

results = vsc.get_index(VECTOR_SEARCH_ENDPOINT_NAME, vs_index_fullname).similarity_search(
  query_text=question,
  columns=["url", "content"],
  num_results=1)
docs = results.get('result', {}).get('data_array', [])
docs

# COMMAND ----------

# MAGIC %md-sandbox 
# MAGIC # 2/ Deploy our chatbot model with RAG using LLAMA
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/chatbot-rag/rag-basic-chain-1.png?raw=true" style="float: right" width="500px">
# MAGIC
# MAGIC We've seen how Databricks makes it easy to ingest and prepare your documents, and deploy a Vector Search index on top of it with just clicks.
# MAGIC
# MAGIC Now that our Vector Searc index is ready, let's deploy a langchain application.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.1/ Configuring our Chain parameters
# MAGIC
# MAGIC As any appliaction, a RAG chain needs some configuration for each environement (ex: different catalog for test/prod environement). 
# MAGIC
# MAGIC Databricks makes this easy with Chain Configurations. You can use this object to configure any value within your app, including the different system prompts and make it easy to test and deploy newer version with better prompt.

# COMMAND ----------

# For this first basic demo, we'll keep the configuration as a minimum. In real app, you can make all your RAG as a param (such as your prompt template to easily test different prompts!)
chain_config = {
    "llm_model_serving_endpoint_name": "databricks-meta-llama-3-3-70b-instruct",  # the foundation model we want to use
    "vector_search_endpoint_name": VECTOR_SEARCH_ENDPOINT_NAME,  # the endoint we want to use for vector search
    "vector_search_index": f"{catalog}.{db}.{vs_index_fullname}",
    "llm_prompt_template": """You are an assistant that answers questions. Use the following pieces of retrieved context to answer the question. Some pieces of context may be irrelevant, in which case you should not use them to form the answer.\n\nContext: {context}""",
}

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### 2.2 Building our Langchain retriever
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/chatbot-rag/rag-basic-chain-2.png?raw=true" style="float: right" width="500px">
# MAGIC
# MAGIC Let's start by building our Langchain retriever. 
# MAGIC
# MAGIC It will be in charge of:
# MAGIC
# MAGIC * Creating the input question (our Managed Vector Search Index will compute the embeddings for us)
# MAGIC * Calling the vector search index to find similar documents to augment the prompt with 
# MAGIC
# MAGIC Databricks Langchain wrapper makes it easy to do in one step, handling all the underlying logic and API call for you.

# COMMAND ----------

# DBTITLE 1,Very simple Python function
from databricks.vector_search.client import VectorSearchClient
from databricks_langchain.vectorstores import DatabricksVectorSearch
from langchain.schema.runnable import RunnableLambda
from langchain_core.output_parsers import StrOutputParser

## Enable MLflow Tracing
mlflow.langchain.autolog()

## Load the chain's configuration
model_config = mlflow.models.ModelConfig(development_config=chain_config)

## Turn the Vector Search index into a LangChain retriever
vector_search_as_retriever = DatabricksVectorSearch(
    endpoint=model_config.get("vector_search_endpoint_name"),
    index_name=model_config.get("vector_search_index"),
    columns=["id", "content", "url"],
).as_retriever(search_kwargs={"k": 3})

# Method to format the docs returned by the retriever into the prompt (keep only the text from chunks)
def format_context(docs):
    chunk_contents = [f"Passage: {d.page_content}\n" for d in docs]
    return "".join(chunk_contents)

#Let's try our retriever chain:
relevant_docs = (vector_search_as_retriever | RunnableLambda(format_context)| StrOutputParser()).invoke('How to start a Databricks cluster?')

display_txt_as_html(relevant_docs)

# COMMAND ----------

# DBTITLE 1,Register python function to Unity Catalog
# MAGIC %md-sandbox
# MAGIC You can see in the results that Databricks automatically trace your chain details and you can debug each steps and review the documents retrieved.
# MAGIC
# MAGIC ## 2.3/ Building Databricks Chat Model to query our demo's Foundational LLM
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/chatbot-rag/rag-basic-chain-3.png?raw=true" style="float: right" width="500px">
# MAGIC
# MAGIC Our chatbot will be using Meta's Llama open source model. However, it could be utilized with DBRX (_pictured_), or any other LLMs served on Databricks.  
# MAGIC
# MAGIC Other types of models that could be utilized include:
# MAGIC
# MAGIC - Databricks Foundation models (_what we will use by default in this demo_)
# MAGIC - Your organization's custom, fine-tuned model
# MAGIC - An external model provider (_such as Azure OpenAI_)
# MAGIC

# COMMAND ----------

# DBTITLE 1,Let's take a look at our created functions
from langchain_core.prompts import ChatPromptTemplate
from databricks_langchain.chat_models import ChatDatabricks
from operator import itemgetter

prompt = ChatPromptTemplate.from_messages(
    [  
        ("system", model_config.get("llm_prompt_template")), # Contains the instructions from the configuration
        ("user", "{question}") #user's questions
    ]
)

# Our foundation model answering the final prompt
model = ChatDatabricks(
    endpoint=model_config.get("llm_model_serving_endpoint_name"),
    extra_params={"temperature": 0.01, "max_tokens": 500}
)

#Let's try our prompt:
answer = (prompt | model | StrOutputParser()).invoke({'question':'How to start a Databricks cluster?', 'context': ''})
display_txt_as_html(answer)

# COMMAND ----------

# Return the string contents of the most recent messages: [{...}] from the user to be used as input question
def extract_user_query_string(chat_messages_array):
    return chat_messages_array[-1]["content"]

# RAG Chain
chain = (
    {
        "question": itemgetter("messages") | RunnableLambda(extract_user_query_string),
        "context": itemgetter("messages")
        | RunnableLambda(extract_user_query_string)
        | vector_search_as_retriever
        | RunnableLambda(format_context),
    }
    | prompt
    | model
    | StrOutputParser()
)

# COMMAND ----------

# Let's give it a try:
input_example = {"messages": [ {"role": "user", "content": "What is Retrieval-augmented Generation?"}]}
answer = chain.invoke(input_example)
print(answer)
