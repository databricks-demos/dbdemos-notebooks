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
# MAGIC %pip install -U -qqqq mlflow>=3.1.1 langchain langgraph==0.5.0 databricks-langchain pydantic databricks-agents unitycatalog-langchain[databricks] uv
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

# MAGIC %md
# MAGIC Let's now use Databricks built in `ai_extract` function to automatically parse the PDF document for us, making it super easy to extract the information!
# MAGIC
# MAGIC *Note: in this case, we have relatively small pdf documents, so we'll merge all the pages in one single value for our RAG system to work properly. Bigger docs might need some pre-processing steps to potentially reduce context size and be able to search/retreive more documents.*

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT OVERWRITE TABLE knowledge_base (product_name, title, full_guide, file_path)
# MAGIC SELECT ai_extract.product_name, ai_extract.title, full_guide, file_path
# MAGIC FROM (
# MAGIC   SELECT
# MAGIC     ai_extract(full_guide, array('product_name', 'title')) AS ai_extract,
# MAGIC     full_guide,
# MAGIC     file_path
# MAGIC   FROM (
# MAGIC      -- TODO: review why parsed_document:document.pages[*].content isn't working
# MAGIC     SELECT array_join(
# MAGIC             transform(parsed_document:document.pages::ARRAY<STRUCT<content:STRING>>, x -> x.content), '\n') AS full_guide,
# MAGIC            path as file_path
# MAGIC     FROM (
# MAGIC       SELECT ai_parse_document(content) AS parsed_document, path
# MAGIC       FROM READ_FILES("/Volumes/main_build/dbdemos_ai_agent/raw_data/pdf_documentation/", format => 'binaryFile')
# MAGIC     )
# MAGIC   )
# MAGIC );
# MAGIC
# MAGIC -- Check results
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
    embedding_source_column='full_guide', #The column containing our text
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
  columns=["id", "full_guide"],
  num_results=1)
docs = results.get('result', {}).get('data_array', [])
docs

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.1/ Configuring our Chain parameters
# MAGIC
# MAGIC As any appliaction, a RAG chain needs some configuration for each environement (ex: different catalog for test/prod environement). 
# MAGIC
# MAGIC Databricks makes this easy with Chain Configurations. You can use this object to configure any value within your app, including the different system prompts and make it easy to test and deploy newer version with better prompt.

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

import yaml, sys, os
import mlflow
import mlflow.models
# Add the ../agent_eval path relative to current working directory
agent_eval_path = os.path.abspath(os.path.join(os.getcwd(), "../02_agent_eval"))
#sys.path.append(os.path.join(agent_eval_path, 'agent.py'))
sys.path.append(agent_eval_path)
conf_path = os.path.join(agent_eval_path, 'agent_config.yaml')

try:
    config = yaml.safe_load(open(conf_path))
    config["config_version_name"] = "model_with_retriever"
    config["retriever_config"] =  {
      "index_name": vs_index_fullname,
      "tool_name": "product_technical_docs_retriever",
      "num_results": 1,
      "description": "Retrieves internal documentation about our products, infrastructure, router and other, including features, usage, and troubleshooting. Use this tool for any questions about product documentation."
    }
    yaml.dump(config, open(conf_path, "w"))
except Exception as e:
    print(f"Skipped update - ignore for job run - {e}")

model_config = mlflow.models.ModelConfig(development_config=conf_path)

# COMMAND ----------

import mlflow
request_example = "How do I restart my WIFI router ADSL-R500?"

# Let's reuse the functions we had in our initial notebooks 02.1_agent_evaluation to load and eval the new version of the model (included in the _resource file)
logged_agent_info = log_customer_support_agent_model(AGENT.get_resources(), request_example)
# Load the model and create a prediction function
loaded_model = mlflow.pyfunc.load_model(f"runs:/{logged_agent_info.run_id}/agent")

#Let's give it a try and make sure our retriever gets called
answer = predict_wrapper(request_example)
print(answer)

# COMMAND ----------

from mlflow.genai.scorers import RetrievalGroundedness, RelevanceToQuery, Safety, Guidelines

eval_dataset = mlflow.genai.datasets.get_dataset(f"{catalog}.{dbName}.ai_agent_mlflow_eval")

scorers = [
    RetrievalGroundedness(),  # Checks if email content is grounded in retrieved data
    RelevanceToQuery(),  # Checks if email addresses the user's request
    Safety(),  # Checks for harmful or inappropriate content
    Guidelines(
        guidelines="""Reponse must be done without showing reasoning.
        - don't mention that you need to look up things
        - do not mention tools or function used
        - do not tell your intermediate steps or reasoning""",
        name="steps_and_reasoning",
    )
]

print("Running evaluation...")
with mlflow.start_run(run_name='eval_with_retriever'):
    results = mlflow.genai.evaluate(data=eval_dataset, predict_fn=predict_wrapper, scorers=scorers)
