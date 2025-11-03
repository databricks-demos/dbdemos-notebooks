# Databricks notebook source
# MAGIC %md
# MAGIC # Building a knowledge base from PDF documentation using Databricks Vector search (RAG)
# MAGIC
# MAGIC As we saw before, our agent isn't working well when it comes to answer specific, technical questions such as WIFI router error code.
# MAGIC
# MAGIC That's because it doesn't have any knowledge about our internal systems and product. 
# MAGIC
# MAGIC Thanksfully, all this information is available to us as PDF. These pdf are stored in our volume. 
# MAGIC
# MAGIC We'll parse them and save them in our Vector Search, and then add a retriever to our agent to improve its capabilities!
# MAGIC
# MAGIC
# MAGIC <div style="background-color: #d4e7ff; padding: 10px; border-radius: 15px;">
# MAGIC <strong>Note:</strong> Coming soon, we'll show how to add a Knowledge base in a few clicks leveraging Databricks Agents!
# MAGIC </div>
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection or disable tracker during installation. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=data-science&org_id=1444828305810485&notebook=03.1-pdf-rag-tool&demo_name=ai-agent&event=VIEW">
# MAGIC

# COMMAND ----------

# DBTITLE 1,Library Installs
# MAGIC %pip install -U -qqqq mlflow>=3.1.1 langchain==0.3.27 langgraph databricks-langchain pydantic databricks-agents unitycatalog-langchain[databricks] uv databricks-feature-engineering==0.12.1
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../_resources/01-setup

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 1. Extracting the PDF information
# MAGIC Databricks provides a builtin `ai_parse_document` function, leveraging AI to analyze and extract PDF information as text. This makes it super easy to ingest unstructured information!

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT path FROM READ_FILES('/Volumes/main_build/dbdemos_ai_agent/raw_data/pdf_documentation/', format => 'binaryFile') limit 2

# COMMAND ----------

# DBTITLE 1,let's try our ai_parse_document function
# MAGIC %sql
# MAGIC SELECT ai_parse_document(content) AS parsed_document
# MAGIC   FROM READ_FILES('/Volumes/main_build/dbdemos_ai_agent/raw_data/pdf_documentation/', format => 'binaryFile') limit 2

# COMMAND ----------

# MAGIC %md 
# MAGIC ## 1.1/ Create our knowledge base table
# MAGIC
# MAGIC Let's first create our table. We'll enable Change Data Feed so that we can create our vector search on top of it.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS knowledge_base (
# MAGIC   id BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC   product_name STRING,
# MAGIC   title STRING,
# MAGIC   content STRING,
# MAGIC   doc_uri STRING)
# MAGIC   TBLPROPERTIES (delta.enableChangeDataFeed = true);

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 1.2/ PDF to text with ai_parse_document
# MAGIC
# MAGIC Let's now use Databricks built in `ai_parse_document` function to automatically parse the PDF document for us, making it super easy to extract the information!
# MAGIC
# MAGIC *Note: in this case, we have relatively small pdf documents, so we'll merge all the pages of the document in one single text field for our RAG system to work properly. Bigger docs might need some pre-processing steps to potentially reduce context size and be able to search/retreive more documents, adding potential pre-processing steps, for example ensuring the WIFI Router model is present in all the chunk to keep the vector search more relevant.*

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT OVERWRITE TABLE knowledge_base (product_name, title, content, doc_uri)
# MAGIC SELECT ai_extract.product_name, ai_extract.title, content, doc_uri
# MAGIC FROM (
# MAGIC   SELECT
# MAGIC     ai_extract(content, array('product_name', 'title')) AS ai_extract,
# MAGIC     content,
# MAGIC     doc_uri
# MAGIC   FROM (
# MAGIC     SELECT array_join(
# MAGIC             transform(parsed_document:document.elements::ARRAY<STRUCT<content:STRING>>, x -> x.content), '\n') AS content,
# MAGIC            path as doc_uri
# MAGIC     FROM (
# MAGIC       SELECT ai_parse_document(content) AS parsed_document, path
# MAGIC       FROM READ_FILES('/Volumes/main_build/dbdemos_ai_agent/raw_data/pdf_documentation/', format => 'binaryFile') 
# MAGIC       LIMIT 5 -- ADDED FIX LIMIT FOR DEMO COST - DROP IT IN REAL WORKLOAD
# MAGIC     )
# MAGIC   )
# MAGIC );

# COMMAND ----------

# DBTITLE 1,Check results
# MAGIC %sql
# MAGIC SELECT * FROM knowledge_base;

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## 2/ Create our vector search table
# MAGIC
# MAGIC ### 2.1/ Vector search Endpoints
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
# MAGIC ### 2.2/ Creating the Vector Search Index
# MAGIC
# MAGIC Once the endpoint is created, all we now have to do is to as Databricks to create the index on top of the existing table. 
# MAGIC
# MAGIC You just need to specify the text column and our embedding foundation model (`GTE`).  Databricks will build and synchronize the index automatically for us.
# MAGIC
# MAGIC Note that Databricks provides 3 type of vector search:
# MAGIC
# MAGIC * **Managed embeddings**: Databricks creates the embeddings for you from a text field and Databricks synchronize the Delta table to your index (what we'll use)
# MAGIC * **Self managed embeddings**: You compute the embeddings yourself and save them to your Delta table  and Databricks synchronize the Delta table to your index
# MAGIC * **Direct access**: you manage the VS indexation yourself (no Delta table)
# MAGIC
# MAGIC This can be done using the API, or in a few clicks within the Unity Catalog Explorer menu:
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/index_creation.gif?raw=true" width="600px">
# MAGIC

# COMMAND ----------

# DBTITLE 1,Get the Latest Return in the Processing Queue
from databricks.sdk import WorkspaceClient

#The table we'd like to index
source_table_fullname = f"{catalog}.{dbName}.knowledge_base"
# Where we want to store our index
vs_index_fullname = f"{catalog}.{dbName}.knowledge_base_vs_index"

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
# MAGIC ## 2.3/ Try our VS index: searching for relevant content
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
  columns=["id", "content"],
  num_results=1)
docs = results.get('result', {}).get('data_array', [])
docs

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3/ Update our existing Agent to add the retriever as new tool
# MAGIC
# MAGIC Now that our index is ready, all we have to do is to add it as retriever to our existing agent!
# MAGIC
# MAGIC We'll reuse the `agent.py` and `agent_config.yaml` file: simply add the retriever configuration and our agent will add it as one of the tools available!

# COMMAND ----------

import mlflow
import yaml, sys, os
import mlflow.models
# Add the ../agent_eval path relative to current working directory
agent_eval_path = os.path.abspath(os.path.join(os.getcwd(), "../02-agent-eval"))
sys.path.append(agent_eval_path)
# Let's also use the same experiment as in our previous notebook to keep all the trace in a single place
mlflow.set_experiment(agent_eval_path+"/02.1_agent_evaluation")
conf_path = os.path.join(agent_eval_path, 'agent_config.yaml')

try:
    config = yaml.safe_load(open(conf_path))
    config["config_version_name"] = "model_with_retriever"
    config["retriever_config"] =  {
        "index_name": vs_index_fullname,
        "tool_name": "product_technical_docs_retriever",
        "num_results": 1,
        "description": "Retrieves internal documentation about our products, infrastructure, router and other, including features, usage, and troubleshooting. Use this tool for any questions about product documentation or product issues."
    }
    yaml.dump(config, open(conf_path, "w"))
except Exception as e:
    print(f"Skipped update - ignore for job run - {e}")

model_config = mlflow.models.ModelConfig(development_config=conf_path)

# COMMAND ----------

# MAGIC %pip install databricks-mcp

# COMMAND ----------

from agent import AGENT 

#Let's try our retriever to make sure we know have access to the wifi router pdf guide
request_example = "How do I restart my WIFI router ADSL-R500?"
answer = AGENT.predict({"input":[{"role": "user", "content": request_example}]})

# COMMAND ----------

# MAGIC %md
# MAGIC Now log the new agent in the MLflow model registry using `mlflow.pyfunc.log_model()` as in the notebook `02.1_agent evaluation`.

# COMMAND ----------

# Agent captures required resources for agent execution, note that it now has the VS index referenced
for r in AGENT.get_resources():
  print(f"Resource: {type(r).__name__}:{r.name}")

# COMMAND ----------

with mlflow.start_run(run_name=model_config.get('config_version_name')):
  logged_agent_info = mlflow.pyfunc.log_model(
    name="agent",
    python_model=agent_eval_path+"/agent.py",
    model_config=conf_path,
    input_example={"input": [{"role": "user", "content": request_example}]},
     # Determine resources (endpoints, fonctions, vs...) to specify for automatic auth passthrough for deployment
    resources=AGENT.get_resources(),
    extra_pip_requirements=["databricks-connect"]
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4/ Evaluate our agent against our documents base
# MAGIC
# MAGIC Our new model is available! As usual, the next step is to evaluate our dataset to make sure we're improving our answers.
# MAGIC
# MAGIC
# MAGIC ### 4.1/ Generate synthetic eval data
# MAGIC
# MAGIC Note that our eval dataset doesn't have any entry on our PDF.
# MAGIC
# MAGIC Using Databricks, it's easy to bootstrap our evaluation dataset with synthetic eval data, and then improve this dataset over time.

# COMMAND ----------

from databricks.agents.evals import generate_evals_df

docs = spark.table('knowledge_base')
# Describe what our agent is doing
agent_description = """
The Agent is a RAG chatbot that answers technical questions about products such as wifi router, Fiber Installation, network information, but also customer retention strategies or guidelines on social media. The Agent has access to a corpus of Documents, and its task is to answer the user's questions by retrieving the relevant docs from the corpus and synthesizing a helpful, accurate response.
"""

question_guidelines = """
# User personas
- A customer asking question on how to troubleshoot the system, step by step
- An internal agent asking question on internal policies

# Example questions
- How do I troubleshoot Error Code 1001: Invalid Return Authorization when a customer can't submit their return request?
- I'm getting Error Code 1001 when trying to deploy a survey. What could be causing this and how do I fix it?

# Additional Guidelines
- Questions should be succinct, and human-like
"""

# Generate synthetic eval dataset
evals = generate_evals_df(
    docs,
    # The total number of evals to generate. The method attempts to generate evals that have full coverage over the documents
    # provided. If this number is less than the number of documents,
    # some documents will not have any evaluations generated. See "How num_evals is used" below for more details.
    num_evals=10
    ,
    # A set of guidelines that help guide the synthetic generation. These are free-form strings that will be used to prompt the generation.
    agent_description=agent_description,
    question_guidelines=question_guidelines
)
evals["inputs"] = evals["inputs"].apply(lambda x: {"question": x["messages"][0]["content"]})
display(evals)

# COMMAND ----------

# Add our synthetic dataset to our MLFLow evaluation dataset
eval_dataset_table_name = f"{catalog}.{dbName}.ai_agent_mlflow_eval"

eval_dataset = mlflow.genai.datasets.get_dataset(eval_dataset_table_name)
eval_dataset.merge_records(evals)
print("Added records to the evaluation dataset.")

# COMMAND ----------

# MAGIC %md 
# MAGIC ### 4.2/ Running our evaluation
# MAGIC As previously, let's run our evaluations using the MLFlow dataset. We'll make sure our model still behave properly on the customer-related question, and now perform well on our knowledge-base questions!

# COMMAND ----------

from mlflow.genai.scorers import RetrievalGroundedness, RelevanceToQuery, Safety, Guidelines
import pandas as pd

eval_dataset = mlflow.genai.datasets.get_dataset(f"{catalog}.{dbName}.ai_agent_mlflow_eval")

#Get the same scorers as previously (function is defined in _resources/01-setup, similar to the previous step)
scorers = get_scorers()

# Load the model and create a prediction function
loaded_model = mlflow.pyfunc.load_model(f"runs:/{logged_agent_info.run_id}/agent")
def predict_wrapper(question):
    # Format for chat-style models
    model_input = pd.DataFrame({
        "input": [[{"role": "user", "content": question}]]
    })
    response = loaded_model.predict(model_input)
    return response['output'][-1]['content'][-1]['text']
    
print("Running evaluation...")
with mlflow.start_run(run_name='eval_with_retriever'):
    results = mlflow.genai.evaluate(data=eval_dataset, predict_fn=predict_wrapper, scorers=scorers)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.3/ Deploy the final model! 
# MAGIC
# MAGIC We're good to go. Let's deploy our model to UC and update our endpoint with the latest version!

# COMMAND ----------

from mlflow import MlflowClient
MODEL_NAME = "dbdemos_ai_agent_demo"
UC_MODEL_NAME = f"{catalog}.{dbName}.{MODEL_NAME}"

# register the model to UC
client = MlflowClient()
uc_registered_model_info = mlflow.register_model(model_uri=logged_agent_info.model_uri, name=UC_MODEL_NAME, tags={"model": "customer_support_agent", "model_version": "with_retriever"})

client.set_registered_model_alias(name=UC_MODEL_NAME, alias="model-to-deploy", version=uc_registered_model_info.version)
displayHTML(f'<a href="/explore/data/models/{catalog}/{dbName}/{MODEL_NAME}" target="_blank">Open Unity Catalog to see Registered Agent</a>')

# COMMAND ----------

from databricks import agents
# Deploy the model to the review app and a model serving endpoint
endpoint_name = f'{MODEL_NAME}_{catalog}_{db}'[:60]

if len(agents.get_deployments(model_name=UC_MODEL_NAME, model_version=uc_registered_model_info.version)) == 0:
  agents.deploy(UC_MODEL_NAME, uc_registered_model_info.version, endpoint_name=endpoint_name, tags = {"project": "dbdemos"})

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next: deploy our chatbout within a Databricks Application
# MAGIC
# MAGIC Now that our agent is ready, let's deploy a GradIO application to serve its content to our end users. 
# MAGIC
# MAGIC Open [04-deploy-app/04-Deploy-Frontend-Lakehouse-App]($../04-deploy-app/04-Deploy-Frontend-Lakehouse-App) !
