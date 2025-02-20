# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC # 1/ Deploying our first RAG application with Mosaic AI Agent Framework & Agent Evaluation
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/chatbot-rag/rag-basic.png?raw=true" style="width: 800px; margin-left: 10px">
# MAGIC
# MAGIC <br/>
# MAGIC
# MAGIC ## From data to chatbot in 10 minutes
# MAGIC
# MAGIC Rag applications are decoupled in 2 main parts:
# MAGIC - The knowledge database used to add additional context and improve the bot answer
# MAGIC - The actual chatbot application and its review / feedback mechanism
# MAGIC
# MAGIC ## 1.1/ Data preparation for RAG: building and indexing our knowledge base into Databricks Vector Search
# MAGIC
# MAGIC Let's start by prepraing our knowledge database. In this simple first demo, we'll be using data from Databricks Documentation already prepared and chuncked.
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection or disable tracker during installation. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=data-science&org_id=1444828305810485&notebook=01-First-Step-RAG-On-Databricks&demo_name=chatbot-rag-llm&event=VIEW">

# COMMAND ----------

# MAGIC %pip install -U --quiet databricks-sdk==0.40.0 databricks-agents==0.15.0 mlflow[databricks]==2.20.1 databricks-vectorsearch==0.44 langchain==0.2.1 langchain_core==0.2.5 langchain_community==0.2.4 markdownify==0.14.1 grpcio-status==1.59.3 # Temporary pin: grpcio version to avoid protobuf conflict.
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../_resources/00-init $reset_all_data=false

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- The dataset for your knowledge base has been loaded for you in the init notebook.
# MAGIC SELECT * FROM databricks_documentation

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

from databricks.sdk import WorkspaceClient
import databricks.sdk.service.catalog as c

#The table we'd like to index
source_table_fullname = f"{catalog}.{db}.databricks_documentation"
# Where we want to store our index
vs_index_fullname = f"{catalog}.{db}.databricks_documentation_vs_index"

if not index_exists(vsc, VECTOR_SEARCH_ENDPOINT_NAME, vs_index_fullname):
  print(f"Creating index {vs_index_fullname} on endpoint {VECTOR_SEARCH_ENDPOINT_NAME}...")
  try:
    vsc.create_delta_sync_index(
      endpoint_name=VECTOR_SEARCH_ENDPOINT_NAME,
      index_name=vs_index_fullname,
      source_table_name=source_table_fullname,
      pipeline_type="TRIGGERED",
      primary_key="id",
      embedding_source_column='content', #The column containing our text
      embedding_model_endpoint_name='databricks-gte-large-en' #The embedding endpoint used to create the embeddings
    )
  except Exception as e:
    display_quota_error(e, VECTOR_SEARCH_ENDPOINT_NAME)
    raise e
  #Let's wait for the index to be ready and all our embeddings to be created and indexed
  wait_for_index_to_be_ready(vsc, VECTOR_SEARCH_ENDPOINT_NAME, vs_index_fullname)
else:
  #Trigger a sync to update our vs content with the new data saved in the table
  wait_for_index_to_be_ready(vsc, VECTOR_SEARCH_ENDPOINT_NAME, vs_index_fullname)
  vsc.get_index(VECTOR_SEARCH_ENDPOINT_NAME, vs_index_fullname).sync()

print(f"index {vs_index_fullname} on table {source_table_fullname} is ready")

# COMMAND ----------

# MAGIC %md-sandbox 
# MAGIC # 2/ Deploy our chatbot model with RAG using DBRX
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
    "llm_model_serving_endpoint_name": "databricks-meta-llama-3-1-70b-instruct",  # the foundation model we want to use
    "vector_search_endpoint_name": VECTOR_SEARCH_ENDPOINT_NAME,  # the endoint we want to use for vector search
    "vector_search_index": f"{catalog}.{db}.databricks_documentation_vs_index",
    "llm_prompt_template": """You are an assistant that answers questions. Use the following pieces of retrieved context to answer the question. Some pieces of context may be irrelevant, in which case you should not use them to form the answer.\n\nContext: {context}""",
}

input_example = {"messages": [ {"role": "user", "content": "What is Retrieval-augmented Generation?"}]}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.2/ Deploy a RAG Chain to a web-based UI for stakeholder feedback
# MAGIC
# MAGIC Our chain is now ready! 
# MAGIC
# MAGIC We first register the MLflow model to Unity Catalog, and then use Agent Framework to deploy to the Quality Lab stakeholder review application which is backed by a scalable, production-ready Model Serving endpoint.
# MAGIC
# MAGIC <br/><img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/chatbot-rag/quality-lab.png?raw=true" width="1000px">

# COMMAND ----------

#dbdemos__delete_this_cell
#force the experiment to the field demos one. Required to launch as a batch
init_experiment_for_batch("chatbot-rag-llm-first-step", "simple")

# COMMAND ----------

# DBTITLE 1,Deploy the chain in Unity Catalog
# Log the model to MLflow
with mlflow.start_run(run_name="basic_rag_bot"):
  logged_chain_info = mlflow.langchain.log_model(
          #Note: In classical ML, MLflow works by serializing the model object.  In generative AI, chains often include Python packages that do not serialize.  Here, we use MLflow's new code-based logging, where we saved our chain under the chain notebook and will use this code instead of trying to serialize the object.
          lc_model=os.path.join(os.getcwd(), 'chain'),  # Chain code file e.g., /path/to/the/chain.py 
          model_config=chain_config, # Chain configuration 
          artifact_path="chain", # Required by MLflow, the chain's code/config are saved in this directory
          input_example=input_example,
          example_no_conversion=True,  # Required by MLflow to use the input_example as the chain's schema
      )

MODEL_NAME = "basic_rag_demo_ep"
MODEL_NAME_FQN = f"{catalog}.{db}.{MODEL_NAME}"
# Register to UC
uc_registered_model_info = mlflow.register_model(model_uri=logged_chain_info.model_uri, name=MODEL_NAME_FQN)

# COMMAND ----------

# MAGIC %md
# MAGIC Let's now deploy the Mosaic AI **Quality Lab review application** using the model we just created!

# COMMAND ----------

from databricks import agents
# Deploy to enable the Review APP and create an API endpoint
# Note: scaling down to zero will provide unexpected behavior for the chat app. Set it to false for a prod-ready application.
endpoint_name = f"dbdemos_rag_{catalog}-{db}-{MODEL_NAME}"[:63]
deployment_info = agents.deploy(MODEL_NAME_FQN, model_version=uc_registered_model_info.version, endpoint_name=endpoint_name, scale_to_zero=True)

instructions_to_reviewer = f"""## Instructions for Testing the Databricks Documentation Assistant chatbot

Your inputs are invaluable for the development team. By providing detailed feedback and corrections, you help us fix issues and improve the overall quality of the application. We rely on your expertise to identify any gaps or areas needing enhancement."""

# Add the user-facing instructions to the Review App
agents.set_review_instructions(MODEL_NAME_FQN, instructions_to_reviewer)

wait_for_model_serving_endpoint_to_be_ready(endpoint_name)

# COMMAND ----------

# MAGIC %md
# MAGIC # 3/ Use the Mosaic AI Quality Lab to evaluate your RAG applications
# MAGIC
# MAGIC ## 3.1/ Chat with your bot and build your validation dataset!
# MAGIC
# MAGIC Our Chat Bot is now live. Databricks provides a built-in chatbot application that you can use to test the chatbot and give feedbacks on its answer.
# MAGIC
# MAGIC You can easily give access to external domain experts and have them test and review the bot.  **Your domain experts do NOT need to have Databricks Workspace access** - you can assign permissions to any user in your SSO if you have enabled [SCIM](https://docs.databricks.com/en/admin/users-groups/scim/index.html)
# MAGIC
# MAGIC This is a critical step to build or improve your evaluation dataset: have users ask questions to your bot, and provide the bot with output answer when they don't answer properly.
# MAGIC
# MAGIC Your Chatbot is automatically capturing all stakeholder questions and bot responses, including an MLflow trace for each, into Delta Tables in your Lakehouse. On top of that, Databricks makes it easy to track feedback from your end user: if the chatbot doesn't give a good answer and the user gives a thumbdown, their feedback is included in the Delta Tables.
# MAGIC
# MAGIC Once your eval dataset is ready, you'll then be able to leverage it for offline evaluation to measure your new chatbot performance, and also potentially to Fine Tune your model.
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/chatbot-rag/quality-lab.png?raw=true" width="1000px">
# MAGIC

# COMMAND ----------

print(f"\n\nReview App URL to share with your stakeholders: {deployment_info.review_app_url}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 3.2/ Evaluate your bot's quality with Mosaic AI Quality Lab specialized LLM judge models
# MAGIC
# MAGIC Our bot is now Live. 
# MAGIC
# MAGIC Evaluation is a key part of deploying a RAG application. Databricks simplify this tasks with specialized LLM models tuned to evaluate your bot's quality/cost/latency, even if ground truth is not available.
# MAGIC
# MAGIC Quality Lab evaluates:
# MAGIC 1. Answer correctness - requires ground truth
# MAGIC 2. Hallucination / groundness - no ground truth required
# MAGIC 3. Answer relevance - no ground truth required
# MAGIC 4. Retrieval precision - no ground truth required
# MAGIC 5. (Lack of) Toxicity - no ground truth required
# MAGIC
# MAGIC In this example, we'll use an evaluation set that we curated based on our internal experts using the Quality Lab review app interface.  This proper Eval Dataset is saved as a Delta Table.
# MAGIC
# MAGIC To see how to collect the dataset from the Eval App, see the [03-advanced-app/03-Offline-Evaluation]($../03-advanced-app/03-Offline-Evaluation) notebook.

# COMMAND ----------

eval_dataset = spark.table("eval_set_databricks_documentation").limit(10).toPandas()
display(eval_dataset)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.1/ Run Evaluation of your Chain
# MAGIC
# MAGIC Let's leverage the Quality Lab specialized LLM to evaluate our model performance (make sure you use `databricks-rag`):

# COMMAND ----------

with mlflow.start_run(run_id=logged_chain_info.run_id):
    # Evaluate the logged model
    eval_results = mlflow.evaluate(
        data=eval_dataset,
        model=logged_chain_info.model_uri,
        model_type="databricks-agent",
    )

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC You can open your MLFlow Experiment to review the different evaluation, and compare multiple model response to see how different prompts answer: 
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/chatbot-rag/rag-mlflow-eval.png?raw=true" width="1200px">

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next: Deep dive into details with a more complete chain
# MAGIC
# MAGIC This example was a simple demo. In the next set of notebooks, we'll go into more details and review how to prepare and split your documents, while working with more production-grade chain.
# MAGIC
# MAGIC We will also see how to deploy your [first Lakehouse Application]($../02-simple-app/03-Deploy-Frontend-Lakehouse-App) to deploy the Assistant to your end-users!
# MAGIC
# MAGIC Open the [../02-simple-app/01-Data-Preparation-and-Index]($../02-simple-app/01-Data-Preparation-and-Index) Notebook!
# MAGIC
