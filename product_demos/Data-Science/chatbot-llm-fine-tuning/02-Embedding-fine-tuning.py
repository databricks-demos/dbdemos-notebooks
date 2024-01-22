# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC # 2/ Fine-tuning and deploying a LLM model for embeddings
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/chatbot-rag/llm-rag-data-prep-3.png?raw=true" style="float: right; width: 600px; margin-left: 10px">
# MAGIC
# MAGIC We now have our knowledge base ready, and saved as a Delta Lake table within Unity Catalog (including permission, lineage, audit logs and all UC features).
# MAGIC
# MAGIC We'll start by fine-tuning an embedding model, specialized in retrieving similar documents based on question.
# MAGIC
# MAGIC You can see this as a classification task. For any question asked by our customers, we want our LLM to output a vector (embedding) representing the question. This vector is then used within our Vector Search database to find the most similar document.
# MAGIC
# MAGIC This is a critical task as our model will use this content to deliver proper answer, reducing hallucination risk.
# MAGIC
# MAGIC To improve our search accuracy, we can fine-tune an LLM model (text to vector) to be more specific and better classify our Databricks questions based on the documentation pages available.
# MAGIC
# MAGIC ## Using hugging Face transformers
# MAGIC
# MAGIC Hugging Face `transformers`library is becoming the de-facto standard to load, query and fine-tune LLMs.
# MAGIC
# MAGIC Many LLMs are available in the hugging Face [Model Repository](https://huggingface.co/models). We will use it to load a first embedding model and then fine-tune it for our dataset.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Document Embeddings 
# MAGIC
# MAGIC The first step is to create embeddings from the documents saved in our Delta Lake table. To do so, we need an LLM model specialized in taking a text of arbitrary length, and turning it into an embedding (vector of fixed size representing our document). 
# MAGIC
# MAGIC *Note: It is critical to ensure that the model is always the same for both embedding index creation and real-time similarity search. Remember that if your embedding model changes, you'll have to re-index your entire set of vectors, otherwise similarity search won't return relevant results.*

# COMMAND ----------

# DBTITLE 1,Install vector search package
# MAGIC %pip install transformers
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./_resources/00-init $catalog=dbdemos $db=chatbot $reset_all_data=false

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Creating and registring our embedding model in UC
# MAGIC
# MAGIC Let's create an embedding model and save it in Unity Catalog. We'll then deploy it as serverless model serving endpoint. Vector Search will call this endpoint to create embeddings from our documents, and then index them.
# MAGIC
# MAGIC The model will also be used during realtime similarity search to convert the queries into vectors. This will be taken care of by Databricks Vector Search.
# MAGIC
# MAGIC #### Choosing an embeddings model
# MAGIC There are multiple choices for the embeddings model:
# MAGIC
# MAGIC * **SaaS API embeddings model**:
# MAGIC Starting simple with a SaaS API is a good option. If you want to avoid vendor dependency as a result of proprietary SaaS API solutions (e.g. Open AI), you can build with a SaaS API that is pointing to an OSS model. You can use the new [MosaicML Embedding](https://docs.mosaicml.com/en/latest/inference.html) endpoint: `/instructor-large/v1`. See more in [this blogpost](https://www.databricks.com/blog/using-ai-gateway-llama2-rag-apps)
# MAGIC * **Deploy an OSS embeddings model**: On Databricks, you can deploy a custom copy of any OSS embeddings model behind a production-grade Model Serving endpoint.
# MAGIC * **Fine-tune an embeddings model**: On Databricks, you can use AutoML to fine-tune an embeddings model to your data. This has shown to improve relevance of retrieval. AutoML is in Private Preview - [Request Access Here](https://docs.google.com/forms/d/1MZuSBMIEVd88EkFj1ehN3c6Zr1OZfjOSvwGu0FpwWgo/edit)
# MAGIC
# MAGIC Because we want to keep this demo simple, we'll directly leverage OpenAI, an external SaaS API. 
# MAGIC `mlflow.openai.log_model()` is a very convenient way to register a model calling OpenAI directly.
# MAGIC
# MAGIC
# MAGIC *Note: The Vector Search Private Preview might soon add support to directly plug an AI gateway for external SaaS API. This will make it even easier, without having to deploy the `mlflow.openai` model model. We will update this content accordingly.*
# MAGIC

# COMMAND ----------

TODO: do embeddings fine tuning with the doc/question dataset

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ### Deploy our realtime model endpoint
# MAGIC
# MAGIC Note that the endpoint uses the `openai` MLFlow flavor, which requires the `OPENAI_API_KEY` environment variable to work. 
# MAGIC
# MAGIC Let's leverage Databricks Secrets to load the key when the endpoint starts. See the [documentation](https://docs.databricks.com/en/machine-learning/model-serving/store-env-variable-model-serving.html) for more details.

# COMMAND ----------

#Helper for the endpoint rest api, see details in _resources/00-init
serving_client = EndpointApiClient()
#Start the enpoint using the REST API (you can do it using the UI directly)
serving_client.create_enpoint_if_not_exists("dbdemos_embedding_endpoint", 
                                            model_name=f"{catalog}.{db}.embedding_gateway", 
                                            model_version = latest_model.version, 
                                            workload_size="Small", 
                                            environment_vars={"OPENAI_API_KEY": "{{secrets/dbdemos/openai}}"})

#Make sure all users can access our endpoint for this demo
set_model_endpoint_permission("dbdemos_embedding_endpoint", "CAN_MANAGE", "users")

# COMMAND ----------

# MAGIC %md
# MAGIC Our endpoint is now deployed! You can directly [open it from the UI](/endpoints/dbdemos_embedding_endpoint) and visualize its performance!
# MAGIC
# MAGIC Let's run a REST query to try it in Python. As you can see, we send the `test sentence` doc and it returns an embedding representing our document.

# COMMAND ----------

# DBTITLE 1,Testing our Embedding endpoint
#Let's try to send some inference to our REST endpoint
dataset =  {"dataframe_split": {'data': ['test sentence']}}
import timeit

endpoint_url = f"{serving_client.base_url}/realtime-inference/dbdemos_embedding_endpoint/invocations"
print(f"Sending requests to {endpoint_url}")
starting_time = timeit.default_timer()
inferences = requests.post(endpoint_url, json=dataset, headers=serving_client.headers).json()
print(f"Embedding inference, end 2 end :{round((timeit.default_timer() - starting_time)*1000)}ms {inferences}")
