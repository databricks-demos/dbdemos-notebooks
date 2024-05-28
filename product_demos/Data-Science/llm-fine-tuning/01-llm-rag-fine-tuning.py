# Databricks notebook source
# MAGIC %md
# MAGIC # Fine-tuning your LLM on Databricks
# MAGIC
# MAGIC Fine Tuning LLMs is the action of specializing an existing model to your own requirements.
# MAGIC Technically speaking, you start from the weights of an existing foundation model (such as DBRX or LLAMA), and add another training round on existing dataset.
# MAGIC
# MAGIC
# MAGIC ## Why Fine Tuning?
# MAGIC Databricks provides an easy way to specialize existing foundation model, making sure you own and control your model with better performance, cost, security and privacy.
# MAGIC
# MAGIC The typical fine-tuning use cases are:
# MAGIC * Train the model on very specific, internal knowledge
# MAGIC * Custom model behavior, like specialized entity extraction
# MAGIC * Reduce model size (and inference cost) while improving answer quality
# MAGIC
# MAGIC ## Continued training vs instruction Fine Tuning?
# MAGIC
# MAGIC Databricks Fine Tuning API let you train your model, typically for:
# MAGIC * **Supervised fine-tuning**: Train your model on structured prompt-response data. Use this to adapt your model to a new task, change its response style, or add instruction-following capabilities, such as NER (see [instruction-fine-tuning/01-llm-instruction-drug-extraction-fine-tuning]($./instruction-fine-tuning/01-llm-instruction-drug-extraction-fine-tuning)).
# MAGIC * **Continued pre-training**: Train your model with additional text data. Use this to add new knowledge to a model or focus a model on a specific domain. Requires several millions of token to be relevant.
# MAGIC * **Chat completion**: Train your model on chat logs between a user and an AI assistant. This format can be used both for actual chat logs, and as a standard format for question answering and conversational text. The text is automatically formatted into the appropriate chat format for the specific model (what this demo will focus on).
# MAGIC
# MAGIC ## Fine Tuning or RAG?
# MAGIC RAG and Instruction fine-tuning work together! If you have a relevant RAG use-case, you can start with RAG leveraging a foundational model, and then specialize your model if your corpus is specific to your business (ex: not part of the foundation model training) or need specific behavior (ex: answer to a specific task such as entity extraction)
# MAGIC
# MAGIC Start with RAG on a Foundation Model, evaluate how your model is working and where it can be improved, and build a Fine Tuned dataset accordingly!

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## Fine tuning for a Databricks Chatbot Documentation 
# MAGIC
# MAGIC In this demo, we'll fine tune Mistral (or llama) on Databricks Documentation for a RAG chatbot helping a customer to answer Databricks-related questions. 
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/llm-fine-tuning/databricks-llm-fine-tuning-0.png?raw=true" width="1200px">
# MAGIC
# MAGIC Databricks provides simple, built-in API to fine tune the model and evaluate its performance. Let's get started!
# MAGIC
# MAGIC Documentation for Fine Tuning on Databricks:
# MAGIC - Overview page: [Databricks Fine-tuning APIs](https://pr-14641-aws.dev-docs.dev.databricks.com/large-language-models/fine-tuning-api/index.html)
# MAGIC - SDK Guide: [Setting up a fine-tuning session with Fine-tuning APIs](https://pr-14641-aws.dev-docs.dev.databricks.com/large-language-models/fine-tuning-api/create-fine-tune-run.html)
# MAGIC

# COMMAND ----------

# Let's start by installing our products
%pip install databricks-genai==1.0.2
%pip install databricks-sdk==0.27.1
%pip install "mlflow==2.12.2"
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./_resources/00-setup

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Fine tuning dataset
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/llm-fine-tuning/databricks-llm-fine-tuning-1.png?raw=true" width="700px" style="float: right">
# MAGIC
# MAGIC Building a high quality fine tuning dataset is key for final your model performance. 
# MAGIC
# MAGIC The training dataset needs to match what you will be sending to your final model. <br/>
# MAGIC If you have a RAG application, you need to fine tune with the full RAG instruction so that your model can learn how to extract the proper information from the context and answer the way you want.
# MAGIC
# MAGIC The demo loaded a fine tuning dataset for you containing the following:
# MAGIC
# MAGIC * A Databricks user question (ex: how do I start a Warehouse?)
# MAGIC * A page or chunk from Databricks documentation relevant to this question
# MAGIC * The expected answer, reviewed by a human

# COMMAND ----------

training_dataset = spark.sql("""
  SELECT q.id as question_id, q.question, a.answer, d.url, d.content FROM training_dataset_question q
      INNER JOIN databricks_documentation d on q.doc_id = d.id
      INNER JOIN training_dataset_answer   a on a.question_id = q.id 
    WHERE answer IS NOT NULL""")
display(training_dataset)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Preparing the dataset for Chat Completion
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/llm-fine-tuning/databricks-llm-fine-tuning-2.png?raw=true" width="700px" style="float: right">
# MAGIC
# MAGIC Because we're fine tuning on a chatbot, we need to prepare our dataset for **Chat completion**.
# MAGIC
# MAGIC Chat completion requires a list of role prompt, following the openai standard. It has the benefit of transforming it to a prompt following your llm instruction pattern (each foundation model might be trained with different instruction type, and it's best to use the same for fine tuning).<br/>
# MAGIC *We recommend using Chat Completion whenever possible.*
# MAGIC
# MAGIC ```
# MAGIC [
# MAGIC   {"role": "system", "content": "[system prompt]"},
# MAGIC   {"role": "user", "content": "Here is a documentation page:[RAG context]. Based on this, answer the following question: [user question]"},
# MAGIC   {"role": "assistant", "content": "[answer]"}
# MAGIC ]
# MAGIC ```
# MAGIC
# MAGIC *Remember that your Fine Tuning dataset should be the same format as the one you're using for your RAG application.<br/>*
# MAGIC
# MAGIC Databricks supports all kind of dataset format (Volume files, Delta tables, and public Hugging Face datasets), but we recommend preparing the dataset as Delta tables within your catalog within a proper data pipeline.
# MAGIC *Remember, this step is critical and you need to make sure your training dataset is of high quality.*
# MAGIC
# MAGIC Let's create a small pandas UDF and create our final chat completion dataset.<br/>

# COMMAND ----------

from pyspark.sql.functions import pandas_udf
import pandas as pd

#base_model_name = "meta-llama/Llama-2-7b-hf"
base_model_name = "mistralai/Mistral-7B-Instruct-v0.2"

system_prompt = """You are a highly knowledgeable and professional Databricks Support Agent. Your goal is to assist users with their questions and issues related to Databricks. Answer questions as precisely and accurately as possible, providing clear and concise information. If you do not know the answer, respond with "I don't know." Be polite and professional in your responses. Provide accurate and detailed information related to Databricks. If the question is unclear, ask for clarification.\n"""

@pandas_udf("array<struct<role:string, content:string>>")
def create_conversation(content: pd.Series, question: pd.Series, answer: pd.Series) -> pd.Series:
    def build_message(c,q,a):
        user_input = f"Here is a documentation page that could be relevant: {c}. Based on this, answer the following question: {q}"
        if "mistral" in base_model_name:
            #Mistral doesn't support system prompt
            return [
                {"role": "user", "content": f"{system_prompt} \n{user_input}"},
                {"role": "assistant", "content": a}]
        else:
            return [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_input},
                {"role": "assistant", "content": a}]
    return pd.Series([build_message(c,q,a) for c, q, a in zip(content, question, answer)])

training_dataset.select(create_conversation("content", "question", "answer").alias('messages')).write.mode('overwrite').saveAsTable("chat_completion_training_dataset")
display(spark.table('chat_completion_training_dataset'))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## Starting a finetuning run
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/llm-fine-tuning/databricks-llm-fine-tuning-3.png?raw=true" width="700px" style="float: right">
# MAGIC
# MAGIC #### Training data type
# MAGIC
# MAGIC Databricks supports all kind of dataset (Volume files, Delta tables, and public Hugging Face datasets). We recommend preparing the dataset as Delta tables within your catalog within a proper data pipeline. Remember, this step is critical and you need to make sure your training dataset is of high quality.
# MAGIC
# MAGIC Once the training is done, your model will automatically be saved within Unity Catalog and available for you to serve!

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC #### 1.1) Instruction finetune our baseline model.
# MAGIC
# MAGIC <img src="https://docs.databricks.com/en/_images/finetuning-expt.png" style="float: right" width="500px">
# MAGIC
# MAGIC In this demo, we'll be using the API on the table we just created to programatically finetune our LLM.
# MAGIC
# MAGIC However, you can also create a new Fine Tuning experiment from the UI!

# COMMAND ----------

# DBTITLE 1,EngineerCodeSnippetName
from databricks.model_training import foundation_model as fm
#Return the current cluster id to use to read the dataset and send it to the fine tuning cluster. See https://docs.databricks.com/en/large-language-models/foundation-model-training/create-fine-tune-run.html#cluster-id
def get_current_cluster_id():
  import json
  return json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().safeToJson())['attributes']['clusterId']


#Let's clean the model name
registered_model_name = f"{catalog}.{db}." + re.sub(r'[^a-zA-Z0-9]', '_',  base_model_name)

run = fm.create(
    data_prep_cluster_id=get_current_cluster_id(),  # required if you are using delta tables as training data source. This is the cluster id that we want to use for our data prep job.
    model=base_model_name,  # Here we define what model we used as our baseline
    train_data_path=f"{catalog}.{db}.chat_completion_training_dataset",
    task_type="CHAT_COMPLETION",  # Change task_type="INSTRUCTION_FINETUNE" if you are using the fine-tuning API for completion.
    register_to=registered_model_name,
    training_duration="5ep", #only 5 epoch to accelerate the demo. Check the mlflow experiment metrics to see if you should increase this number
    learning_rate="5e-7",
)

print(run)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Tracking model fine tuning through your MLFlow experiment
# MAGIC Your can open the MLFlow Experiment run to track your fine tuning experiment. This is useful for you to know how to tune the training run (ex: add more epoch if you see your model still improves at the end of your run).
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/llm-fine-tuning/databricks-llm-fine-tuning-experiment.png?raw=true" width="1200px">

# COMMAND ----------

displayHTML(f'Open the <a href="/ml/experiments/{run.experiment_id}/runs/{run.run_id}/model-metrics">training run on MLFlow</a> to track the metrics')
#Track the run details
display(run.get_events())

#helper function waiting on the run to finish - see the _resources folder for more details
wait_for_run_to_finish(run)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC #### 1.3) Deploy Fine-Tuned model to serving endpoint
# MAGIC
# MAGIC <img src="https://docs.databricks.com/en/_images/create-provisioned-throughput-ui.png" width="600px" style="float: right">
# MAGIC
# MAGIC Once ready, the model will be available in Unity Catalog.
# MAGIC
# MAGIC You can use the UI to deploy your model. We'll do it using the API directyl:
# MAGIC
# MAGIC

# COMMAND ----------


from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import ServedEntityInput, EndpointCoreConfigInput, AutoCaptureConfigInput

serving_endpoint_name = "dbdemos_llm_fine_tuned"
w = WorkspaceClient()
endpoint_config = EndpointCoreConfigInput(
    name=serving_endpoint_name,
    served_entities=[
        ServedEntityInput(
            entity_name=registered_model_name,
            entity_version=get_latest_model_version(registered_model_name),
            min_provisioned_throughput=0, # The minimum tokens per second that the endpoint can scale down to.
            max_provisioned_throughput=100,# The maximum tokens per second that the endpoint can scale up to.
            scale_to_zero_enabled=True
        )
    ],
    auto_capture_config = AutoCaptureConfigInput(catalog_name=catalog, schema_name=db, enabled=True, table_name_prefix="fine_tuned_llm_inference" )
)

force_update = False #Set this to True to release a newer version (the demo won't update the endpoint to a newer model version by default)
existing_endpoint = next(
    (e for e in w.serving_endpoints.list() if e.name == serving_endpoint_name), None
)
if existing_endpoint == None:
    print(f"Creating the endpoint {serving_endpoint_name}, this will take a few minutes to package and deploy the endpoint...")
    w.serving_endpoints.create_and_wait(name=serving_endpoint_name, config=endpoint_config)
else:
  print(f"endpoint {serving_endpoint_name} already exist...")
  if force_update:
    w.serving_endpoints.update_config_and_wait(served_entities=endpoint_config.served_entities, name=serving_endpoint_name)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC #### 1.4) Trying our model endpoint
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/llm-fine-tuning/databricks-llm-fine-tuning-4.png?raw=true" width="600px" style="float: right">
# MAGIC
# MAGIC That's it, we're ready to serve our Fine Tune model and start asking questions!
# MAGIC
# MAGIC The reponses will be improved and specialized with Databricks documentation and our RAG expected output!

# COMMAND ----------

import mlflow
from mlflow import deployments
#remove the answer to get only the system + user role.
test_dataset = spark.table('chat_completion_training_dataset').selectExpr("slice(messages, 1, size(messages)-1) as messages").limit(1)
#Get the first messages
messages = test_dataset.toPandas().iloc[0].to_dict()['messages'].tolist()

client = mlflow.deployments.get_deploy_client("databricks")
client.predict(endpoint="dbdemos_fine_tuned", inputs={"messages": messages, "max_tokens": 100})

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Next step: evaluating our fine tuned model
# MAGIC
# MAGIC This is looking good! Fine tuning our model was just a simple API call. But how can we measure the improvement compared to the plain model?
# MAGIC
# MAGIC Databricks makes it easy! Let's leverage MLFlow Evaluate capabilities to compare the fine tune model against the baseline Foundation Model.
# MAGIC
# MAGIC Open the [02-llm-evaluation]($./02-llm-evaluation) notebook to benchmark our new custom LLM!
