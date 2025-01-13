# Databricks notebook source
# MAGIC %md
# MAGIC # Fine-tuning your RAG Chatbot LLM with Databricks Mosaic AI
# MAGIC
# MAGIC ## Fine Tuning or RAG?
# MAGIC RAG and Instruction fine-tuning work together! If you have a relevant RAG use-case, you can start with RAG leveraging a foundational model, and then specialize your model if your corpus is specific to your business (ex: not part of the foundation model training) or need specific behavior (ex: answer to a specific task such as entity extraction)
# MAGIC
# MAGIC Start with RAG on a Foundation Model, evaluate how your model is working and where it can be improved, and build a Fine Tuned dataset accordingly!

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## Fine Tuning a Chatbot on Databricks Documentation 
# MAGIC
# MAGIC In this demo, we will fine tune Mistral (or llama) on Databricks Documentation for a RAG chatbot to help a customer answer Databricks-related questions. 
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/llm-fine-tuning/databricks-llm-fine-tuning-0.png?raw=true" width="1200px">
# MAGIC
# MAGIC Databricks provides a simple, built-in API to fine tune the model and evaluate its performance. Let's get started!
# MAGIC
# MAGIC Documentation for Fine Tuning on Databricks:
# MAGIC - Overview page: [Databricks Fine-tuning APIs](https://docs.databricks.com/en/large-language-models/foundation-model-training/index.html)
# MAGIC - SDK Guide: [Setting up a fine-tuning session with Fine-tuning APIs](https://docs.databricks.com/en/large-language-models/foundation-model-training/create-fine-tune-run.html)
# MAGIC

# COMMAND ----------

# Let's start by installing our libraries
%pip install --quiet databricks-genai==1.1.4 mlflow==2.16.2
%pip install --quiet databricks-sdk==0.40.0
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../_resources/00-setup

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Fine Tuning Dataset
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/llm-fine-tuning/databricks-llm-fine-tuning-1.png?raw=true" width="700px" style="float: right">
# MAGIC
# MAGIC Building a high quality fine tuning dataset is key for improving your model's performance. 
# MAGIC
# MAGIC The training dataset needs to match the data you will be sending to your final model. <br/>
# MAGIC If you have a RAG application, you need to fine tune with the full RAG instruction pipeline so that your model can learn how to extract the proper information from the context and answer the way you want.
# MAGIC
# MAGIC For this demo, we have loaded a fine tuning dataset for you containing the following:
# MAGIC
# MAGIC * A Databricks user question (ex: how do I start a Warehouse?)
# MAGIC * A page or chunk from Databricks documentation relevant to this question
# MAGIC * The expected answer, reviewed by a human

# COMMAND ----------

# MAGIC %md
# MAGIC ## A note on Databricks Mosaic AI Evaluation Framework
# MAGIC
# MAGIC Databricks makes it easy to build, deploy and review RAG application. Using Databricks review application, it's easy to build your Fine Tuning Dataset.
# MAGIC
# MAGIC To see how it's done, install the `dbdemos.install('llm-rag-chatbot')` demo!

# COMMAND ----------

training_dataset = spark.sql("""
  SELECT q.id as question_id, q.question, a.answer, d.url, d.content FROM training_dataset_question q
      INNER JOIN databricks_documentation d on q.doc_id = d.id
      INNER JOIN training_dataset_answer   a on a.question_id = q.id 
    WHERE answer IS NOT NULL""")
display(training_dataset)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Preparing the Dataset for Chat Completion
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/llm-fine-tuning/databricks-llm-fine-tuning-2.png?raw=true" width="700px" style="float: right">
# MAGIC
# MAGIC Because we're fine tuning a chatbot, we need to prepare our dataset for **Chat completion**.
# MAGIC
# MAGIC Chat completion requires a list of **role** and **prompt**, following the OpenAI standard. This standard has the benefit of transforming our input into a prompt following our LLM instruction pattern. <br/>
# MAGIC Note that each foundation model might be trained with a different instruction type, so it's best to use the same type when fine tuning.<br/>
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
# MAGIC #### Training Data Type
# MAGIC
# MAGIC Databricks supports a large variety of dataset formats (Volume files, Delta tables, and public Hugging Face datasets in .jsonl format), but we recommend preparing the dataset as Delta tables within your Catalog as part of a proper data pipeline to ensure production quality.
# MAGIC *Remember, this step is critical and you need to make sure your training dataset is of high quality.*
# MAGIC
# MAGIC Let's create a small pandas UDF to help create our final chat completion dataset.<br/>

# COMMAND ----------

from pyspark.sql.functions import pandas_udf
import pandas as pd

base_model_name = "meta-llama/Llama-3.2-3B-Instruct"

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


training_data, eval_data = training_dataset.randomSplit([0.9, 0.1], seed=42)

training_data.select(create_conversation("content", "question", "answer").alias('messages')).write.mode('overwrite').saveAsTable("chat_completion_training_dataset")
eval_data.write.mode('overwrite').saveAsTable("chat_completion_evaluation_dataset")

display(spark.table('chat_completion_training_dataset'))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## Starting a Fine Tuning Run
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/llm-fine-tuning/databricks-llm-fine-tuning-3.png?raw=true" width="700px" style="float: right">
# MAGIC
# MAGIC Once the training is done, your model will automatically be saved within Unity Catalog and available for you to serve!

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC #### 1.1) Instruction Fine Tune our Baseline Model
# MAGIC
# MAGIC In this demo, we'll be using the API on the table we just created to programatically fine tune our LLM.
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
# MAGIC #### 1.2) Tracking Fine Tuning Runs via the MLFlow Experiment
# MAGIC
# MAGIC To monitor the progress of an ongoing or past fine tuning run, you can open the run from the MLFlow Experiment. Here you will find valuable information on how you may wish to tweak future runs to get better results. For example:
# MAGIC * Adding more epochs if you see your model still improving at the end of your run
# MAGIC * Increasing learning rate if loss is decreasing, but very slowly
# MAGIC * Decreasing learning rate if loss is fluctuating widely
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
# MAGIC #### 1.3) Deploy Fine Tuned Model to a Serving Endpoint
# MAGIC
# MAGIC <img src="https://docs.databricks.com/en/_images/create-provisioned-throughput-ui.png" width="600px" style="float: right">
# MAGIC
# MAGIC Once ready, the model will be available in Unity Catalog.
# MAGIC
# MAGIC From here, you can use the UI to deploy your model, or you can use the API. For reproducibility, we'll be using the API below:
# MAGIC

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import (
    ServedEntityInput,
    EndpointCoreConfigInput,
    AiGatewayConfig,
    AiGatewayInferenceTableConfig
)

serving_endpoint_name = "dbdemos_llm_fine_tuned_llama3p2_3B"
w = WorkspaceClient()

# Create the AI Gateway configuration
ai_gateway_config = AiGatewayConfig(
    inference_table_config=AiGatewayInferenceTableConfig(
        enabled=True,
        catalog_name=catalog,
        schema_name=db,
        table_name_prefix="fine_tuned_llm_inference"
    )
)

endpoint_config = EndpointCoreConfigInput(
    name=serving_endpoint_name,
    served_entities=[
        ServedEntityInput(
            entity_name=registered_model_name,
            entity_version=get_latest_model_version(registered_model_name),
            min_provisioned_throughput=0,
            max_provisioned_throughput=100,
            scale_to_zero_enabled=True
        )
    ]
)

force_update = False
existing_endpoint = next(
    (e for e in w.serving_endpoints.list() if e.name == serving_endpoint_name), None
)

if existing_endpoint is None:
    print(f"Creating the endpoint {serving_endpoint_name}, this will take a few minutes to package and deploy the endpoint...")
    w.serving_endpoints.create_and_wait(name=serving_endpoint_name, config=endpoint_config, ai_gateway=ai_gateway_config)
else:
    print(f"Endpoint {serving_endpoint_name} already exists...")
    if force_update:
        w.serving_endpoints.update_config_and_wait(
            served_entities=endpoint_config.served_entities,
            name=serving_endpoint_name
        )


# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC #### 1.4) Testing the Model Endpoint
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/llm-fine-tuning/databricks-llm-fine-tuning-4.png?raw=true" width="600px" style="float: right">
# MAGIC
# MAGIC That's it! We're now ready to serve our Fine Tuned model and start asking questions!
# MAGIC
# MAGIC The reponses will now be improved and specialized from the Databricks documentation and our RAG chatbot formatted output!

# COMMAND ----------

import mlflow
from mlflow import deployments
#remove the answer to get only the system + user role.
test_dataset = spark.table('chat_completion_training_dataset').selectExpr("slice(messages, 1, size(messages)-1) as messages").limit(1)
#Get the first messages
messages = test_dataset.toPandas().iloc[0].to_dict()['messages'].tolist()

client = mlflow.deployments.get_deploy_client("databricks")
client.predict(endpoint=serving_endpoint_name, inputs={"messages": messages, "max_tokens": 100})

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Next Step: Evaluating our Fine Tuned Model
# MAGIC
# MAGIC This is looking good! Fine tuning our model was just a simple API call. But how can we measure the improvement compared to the baseline model?
# MAGIC
# MAGIC Databricks makes this easy too! In the next section, we'll leverage MLFlow Evaluate capabilities to compare the fine tune model against the baseline Foundation Model to see how successful our tuning run was.
# MAGIC
# MAGIC Open the [02.2-llm-evaluation]($./02.2-llm-evaluation) notebook to benchmark our new custom LLM!
