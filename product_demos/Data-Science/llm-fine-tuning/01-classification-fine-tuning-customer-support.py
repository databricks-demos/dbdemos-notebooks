# Databricks notebook source
# MAGIC %md
# MAGIC # Fine-tuning your LLM with Databricks Mosaic AI
# MAGIC
# MAGIC Fine Tuning LLMs is the action of specializing an existing model to your own requirements.
# MAGIC Technically speaking, you start from the weights of an existing foundation model (such as DBRX or LLAMA), and add another training round based on your own dataset.
# MAGIC
# MAGIC
# MAGIC ## Why Fine Tuning?
# MAGIC Databricks provides an easy way to specialize existing foundation models, making sure you own and control your model while also getting better performance, lower cost, and tighter security and privacy.
# MAGIC
# MAGIC The typical fine-tuning use cases are:
# MAGIC * Train a model on specific, internal knowledge
# MAGIC * Customize model behavior, ex: specialized entity extraction
# MAGIC * Reduce model size and inference cost, while improving answer quality
# MAGIC
# MAGIC ## Continued Pre-Training vs Instruction Fine Tuning?
# MAGIC
# MAGIC Databricks Fine Tuning API enables you to adapt your model in several different ways:
# MAGIC * **Supervised fine-tuning**: Train your model on structured prompt-response data. Use this to adapt your model to a new task, change its response style, or add instruction-following capabilities).
# MAGIC * **Continued pre-training**: Train your model with additional unlabeled text data. Use this to add new knowledge to a model or focus a model on a specific domain. Requires several millions of token to be relevant.
# MAGIC * **Chat completion**: Train your model on chat logs between a user and an AI assistant. This format can be used both for actual chat logs, and as a standard format for question answering and conversational text. The text is automatically formatted into the appropriate chat format for the specific model.
# MAGIC
# MAGIC we'll use the **Chat Completion** api as it is the recommended way. Using this, Databricks will properly format the system prompt based on your underlying model.
# MAGIC
# MAGIC ### Want to access our more advanced demos?
# MAGIC If you already know how to use the Fine Tuning API, you can directly jump to the advanced demos:
# MAGIC - [Named Entity Extraction and evaluation]($./03-entity-extraction-fine-tuning/03.1-llm-entity-extraction-drug-fine-tuning)
# MAGIC - [RAG LLM fine tuning]($./02-chatbot-rag-fine-tuning/02.1-llm-rag-fine-tuning) 
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## Fine Tuning to classify our customer tickets and accelerate time to resolution 
# MAGIC
# MAGIC In this demo, we will show you how to specialize a LLM to classify URGENT / CRITICAL tickets and put them with a high priority in the queue.
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/llm-fine-tuning/databricks-llm-fine-tuning-classif-0.png?raw=true" width="1200px">
# MAGIC
# MAGIC We'll fine tune a small Llama 3.2-3B to improve accuracy while reducing cost.
# MAGIC - We will compare this to a Llama 3.1-8B baseline model to show how a fine-tuned, smaller model can perform better than a larger one.
# MAGIC
# MAGIC To do so, Databricks provides a simple, built-in API to fine tune the model and evaluate its performance. Let's get started!
# MAGIC
# MAGIC Documentation for Fine Tuning on Databricks:
# MAGIC - Overview page: [Databricks Fine-tuning APIs](https://docs.databricks.com/en/large-language-models/foundation-model-training/index.html)
# MAGIC - SDK Guide: [Setting up a fine-tuning session with Fine-tuning APIs](https://docs.databricks.com/en/large-language-models/foundation-model-training/create-fine-tune-run.html)
# MAGIC

# COMMAND ----------

# Let's start by installing our libraries
%pip install --quiet databricks-genai==1.1.4 mlflow==2.16.2
%pip install --quiet databricks-sdk==0.39.0
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./_resources/00-setup

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## Preparing our Training Dataset
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/llm-fine-tuning/databricks-llm-fine-tuning-classif-1.png?raw=true" width="700px" style="float: right">
# MAGIC
# MAGIC ### Lets take a look at our current support tickets
# MAGIC
# MAGIC Based on the ticket text (emails or other), we want to train our model to predict its priority:

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from customer_tickets limit 10

# COMMAND ----------

# MAGIC %md
# MAGIC ### Crafting our prompt to classify the ticket
# MAGIC Let's craft a prompt example to classify the tickets, including a few shot prompt example to make it as good as possible:

# COMMAND ----------

system_prompt = """
### Instruction
You are tasked as a service request processing agent responsible for reading the descriptions of tickets and categorizing each into one of the predefined categories. Please categorize each ticket into one of the following specific categories: 

Not Urgent 
Urgent
Impacting the Prod

Do not create or use any categories other than those explicitly listed above. Return only single category as response. If there is confusion between multiple categories error on the side of assigning higher severity.

Impacting the Prod is more severe than Urgent
Urgent is more severe than Not Urgent

###Example Input
We have noticed an issue with our Databricks workspace objects, specifically with clusters. Some of our production ETL pipelines and ad-hoc analytics jobs are being affected. The clusters seem to be unresponsive and we are unable to run any commands. This is impacting our prod and we need urgent assistance.

###Response
Impacting the Prod

Based on the above categorize the following issue: \n\n"""

# COMMAND ----------

# MAGIC %md
# MAGIC Lets use the standalone llama 3.1 8b model to test it on a vanilla model first. Set up a [provisioned throughput endpoint](https://docs.databricks.com/en/machine-learning/foundation-models/deploy-prov-throughput-foundation-model-apis.html) for llama 3.1 8b by going to unity catalog, (under `system.ai.meta_llama_3_8b_instruct`), clicking "Serve this model" and making the endpoint.
# MAGIC - Be sure to check the "Scale to zero" field to ensure that the endpoint doesn't continue running when not in use.
# MAGIC - Give the endpoint a name and reference it below.

# COMMAND ----------

# Make sure you put your PROVISIONED THROUGHPUT ENDPOINT NAME HERE.
# You can also try the foundation model API, however this will be slower: databricks-meta-llama-3-1-70b-instruct
llama_3_1_8b_endpoint = "databricks-meta-llama-3-1-70b-instruct" #"meta_llama_3_8b_instruct"

# COMMAND ----------

spark.sql(f"""SELECT 
            ai_query("{llama_3_1_8b_endpoint}", concat("{system_prompt}", description)) AS llama_3_1_8b,
            description
        FROM customer_tickets 
        LIMIT 5""").display()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC As you can see, this isn't ideal. It's adding lot of text, and isn't classifying properly our dataset (resultat might vary depending on the size of the model used).
# MAGIC
# MAGIC ## Preparing the Dataset for Chat Completion for Fine Tuning
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/llm-fine-tuning/databricks-llm-fine-tuning-classif-2.png?raw=true" width="700px" style="float: right">
# MAGIC
# MAGIC Using the completion API is always recommended as default option as Databricks will properly format the final training prompt for you.
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

spark.sql(f"""
CREATE OR REPLACE TABLE ticket_priority_training_dataset AS
SELECT 
    ARRAY(
        STRUCT('user' AS role, CONCAT('{system_prompt}', '\n', description) AS content),
        STRUCT('assistant' AS role, priority AS content)
    ) AS messages
FROM customer_tickets;
""")

spark.table('ticket_priority_training_dataset').display()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## Starting a Fine Tuning Run
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/llm-fine-tuning/databricks-llm-fine-tuning-classif-3.png?raw=true" width="700px" style="float: right">
# MAGIC
# MAGIC Once the training is done, your model will automatically be saved within Unity Catalog and available for you to serve!
# MAGIC
# MAGIC In this demo, we'll be using the API on the table we just created to programatically fine tune our LLM.
# MAGIC
# MAGIC However, you can also create a new Fine Tuning experiment from the UI!

# COMMAND ----------

# MAGIC %md
# MAGIC Let's fine tune a Llama 3.2-3B model to see how it performs by comparison.

# COMMAND ----------

from databricks.model_training import foundation_model as fm
import mlflow

mlflow.set_registry_uri("databricks-uc")

base_model_name = "meta-llama/Llama-3.2-3B-Instruct"

#Let's clean the model name
registered_model_name = f"{catalog}.{db}.classif_" + re.sub(r'[^a-zA-Z0-9]', '_',  base_model_name)


# COMMAND ----------

# MAGIC %md 
# MAGIC #### Tracking Fine Tuning Runs via the MLFlow Experiment
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
# MAGIC #### Deploy Fine Tuned Model to a Serving Endpoint
# MAGIC
# MAGIC <img src="https://docs.databricks.com/en/_images/create-provisioned-throughput-ui.png" width="600px" style="float: right">
# MAGIC
# MAGIC Once ready, the model will be available in Unity Catalog.
# MAGIC
# MAGIC From here, you can use the UI to deploy your model, or you can use the API. For reproducibility, we'll be using the API below:
# MAGIC

# COMMAND ----------


from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import ServedEntityInput, EndpointCoreConfigInput

serving_endpoint_name = "dbdemos_classification_fine_tuned_01_llama_3_2_3B_Instruct"
w = WorkspaceClient()
endpoint_config = EndpointCoreConfigInput(
    name=serving_endpoint_name,
    served_entities=[
        ServedEntityInput(
            entity_name=registered_model_name,
            entity_version=get_latest_model_version(registered_model_name),
            min_provisioned_throughput=0, # The minimum tokens per second that the endpoint can scale down to.
            max_provisioned_throughput=1000,# The maximum tokens per second that the endpoint can scale up to. 
            scale_to_zero_enabled=True
        )
    ]
)

force_update = False #Set this to True to release a newer version (the demo won't update the endpoint to a newer model version by default)
try:
  existing_endpoint = w.serving_endpoints.get(serving_endpoint_name)
  print(f"endpoint {serving_endpoint_name} already exist - force update = {force_update}...")
  if force_update:
    w.serving_endpoints.update_config_and_wait(served_entities=endpoint_config.served_entities, name=serving_endpoint_name)
except:
    print(f"Creating the endpoint {serving_endpoint_name}, this will take a few minutes to package and deploy the endpoint...")
    w.serving_endpoints.create_and_wait(name=serving_endpoint_name, config=endpoint_config)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC #### Testing the Model Endpoint
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/llm-fine-tuning/databricks-llm-fine-tuning-classif-4.png?raw=true" width="700px" style="float: right">
# MAGIC
# MAGIC
# MAGIC That's it! We're now ready to serve our Fine Tuned model and start asking questions!
# MAGIC
# MAGIC The reponses will now be improved and specialized from the Databricks documentation and our RAG chatbot formatted output!

# COMMAND ----------

df = spark.sql(f""" 
        SELECT 
            ai_query("{serving_endpoint_name}", concat("{system_prompt}", description)) AS fine_tuned_prediction,
            description,
            email
        FROM customer_tickets 
        LIMIT 5""")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next steps
# MAGIC
# MAGIC In this notebook, we covered the basics on Fine Tuning using Databricks Mosaic AI FT API.
# MAGIC
# MAGIC A few extra steps are usually required to test and evaluate your Fine Tune model before deploying them in production.
# MAGIC
# MAGIC Explore the 2 other use-cases to discover:
# MAGIC
# MAGIC ### Fine tune your Chat Bot / Assistant RAG model
# MAGIC
# MAGIC [Open the 02.1-llm-rag-fine-tuning]($./02-chatbot-rag-fine-tuning/02.1-llm-rag-fine-tuning) notebook to explore how to evaluate your LLM using Databricks built-in eval capabilities.
# MAGIC
# MAGIC ### Entity extraction and evaluation
# MAGIC
# MAGIC [Open the 03.1-llm-entity-extraction-drug-fine-tuning]($./03-entity-extraction-fine-tuning/03.1-llm-entity-extraction-drug-fine-tuning) notebook for a Named Entity Extraction (NER) example, benchmarking the based model with the Fine Tuned one.
# MAGIC
