# Databricks notebook source
# MAGIC %md
# MAGIC # Part 2 - Evaluating and Deploying our Agent Systems
# MAGIC
# MAGIC We are going to pacakge our tools together using Langchain (in the agent.py file), and use it as a first agent version to run our evaluation!
# MAGIC
# MAGIC ## Agent Evaluation with MLFlow 3
# MAGIC Now that we've created an agent, we need to measure its performance, and find a way to compare it with previous versions.
# MAGIC
# MAGIC Databricks makes it very easy with MLFlow 3. You can automatically:
# MAGIC
# MAGIC - Trace all your agent input/output
# MAGIC - Capture end user feedback
# MAGIC - Evaluate your agent against custom or synthetic evaluation dataset
# MAGIC - Build labeled dataset with your business expert
# MAGIC - Compare each evaluation against the previous one
# MAGIC - Deploy and track your evaluations once deployed in production 
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/ai-agent/mlflow-evaluate-0.png?raw=true?raw=true" width="800px">
# MAGIC
# MAGIC ### Our agent is composed of:
# MAGIC
# MAGIC - [**agent.py**]($./agent.py): in this file, we used Langchain to prepare an agent ready to be used.
# MAGIC - [**agent_config.yaml**]($./agent_config.yaml): this file contains our agent configuration, including the system prompt and the LLM endpoint that we'll use
# MAGIC
# MAGIC Let's get started and try our Langchain agent in this notebook!
# MAGIC
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection or disable tracker during installation. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=data-science&org_id=1444828305810485&notebook=02.1_agent_evaluation&demo_name=ai-agent&event=VIEW">
# MAGIC

# COMMAND ----------

# MAGIC %pip install -U -qqqq mlflow>=3.1.4 langchain langgraph databricks-langchain pydantic databricks-agents unitycatalog-langchain[databricks] uv
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../_resources/01-setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1/ Build and register our agent
# MAGIC
# MAGIC ### 1.1/ Define our agent configuration
# MAGIC Let's first update our configuration file with the tools we want our langchain agent to use, a basic system prompt and the endpoint we want to use.

# COMMAND ----------

# DBTITLE 1,update our configuration file
import yaml
import mlflow

rag_chain_config = {
    "config_version_name": "first_config",
    "input_example": [{"role": "user", "content": "Give me the orders for john21@example.net"}],
    "uc_tool_names": [f"{catalog}.{dbName}.*"],
    "system_prompt": "Your job is to provide customer help. call the tool to answer.",
    "llm_endpoint_name": "databricks-claude-3-7-sonnet",
    "max_history_messages": 20,
    "retriever_config": None
}
try:
    with open('agent_config.yaml', 'w') as f:
        yaml.dump(rag_chain_config, f)
except:
    print('pass to work on build job')
model_config = mlflow.models.ModelConfig(development_config='agent_config.yaml')

# COMMAND ----------

# MAGIC %md
# MAGIC We created our AGENT using langchain in the `agent.py` file. You can explore it to see the code behind the scene.
# MAGIC
# MAGIC In this notebook, we'll keep it simple and just import it and send a request to explore its internal tracing with MLFlow Trace UI:

# COMMAND ----------

from agent import AGENT

# Correct request format
request_example = "Give me the information about john21@example.net"
answer = AGENT.predict({"input":[{"role": "user", "content": request_example}]})

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ### 1.2/ Open MLFlow tracing
# MAGIC
# MAGIC <img src="https://i.imgur.com/tNYUHdC.gif" style="float: right" width="700px">
# MAGIC
# MAGIC Open now the experiment from the right notebook menu. You'll see in the traces the message we just sent: `Give me the information about john21@example.net`.
# MAGIC
# MAGIC MLFlow keeps track of all the input/output and internal tracing so that we can analyze existing request, and create better evaluation dataset over time!
# MAGIC
# MAGIC Not only MLFlow traces all your agent request, but you can also easily capture end-users feedback to quickly detect which answer was wrong and improve your agent accordingly! 
# MAGIC
# MAGIC *We'll show you how to capture feedback when we'll deploy the application!*

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.3/ Log the `agent` as an MLflow model
# MAGIC
# MAGIC This looks good! Let's log the agent in our MLFlow registry using the [agent]($./agent) python file to avoid any serialization issue. See [MLflow - Models from Code](https://mlflow.org/docs/latest/models.html#models-from-code).
# MAGIC
# MAGIC *Note that we'll also pass the list of Databricks resources (functions, warehouse etc) that our agent need to use to properly work. This will handle the permissions for us during its deployment!*

# COMMAND ----------

import mlflow
def log_customer_support_agent_model(resources, request_example):
    with mlflow.start_run(run_name=model_config.get('config_version_name')):
        return mlflow.pyfunc.log_model(
            name="agent",
            python_model="agent.py",
            model_config="agent_config.yaml",
            input_example={"input": [{"role": "user", "content": request_example}]},
            resources=resources, # Determine Databricks resources (endpoints, fonctions, vs...) to specify for automatic auth passthrough at deployment time
            extra_pip_requirements=["databricks-connect"]
        )
logged_agent_info = log_customer_support_agent_model(AGENT.get_resources(), request_example)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.4/ Let's load and try our model
# MAGIC Our model is saved on MLFlow! Let's load it and give it a try. We'll wrap our predict function so that we can extract more easily the final answer, and also make our evaluation easier:

# COMMAND ----------

import pandas as pd
# Load the model and create a prediction function
loaded_model = mlflow.pyfunc.load_model(f"runs:/{logged_agent_info.run_id}/agent")
def predict_wrapper(question):
    # Format for chat-style models
    model_input = pd.DataFrame({
        "input": [[{"role": "user", "content": question}]]
    })
    response = loaded_model.predict(model_input)
    return response['output'][-1]['content'][-1]['text']

answer = predict_wrapper("Give me the orders for john21@example.net.")
print(answer)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2/ Evaluation
# MAGIC
# MAGIC ### 2.1/ Evaluate the agent with [Agent Evaluation](https://docs.databricks.com/aws/en/mlflow3/genai/eval-monitor/)
# MAGIC
# MAGIC We prepared an evaluation dataset ready to use as part of this demo. However, multiple strateges exist:
# MAGIC
# MAGIC - create your own evaluation dataset (what we'll do)
# MAGIC - use existing traces from MLFlow and add them to your dataset (from the API or your experiment UI)
# MAGIC - use Databricks genai eval synthetic dataset creation (see the [PDF RAG notebook]($../03-knowledge-base-rag/03.1-pdf-rag-tool) for an example)
# MAGIC - Create labeling session where you can get insights from expert, using the MLFlow UI directly!
# MAGIC
# MAGIC Note: you can also select the existing LLM call from the traces and add to your eval dataset with the UI, or use the API directly:
# MAGIC
# MAGIC ```
# MAGIC traces = mlflow.search_traces(filter_string=f"attributes.timestamp_ms > {ten_minutes_ago} AND attributes.status = 'OK'", order_by=["attributes.timestamp_ms DESC"])
# MAGIC ```

# COMMAND ----------

eval_example = spark.read.json(f"/Volumes/{catalog}/{dbName}/{volume_name}/eval_dataset")
display(eval_example)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### 2.2/ Create our MLFlow dataset
# MAGIC Let's use the API to create our dataset. You can also directly do it from the Experiment UI!

# COMMAND ----------

import mlflow
import mlflow.genai.datasets

eval_dataset_table_name = f"{catalog}.{dbName}.ai_agent_mlflow_eval"

try:
  eval_dataset = mlflow.genai.datasets.get_dataset(eval_dataset_table_name)
except Exception as e:
  if 'does not exist' in str(e):
    eval_dataset = mlflow.genai.datasets.create_dataset(eval_dataset_table_name)
    # Add your examples to the evaluation dataset
    eval_dataset.merge_records(eval_example)
    print("Added records to the evaluation dataset.")

# Preview the dataset
display(eval_dataset.to_df())

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## 2.3/ Adding guidelines to track our agent behavior
# MAGIC
# MAGIC <img src="https://i.imgur.com/M3kLBHF.gif" style="float:right" width="700px">
# MAGIC
# MAGIC MLFlow 3.0 lets you create custom guidelines to evaluate your agent behavior.
# MAGIC
# MAGIC We'll use a few of the built-in one, and add a custome `Guidelines` on steps and reasoning: we want our LLM to output the answer without mentioning the internal tools it has.

# COMMAND ----------

from mlflow.genai.scorers import RetrievalGroundedness, RelevanceToQuery, Safety, Guidelines

def get_scorers():
    return [
        RetrievalGroundedness(),  # Checks if email content is grounded in retrieved data
        RelevanceToQuery(),  # Checks if email addresses the user's request
        Safety(),  # Checks for harmful or inappropriate content
        Guidelines(
            guidelines="""Reponse must be done without showing reaso
            ning.
            - don't mention that you need to look up things
            - do not mention tools or function used
            - do not tell your intermediate steps or reasoning""",
            name="steps_and_reasoning",
        )
    ]

scorers = get_scorers()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.4/ Run the evaluations against our guidelines
# MAGIC
# MAGIC That's it, let's now evaluate our dataset with our guidelines:

# COMMAND ----------

with mlflow.start_run(run_name='first_eval'):
    results = mlflow.genai.evaluate(data=eval_dataset, predict_fn=predict_wrapper, scorers=scorers)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3/ Improving our eval metrics with a better system prompt
# MAGIC
# MAGIC As we can see in the eval, the agent emits a lot of information on the internal tools and steps. 
# MAGIC For example; it would mention things like:
# MAGIC
# MAGIC `First, I need to find his customer record using his email address. Since I don't have Thomas Green's email address yet, I need to ask for it.`
# MAGIC
# MAGIC While this is good reasoning, we do not want this in the final answer!
# MAGIC
# MAGIC ### 3.1/ Deploying a new model version with a better system prompt
# MAGIC
# MAGIC Let's update our system prompt with better instruction to avoid this behavior, and run our eval to make sure this improved!

# COMMAND ----------

try:
    config = yaml.safe_load(open("agent_config.yaml"))
    config["config_version_name"] = "better_prompt"
    config["system_prompt"] = (
        "You are a telco assistant. Call the appropriate tool to help the user with billing, support, or account info. "
        "DO NOT mention any internal tool or reasoning steps in your final answer."
    )
    yaml.dump(config, open("agent_config.yaml", "w"))
except Exception as e:
    print(f"Skipped update - ignore for job run - {e}")

# COMMAND ----------

with mlflow.start_run(run_name='eval_with_no_reasoning_instructions'):
    results = mlflow.genai.evaluate(data=eval_dataset, predict_fn=predict_wrapper, scorers=scorers)

# Let's relog our agent to capture the new prompt
logged_agent_info = log_customer_support_agent_model(AGENT.get_resources(), request_example)

# COMMAND ----------

# MAGIC %md 
# MAGIC Open your experiment and check the results!
# MAGIC
# MAGIC Select the previous run and this one, and compare them. You should see some improvements!

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4/ Deploy our agent as an endpoint!
# MAGIC
# MAGIC Everything looks good! Our latest version now has decent eval score. Let's deploy it as a realtime endpoint for our end user chat application.
# MAGIC
# MAGIC ### 4.1/ Register our new model version to Unity Catalog
# MAGIC

# COMMAND ----------

from mlflow import MlflowClient
UC_MODEL_NAME = f"{catalog}.{dbName}.{MODEL_NAME}"

# register the model to UC
client = MlflowClient()
uc_registered_model_info = mlflow.register_model(model_uri=logged_agent_info.model_uri, name=UC_MODEL_NAME, tags={"model": "customer_support_agent"})
client.set_registered_model_alias(name=UC_MODEL_NAME, alias="model-to-deploy", version=uc_registered_model_info.version)

# Create HTML link to created agent
displayHTML(f'<a href="/explore/data/models/{catalog}/{dbName}/{MODEL_NAME}" target="_blank">Open Unity Catalog to see Registered Agent</a>')

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2/ Deploy the agent
# MAGIC
# MAGIC Let's now start our model endpoint:

# COMMAND ----------

from databricks import agents
# Deploy the model to the review app and a model serving endpoint
if len(agents.get_deployments(model_name=UC_MODEL_NAME, model_version=uc_registered_model_info.version)) == 0:
  agents.deploy(UC_MODEL_NAME, uc_registered_model_info.version, endpoint_name=ENDPOINT_NAME, tags = {"project": "dbdemos"})

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Next: adding a tool to answer questions about our knowledge base (RAG + Vector Search on PDF)
# MAGIC
# MAGIC Our model is working well, but it can't answer specific questions that our customer support might have about their subscription.
# MAGIC
# MAGIC For example, if we ask our Agent how to solve a specific error code in our WIFI router, it'll fail as it doesn't have any valuable information about it.
# MAGIC
# MAGIC Open the [03-knowledge-base-rag/03.1-pdf-rag-tool]($../03-knowledge-base-rag/03.1-pdf-rag-tool)
