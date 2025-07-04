# Databricks notebook source
# MAGIC %pip install -U -qqqq mlflow>=3.1.1 langchain langgraph==0.5.0 databricks-langchain pydantic databricks-agents unitycatalog-langchain[databricks] uv
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC # Hands-On Lab: Building Agent Systems with Databricks
# MAGIC
# MAGIC ## Part 2 - Agent Evaluation
# MAGIC Now that we've created an agent, how do we evaluate its performance?
# MAGIC For the second part, we're going to create a product support agent so we can focus on evaluation.
# MAGIC This agent will use a RAG approach to help answer questions about products using the product documentation.
# MAGIC
# MAGIC ### 2.1 Define our new Agent and retriever tool
# MAGIC - [**agent.py**]($./agent.py): An example Agent has been configured - first we'll explore this file and understand the building blocks
# MAGIC - **Vector Search**: We've created a Vector Search endpoint that can be queried to find related documentation about a specific product.
# MAGIC - **Create Retriever Function**: Define some properties about our retriever and package it so it can be called by our LLM.
# MAGIC
# MAGIC ### 2.2 Create Evaluation Dataset
# MAGIC - We've provided an example evaluation dataset - though you can also generate this [synthetically](https://www.databricks.com/blog/streamline-ai-agent-evaluation-with-new-synthetic-data-capabilities).
# MAGIC
# MAGIC ### 2.3 Run MLflow.evaluate() 
# MAGIC - MLflow will take your evaluation dataset and test your agent's responses against it
# MAGIC - LLM Judges will score the outputs and collect everything in a nice UI for review
# MAGIC
# MAGIC ### 2.4 Make Needed Improvements and re-run Evaluations
# MAGIC - Take feedback from our evaluation run and change retrieval settings
# MAGIC - Run evals again and see the improvement!

# COMMAND ----------

# MAGIC %run ../_resources/01-setup

# COMMAND ----------

import yaml
import mlflow

rag_chain_config = {
    "config_version_name": "first_config",
    "input_example": [{"role": "user", "content": "Give me the orders for john21@example.net"}],
    "uc_tool_names": [f"{catalog}.{dbName}.*"],
    "system_prompt": "Your job is to provide customer help. call the tool to answer.",
    "llm_endpoint_name": "databricks-claude-3-7-sonnet",
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
# MAGIC Let's just import it and send a request to explore its internal tracing with MLFlow Trace UI:

# COMMAND ----------

from agent import AGENT
from mlflow.types.responses import ResponsesAgentRequest

# Correct request format
request_example = "Give me the information about john21@example.net"
answer = AGENT.predict(ResponsesAgentRequest(input=[{"role": "user", "content": request_example}]))

# COMMAND ----------

# MAGIC %md
# MAGIC ### MLFlow tracing
# MAGIC Open now the experiment from the right notebook menu. you'll see the message we just sent: `Give me the information about john21@example.net`.
# MAGIC
# MAGIC MLFlow keep tracks of all the input/output and internal tracing so that we can analyze existing request, and create better evaluation dataset!

# COMMAND ----------

# MAGIC %md
# MAGIC ### Log the `agent` as an MLflow model
# MAGIC This looks good! let's log the agent in our MLFlow registry using the [agent]($./agent) python file to avoid any serialization issue. See [MLflow - Models from Code](https://mlflow.org/docs/latest/models.html#models-from-code).
# MAGIC
# MAGIC Note that we'll also pass the list of Databricks resources (functions, warehouse etc) that our agent need to use to properly work. This will handle the permissions for us during its deployment!

# COMMAND ----------

import mlflow
def log_customer_support_agent_model(resources, request_example, run_name=None):
    with mlflow.start_run(run_name=model_config.get('config_version_name')):
        return mlflow.pyfunc.log_model(
            name="agent",
            python_model="agent.py",
            model_config="agent_config.yaml",
            input_example=ResponsesAgentRequest(input=[{"role": "user", "content": request_example}]),
            resources=resources, # Determine Databricks resources (endpoints, fonctions, vs...) to specify for automatic auth passthrough at deployment time
            extra_pip_requirements=["databricks-connect"]
        )
logged_agent_info = log_customer_support_agent_model(AGENT.get_resources(), request_example, "first_model")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Let's try our model
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
# MAGIC ## Evaluate the agent with [Agent Evaluation](https://docs.databricks.com/generative-ai/agent-evaluation/index.html)
# MAGIC
# MAGIC We prepared an evaluation dataset ready to use as part of this demo. However, multiple strateges exist:
# MAGIC
# MAGIC - create your own evaluation dataset (what we'll do)
# MAGIC - use existing traces from MLFlow and add them to your dataset
# MAGIC - use Databricks genai eval synthetic dataset creation (see the PDF RAG folder for an example)
# MAGIC - Create labeling session where you can get insights from expert, using the MLFlow UI directly!
# MAGIC
# MAGIC Note: you can also select the existing LLM call from the traces and add to your eval dataset with the UI, or use the API directly:
# MAGIC
# MAGIC ```
# MAGIC traces = mlflow.search_traces(filter_string=f"attributes.timestamp_ms > {ten_minutes_ago} AND attributes.status = 'OK'", order_by=["attributes.timestamp_ms DESC"])
# MAGIC ```

# COMMAND ----------

eval_example = spark.read.json(f"/Volumnes/{catalog}/{dbName}/{volume_name}/eval_dataset")
display(eval_example)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Create our MLFlow dataset
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

# MAGIC %md
# MAGIC ## Adding guidelines to track our agent behavior
# MAGIC MLFlow 3.0 lets you create custom guidelines to evaluate your agent behavior.
# MAGIC
# MAGIC We'll use a few of the built-in one, and add a custome `Guidelines` on steps and reasoning: we want our LLM to output the answer without mentioning the internal tools it has.

# COMMAND ----------

from mlflow.genai.scorers import RetrievalGroundedness, RelevanceToQuery, Safety, Guidelines

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

# COMMAND ----------

print("Running evaluation...")
with mlflow.start_run(run_name='first_eval'):
    results = mlflow.genai.evaluate(data=eval_dataset, predict_fn=predict_wrapper, scorers=scorers)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Improving our eval metrics with a better system prompt
# MAGIC
# MAGIC As we can see in the eval, the agent emits a lot of information on the internal tools and steps. 
# MAGIC For example; it would mention things like:
# MAGIC
# MAGIC `First, I need to find his customer record using his email address. Since I don't have Thomas Green's email address yet, I need to ask for it.`
# MAGIC
# MAGIC While this is good reasoning, we do not want this in the final answer!
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

print("Running evaluation...")
with mlflow.start_run(run_name='eval_with_no_reasoning_instructions'):
    results = mlflow.genai.evaluate(data=eval_dataset, predict_fn=predict_wrapper, scorers=scorers)

# Let's relog our agent to capture the new prompt
logged_agent_info = log_customer_support_agent_model(AGENT.get_resources(), request_example)

# COMMAND ----------

# MAGIC %md 
# MAGIC Open your experiment and check the results!
# MAGIC
# MAGIC Compare the previous run and the new one, we should see some improvements

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register the model to Unity Catalog
# MAGIC
# MAGIC Update the `catalog`, `schema`, and `model_name` below to register the MLflow model to Unity Catalog.

# COMMAND ----------

model_name = "customer_support_agent"
UC_MODEL_NAME = f"{catalog}.{dbName}.{model_name}"

# register the model to UC
uc_registered_model_info = mlflow.register_model(model_uri=logged_agent_info.model_uri, name=UC_MODEL_NAME)

# COMMAND ----------

# Create HTML link to created agent
displayHTML(f'<a href="/explore/data/models/{catalog}/{dbName}/product_agent" target="_blank">Open Unity Catalog to see Registered Agent</a>')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deploy the agent
# MAGIC
# MAGIC ##### Note: This is disabled for lab users but will work on your own workspace

# COMMAND ----------

from databricks import agents

# Deploy the model to the review app and a model serving endpoint

#Disabled for the lab environment but we've deployed the agent already!
# agents.deploy(UC_MODEL_NAME, uc_registered_model_info.version, tags = {"endpointSource": "Lab Admin", "project": "dbdemos"})
