# Databricks notebook source
# MAGIC %md
# MAGIC #Agent notebook
# MAGIC
# MAGIC This is an auto-generated notebook created by an AI Playground export. We generated three notebooks in the same folder:
# MAGIC - [**agent**]($./agent): contains the code to build the agent.
# MAGIC - [config.yml]($./config.yml): contains the configurations.
# MAGIC - [driver]($./driver): logs, evaluate, registers, and deploys the agent.
# MAGIC
# MAGIC This notebook uses Mosaic AI Agent Framework ([AWS](https://docs.databricks.com/en/generative-ai/retrieval-augmented-generation.html) | [Azure](https://learn.microsoft.com/en-us/azure/databricks/generative-ai/retrieval-augmented-generation)) to recreate your agent from the AI Playground. It defines a Pyfunc agent that has access to the tools from the source Playground session.
# MAGIC
# MAGIC Use this notebook to iterate on and modify the agent. For example, you could add more tools.
# MAGIC
# MAGIC ## Prerequisites
# MAGIC
# MAGIC - Address all `TODO`s in this notebook.
# MAGIC - Review the contents of [config.yml]($./config.yml) as it defines the tools available to your agent and the LLM endpoint.
# MAGIC
# MAGIC ## Next steps
# MAGIC
# MAGIC After testing and iterating on your agent in this notebook, go to the the auto-generated [driver]($./driver) notebook in this folder to log, register, and deploy the agent.

# COMMAND ----------

# MAGIC %pip install -U -qqqq mlflow-skinny
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import and setup
# MAGIC

# COMMAND ----------

import mlflow
from mlflow.models import ModelConfig

config = ModelConfig(development_config="config.yml")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define the Agent
# MAGIC Create a Pyfunc model that will call the Agents API with a set of tools.
# MAGIC These tools are executed within the model serving API request using serverless compute.

# COMMAND ----------

import mlflow
import mlflow.deployments

from mlflow.models.rag_signatures import (
    ChatCompletionRequest,
    ChatCompletionResponse,
    Message,
)
from dataclasses import asdict, is_dataclass

class ManagedAgentAPI(mlflow.pyfunc.PythonModel):
    def predict(
        self, context, model_input: ChatCompletionRequest
    ) -> ChatCompletionResponse:
        from mlflow.entities import Trace

        # Send a request to Databricks Model Serving with tools execution

        # Convert input to dict if necessary for model serving compatibility
        if is_dataclass(model_input):
            req = asdict(model_input).copy()
        else:
            req = model_input.copy()

        # Prepend system message if it exists
        try:
            system_message = config.get("agent_prompt")
            req["messages"].insert(0, asdict(Message(role="system", content=system_message)))
        except KeyError:
            pass

        # Add UC functions if they exist
        try:
            uc_functions = config.get("tools").get("uc_functions")
            if uc_functions:
                req["tools"] = [
                    {"type": "uc_function", "uc_function": {"name": name}}
                    for name in uc_functions
                ]
                req["databricks_options"] = {"return_trace": True}
        except KeyError:
            req["tools"] = []

        client = mlflow.deployments.get_deploy_client("databricks")

        completion = client.predict(endpoint=config.get("llm_endpoint"), inputs=req)

        # Add the trace from model serving API call to the active trace
        if trace := completion.pop("databricks_output", {}).get("trace"):
            trace = Trace.from_dict(trace)
            mlflow.add_trace(trace)

        return completion

agent = ManagedAgentAPI()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test the agent
# MAGIC
# MAGIC Interact with the agent to test its output. You can view the trace for each step the agent takes.
# MAGIC
# MAGIC Replace this placeholder input with an appropriate domain-specific example for your agent.

# COMMAND ----------

# TODO: replace this placeholder input example with an appropriate domain-specific example for your agent
agent.predict(
    None,
    ChatCompletionRequest(
        messages=[Message(role="user", content="I need a dress for an interview I have today. What style would you recommend for today in Bengalore India?")]
    ),
)

# COMMAND ----------

mlflow.models.set_model(agent)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next steps
# MAGIC
# MAGIC You can rerun the cells above to iterate and test the agent.
# MAGIC
# MAGIC Go to the auto-generated [driver]($./driver) notebook in this folder to log, register, and deploy the agent.
