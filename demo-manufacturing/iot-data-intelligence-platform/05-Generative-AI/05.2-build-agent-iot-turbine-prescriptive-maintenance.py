# Databricks notebook source
# MAGIC %md-sandbox # Building the Agent System for Prescriptive Maintenance using the Mosaic AI Agent Framework
# MAGIC
# MAGIC Now that have created the Mosaic AI Tools in Unity Catalog, we will leverage the Mosaic AI Agent Framework to build, deploy and evaluate an AI agent for Prescriptive Maintenance. The Agent Framework comprises a set of tools on Databricks designed to help developers build, deploy, and evaluate production-quality AI agents like Retrieval Augmented Generation (RAG) applications. Moreover, Mosaic AI Agent Evaluation provides a platform to capture and implement human feedback, ground truth, response and request logs, LLM judge feedback, chain traces, and more.
# MAGIC
# MAGIC This notebook uses Mosaic AI Agent Framework ([AWS](https://docs.databricks.com/en/generative-ai/retrieval-augmented-generation.html) | [Azure](https://learn.microsoft.com/en-us/azure/databricks/generative-ai/retrieval-augmented-generation)) to build an agent for Prescriptive Maintenance by defining a Pyfunc agent that has access to the Mosaic AI tools create in notebook [05.1-ai-tools-iot-turbine-prescriptive-maintenance]($./05-Generative-AI/05.1-ai-tools-iot-turbine-prescriptive-maintenance). Use this notebook to iterate on and modify the agent. For example, you could add more tools or change the system prompt.
# MAGIC
# MAGIC  **_NOTE:_**  This notebook uses Pyfunc, however AI Agent Framework is compatible with other agent frameworks like Langchain and LlamaIndex.
# MAGIC
# MAGIC Thi is an high-level overview of the agent system that we will build in this demo:
# MAGIC
# MAGIC
# MAGIC <div style="text-align: center;">
# MAGIC     <img src="https://github.com/Datastohne/demo/blob/main/agent2.png?raw=true" width="900px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %pip install -U -qqqq mlflow-skinny==2.17.2
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

# COMMAND ----------

agent.predict(
    None,
    ChatCompletionRequest(
        messages=[Message(role="user", content="turbine_id = 5ef39b37-7f89-b8c2-aff1-5e4c0453377d, sensor_readings = (0.9901583695625525,2.2170412500371417,3.2607344819672837,2.3033028001321516,2.4663900152731313,4.575124113082638)")]
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
# MAGIC Go to the [05.3-deploy-agent-iot-turbine-prescriptive-maintenance]($./05.3-deploy-agent-iot-turbine-prescriptive-maintenance) notebook to log, register, and deploy the agent.
