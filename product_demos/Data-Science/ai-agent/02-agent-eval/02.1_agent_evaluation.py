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
# MAGIC ### our agent is composed of:
# MAGIC
# MAGIC - [**agent.py**]($./agent.py): in this file, we used Langchain to prepare an agent ready to be used.
# MAGIC - [**agent_config.yaml**]($./agent_config.yaml): this file contains our agent configuration, including the system prompt and the LLM endpoint that we'll use
# MAGIC
# MAGIC Let's get started and try our Langchain agent in this notebook!

# COMMAND ----------

# MAGIC %pip install -U -qqqq mlflow>=3.1.1 langchain langgraph==0.5.0 databricks-langchain pydantic databricks-agents unitycatalog-langchain[databricks] uv
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




# COMMAND ----------

import mlflow
mlflow.set_experiment(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().rsplit('/', 1)[0]+'/dbdemos-ai-agent-experiment')

# COMMAND ----------

from typing import Any, Generator, Optional, Sequence, List, Dict
import mlflow
from mlflow.entities import SpanType
from mlflow.pyfunc import ResponsesAgent
from mlflow.types.responses import ResponsesAgentRequest, ResponsesAgentResponse, ResponsesAgentStreamEvent

from databricks_langchain import (
    ChatDatabricks,
    VectorSearchRetrieverTool,
    DatabricksFunctionClient,
    UCFunctionToolkit,
    set_uc_function_client
)

from mlflow.models.resources import DatabricksFunction, DatabricksServingEndpoint
from unitycatalog.ai.langchain.toolkit import UnityCatalogTool

from langchain_core.messages.ai import AIMessage
from langchain_core.messages.tool import ToolMessage
from langchain_core.runnables import RunnableLambda
from langchain_core.language_models import LanguageModelLike
from langchain_core.tools import BaseTool

from langgraph.graph import END, StateGraph
from langgraph.graph.state import CompiledStateGraph
from langgraph.prebuilt.tool_node import ToolNode
import uuid

# Automatically log LangChain events to MLflow
mlflow.langchain.autolog()

# Set up Databricks Function client for Unity Catalog function toolkit
set_uc_function_client(DatabricksFunctionClient())

class ToolCallingAgent(ResponsesAgent):
    """
    Agent class using LangGraph and MLflow to orchestrate tool-calling behavior
    based on user queries and long-running interactions. Supports tool chaining,
    conversational history, summarization, and streaming output.
    """

    def __init__(
        self,
        uc_tool_names: Sequence[str] = ("main_build.dbdemos_ai_agent.*",),
        llm_endpoint_name: str = "databricks-claude-sonnet-4",
        system_prompt: Optional[str] = None,
        retriever_config: Optional[dict] = None,
        max_history_messages: int = 20,
    ):
        # Initialize LLM and tools from Unity Catalog and optionally Vector Search
        self.llm_endpoint_name = llm_endpoint_name
        self.llm = ChatDatabricks(endpoint=llm_endpoint_name)
        self.tools: List[BaseTool] = UCFunctionToolkit(function_names=list(uc_tool_names)).tools

        # Optionally add a vector search tool to the agent
        if retriever_config:
            self.tools.append(
                VectorSearchRetrieverTool(
                    index_name=retriever_config.get("index_name"),
                    name=retriever_config.get("tool_name"),
                    description=retriever_config.get("description"),
                    num_results=retriever_config.get("num_results"),
                )
            )

        self.max_history_messages = max_history_messages
        self.system_prompt = system_prompt
        self.graph: CompiledStateGraph = self._build_graph(system_prompt)

    def _build_graph(self, system_prompt: Optional[str]) -> CompiledStateGraph:
        """
        Construct a LangGraph-based loop: [agent -> tool -> agent -> ...] until no tool calls.
        """

        model = self.llm.bind_tools(self.tools)

        def should_continue(state: dict):
            last = state["messages"][-1]
            tc = (last.get("tool_calls") if isinstance(last, dict)
                  else last.tool_calls if isinstance(last, AIMessage)
                  else None)
            return "continue" if tc else "end"

        # Optionally prepend system prompt
        pre = RunnableLambda(lambda s: [{"role": "system", "content": system_prompt}] + s["messages"]) if system_prompt \
              else RunnableLambda(lambda s: s["messages"])
        runnable = pre | model

        def call_agent(state, config):
            ai_msg = runnable.invoke(state, config)
            return {"messages": state["messages"] + [ai_msg]}

        def call_tool(state, config):
            # Use LangGraph tool node to execute any tools requested
            tool_node = ToolNode(self.tools)
            result = tool_node.invoke(state, config)
            tool_messages = result.get("messages", [])
            if not all(isinstance(m, ToolMessage) for m in tool_messages):
                raise ValueError("Expected ToolMessage list in tool node result")
            return {"messages": state["messages"] + tool_messages}

        # Define graph structure
        graph = StateGraph(dict)
        graph.add_node("agent", RunnableLambda(call_agent))
        graph.add_node("tools", RunnableLambda(call_tool))
        graph.set_entry_point("agent")
        graph.add_conditional_edges("agent", should_continue, {"continue": "tools", "end": END})
        graph.add_edge("tools", "agent")
        return graph.compile()

    def _mlflow_messages_to_dicts(self, messages: Sequence[Any]) -> List[Dict[str, str]]:
        # Convert MLflow message objects to simple {role, content} dictionaries
        return [{"role": m.role, "content": m.content,} for m in messages]

    def _summarize_history(self, messages: List[Dict[str, str]]) -> str:
        # Summarize older chat history using LLM if the message count exceeds max_history
        llm = ChatDatabricks(endpoint=self.llm_endpoint_name)
        prompt = "Summarize the following conversation between a user and an assistant:\n\n"
        for m in messages:
            prompt += f"{m['role'].capitalize()}: {m['content']}\n"
        prompt += "\nSummary:"
        response = llm.invoke([{"role": "user", "content": prompt}])
        return response.content if hasattr(response, "content") else str(response)

    def _truncate_and_summarize_history(self, messages: List[Dict[str, str]]) -> List[Dict[str, str]]:
        # If history is too long, summarize older messages and keep the recent ones
        if len(messages) <= self.max_history_messages:
            return messages
        to_summarize = messages[:-self.max_history_messages]
        recent = messages[-self.max_history_messages:]
        summary = self._summarize_history(to_summarize)
        summarized_history = [{"role": "system", "content": f"Summary of earlier conversation: {summary}"}] + recent
        return summarized_history

    def _stream_events(self, request: ResponsesAgentRequest):
        full_history = self._mlflow_messages_to_dicts(request.input)
        processed_history = self._truncate_and_summarize_history(full_history)
        state = {"messages": processed_history}
        for event in self.graph.stream(state, stream_mode="updates"):
            for node_out in event.values():
                for msg in node_out.get("messages", []):
                    if isinstance(msg, AIMessage):
                        # Ensure every message has a unique id
                        if not hasattr(msg, "id") or not msg.id:
                            msg.id = str(uuid.uuid4())
                        content, msg_id = msg.content, msg.id
                    elif isinstance(msg, ToolMessage):
                        if not hasattr(msg, "tool_call_id") or not msg.tool_call_id:
                            msg.tool_call_id = str(uuid.uuid4())
                        content, msg_id = msg.content, msg.tool_call_id
                    else:
                        content, msg_id = msg.get("content"), msg.get("id", str(uuid.uuid4()))
                    yield content, msg_id

    @mlflow.trace(span_type=SpanType.AGENT)
    def predict(self, request: ResponsesAgentRequest) -> ResponsesAgentResponse:
        """
        Non-streaming predict call for synchronous APIs (returns full response).
        """
        items = []
        for content, msg_id in self._stream_events(request):
            items.append(self.create_text_output_item(text=content, id=msg_id or ""))
        return ResponsesAgentResponse(output=items, custom_outputs = {"trace_id": self.get_active_trace_id()})

    def get_active_trace_id(self):
      active_span = mlflow.get_current_active_span()
      if active_span:
        return active_span.trace_id
      return None

    @mlflow.trace(span_type=SpanType.AGENT)
    def predict_stream(self, request: ResponsesAgentRequest):
        """
        Streaming predict call for real-time apps (yields deltas + final message).
        """
        for content, msg_id in self._stream_events(request):
            yield ResponsesAgentStreamEvent(**self.create_text_delta(delta=content, item_id=msg_id or ""))
            yield ResponsesAgentStreamEvent(
                type="response.output_item.done",
                item=self.create_text_output_item(text=content, id=msg_id or ""), 
                custom_outputs = {"trace_id": self.get_active_trace_id()}
            )

    def get_resources(self):
        """
        Declare all external tools and endpoints used, for audit/visualization.
        """
        res = [DatabricksServingEndpoint(endpoint_name=self.llm.endpoint)]
        for t in self.tools:
            if isinstance(t, VectorSearchRetrieverTool):
                res.extend(t.resources)
            elif isinstance(t, UnityCatalogTool):
                res.append(DatabricksFunction(function_name=t.uc_function_name))
        return res

# Load configuration from YAML (for local dev or MLF model eval)
model_config = mlflow.models.ModelConfig(development_config='../02-agent-eval/agent_config.yaml')

# Instantiate agent
AGENT = ToolCallingAgent(
    uc_tool_names=model_config.get('uc_tool_names'),
    llm_endpoint_name=model_config.get('llm_endpoint_name'),
    system_prompt=model_config.get('system_prompt'),
    retriever_config=model_config.get('retriever_config'),
    max_history_messages=20
)

# Register the agent with MLflow
mlflow.models.set_model(AGENT)

# COMMAND ----------

#from agent import AGENT
from mlflow.types.responses import ResponsesAgentRequest

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

mlflow.active_run().info.experiment_id

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

def get_scorers()
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
