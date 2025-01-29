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

# MAGIC %pip install -U -qqqq mlflow-skinny==2.20.0 langchain==0.2.16 langgraph-checkpoint==1.0.12 langchain_core langchain-community==0.2.16 langgraph==0.2.16 pydantic langchain_databricks
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../_resources/00-setup $reset_all_data=false

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import and setup
# MAGIC Use `mlflow.langchain.autolog()` to set up [MLflow traces](https://docs.databricks.com/en/mlflow/mlflow-tracing.html).

# COMMAND ----------

# Note: this is for the demo only to parametrize the value with the right values (warehouse ID and catalog/schema). 
# In a real deployment, just put your configuration in the config.yaml file and skip this cell

try:
  wh = get_shared_warehouse(name = None)
  config = {
    "agent_prompt": """Act as an assistant for wind turbine maintenance technicians.\n
    These are the tools you can use to answer questions:
    \n- turbine_maintenance_predictor: takes as input sensor_readings and predicts whether or not a turbine is at risk of failure.
    \n- turbine_maintenance_reports_predictor: takes sensor_readings as input and retrieves historical maintenance reports with similar sensor_readings. Critical for prescriptive maintenance.
    \n- turbine_specifications_retriever: takes turbine_id as input and retrieves turbine specifications.
    

    \nIf a user gives you a turbine ID, first look up that turbine's information with turbine_specifications_retriever. 
    \nIf a user asks for recommendations on how to do maintenance on a turbine, use the turbine reading and search for similar reports matching the turbine readings using the  turbine_maintenance_reports_predictor. Use the report retrived from other turbines to understand what could be happening and suggest maintenance recommendation.
    """,
    "llm_endpoint": "databricks-meta-llama-3-3-70b-instruct",
    "warehouse_id": wh.id,
    "uc_functions": [
      f"{catalog}.{schema}.turbine_specifications_retriever",
      f"{catalog}.{schema}.turbine_maintenance_reports_retriever",
      f"{catalog}.{schema}.turbine_maintenance_predictor"
    ]
  }

  import yaml
  with open('config.yml', 'w') as f:
      yaml.dump(config, f)
except Exception as e:
    print("Could not write the file - make sure it exists in the local folder")


# COMMAND ----------

import mlflow
from mlflow.models import ModelConfig

mlflow.langchain.autolog()
config = ModelConfig(development_config="config.yml")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define the chat model and tools
# MAGIC Create a LangChain chat model that supports [LangGraph tool](https://langchain-ai.github.io/langgraph/how-tos/tool-calling/) calling.
# MAGIC
# MAGIC Modify the tools your agent has access to by modifying the `uc_functions` list in [config.yml]($./config.yml). Any non-UC function spec tools can be defined in this notebook. See [LangChain - How to create tools](https://python.langchain.com/v0.2/docs/how_to/custom_tools/) and [LangChain - Using built-in tools](https://python.langchain.com/v0.2/docs/how_to/tools_builtin/).
# MAGIC
# MAGIC  **_NOTE:_**  This notebook uses LangChain, however AI Agent Framework is compatible with other agent frameworks like Pyfunc and LlamaIndex.

# COMMAND ----------

from langchain_community.chat_models import ChatDatabricks
from langchain_community.tools.databricks import UCFunctionToolkit

# Create the llm
llm = ChatDatabricks(endpoint=config.get("llm_endpoint"))

uc_functions = config.get("uc_functions")

tools = (
    UCFunctionToolkit(warehouse_id=config.get("warehouse_id"))
    .include(*uc_functions)
    .get_tools()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Output parsers
# MAGIC Databricks interfaces, such as the AI Playground, can optionally display pretty-printed tool calls.
# MAGIC
# MAGIC Use the following helper functions to parse the LLM's output into the expected format.

# COMMAND ----------

from typing import Iterator, Dict, Any
from langchain_core.messages import (
    AIMessage,
    HumanMessage,
    ToolMessage,
    MessageLikeRepresentation,
)

import json

def stringify_tool_call(tool_call: Dict[str, Any]) -> str:
    """
    Convert a raw tool call into a formatted string that the playground UI expects if there is enough information in the tool_call
    """
    try:
        request = json.dumps(
            {
                "id": tool_call.get("id"),
                "name": tool_call.get("name"),
                "arguments": json.dumps(tool_call.get("args", {})),
            },
            indent=2,
        )
        return f"<tool_call>{request}</tool_call>"
    except:
        return str(tool_call)


def stringify_tool_result(tool_msg: ToolMessage) -> str:
    """
    Convert a ToolMessage into a formatted string that the playground UI expects if there is enough information in the ToolMessage
    """
    try:
        result = json.dumps(
            {"id": tool_msg.tool_call_id, "content": tool_msg.content}, indent=2
        )
        return f"<tool_call_result>{result}</tool_call_result>"
    except:
        return str(tool_msg)


def parse_message(msg) -> str:
    """Parse different message types into their string representations"""
    # tool call result
    if isinstance(msg, ToolMessage):
        return stringify_tool_result(msg)
    # tool call
    elif isinstance(msg, AIMessage) and msg.tool_calls:
        tool_call_results = [stringify_tool_call(call) for call in msg.tool_calls]
        return "".join(tool_call_results)
    # normal HumanMessage or AIMessage (reasoning or final answer)
    elif isinstance(msg, (AIMessage, HumanMessage)):
        return msg.content
    else:
        print(f"Unexpected message type: {type(msg)}")
        return str(msg)


def wrap_output(stream: Iterator[MessageLikeRepresentation]) -> Iterator[str]:
    """
    Process and yield formatted outputs from the message stream.
    The invoke and stream langchain functions produce different output formats.
    This function handles both cases.
    """
    for event in stream:
        # the agent was called with invoke()
        if "messages" in event:
            for msg in event["messages"]:
                yield parse_message(msg) + "\n\n"
        # the agent was called with stream()
        else:
            for node in event:
                for key, messages in event[node].items():
                    if isinstance(messages, list):
                        for msg in messages:
                            yield parse_message(msg) + "\n\n"
                    else:
                        print("Unexpected value {messages} for key {key}. Expected a list of `MessageLikeRepresentation`'s")
                        yield str(messages)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create the agent
# MAGIC Here we provide a simple graph that uses the model and tools defined by [config.yml]($./config.yml). This graph is adapated from [this LangGraph guide](https://langchain-ai.github.io/langgraph/how-tos/react-agent-from-scratch/).
# MAGIC
# MAGIC
# MAGIC To further customize your LangGraph agent, you can refer to:
# MAGIC * [LangGraph - Quick Start](https://langchain-ai.github.io/langgraph/tutorials/introduction/) for explanations of the concepts used in this LangGraph agent
# MAGIC * [LangGraph - How-to Guides](https://langchain-ai.github.io/langgraph/how-tos/) to expand the functionality of your agent
# MAGIC

# COMMAND ----------

from typing import (
    Annotated,
    Optional,
    Sequence,
    TypedDict,
    Union,
)

from langchain_core.language_models import LanguageModelLike
from langchain_core.messages import (
    BaseMessage,
    SystemMessage,
)
from langchain_core.runnables import RunnableConfig, RunnableLambda
from langchain_core.tools import BaseTool

from langgraph.graph import END, StateGraph
from langgraph.graph.graph import CompiledGraph
from langgraph.graph.message import add_messages
from langgraph.prebuilt.tool_executor import ToolExecutor
from langgraph.prebuilt.tool_node import ToolNode


# We create the AgentState that we will pass around
# This simply involves a list of messages
class AgentState(TypedDict):
    """The state of the agent."""

    messages: Annotated[Sequence[BaseMessage], add_messages]


def create_tool_calling_agent(
    model: LanguageModelLike,
    tools: Union[ToolExecutor, Sequence[BaseTool]],
    agent_prompt: Optional[str] = None,
) -> CompiledGraph:
    model = model.bind_tools(tools)

    # Define the function that determines which node to go to
    def should_continue(state: AgentState):
        messages = state["messages"]
        last_message = messages[-1]
        # If there is no function call, then we finish
        if not last_message.tool_calls:
            return "end"
        else:
            return "continue"

    if agent_prompt:
        system_message = SystemMessage(content=agent_prompt)
        preprocessor = RunnableLambda(
            lambda state: [system_message] + state["messages"]
        )
    else:
        preprocessor = RunnableLambda(lambda state: state["messages"])
    model_runnable = preprocessor | model

    # Define the function that calls the model
    def call_model(
        state: AgentState,
        config: RunnableConfig,
    ):
        response = model_runnable.invoke(state, config)
        return {"messages": [response]}

    workflow = StateGraph(AgentState)

    workflow.add_node("agent", RunnableLambda(call_model))
    workflow.add_node("tools", ToolNode(tools))

    workflow.set_entry_point("agent")
    workflow.add_conditional_edges(
        # First, we define the start node. We use agent.
        # This means these are the edges taken after the agent node is called.
        "agent",
        # Next, we pass in the function that will determine which node is called next.
        should_continue,
        # The mapping below will be used to determine which node to go to
        {
            # If tools, then we call the tool node.
            "continue": "tools",
            # END is a special node marking that the graph should finish.
            "end": END,
        },
    )
    # We now add a unconditional edge from tools to agent.
    workflow.add_edge("tools", "agent")

    return workflow.compile()

# COMMAND ----------

from langchain_core.runnables import RunnableGenerator
from mlflow.langchain.output_parsers import ChatCompletionsOutputParser

# Create the agent with the system message if it exists
try:
    agent_prompt = config.get("agent_prompt")
    agent_with_raw_output = create_tool_calling_agent(
        llm, tools, agent_prompt=agent_prompt
    )
except KeyError:
    agent_with_raw_output = create_tool_calling_agent(llm, tools)
agent = agent_with_raw_output | RunnableGenerator(wrap_output) | ChatCompletionsOutputParser()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test the agent
# MAGIC
# MAGIC Interact with the agent to test its output. Since this notebook called `mlflow.langchain.autolog()` you can view the trace for each step the agent takes.
# MAGIC
# MAGIC Replace this placeholder input with an appropriate domain-specific example for your agent.

# COMMAND ----------

for event in agent.stream({"messages": [{"role": "user", "content": "How is turbine 5ef39b37-7f89-b8c2-aff1-5e4c0453377d performing?"}]}):
    print(event, "---" * 20 + "\n")

# COMMAND ----------

for event in agent.stream({"messages": [{"role": "user", "content": "Fetch me information and readings for turbine 004a641f-e9e5-9fff-d421-1bf88319420b. Give me maintenance recommendation based on existing reports"}]}):
    print(event, "---" * 20 + "\n")

# COMMAND ----------

mlflow.models.set_model(agent)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next steps
# MAGIC
# MAGIC You can rerun the cells above to iterate and test the agent.
# MAGIC
# MAGIC Go to the [05.3-deploy-agent-iot-turbine-prescriptive-maintenance]($./05.3-deploy-agent-iot-turbine-prescriptive-maintenance) notebook to log, register, and deploy the agent.
