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

# MAGIC %pip install -U -qqqq mlflow langchain langgraph==0.3.4 databricks-langchain pydantic databricks-agents unitycatalog-langchain[databricks] uv
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
    "tools": [
      f"{catalog}.{schema}.turbine_specifications_retriever",
      f"{catalog}.{schema}.turbine_maintenance_reports_retriever",
      f"{catalog}.{schema}.turbine_maintenance_predictor"
    ]
  }

  import yaml
  with open('config.yml', 'w') as f:
      yaml.dump(config, f)
except Exception as e:
    print(f"Could not write the file - make sure it exists in the local folder: {e}")


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

# MAGIC %%writefile agent.py
# MAGIC from typing import Any, Generator, Optional, Sequence, Union
# MAGIC
# MAGIC import mlflow
# MAGIC from databricks_langchain import (
# MAGIC     ChatDatabricks,
# MAGIC     VectorSearchRetrieverTool,
# MAGIC     DatabricksFunctionClient,
# MAGIC     UCFunctionToolkit,
# MAGIC     set_uc_function_client,
# MAGIC )
# MAGIC from langchain_core.language_models import LanguageModelLike
# MAGIC from langchain_core.runnables import RunnableConfig, RunnableLambda
# MAGIC from langchain_core.tools import BaseTool
# MAGIC from langgraph.graph import END, StateGraph
# MAGIC from langgraph.graph.graph import CompiledGraph
# MAGIC from langgraph.graph.state import CompiledStateGraph
# MAGIC from langgraph.prebuilt.tool_node import ToolNode
# MAGIC from mlflow.langchain.chat_agent_langgraph import ChatAgentState, ChatAgentToolNode
# MAGIC from mlflow.pyfunc import ChatAgent
# MAGIC from mlflow.models import ModelConfig
# MAGIC from mlflow.types.agent import (
# MAGIC     ChatAgentChunk,
# MAGIC     ChatAgentMessage,
# MAGIC     ChatAgentResponse,
# MAGIC     ChatContext,
# MAGIC )
# MAGIC
# MAGIC mlflow.langchain.autolog()
# MAGIC config = ModelConfig(development_config="config.yml")
# MAGIC
# MAGIC client = DatabricksFunctionClient()
# MAGIC set_uc_function_client(client)
# MAGIC
# MAGIC ############################################
# MAGIC # Define your LLM endpoint and system prompt
# MAGIC ############################################
# MAGIC LLM_ENDPOINT_NAME = config.get("llm_endpoint")
# MAGIC llm = ChatDatabricks(endpoint=LLM_ENDPOINT_NAME)
# MAGIC
# MAGIC system_prompt = config.get("agent_prompt")
# MAGIC
# MAGIC ###############################################################################
# MAGIC ## Define tools for your agent, enabling it to retrieve data or take actions
# MAGIC ## beyond text generation
# MAGIC ## To create and see usage examples of more tools, see
# MAGIC ## https://docs.databricks.com/generative-ai/agent-framework/agent-tool.html
# MAGIC ###############################################################################
# MAGIC tools = []
# MAGIC
# MAGIC # You can use UDFs in Unity Catalog as agent tools
# MAGIC uc_tool_names = config.get("tools")
# MAGIC uc_toolkit = UCFunctionToolkit(function_names=uc_tool_names)
# MAGIC tools.extend(uc_toolkit.tools)
# MAGIC
# MAGIC
# MAGIC #####################
# MAGIC ## Define agent logic
# MAGIC #####################
# MAGIC
# MAGIC
# MAGIC def create_tool_calling_agent(
# MAGIC     model: LanguageModelLike,
# MAGIC     tools: Union[Sequence[BaseTool], ToolNode],
# MAGIC     system_prompt: Optional[str] = None,
# MAGIC ) -> CompiledGraph:
# MAGIC     model = model.bind_tools(tools)
# MAGIC
# MAGIC     # Define the function that determines which node to go to
# MAGIC     def should_continue(state: ChatAgentState):
# MAGIC         messages = state["messages"]
# MAGIC         last_message = messages[-1]
# MAGIC         # If there are function calls, continue. else, end
# MAGIC         if last_message.get("tool_calls"):
# MAGIC             return "continue"
# MAGIC         else:
# MAGIC             return "end"
# MAGIC
# MAGIC     if system_prompt:
# MAGIC         preprocessor = RunnableLambda(
# MAGIC             lambda state: [{"role": "system", "content": system_prompt}]
# MAGIC             + state["messages"]
# MAGIC         )
# MAGIC     else:
# MAGIC         preprocessor = RunnableLambda(lambda state: state["messages"])
# MAGIC     model_runnable = preprocessor | model
# MAGIC
# MAGIC     def call_model(
# MAGIC         state: ChatAgentState,
# MAGIC         config: RunnableConfig,
# MAGIC     ):
# MAGIC         response = model_runnable.invoke(state, config)
# MAGIC
# MAGIC         return {"messages": [response]}
# MAGIC
# MAGIC     workflow = StateGraph(ChatAgentState)
# MAGIC
# MAGIC     workflow.add_node("agent", RunnableLambda(call_model))
# MAGIC     workflow.add_node("tools", ChatAgentToolNode(tools))
# MAGIC
# MAGIC     workflow.set_entry_point("agent")
# MAGIC     workflow.add_conditional_edges(
# MAGIC         "agent",
# MAGIC         should_continue,
# MAGIC         {
# MAGIC             "continue": "tools",
# MAGIC             "end": END,
# MAGIC         },
# MAGIC     )
# MAGIC     workflow.add_edge("tools", "agent")
# MAGIC
# MAGIC     return workflow.compile()
# MAGIC
# MAGIC
# MAGIC class LangGraphChatAgent(ChatAgent):
# MAGIC     def __init__(self, agent: CompiledStateGraph):
# MAGIC         self.agent = agent
# MAGIC
# MAGIC     def predict(
# MAGIC         self,
# MAGIC         messages: list[ChatAgentMessage],
# MAGIC         context: Optional[ChatContext] = None,
# MAGIC         custom_inputs: Optional[dict[str, Any]] = None,
# MAGIC     ) -> ChatAgentResponse:
# MAGIC         request = {"messages": self._convert_messages_to_dict(messages)}
# MAGIC
# MAGIC         messages = []
# MAGIC         for event in self.agent.stream(request, stream_mode="updates"):
# MAGIC             for node_data in event.values():
# MAGIC                 messages.extend(
# MAGIC                     ChatAgentMessage(**msg) for msg in node_data.get("messages", [])
# MAGIC                 )
# MAGIC         return ChatAgentResponse(messages=messages)
# MAGIC
# MAGIC     def predict_stream(
# MAGIC         self,
# MAGIC         messages: list[ChatAgentMessage],
# MAGIC         context: Optional[ChatContext] = None,
# MAGIC         custom_inputs: Optional[dict[str, Any]] = None,
# MAGIC     ) -> Generator[ChatAgentChunk, None, None]:
# MAGIC         request = {"messages": self._convert_messages_to_dict(messages)}
# MAGIC         for event in self.agent.stream(request, stream_mode="updates"):
# MAGIC             for node_data in event.values():
# MAGIC                 yield from (
# MAGIC                     ChatAgentChunk(**{"delta": msg}) for msg in node_data["messages"]
# MAGIC                 )
# MAGIC
# MAGIC
# MAGIC # Create the agent object, and specify it as the agent object to use when
# MAGIC # loading the agent back for inference via mlflow.models.set_model()
# MAGIC agent = create_tool_calling_agent(llm, tools, system_prompt)
# MAGIC AGENT = LangGraphChatAgent(agent)
# MAGIC mlflow.models.set_model(AGENT)

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test the agent
# MAGIC
# MAGIC Interact with the agent to test its output. Since this notebook called `mlflow.langchain.autolog()` you can view the trace for each step the agent takes.
# MAGIC
# MAGIC Replace this placeholder input with an appropriate domain-specific example for your agent.

# COMMAND ----------

import sys
sys.path.append(".")
from agent import AGENT

AGENT.predict({"messages": [{"role": "user", "content": "Hello!"}]})

# COMMAND ----------

from agent import AGENT
for event in AGENT.predict_stream({"messages": [{"role": "user", "content": "How is turbine 5ef39b37-7f89-b8c2-aff1-5e4c0453377d performing?"}]}):
    print(event, "---" * 20 + "\n")

# COMMAND ----------

for event in AGENT.predict_stream({"messages": [{"role": "user", "content": "Fetch me information and readings for turbine 004a641f-e9e5-9fff-d421-1bf88319420b. Give me maintenance recommendation based on existing reports"}]}):
    print(event, "---" * 20 + "\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next steps
# MAGIC
# MAGIC You can rerun the cells above to iterate and test the agent.
# MAGIC
# MAGIC Go to the [05.3-deploy-agent-iot-turbine-prescriptive-maintenance]($./05.3-deploy-agent-iot-turbine-prescriptive-maintenance) notebook to log, register, and deploy the agent.
