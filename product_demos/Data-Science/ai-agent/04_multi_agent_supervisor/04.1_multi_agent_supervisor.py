# Databricks notebook source
# MAGIC %md
# MAGIC # Multi agent system
# MAGIC
# MAGIC Let's deploy our updated LLM, composed of 2 agent and 1 supervisor:
# MAGIC
# MAGIC The supervisor will decide which agent to use:
# MAGIC
# MAGIC - the technical support with our RAG
# MAGIC - The billing agent with all the customer information tool

# COMMAND ----------

# DBTITLE 1,Library Installs
# MAGIC %pip install databricks-agents mlflow>=3.1.0 databricks-sdk==0.55.0 langchain langgraph databricks-vectorsearch databricks-sdk
# MAGIC # Restart to load the packages into the Python environment
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC
# MAGIC %run ../_resources/01-setup

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 1. Define your two specialized agents
# MAGIC
# MAGIC You already have the logic for each:
# MAGIC 	•	Billing Agent using UCFunctionToolkit
# MAGIC 	•	Tech Support Agent using VectorSearchRetrieverTool and RAG chain
# MAGIC
# MAGIC Wrap each into a LangGraphChatAgent as you did before.

# COMMAND ----------

from databricks_langchain import ChatDatabricks, VectorSearchRetrieverTool, UCFunctionToolkit
from langgraph.graph import StateGraph, END
from langchain_core.runnables import RunnableLambda
from langchain_core.prompts import PromptTemplate
from langchain_core.output_parsers import StrOutputParser

from mlflow.langchain.chat_agent_langgraph import ChatAgent, ChatAgentMessage, ChatAgentResponse

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🤖 Step 1 – Define the LLM and agent tools
# MAGIC
# MAGIC We use:
# MAGIC - A Databricks LLM endpoint (Claude Sonnet or DBRX)
# MAGIC - Unity Catalog functions as tools for billing
# MAGIC - A Vector Search tool for technical support

# COMMAND ----------

# Use any LLM endpoint like databricks-claude-sonnet-4 or databricks-dbrx-instruct
llm = ChatDatabricks(endpoint="databricks-claude-sonnet-4")

# Tools for the billing agent (UC function-based)
billing_tools = UCFunctionToolkit(function_names=[f"{catalog}.{db}.*"]).tools
vs_index_fullname = f"{catalog}.{db}.knowledge_base_vs_index"

# Tool for the tech agent (Vector Search)
tech_tools = [
    VectorSearchRetrieverTool(
        index_name=vs_index_fullname,
        tool_name="search_product_docs",
        tool_description="Search product documentation",
        num_results=3,
        disable_notice=True
    )
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧠 Step 2 – Create two LangGraph agents
# MAGIC
# MAGIC Each agent uses tools:
# MAGIC - The billing agent responds to account-related requests.
# MAGIC - The tech agent answers product and setup questions.

# COMMAND ----------

from langgraph.prebuilt.tool_node import ToolNode
from langgraph.graph.state import CompiledStateGraph
from langchain_core.language_models import LanguageModelLike

def create_tool_agent(model: LanguageModelLike, tools, system_prompt: str) -> CompiledStateGraph:
    model = model.bind_tools(tools)

    def should_continue(state):  # Check if tool call needed
        return "continue" if state["messages"][-1].get("tool_calls") else "end"

    model_step = RunnableLambda(lambda state: [{"role": "system", "content": system_prompt}] + state["messages"]) | model
    call_model = RunnableLambda(lambda state, cfg: {"messages": [model_step.invoke(state, cfg)]})

    graph = StateGraph(dict)
    graph.add_node("agent", call_model)
    graph.add_node("tools", ToolNode(tools))
    graph.set_entry_point("agent")
    graph.add_conditional_edges("agent", should_continue, {"continue": "tools", "end": END})
    graph.add_edge("tools", "agent")
    return graph.compile()

# Compile billing and tech agent graphs
billing_agent = create_tool_agent(llm, billing_tools, "You are a helpful billing assistant.")
tech_agent = create_tool_agent(llm, tech_tools, "You are a helpful tech support assistant.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🚦 Step 3 – Define router using LLM
# MAGIC
# MAGIC This model-based router classifies the user message into either:
# MAGIC - `billing`
# MAGIC - `tech`
# MAGIC
# MAGIC We use a simple prompt and the same Databricks LLM.

# COMMAND ----------

router_prompt = """
You are a routing assistant.
Classify the user message below as either:
- "billing" (for anything about payments, invoices, account info)
- "tech" (for anything about setup, errors, connection, devices)

Message: {input}

Respond with just one word: billing or tech.
"""

router_chain = (
    {"input": lambda state: state["messages"][-1]["content"]}
    | PromptTemplate.from_template(router_prompt)
    | llm
    | StrOutputParser()
    | RunnableLambda(lambda x: x.strip().lower())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧭 Step 4 – Create the supervisor LangGraph
# MAGIC
# MAGIC This supervisor graph:
# MAGIC - Calls the router LLM
# MAGIC - Forwards the message to the correct sub-agent

# COMMAND ----------

# Wrapper to use each sub-agent as a node
def wrap_agent(agent: CompiledStateGraph):
    return RunnableLambda(lambda state, cfg: agent.invoke(state))

# Supervisor graph
supervisor = StateGraph(dict)
supervisor.add_node("router", router_chain)
supervisor.add_node("billing", wrap_agent(billing_agent))
supervisor.add_node("tech", wrap_agent(tech_agent))
supervisor.set_entry_point("router")
supervisor.add_conditional_edges("router", lambda x: x, {
    "billing": "billing",
    "tech": "tech"
})
supervisor.add_edge("billing", END)
supervisor.add_edge("tech", END)

supervisor_graph = supervisor.compile()

# COMMAND ----------

## 🤝 Step 5 – Wrap the graph as a standard ChatAgent

This lets us call `.predict()` like a regular LangChain agent.

# COMMAND ----------

class SupervisorChatAgent(ChatAgent):
    def __init__(self, graph: CompiledStateGraph):
        self.graph = graph

    def predict(self, messages, context=None, custom_inputs=None):
        state = {"messages": [m.model_dump() for m in messages]}
        result = self.graph.invoke(state)
        return ChatAgentResponse(messages=result["messages"])

SUPERVISOR_AGENT = SupervisorChatAgent(supervisor_graph)

# COMMAND ----------

test_messages = [
    ChatAgentMessage(role="user", content="hey, I'm with onelson@example.net. Can you pull his last invoice?"),
]

response = SUPERVISOR_AGENT.predict(test_messages)
for msg in response.messages:
    print(f"{msg.role}: {msg.content}")
