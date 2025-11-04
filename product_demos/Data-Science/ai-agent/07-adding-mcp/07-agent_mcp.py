# Databricks notebook source
# MAGIC %md
# MAGIC ## Exposing your tools from the MCP server
# MAGIC
# MAGIC If you want to expose your tools or data so that other agents would be able to access it, then you could leverage MCP servers. 
# MAGIC
# MAGIC Databricks supports 3 types of MCPs:
# MAGIC 	
# MAGIC - **Databricks-managed MCP servers**:
# MAGIC Databricks has ready-to-use servers that let agents query data and access tools in Unity Catalog. This exposes Databricks services as MCP resources (Databricks Vector Search, UC functions, Genie Spaces, DBSQL...)
# MAGIC
# MAGIC - **External MCP servers**:
# MAGIC Connect to third-party MCP servers hosted outside Databricks using managed proxies and Unity Catalog connections.
# MAGIC
# MAGIC - **Custom MCP servers**:
# MAGIC Securely host your own MCP server as a Databricks app to bring your own server or run a third-party MCP server.
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC See Databricks [managed MCP servers Documentation](https://docs.databricks.com/aws/en/generative-ai/mcp/managed-mcp#available-managed-servers) for more details
# MAGIC
# MAGIC
# MAGIC In this example, we will show how you can provide the agent, the tools from the Databricks Managed MCP servers such as the UC system ai functions. You will then be able to  configure your agent with any other MCP tools, either Databricks managed MCP server or a custom one! 

# COMMAND ----------

# MAGIC %pip install -U -qqqq mlflow>=3.1.4 langchain==0.3.27 langgraph==0.6.11 databricks-langchain pydantic databricks-agents unitycatalog-langchain[databricks] databricks-feature-engineering==0.12.1 protobuf<5  cryptography<43 databricks-mcp
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../_resources/01-setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## Databricks Managed MCP
# MAGIC
# MAGIC To use the UC system ai function from the Databricks Managed MCP server, we'd just need to add the relevant url in the [agent_config.yaml](https://adb-984752964297111.11.azuredatabricks.net/editor/files/2254899697811646?o=984752964297111) file.
# MAGIC
# MAGIC - **Unity Catalog system ai functions**: https://{workspace-hostname}/api/2.0/mcp/functions/system/ai

# COMMAND ----------

from databricks.sdk import WorkspaceClient
import os, sys, yaml, mlflow
import nest_asyncio
nest_asyncio.apply()

# --- Paths ---
agent_eval_path = os.path.abspath(os.path.join(os.getcwd(), "../02-agent-eval"))
sys.path.append(agent_eval_path)
conf_path = os.path.join(agent_eval_path, "agent_config.yaml")

# --- Use Databricks SDK to detect workspace URL ---
ws = WorkspaceClient()
workspace_url = ws.config.host.rstrip("/") 
mcp_url = f"{workspace_url}/api/2.0/mcp/functions/system/ai"

# ==========================
# Update config for MCP
# ==========================
try:
    config = yaml.safe_load(open(conf_path))
    config["config_version_name"] = "model_with_mcp"
    config["mcp_server_urls"] = [mcp_url]

    with open(conf_path, "w") as f:
        yaml.safe_dump(config, f, sort_keys=False)

except Exception as e:
    print(f"Skipped MCP update: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Instantiate the agent
# MAGIC
# MAGIC Next, we will instantiate the agent and to make sure we have provided the tools from the managed MCP server, we will check all the available tools that the agent has. In the list below, besides the UC functions we have created, we will see also the tools from MCP server which has the UC system ai functions i.e **system__ai__python_exec**.

# COMMAND ----------

# ==========================
# Instantiate the agent
# ==========================
from agent import LangGraphResponsesAgent
import mlflow.models

model_config = mlflow.models.ModelConfig(development_config=conf_path)

AGENT = LangGraphResponsesAgent(
    uc_tool_names=model_config.get("uc_tool_names"),
    llm_endpoint_name=model_config.get("llm_endpoint_name"),
    system_prompt=model_config.get("system_prompt"),
    mcp_server_urls=model_config.get("mcp_server_urls"),
    max_history_messages=model_config.get("max_history_messages"),
)

print("✅ Available tools:", AGENT.list_tools())

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Test the agent in the AI Playground

# COMMAND ----------

# MAGIC %md
# MAGIC To test the agent, you would just need to choose the end of your choice and add the neccessary tools. In our case we will add the tools from the managed MCP server option, by selecting the system ai functions in the UC function toolbox. Similarly, you could add any tool of your choice, also if you'd have custom or external MCP servers.

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/product/ai-agent/pg-mcp-img1.png" width="800px">

# COMMAND ----------

# MAGIC %md When we start to ask questions, we will see that the agent has properly loaded the tool from the Managed MCP server we added above.

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/product/ai-agent/pg-mcp-img2.png" width="800">

# COMMAND ----------

# MAGIC %md Now we can start exploring more...

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/product/ai-agent/pg-mcp-img3.png" width="800">
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next steps
# MAGIC If you would like to further explore other MCP server options in Databricks, please refer to [Databricks MCP documentation](https://docs.databricks.com/aws/en/generative-ai/mcp/).
