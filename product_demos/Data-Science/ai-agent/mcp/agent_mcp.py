# Databricks notebook source
# MAGIC %md
# MAGIC ## Exposing your tools from the MCP server
# MAGIC
# MAGIC If you want to expose your tools or data so that other agents would be able to access it, then you could leverage MCP servers. Databricks already provides [managed MCP servers](https://docs.databricks.com/aws/en/generative-ai/mcp/managed-mcp#available-managed-servers), so that you do not need to start from scratch. 
# MAGIC
# MAGIC
# MAGIC These managed servers include the UC functions, vector search indexes, and Genie spaces. In our example, we will show how you can provide the agent, the tools from the Databricks Managed MCP servers such as the UC system ai functions, but in similar fashion you can configure it for any other either Databricks managed MCP server or custom. 

# COMMAND ----------

# MAGIC %pip install -U -qqqq mlflow>=3.1.1 langchain langgraph databricks-langchain pydantic databricks-agents databricks-mcp  unitycatalog-langchain[databricks] uv databricks-feature-engineering==0.12.1
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


import os, sys, yaml, mlflow
import nest_asyncio
nest_asyncio.apply()

# ==========================
# Locate and load base config
# ==========================
agent_eval_path = os.path.abspath(os.path.join(os.getcwd(), "../02-agent-eval"))
sys.path.append(agent_eval_path)
conf_path = os.path.join(agent_eval_path, "agent_config.yaml")

# ==========================
# Update config for MCP
# ==========================
try:
    config = yaml.safe_load(open(conf_path))
    config["config_version_name"] = "model_with_mcp"
    config["mcp_server_urls"] = [
        "https://adb-984752964297111.11.azuredatabricks.net/api/2.0/mcp/functions/system/ai"
    ]

    with open(conf_path, "w") as f:
        yaml.safe_dump(config, f, sort_keys=False)


except Exception as e:
    print(f"⚠️ Skipped MCP update: {e}")

# COMMAND ----------

# Use same experiment to keep all traces together
#--------Here it exceeds the recursion depth
#mlflow.set_experiment(agent_eval_path + "/02.1_agent_evaluation")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Instantiate the agent
# MAGIC
# MAGIC Next we will instantiate the agent and to make sure we have provided the tools from the managed MCP server, we will check all the available tools that the agent has. In the list, besides the UC functions we have created, we will see also the tools from MCP server which has the UC system ai functions i.e **system__ai__python_exec**.

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
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/ai-agent/pg-mcp-img1?raw=true?raw=true" width="800px">

# COMMAND ----------

# MAGIC %md When we start to ask questions, we will see that the agent has properly loaded the tool from the Managed MCP server we added above.

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/ai-agent/pg-mcp-img2?raw=true?raw=true" width="800px">

# COMMAND ----------

# MAGIC %md Now we can start exploring more...

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/ai-agent/pg-mcp-img3?raw=true?raw=true" width="800px">

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next steps
# MAGIC If you would like to further explore other MCP server options in Databricks, how to test and use them in your agentic system, you could also leverage this Databricks MCP kit in [here](https://github.com/natyra-bajraktari/mcp-accl).

# COMMAND ----------


