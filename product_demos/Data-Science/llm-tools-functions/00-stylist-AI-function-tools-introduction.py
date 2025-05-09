# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC # Composable AI systems: Building and AI Stylist Specialist selling our products
# MAGIC
# MAGIC ## What's a composable AI system
# MAGIC
# MAGIC LLMs are great at answering general questions. However, general intelligence alone isn't enough to provide value to your customers.
# MAGIC
# MAGIC To be able to provide valuable answers, extra information is required, specific to your business and the user asking the question (your customer contract ID, the last email they sent to your support, your most recent sales report etc.).
# MAGIC
# MAGIC Composable AI systems are designed to answer this challenge. They are more advanced AI deployments, composed of multiple entities (tools) specialized in different action (retrieving information or acting on external systems). <br/>
# MAGIC
# MAGIC At a high level, you build & present a set of custom functions to the AI. The LLM can then reason about it, deciding which tool should be called and information gathered to answer the customer need.
# MAGIC
# MAGIC ## Building Composable AI Systems with Databricks Mosaic AI agent framework
# MAGIC
# MAGIC
# MAGIC Databricks simplifies this by providing a built-in service to:
# MAGIC
# MAGIC - Create and store your functions (tools) leveraging UC
# MAGIC - Execute the functions in a safe way
# MAGIC - Reason about the tools you selected and chain them together to properly answer your question. 
# MAGIC
# MAGIC At a high level, here is the AI system we will implement in this demo:
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/llm-tools-functions/llm-tools-functions-flow.png?raw=true" width="900px">
# MAGIC
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection or disable tracker during installation. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=data-science&notebook=00-stylist-AI-function-tools-introduction&demo_name=llm-tools-functions&event=VIEW">

# COMMAND ----------

# MAGIC %pip install databricks-sdk==0.41.0 langchain-community==0.2.10 langchain-openai==0.1.19 mlflow==2.20.2 faker==33.1.0
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./_resources/00-init-stylist $reset_all=false

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating our tools: Using Unity Catalog Functions
# MAGIC
# MAGIC Let's start by defining the functions our LLM will be able to execute. These functions can contain any logic, from simple SQL to advanced python.

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Computing Mathematics: converting inches to centimeters
# MAGIC
# MAGIC Our online shop is selling across all regions, and we know our customers often ask to convert cm to inches. However, plain LLMs are quite bad at executing math. To solve this, let's create a simple function doing the conversion.
# MAGIC
# MAGIC We'll save this function within Unity Catalog. You can open the explorer to review the functions created in this notebook.
# MAGIC
# MAGIC *Note: This is a very simple first example for this demo. We'll implement a broader math tool later on.*

# COMMAND ----------

# DBTITLE 1,Convert inch to cm
# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION convert_inch_to_cm(size_in_inch FLOAT)
# MAGIC RETURNS FLOAT
# MAGIC LANGUAGE SQL
# MAGIC COMMENT 'convert size from inch to cm'
# MAGIC RETURN size_in_inch * 2.54;
# MAGIC
# MAGIC -- let's test our function:
# MAGIC SELECT convert_inch_to_cm(10) as 10_inches_in_cm;

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Executing a tool to fetch internal data: getting the latest customer orders
# MAGIC
# MAGIC We want our stylist assistant to be able to list all existing customer orders

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION get_customer_orders ()
# MAGIC RETURNS TABLE(user_id STRING,
# MAGIC   id STRING,
# MAGIC   transaction_date STRING,
# MAGIC   item_count DOUBLE,
# MAGIC   amount DOUBLE,
# MAGIC   order_status STRING)
# MAGIC COMMENT 'Returns a list of customer orders for the given customer ID (expect a UUID)'
# MAGIC LANGUAGE SQL
# MAGIC     RETURN
# MAGIC     SELECT o.* from tools_orders o 
# MAGIC     inner join tools_customers c on c.id = o.user_id 
# MAGIC     where email=current_user() ORDER BY transaction_date desc;
# MAGIC
# MAGIC SELECT * FROM get_customer_orders();

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Executing a Python function to fetch external dataset in realtime: getting the weather
# MAGIC
# MAGIC We want our stylist assistant to give us recommendations based on the weather. Let's add a tool to fetch the weather based on longitude/latitude, using Python to call an external Weather API.
# MAGIC
# MAGIC **Note: This will be run by a serverless compute, and accessing external data, therefore requires serverless network egress access. If this fails in a serverless setup/playground, make sure you are allowing it in your networking configuration (open the workspace network option at admin account level)**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION get_weather(latitude DOUBLE, longitude DOUBLE)
# MAGIC RETURNS STRUCT<temperature_in_celsius DOUBLE, rain_in_mm DOUBLE>
# MAGIC LANGUAGE PYTHON
# MAGIC COMMENT 'This function retrieves the current temperature and rain information for a given latitude and longitude using the Open-Meteo API.'
# MAGIC AS
# MAGIC $$
# MAGIC   try:
# MAGIC     import requests as r
# MAGIC     #Note: this is provided for education only, non commercial - please get a license for real usage: https://api.open-meteo.com. Let s comment it to avoid issues for now
# MAGIC     #weather = r.get(f'https://api.open-meteo.com/v1/forecast?latitude={latitude}&longitude={longitude}&current=temperature_2m,rain&forecast_days=1').json()
# MAGIC     return {
# MAGIC       "temperature_in_celsius": weather["current"]["temperature_2m"],
# MAGIC       "rain_in_mm": weather["current"]["rain"]
# MAGIC     }
# MAGIC   except:
# MAGIC     return {"temperature_in_celsius": 25.0, "rain_in_mm": 0.0}
# MAGIC $$;
# MAGIC
# MAGIC -- let's test our function:
# MAGIC SELECT get_weather(52.52, 13.41) as weather;

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Creating a function calling LLMs with specific prompt as a tool
# MAGIC
# MAGIC You can also register tools containing custom prompts that your LLM can use to to execute actions based on the customer context.
# MAGIC
# MAGIC Let's create a tool that recommend the style for our user, based on the current weather.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION recommend_outfit_description(requested_style STRING, temperature_in_celsius FLOAT, rain_in_mm FLOAT)
# MAGIC RETURNS STRING
# MAGIC LANGUAGE SQL
# MAGIC COMMENT 'This function generate a stylist outfit description based on initial request and the current weather.'
# MAGIC RETURN SELECT ai_query('databricks-meta-llama-3-3-70b-instruct',
# MAGIC     CONCAT("You are a stylist assistant. Your goal is to give recommendation on what would be the best outfit for today. The current temperature is ",temperature_in_celsius ," celsius and rain is:", rain_in_mm, "mm. Give size in inches if any. Don't assume customer size if they don't share it. Give ideas of colors and other items to match. The user added this instruction based on what they like: ", requested_style)
# MAGIC   ) AS recommended_outfit;
# MAGIC
# MAGIC -- let's test our function:
# MAGIC SELECT recommend_outfit_description("I need a dress for an interview.", 30.1, 0.0)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Using Vector search to find similar content as a tool
# MAGIC
# MAGIC Let's now add a tool to recomment similar items, based on an article description.
# MAGIC
# MAGIC We'll be using Databricks Vector Search to perform a realtime similarity search and return articles that we could suggest from our database.
# MAGIC
# MAGIC To do so, you can leverage the new `vector_search` SQL function. See [Documentation](https://docs.databricks.com/en/sql/language-manual/functions/vector_search.html) for more details.
# MAGIC
# MAGIC <div style="background-color: #e3efff"> To simplify this demo, we'll fake the call to the vector_search and make it work without pre-loading the demo</div>

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Commenting this as the index isn't pre-loaded in the demo to simplify the experience.
# MAGIC -- CREATE OR REPLACE FUNCTION find_clothes_matching_description (description STRING)
# MAGIC -- RETURNS TABLE (category STRING, color STRING, id STRING, price DOUBLE, search_score DOUBLE)
# MAGIC -- LANGUAGE SQL
# MAGIC -- COMMENT 'Finds clothes in our catalog matching the description using AI query and returns a table of results.'
# MAGIC -- RETURN
# MAGIC --  SELECT * FROM VECTOR_SEARCH(index => 'catalog.schema.clothing_index', query => query, num_results => 3) vs
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Generate fake data instead of calling the VS index with vector_search;
# MAGIC CREATE OR REPLACE FUNCTION find_clothes_matching_description (description STRING)
# MAGIC RETURNS TABLE (id BIGINT, name STRING, color STRING, category STRING, price DOUBLE, description STRING)
# MAGIC COMMENT 'Finds existing clothes in our catalog matching the description using AI query and returns a table of results.'
# MAGIC RETURN
# MAGIC SELECT clothes.* FROM (
# MAGIC   SELECT explode(from_json(
# MAGIC     ai_query(
# MAGIC       'databricks-meta-llama-3-3-70b-instruct', 
# MAGIC       CONCAT(
# MAGIC         'returns a json list of 3 json object clothes: <"id": bigint, "name": string, "color": string, "category": string, "price": double, "description": string>. These clothes should match the following user description: ', description, '. Return only the answer as a javascript json object ready to be parsed, no comment or text or javascript or ``` at the beginning.' ) ), 'ARRAY<STRUCT<id: BIGINT, name: STRING, color: STRING, category: STRING, price: DOUBLE, description: STRING>>' )) AS clothes );
# MAGIC
# MAGIC -- let's test our function:
# MAGIC SELECT * FROM find_clothes_matching_description('a red dress');

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Using AI/BI Genie Conversation API to query our data as a tool
# MAGIC
# MAGIC Let's now add a tool to query our data in natural language.
# MAGIC
# MAGIC We'll be using Databricks AI/BI Genie to query our data in natural language, convert it in sql, run it and return the result.
# MAGIC
# MAGIC To do so, you can leverage the new `AI/BI Genie` Conversation APIs. See [Documentation](https://docs.databricks.com/aws/en/genie/conversation-api) for more details.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION _genie_query(databricks_host STRING, 
# MAGIC                   databricks_token STRING,
# MAGIC                   space_id STRING,
# MAGIC                   question STRING,
# MAGIC                   contextual_history STRING)
# MAGIC RETURNS STRING
# MAGIC LANGUAGE PYTHON
# MAGIC COMMENT 'This is a agent that you can converse with to get answers to questions. Try to provide simple questions and provide history if you had prior conversations.'
# MAGIC AS
# MAGIC $$
# MAGIC     import json
# MAGIC     import os
# MAGIC     import time
# MAGIC     from dataclasses import dataclass
# MAGIC     from datetime import datetime
# MAGIC     from typing import Optional
# MAGIC     
# MAGIC     import pandas as pd
# MAGIC     import requests
# MAGIC     
# MAGIC     
# MAGIC     @dataclass
# MAGIC     class GenieResult:
# MAGIC         space_id: str
# MAGIC         conversation_id: str
# MAGIC         question: str
# MAGIC         content: Optional[str]
# MAGIC         sql_query: Optional[str] = None
# MAGIC         sql_query_description: Optional[str] = None
# MAGIC         sql_query_result: Optional[pd.DataFrame] = None
# MAGIC         error: Optional[str] = None
# MAGIC     
# MAGIC         def to_json_results(self):
# MAGIC             result = {
# MAGIC                 "space_id": self.space_id,
# MAGIC                 "conversation_id": self.conversation_id,
# MAGIC                 "question": self.question,
# MAGIC                 "content": self.content,
# MAGIC                 "sql_query": self.sql_query,
# MAGIC                 "sql_query_description": self.sql_query_description,
# MAGIC                 "sql_query_result": self.sql_query_result.to_dict(
# MAGIC                     orient="records") if self.sql_query_result is not None else None,
# MAGIC                 "error": self.error,
# MAGIC             }
# MAGIC             jsonified_results = json.dumps(result)
# MAGIC             return f"Genie Results are: {jsonified_results}"
# MAGIC     
# MAGIC         def to_string_results(self):
# MAGIC             results_string = self.sql_query_result.to_dict(orient="records") if self.sql_query_result is not None else None
# MAGIC             return ("Genie Results are: \n"
# MAGIC                     f"Space ID: {self.space_id}\n"
# MAGIC                     f"Conversation ID: {self.conversation_id}\n"
# MAGIC                     f"Question That Was Asked: {self.question}\n"
# MAGIC                     f"Content: {self.content}\n"
# MAGIC                     f"SQL Query: {self.sql_query}\n"
# MAGIC                     f"SQL Query Description: {self.sql_query_description}\n"
# MAGIC                     f"SQL Query Result: {results_string}\n"
# MAGIC                     f"Error: {self.error}")
# MAGIC     
# MAGIC     class GenieClient:
# MAGIC     
# MAGIC         def __init__(self, *,
# MAGIC                      host: Optional[str] = None,
# MAGIC                      token: Optional[str] = None,
# MAGIC                      api_prefix: str = "/api/2.0/genie/spaces"):
# MAGIC             self.host = host or os.environ.get("DATABRICKS_HOST")
# MAGIC             self.token = token or os.environ.get("DATABRICKS_TOKEN")
# MAGIC             assert self.host is not None, "DATABRICKS_HOST is not set"
# MAGIC             assert self.token is not None, "DATABRICKS_TOKEN is not set"
# MAGIC             self._workspace_client = requests.Session()
# MAGIC             self._workspace_client.headers.update({"Authorization": f"Bearer {self.token}"})
# MAGIC             self._workspace_client.headers.update({"Content-Type": "application/json"})
# MAGIC             self.api_prefix = api_prefix
# MAGIC             self.max_retries = 300
# MAGIC             self.retry_delay = 1
# MAGIC             self.new_line = "\r\n"
# MAGIC     
# MAGIC         def _make_url(self, path):
# MAGIC             return f"{self.host.rstrip('/')}/{path.lstrip('/')}"
# MAGIC     
# MAGIC         def start(self, space_id: str, start_suffix: str = "") -> str:
# MAGIC             path = self._make_url(f"{self.api_prefix}/{space_id}/start-conversation")
# MAGIC             resp = self._workspace_client.post(
# MAGIC                 url=path,
# MAGIC                 headers={"Content-Type": "application/json"},
# MAGIC                 json={"content": "starting conversation" if not start_suffix else f"starting conversation {start_suffix}"},
# MAGIC             )
# MAGIC             resp = resp.json()
# MAGIC             print(resp)
# MAGIC             return resp["conversation_id"]
# MAGIC     
# MAGIC         def ask(self, space_id: str, conversation_id: str, message: str) -> GenieResult:
# MAGIC             path = self._make_url(f"{self.api_prefix}/{space_id}/conversations/{conversation_id}/messages")
# MAGIC             # TODO: cleanup into a separate state machine
# MAGIC             resp_raw = self._workspace_client.post(
# MAGIC                 url=path,
# MAGIC                 headers={"Content-Type": "application/json"},
# MAGIC                 json={"content": message},
# MAGIC             )
# MAGIC             resp = resp_raw.json()
# MAGIC             message_id = resp.get("message_id", resp.get("id"))
# MAGIC             if message_id is None:
# MAGIC                 print(resp, resp_raw.url, resp_raw.status_code, resp_raw.headers)
# MAGIC                 return GenieResult(content=None, error="Failed to get message_id")
# MAGIC     
# MAGIC             attempt = 0
# MAGIC             query = None
# MAGIC             query_description = None
# MAGIC             content = None
# MAGIC     
# MAGIC             while attempt < self.max_retries:
# MAGIC                 resp_raw = self._workspace_client.get(
# MAGIC                     self._make_url(f"{self.api_prefix}/{space_id}/conversations/{conversation_id}/messages/{message_id}"),
# MAGIC                     headers={"Content-Type": "application/json"},
# MAGIC                 )
# MAGIC                 resp = resp_raw.json()
# MAGIC                 status = resp["status"]
# MAGIC                 if status == "COMPLETED":
# MAGIC                     try:
# MAGIC     
# MAGIC                         query = resp["attachments"][0]["query"]["query"]
# MAGIC                         query_description = resp["attachments"][0]["query"].get("description", None)
# MAGIC                         content = resp["attachments"][0].get("text", {}).get("content", None)
# MAGIC                     except Exception as e:
# MAGIC                         return GenieResult(
# MAGIC                             space_id=space_id,
# MAGIC                             conversation_id=conversation_id,
# MAGIC                             question=message,
# MAGIC                             content=resp["attachments"][0].get("text", {}).get("content", None)
# MAGIC                         )
# MAGIC                     break
# MAGIC     
# MAGIC                 elif status == "EXECUTING_QUERY":
# MAGIC                     self._workspace_client.get(
# MAGIC                         self._make_url(
# MAGIC                             f"{self.api_prefix}/{space_id}/conversations/{conversation_id}/messages/{message_id}/query-result"),
# MAGIC                         headers={"Content-Type": "application/json"},
# MAGIC                     )
# MAGIC                 elif status in ["FAILED", "CANCELED"]:
# MAGIC                     return GenieResult(
# MAGIC                         space_id=space_id,
# MAGIC                         conversation_id=conversation_id,
# MAGIC                         question=message,
# MAGIC                         content=None,
# MAGIC                         error=f"Query failed with status {status}"
# MAGIC                     )
# MAGIC                 elif status != "COMPLETED" and attempt < self.max_retries - 1:
# MAGIC                     time.sleep(self.retry_delay)
# MAGIC                 else:
# MAGIC                     return GenieResult(
# MAGIC                         space_id=space_id,
# MAGIC                         conversation_id=conversation_id,
# MAGIC                         question=message,
# MAGIC                         content=None,
# MAGIC                         error=f"Query failed or still running after {self.max_retries * self.retry_delay} seconds"
# MAGIC                     )
# MAGIC                 attempt += 1
# MAGIC             resp = self._workspace_client.get(
# MAGIC                 self._make_url(
# MAGIC                     f"{self.api_prefix}/{space_id}/conversations/{conversation_id}/messages/{message_id}/query-result"),
# MAGIC                 headers={"Content-Type": "application/json"},
# MAGIC             )
# MAGIC             resp = resp.json()
# MAGIC             columns = resp["statement_response"]["manifest"]["schema"]["columns"]
# MAGIC             header = [str(col["name"]) for col in columns]
# MAGIC             rows = []
# MAGIC             output = resp["statement_response"]["result"]
# MAGIC             if not output:
# MAGIC                 return GenieResult(
# MAGIC                     space_id=space_id,
# MAGIC                     conversation_id=conversation_id,
# MAGIC                     question=message,
# MAGIC                     content=content,
# MAGIC                     sql_query=query,
# MAGIC                     sql_query_description=query_description,
# MAGIC                     sql_query_result=pd.DataFrame([], columns=header),
# MAGIC                 )
# MAGIC             for item in resp["statement_response"]["result"]["data_typed_array"]:
# MAGIC                 row = []
# MAGIC                 for column, value in zip(columns, item["values"]):
# MAGIC                     type_name = column["type_name"]
# MAGIC                     str_value = value.get("str", None)
# MAGIC                     if str_value is None:
# MAGIC                         row.append(None)
# MAGIC                         continue
# MAGIC                     match type_name:
# MAGIC                         case "INT" | "LONG" | "SHORT" | "BYTE":
# MAGIC                             row.append(int(str_value))
# MAGIC                         case "FLOAT" | "DOUBLE" | "DECIMAL":
# MAGIC                             row.append(float(str_value))
# MAGIC                         case "BOOLEAN":
# MAGIC                             row.append(str_value.lower() == "true")
# MAGIC                         case "DATE":
# MAGIC                             row.append(datetime.strptime(str_value, "%Y-%m-%d").date())
# MAGIC                         case "TIMESTAMP":
# MAGIC                             row.append(datetime.strptime(str_value, "%Y-%m-%d %H:%M:%S"))
# MAGIC                         case "BINARY":
# MAGIC                             row.append(bytes(str_value, "utf-8"))
# MAGIC                         case _:
# MAGIC                             row.append(str_value)
# MAGIC                 rows.append(row)
# MAGIC     
# MAGIC             query_result = pd.DataFrame(rows, columns=header)
# MAGIC             return GenieResult(
# MAGIC                 space_id=space_id,
# MAGIC                 conversation_id=conversation_id,
# MAGIC                 question=message,
# MAGIC                 content=content,
# MAGIC                 sql_query=query,
# MAGIC                 sql_query_description=query_description,
# MAGIC                 sql_query_result=query_result,
# MAGIC             )
# MAGIC     
# MAGIC     
# MAGIC     assert databricks_host is not None, "host is not set"
# MAGIC     assert databricks_token is not None, "token is not set"
# MAGIC     assert space_id is not None, "space_id is not set"
# MAGIC     assert question is not None, "question is not set"
# MAGIC     assert contextual_history is not None, "contextual_history is not set"
# MAGIC     client = GenieClient(host=databricks_host, token=databricks_token)
# MAGIC     conversation_id = client.start(space_id)
# MAGIC     formatted_message = f"""Use the contextual history to answer the question. The history may or may not help you. Use it if you find it relevant.
# MAGIC     
# MAGIC     Contextual History: {contextual_history}
# MAGIC     
# MAGIC     Question to answer: {question}
# MAGIC     """
# MAGIC     
# MAGIC     result = client.ask(space_id, conversation_id, formatted_message)
# MAGIC     
# MAGIC     return result.to_string_results()
# MAGIC
# MAGIC $$;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION ask_genie(question STRING COMMENT "The question to ask about your dataset", contextual_history STRING COMMENT "provide relevant history to be able to answer this question, assume genie doesn\'t keep track of history. Use \'no relevant history\' if there is nothing relevant to answer the question.")
# MAGIC RETURNS STRING
# MAGIC LANGUAGE SQL
# MAGIC COMMENT 'Query your tables in natural language using the AI/BI Genie Conversation API to provide answers about the latest fashion trends.'  
# MAGIC RETURN SELECT _genie_query(
# MAGIC   "https://workspace.cloud.databricks.com/",
# MAGIC   'token',
# MAGIC   'space_id',
# MAGIC   question, -- retrieved from function
# MAGIC   contextual_history -- retrieved from function
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT ask_genie('How many people bought a hat the last three months ?', '')

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Using Databricks Playground to test our functions
# MAGIC
# MAGIC Databricks Playground provides a built-in integration with your functions. It'll analyze which functions are available, and call them to properly answer your question.
# MAGIC
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/llm-tools-functions/llm-tools-functions-playground.gif?raw=true" style="float: right; margin-left: 10px; margin-bottom: 10px;">
# MAGIC
# MAGIC To try out our functions with playground:
# MAGIC - Open the [Playground](/ml/playground) 
# MAGIC - Select a model supporting tools (like Llama3.1)
# MAGIC - Add the functions you want your model to leverage (`catalog.schema.function_name`)
# MAGIC - Ask a question (for example to convert inch to cm), and playground will do the magic for you!
# MAGIC
# MAGIC <br/>
# MAGIC <div style="background-color: #d4e7ff; padding: 10px; border-radius: 15px;clear:both">
# MAGIC <strong>Note:</strong> Tools in playground is in preview, reach-out your Databricks Account team for more details and to enable it.
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Building an AI system leveraging our Databricks UC functions with Langchain
# MAGIC
# MAGIC These tools can also directly be leveraged on custom model. In this case, you'll be in charge of chaining and calling the functions yourself (the playground does it for you!)
# MAGIC
# MAGIC Langchain makes it easy for you. You can create your own custom AI System using a Langchain model and a list of existing tools (in our case, the tools will be the functions we just created)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Enable MLflow Tracing
# MAGIC
# MAGIC Enabling MLflow Tracing is required to:
# MAGIC - View the chain's trace visualization in this notebook
# MAGIC - Capture the chain's trace in production via Inference Tables
# MAGIC - Evaluate the chain via the Mosaic AI Evaluation Suite

# COMMAND ----------

import mlflow
mlflow.langchain.autolog()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Start by creating our tools from Unity Catalog
# MAGIC
# MAGIC Let's use UCFunctionToolkit to select which functions we want to use as tool for our demo:

# COMMAND ----------

from langchain_community.tools.databricks import UCFunctionToolkit
import pandas as pd
wh = get_shared_warehouse(name = None) #Get the first shared wh we can. See _resources/01-init for details
print(f'This demo will be using the wg {wh.name} to execute the functions')

def get_tools():
    return (
        UCFunctionToolkit(warehouse_id=wh.id)
        # Include functions as tools using their qualified names.
        # You can use "{catalog_name}.{schema_name}.*" to get all functions in a schema.
        .include(f"{catalog}.{db}.*")
        .get_tools())

display_tools(get_tools()) #display in a table the tools - see _resource/00-init for details

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Let's create our langchain agent using the tools we just created

# COMMAND ----------

# DBTITLE 1,Create the chat

from langchain_openai import ChatOpenAI
from databricks.sdk import WorkspaceClient

# Note: langchain_community.chat_models.ChatDatabricks doesn't support create_tool_calling_agent yet - it'll soon be availableK. Let's use ChatOpenAI for now
llm = ChatOpenAI(
  base_url=f"{WorkspaceClient().config.host}/serving-endpoints/",
  api_key=dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get(),
  model="databricks-meta-llama-3-3-70b-instruct"
)


# COMMAND ----------

# DBTITLE 1,Define the Prompt
from langchain_core.prompts import ChatPromptTemplate
from langchain_community.chat_models import ChatDatabricks

def get_prompt(history = [], prompt = None):
    if not prompt:
            prompt = """You are a helpful fashion assistant. Your task is to recommend cothes to the users. You can use the following tools:
    - Use the convert_inch_to_cm to covert inch to cm if the customer ask for it
    - Use get_customer_orders to get the list of orders and their status, total amount and number of article. 
    - Use find_clothes_matching_description to find existing clothes from our internal catalog and suggest article to the customer
    - Use recommend_outfit if a customer ask you for a style. This function take the weather as parameter. Use the get_weather function first before calling this function to get the current temperature and rain. The current user location is 52.52, 13.41.
    - Use ask_genie if a customer ask you something on what are the latest fashion trends. This function takes the question and the context as parameters. 

    Make sure to use the appropriate tool for each step and provide a coherent response to the user. Don't mention tools to your users. Only answer what the user is asking for. If the question isn't related to the tools or style/clothe, say you're sorry but can't answer"""
    return ChatPromptTemplate.from_messages([
            ("system", prompt),
            ("human", "{input}"),
            ("placeholder", "{agent_scratchpad}"),
    ])

# COMMAND ----------

from langchain.agents import AgentExecutor, create_tool_calling_agent
prompt = get_prompt()
tools = get_tools()
agent = create_tool_calling_agent(llm, tools, prompt)
agent_executor = AgentExecutor(agent=agent, tools=tools, verbose=True)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Let's give it a try: Asking to run a simple size conversion.
# MAGIC Under the hood, we want this to call our Math conversion function:

# COMMAND ----------

#make sure our log is enabled to properly display and trace the call chain 
mlflow.langchain.autolog()
agent_executor.invoke({"input": "what's 12in in cm?"})

# COMMAND ----------

agent_executor.invoke({"input": "what are my latest orders?"})

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Chaining multiple tools
# MAGIC
# MAGIC Let's see if the agent can properly chain the calls, using multiple tools and passing the values from one to another. We'll ask for an outfit for today, and the expectation is that the LLM will fetch the weather before.

# COMMAND ----------

agent_executor = AgentExecutor(agent=agent, tools=tools, verbose=True)
answer = agent_executor.invoke({"input": "I need a dress for an interview I have today. What style would you recommend for today?"})

# COMMAND ----------

#Note: in a real app, we would include the preview discussion as history to keep the reference.
answer = agent_executor.invoke({"input": "Can you give me a list of red dresses I can buy?"})

# COMMAND ----------

displayHTML(answer['output'].replace('\n', '<br>'))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extra: running more advanced functions using python:
# MAGIC
# MAGIC Let's see a few more advanced tools example

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Calculator tool: Supporting math operations 
# MAGIC Let's add a function to allow our LLM to execute any Math operation. 
# MAGIC
# MAGIC Databricks runs the python in a safe container. However, we'll filter what the function can do to avoid any potential issues with prompt injection (so that the user cannot execute other python instructions).

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION compute_math(expr STRING)
# MAGIC RETURNS STRING
# MAGIC LANGUAGE PYTHON
# MAGIC COMMENT 'Run any mathematical function and returns the result as output. Supports python syntax like math.sqrt(13)'
# MAGIC AS
# MAGIC $$
# MAGIC   import ast
# MAGIC   import operator
# MAGIC   import math
# MAGIC   operators = {ast.Add: operator.add, ast.Sub: operator.sub, ast.Mult: operator.mul, ast.Div: operator.truediv, ast.Pow: operator.pow, ast.Mod: operator.mod, ast.FloorDiv: operator.floordiv, ast.UAdd: operator.pos, ast.USub: operator.neg}
# MAGIC     
# MAGIC   # Supported functions from the math module
# MAGIC   functions = {name: getattr(math, name) for name in dir(math) if callable(getattr(math, name))}
# MAGIC
# MAGIC   def eval_node(node):
# MAGIC     if isinstance(node, ast.Num):  # <number>
# MAGIC       return node.n
# MAGIC     elif isinstance(node, ast.BinOp):  # <left> <operator> <right>
# MAGIC       return operators[type(node.op)](eval_node(node.left), eval_node(node.right))
# MAGIC     elif isinstance(node, ast.UnaryOp):  # <operator> <operand> e.g., -1
# MAGIC       return operators[type(node.op)](eval_node(node.operand))
# MAGIC     elif isinstance(node, ast.Call):  # <func>(<args>)
# MAGIC       func = node.func.id
# MAGIC       if func in functions:
# MAGIC         args = [eval_node(arg) for arg in node.args]
# MAGIC         return functions[func](*args)
# MAGIC       else:
# MAGIC         raise TypeError(f"Unsupported function: {func}")
# MAGIC     else:
# MAGIC       raise TypeError(f"Unsupported type: {type(node)}")  
# MAGIC   try:
# MAGIC     if expr.startswith('```') and expr.endswith('```'):
# MAGIC       expr = expr[3:-3].strip()      
# MAGIC     node = ast.parse(expr, mode='eval').body
# MAGIC     return eval_node(node)
# MAGIC   except Exception as ex:
# MAGIC     return str(ex)
# MAGIC $$;
# MAGIC -- let's test our function:
# MAGIC SELECT compute_math("(2+2)/3") as result;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run any python function
# MAGIC
# MAGIC If we are creating a coding assistant, it can be useful to allow our AI System to run python code to try them and output the results to the user. **Be careful doing that, as any code could be executed by the user.**
# MAGIC
# MAGIC Here is an example:

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION execute_python_code(python_code STRING)
# MAGIC RETURNS STRING
# MAGIC LANGUAGE PYTHON
# MAGIC COMMENT "Run python code. The code should end with a return statement and this function will return it as a string. Only send valid python to this function. Here is an exampe of python code input: 'def square_function(number):\\n  return number*number\\n\\nreturn square_function(3)'"
# MAGIC AS
# MAGIC $$
# MAGIC     import traceback
# MAGIC     try:
# MAGIC         import re
# MAGIC         # Remove code block markers (e.g., ```python) and strip whitespace```
# MAGIC         python_code = re.sub(r"^\s*```(?:python)?|```\s*$", "", python_code).strip()
# MAGIC         # Unescape any escaped newline characters
# MAGIC         python_code = python_code.replace("\\n", "\n")
# MAGIC         # Properly indent the code for wrapping
# MAGIC         indented_code = "\n    ".join(python_code.split("\n"))
# MAGIC         # Define a wrapper function to execute the code
# MAGIC         exec_globals = {}
# MAGIC         exec_locals = {}
# MAGIC         wrapper_code = "def _temp_function():\n    "+indented_code
# MAGIC         exec(wrapper_code, exec_globals, exec_locals)
# MAGIC         # Execute the wrapped function and return its output
# MAGIC         result = exec_locals["_temp_function"]()
# MAGIC         return result
# MAGIC     except Exception as ex:
# MAGIC         return traceback.format_exc()
# MAGIC $$;
# MAGIC -- let's test our function:
# MAGIC
# MAGIC SELECT execute_python_code("return 'Hello World! '* 3") as result;

# COMMAND ----------

from langchain.agents import AgentExecutor, create_tool_calling_agent
prompt = get_prompt(prompt="You are an assistant for a python developer. Internally, you have a tool named execute_python_code that can generate and run python code to help answering what the customer is asking. input: valid python code as a string. output: the result of the return value from the code execution. Don't mention you have tools or the tools name. Make sure you send the full python code at once to the function and that the code has a return statement at the end to capture the result. Don't print anything in the code you write, return the result you need as final instruction. Make sure the python code is valid. Only send python. Here is an example: 'def square_function(number):\\n  return number*number\\n\\nreturn square_function(3)'")
tools = get_tools()
agent = create_tool_calling_agent(llm, tools, prompt)
agent_executor = AgentExecutor(agent=agent, tools=tools, verbose=True)

#Let's print the answer! Note that the LLM often makes a few error, but then analyze it and self-correct.
answer = agent_executor.invoke({"input": "What's the result of the fibonacci suite? Display its result for 5."})
displayHTML(answer['output'].replace('\n', '<br>'))

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Deploying our Agent Executor as Model Serving Endpoint
# MAGIC
# MAGIC We're now ready to package our chain within MLFLow, and deploy it as a Model Serving Endpoint, leveraging Databricks Agent Framework and its review application.

# COMMAND ----------

# DBTITLE 1,Create the chain
# TODO: write this as a separate file if you want to deploy it properly
from langchain.schema.runnable import RunnableLambda
from langchain_core.output_parsers import StrOutputParser

# Function to extract the user's query
def extract_user_query_string(chat_messages_array):
    return chat_messages_array[-1]["content"]

# Wrapping the agent_executor invocation
def agent_executor_wrapper(input_data):
    result = agent_executor.invoke({"input": input_data})
    return result["output"]

# Create the chain using the | operator with StrOutputParser
chain = (
    RunnableLambda(lambda data: extract_user_query_string(data["messages"]))  # Extract the user query
    | RunnableLambda(agent_executor_wrapper)  # Pass the query to the agent executor
    | StrOutputParser()  # Optionally parse the output to ensure it's a clean string
)

# COMMAND ----------

# DBTITLE 1,Let's try our chain
# Example input data
input_data = {
    "messages": [
        {"content": "Write a function that computes the Fibonacci sequence in Python and displays its result for 5."}
    ]
}
# Run the chain
answer = chain.invoke(input_data)
displayHTML(answer.replace('\n', '<br>'))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deploying the agent as a model serving endpoint
# MAGIC
# MAGIC Databricks automatically generates all the required notebooks and setup for you to deploy these agents!
# MAGIC
# MAGIC To deploy them with the latest configuration, open [Databricks Playground ](/ml/playground), select the tools you want to use and click on the "Export Notebook" button on the top!
# MAGIC
# MAGIC This will generate notebooks pre-configured for you, including the review application to start testing and collecting your new model!

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conclusion
# MAGIC That's it, we saw how you can leverage Databricks Functions as tools within your AI system.
# MAGIC
# MAGIC This demo gave you a simple overview and some examples to get started. You can take it to the next level within your own AI Systems!
# MAGIC
# MAGIC Once your application is complete and your chain ready to be deployed, you can easily serve your model as a Model Serving Endpoint, acting as your Compound AI System!
# MAGIC
# MAGIC ### Coming soon
# MAGIC
# MAGIC We'll soon update this demo to add more examples, including:
# MAGIC - how to properly leverage secrets within AI functions to call external systems having Authentication
# MAGIC - generating images and calling ML model endpoints
# MAGIC - Use Genie as an agent to query any table!
# MAGIC - More to come!
# MAGIC
# MAGIC Stay tuned!
