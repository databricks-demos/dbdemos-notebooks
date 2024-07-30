# Databricks notebook source
# MAGIC %md
# MAGIC # Building composable AI systems using 
# MAGIC
# MAGIC ## What's a composable AI systems
# MAGIC
# MAGIC Composable AI systems are advanced AI deployments, composed of multiple entities (tools) specialized in different action. <br/>
# MAGIC You can think of a RAG system as a simple version having a single tool: retrieving similar documents.
# MAGIC
# MAGIC However, what if you need extra information (your customer contract ID, the last email they sent to your support, your most recent sales report), but each information might be specific to the context and who is asking it.
# MAGIC
# MAGIC This is what Function calling enable. You present all the functions available to your LLM explaining what they do, and let the AI reason about it, finding what will be relevant to answer the customer question.
# MAGIC
# MAGIC ## Building Composable AI Systems with Databricks Mosaic AI agent framework
# MAGIC
# MAGIC Databricks simplifies this by providing a builtin service, reasonning about the tools you have and acting based on them. 
# MAGIC
# MAGIC
# MAGIC  your own Chatbot Assisstant to help your customers answer questions about Databricks, using Retrieval Augmented Generation (RAG), Databricks State of The Art LLM DBRX Instruct Foundation Model Vector Search.
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection or disable tracker during installation. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=data-science&notebook=00-AI-function-tools-introduction&demo_name=ai-function-calling&event=VIEW">

# COMMAND ----------

# MAGIC %pip install -U databricks-sdk==0.23.0 langchain-community langchain-openai mlflow==2.14.3
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./_resources/00-init $reset_all=false

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Computing Mathematics: converting inches to centimeters
# MAGIC
# MAGIC Our online shop is selling across all regions, and we know our customers often ask to convert cm to inches. However, plain LLMs are very bad at executing math. To solve this, let's create a simple function doing the conversion.
# MAGIC
# MAGIC *Note: This is a very simple first example for this demo. We'll implement a broader math tool later on.*

# COMMAND ----------

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
# MAGIC ## Executing Python and fetching external dataset in realtime: getting the weather
# MAGIC
# MAGIC We want our stylist assistant to give us recommendations based on the weather. Let's add a tool to fetch the weather based on longitude/latitude, using Python to call an external Weather API.

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
# MAGIC ## Using LLMs with specific prompt as a tool
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
# MAGIC RETURN SELECT ai_query('databricks-meta-llama-3-70b-instruct',
# MAGIC     CONCAT("You are a stylist assistant. Your goal is to give recommendation on what would be the best outfit for today. The current temperature is ",temperature_in_celsius ," celsius and rain is:", rain_in_mm, "mm. Give size in inches if any. Don't assume customer size if they don't share it. Give ideas of colors and other items to match. The user added this instruction based on what they like: ", requested_style)
# MAGIC   ) AS recommended_outfit;
# MAGIC
# MAGIC -- let's test our function:
# MAGIC SELECT recommend_outfit_description("I need a dress for an interview.", 30.1, 0.0)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Using Vector search to find similar content as a tool
# MAGIC
# MAGIC Let's now add a tool to recomment similar items, based on an article description.
# MAGIC
# MAGIC We'll be using Databricks Vector Search to perform a realtime similarity search and return articles that we could suggest from our database.
# MAGIC
# MAGIC Do do so, you can leverage the new `vector_search` SQL function. See [Documentation](https://docs.databricks.com/en/sql/language-manual/functions/vector_search.html) for more details.
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
# MAGIC       'databricks-meta-llama-3-70b-instruct', 
# MAGIC       CONCAT(
# MAGIC         'returns a json list of 3 json object clothes: <"id": bigint, "name": string, "color": string, "category": string, "price": double, "description": string>. These clothes should match the following user description: ', description, '. Return only the answer as a javascript json object ready to be parsed, no comment or text or javascript or ``` at the beginning.' ) ), 'ARRAY<STRUCT<id: BIGINT, name: STRING, color: STRING, category: STRING, price: DOUBLE, description: STRING>>' )) AS clothes );
# MAGIC
# MAGIC -- let's test our function:
# MAGIC SELECT * FROM find_clothes_matching_description('a red dress');

# COMMAND ----------

# MAGIC %md
# MAGIC # Building an AI system leveraging our Databricks UC functions with Langchain

# COMMAND ----------

# MAGIC %md
# MAGIC ### Start by creating our tools from Unity Catalog

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
hostname = dbutils.notebook.entry_point.getDbutils().notebook().getContext().browserHostName().get()
# Note: langchain_community.chat_models.ChatDatabricks doesn't support create_tool_calling_agent yet
# Let's use ChatOpenAI for now
llm = ChatOpenAI(
  base_url=f"https://{hostname}/serving-endpoints/",
  api_key=dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get(),
  model="databricks-meta-llama-3-70b-instruct"
)


# COMMAND ----------

# DBTITLE 1,Define the Prompt
from langchain_core.prompts import ChatPromptTemplate
from langchain_community.chat_models import ChatDatabricks

def get_prompt(history = [], prompt = None):
    if not prompt:
            prompt = """You are a helpful fashion assistant. Your task is to recommend cothes to the users. You can use the following tools:
    - Use the convert_inch_to_cm to covert inch to cm if the customer ask for it
    - Use find_clothes_matching_description to find existing clothes from our internal catalog and suggest article to the customer
    - Use recommend_outfit if a customer ask you for a style. This function take the weather as parameter. Use the get_weather function first before calling this function to get the current temperature and rain. The current user location is 52.52, 13.41.

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

agent_executor.invoke({"input": "what's 12in in cm?"})

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Chaining multiple tools
# MAGIC
# MAGIC Let's see if the agent can properly chain the calls, using multiple tools and passing the values from one to another. We'll ask for an outfit for today, and the expectation is that the LLM will fetch the weather before.

# COMMAND ----------

agent_executor = AgentExecutor(agent=agent, tools=tools, verbose=True)
answer = agent_executor.invoke({"input": "I need a dress for an Interview I have today. What style would recommend for today?"})

# COMMAND ----------

#Note: in a real app, we would include the preview discussion as history to keep the reference.
answer = agent_executor.invoke({"input": "Can you give me a list of red dress I can buy?"})

# COMMAND ----------

displayHTML(answer['output'].replace('\n', '<br>'))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extra: running any python code as a tool:
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION execute_python_code(python_code STRING)
# MAGIC RETURNS STRING
# MAGIC LANGUAGE PYTHON
# MAGIC COMMENT 'Run any python code and returns the output'
# MAGIC AS
# MAGIC $$
# MAGIC   import traceback
# MAGIC   try:
# MAGIC       if code_string.startswith('```') and code_string.endswith('```'):
# MAGIC           code_string = code_string[3:-3].strip()      
# MAGIC       result = eval(python_code, globals())
# MAGIC       return str(result)
# MAGIC   except Exception as ex:
# MAGIC       return traceback.format_exc()
# MAGIC $$;
# MAGIC -- let's test our function:
# MAGIC SELECT execute_python_code("2+2") as result;

# COMMAND ----------

from langchain.agents import AgentExecutor, create_tool_calling_agent
prompt = get_prompt(prompt="You are an assistant for python developer. you can run any python code the customer is asking for to output to test and run the code, and display the output. Don't mention you have tools or the tools name. Make sure you send the full python code to the function")
tools = get_tools()
agent = create_tool_calling_agent(llm, tools, prompt)
agent_executor = AgentExecutor(agent=agent, tools=tools, verbose=True)
answer = agent_executor.invoke({"input": "Write me a function that compute fibonacci suite in python, and display its result for 5."})
displayHTML(answer['output'].replace('\n', '<br>'))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conclusion
# MAGIC That's it, we saw how you can leverage Databricks Functions as tools within your AI system.
# MAGIC
# MAGIC This demo gave you a simple overview and some example to get started. You can take it to the next level within your own AI Systems!
# MAGIC
# MAGIC ### Coming soon
# MAGIC
# MAGIC We'll soon update this demo to add more example, including:
# MAGIC - how to properly leverage secrets within AI functions to call external system having Authentication
# MAGIC - generating images and calling ML models endpoints
# MAGIC
# MAGIC Stay tuned!

# COMMAND ----------

# MAGIC %environment
# MAGIC "client": "1"
# MAGIC "base_environment": ""
