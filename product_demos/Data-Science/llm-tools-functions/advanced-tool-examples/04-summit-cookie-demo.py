# Databricks notebook source
# MAGIC %pip install --quiet -U databricks-sdk==0.23.0 langchain-community==0.2.16 langchain-openai==0.1.19 mlflow==2.14.3
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../_resources/00-init $reset_all=false

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating the "Cookie Demo" from Summit 2024!
# MAGIC
# MAGIC In this notebook - we're going to recreate a 'light' version of the cookie demo. This will entail the following:
# MAGIC - Creating simple SQL functions and registering them in UC
# MAGIC - Creating more advanced python functions in UC
# MAGIC - Use Langchain to bind these functions as tools 
# MAGIC - Create an Agent that can execute these tools and ask it some tough questions

# COMMAND ----------

# MAGIC %md
# MAGIC ##Step 0: Data Prep
# MAGIC
# MAGIC The cookie data now exists in the marketplace. There's no API yet, so for now we'll have to manually install. You can go to Marketplace and search cookies or use the following link below (replace the host URL if you're on a different workspace)
# MAGIC
# MAGIC https://e2-demo-field-eng.cloud.databricks.com/marketplace/consumer/listings/f8498740-31ea-49f8-9206-1bbf533f3993
# MAGIC
# MAGIC #### This will install the data into the following catalog:     
# MAGIC *databricks_cookies_dais_2024*
# MAGIC
# MAGIC #### There is a guide in the marketplace listing to the data, but for our purposes lets focus on these tables:    
# MAGIC **sales.franchises** - Contains information about the franchise itself (store name, location, country, size, ect).   
# MAGIC **sales.transactions** - Contains sales data for all franchises.   
# MAGIC     
# MAGIC **media.customer_reviews** - Social media reviews for a bunch of different franchises.   
# MAGIC **media.gold_reviews_chunked**  - Reviews but prepared for a Vector Search Index (added unique ID and chunked)
# MAGIC

# COMMAND ----------

# DBTITLE 1,Data Check
# MAGIC %sql
# MAGIC --Quick check to see if tables can be accessed
# MAGIC
# MAGIC Select * from databricks_cookies_dais_2024.sales.franchises limit 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ##Step 1: Create Functions
# MAGIC
# MAGIC The cookie data now exists in the marketplace. There's no API yet, so for now we'll have to manually install. You can go to Marketplace and search cookies or use the following link below (replace the host URL if you're on a different workspace)
# MAGIC
# MAGIC https://e2-demo-field-eng.cloud.databricks.com/marketplace/consumer/listings/f8498740-31ea-49f8-9206-1bbf533f3993
# MAGIC
# MAGIC #### This will install the data into the following catalog:     
# MAGIC *databricks_cookies_dais_2024*
# MAGIC
# MAGIC #### There is a guide in the marketplace listing to the data, but for our purposes lets focus on these tables:    
# MAGIC **sales.franchises** - Contains information about the franchise itself (store name, location, country, size, ect).   
# MAGIC **sales.transactions** - Contains sales data for all franchises.   
# MAGIC     
# MAGIC **media.customer_reviews** - Social media reviews for a bunch of different franchises.   
# MAGIC **media.gold_reviews_chunked**  - Reviews but prepared for a Vector Search Index (added unique ID and chunked)
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --First lets make sure it doesnt already exist
# MAGIC DROP FUNCTION IF EXISTS franchise_by_city;
# MAGIC --Now we create our first function. This takes in a city name and returns a table of any franchises that are in that city.
# MAGIC --Note that we've added a comment to the input parameter to help guide the agent later on.
# MAGIC CREATE OR REPLACE FUNCTION
# MAGIC franchise_by_city (
# MAGIC   city_name STRING COMMENT 'City to be searched'
# MAGIC )
# MAGIC returns table(franchiseID BIGINT, name STRING, size STRING)
# MAGIC LANGUAGE SQL
# MAGIC COMMENT 'This function takes in a city name and returns a table of any franchises that are in that city.'
# MAGIC return
# MAGIC (SELECT franchiseID, name, size from databricks_cookies_dais_2024.sales.franchises where city=city_name 
# MAGIC      order by size desc)
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test the function we just created
# MAGIC SELECT * from franchise_by_city('Seattle')

# COMMAND ----------

# MAGIC %sql
# MAGIC --Again check that it exists
# MAGIC DROP FUNCTION IF EXISTS franchise_sales;
# MAGIC --This function takes an ID as input, and this time does an aggregate to return the sales for that franchise_id.
# MAGIC CREATE OR REPLACE FUNCTION
# MAGIC franchise_sales (
# MAGIC   franchise_id BIGINT COMMENT 'ID of the franchise to be searched'
# MAGIC )
# MAGIC returns table(total_sales BIGINT, total_quantity BIGINT, product STRING)
# MAGIC LANGUAGE SQL
# MAGIC COMMENT 'This function takes an ID as input, and this time does an aggregate to return the sales for that franchise_id'
# MAGIC return
# MAGIC (SELECT SUM(totalPrice) AS total_sales, SUM(quantity) AS total_quantity, product 
# MAGIC FROM databricks_cookies_dais_2024.sales.transactions 
# MAGIC WHERE franchiseID = franchise_id GROUP BY product)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test the function we just created
# MAGIC SELECT * from franchise_sales(3000038)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create Agent
# MAGIC
# MAGIC In this step, we're going to define three cruicial parts of our agent:
# MAGIC - Tools for the Agent to use
# MAGIC - LLM to serve as the agent's "brains"
# MAGIC - System prompt that defines guidelines for the agent's tasks

# COMMAND ----------

from langchain_community.tools.databricks import UCFunctionToolkit
import pandas as pd
wh = get_shared_warehouse(name = None) #Get the first shared wh we can. See _resources/01-init for details
print(f'This demo will be using the {wh.name} to execute the functions')

def get_tools():
    return (
        UCFunctionToolkit(warehouse_id=wh.id)
        # Include functions as tools using their qualified names.
        # You can use "{catalog_name}.{schema_name}.*" to get all functions in a schema.
        .include(f"{catalog}.{db}.franchise_sales", f"{catalog}.{db}.franchise_by_city")
        #.include(f"{catalog}.{db}.*")
        .get_tools())

display_tools(get_tools()) #display in a table the tools - see _resource/00-init for details

# COMMAND ----------

from langchain_community.chat_models.databricks import ChatDatabricks

#We're going to use llama 3.1 because it's tool enabled and works great. Keep temp at 0 to make it more deterministic.
llm = ChatDatabricks(endpoint="databricks-meta-llama-3-1-70b-instruct",
    temperature=0.0,
    streaming=False)

# COMMAND ----------

from langchain_core.prompts import ChatPromptTemplate
from langchain_community.chat_models import ChatDatabricks

#This defines our agent's system prompt. Here we can tell it what we expect it to do and guide it on using specific functions. 

def get_prompt(history = [], prompt = None):
    if not prompt:
            prompt = """You are a helpful assistant for a global company that oversees cookie stores. Your task is to help store owners understand more about their products and sales metrics. You have the ability to execute functions as follows: 

            Use the franchise_by_city function to retrieve the franchiseID for a given city name.

            Use the franchise_sales function to retrieve the cookie sales for a given franchiseID.

    Make sure to call the function for each step and provide a coherent response to the user. Don't mention tools to your users. Don't skip to the next step without ensuring the function was called and a result was retrieved. Only answer what the user is asking for."""
    return ChatPromptTemplate.from_messages([
            ("system", prompt),
            ("human", "{messages}"),
            ("placeholder", "{agent_scratchpad}"),
    ])

# COMMAND ----------

from langchain.agents import AgentExecutor, create_openai_tools_agent

prompt = get_prompt()
tools = get_tools()
agent = create_openai_tools_agent(llm, tools, prompt)

#Here we're collecting the defined pieces and putting them together to create our Agent
agent_executor = AgentExecutor(agent=agent, tools=tools, verbose=True)

# COMMAND ----------

from operator import itemgetter
from langchain.schema.runnable import RunnableLambda
from langchain_core.output_parsers import StrOutputParser

#Very basic chain that allows us to pass the input (messages) into the Agent and collect the (output) as a string
agent_str = ({ "messages": itemgetter("messages")} | agent_executor | itemgetter("output") | StrOutputParser())

# COMMAND ----------

#Lets ask our Compound AI Agent to generate an Instagram post. This requires it to:
#     1. Look up what stores are in Chicago
#     2. Use sales data to look up the best selling cookie at that store
# COMING SOON:
#     3. Retrieve reviews from a vector search endpoint
#     4. Generate images from the shutterstock model

answer=agent_str.invoke({"messages": "What is the best selling cookie for our Seattle stores?"})
