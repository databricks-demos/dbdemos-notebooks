# Databricks notebook source
# MAGIC %pip install -U databricks-sdk==0.23.0 langchain-community==0.2.16 langchain-openai==0.1.19 mlflow==2.17.0
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## Compound AI system for your Instagram web campaign marketing for our Cookie Franchise!
# MAGIC
# MAGIC <a href="https://youtu.be/UfbyzK488Hk?si=qzMgcSjcBhXOnDBz&t=3496" target="_blank"><img style="float:right; padding: 20px" width="500px" src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/llm-tools-functions/llm-tools-cookie-video.png?raw=true" ></a>
# MAGIC
# MAGIC In this notebook, we're going to see how to create a compound AI system to analyze your data and make recommendation on what to publish in your Instagram web campaign, based on your own customer reviews!
# MAGIC
# MAGIC we will:
# MAGIC - Create simple SQL functions to analyze your data and registering them in UC
# MAGIC - Creating more advanced python functions in UC
# MAGIC - Use Langchain to bind these functions as tools 
# MAGIC - Create an Agent that can execute these tools and ask it some tough questions
# MAGIC
# MAGIC
# MAGIC ### AI with general intelligence isn't enough!
# MAGIC
# MAGIC LLMs are very powerful, but they don't know your business. If we ask an LLM to write a post for our cookies, it'll generate a very general message.
# MAGIC
# MAGIC AI needs be customized to your own business with your own data to be relevant!
# MAGIC
# MAGIC Let's give it a try and ask a general AI to generate an instagram post for our cookie franchise:

# COMMAND ----------

# MAGIC %sql
# MAGIC -- A not inspiring, general-purpose message :/ 
# MAGIC select ai_gen('Help me write an instagram message for the customers of my Seattle store, we want to increase our sales for the top 1 cookie only.')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generating personalized, impactful AI with the Data Intelligence Platform
# MAGIC
# MAGIC Because Databricks makes it easy to bring all your data together, we can easily leverage your own data and use it to super-charge the AI.
# MAGIC
# MAGIC Instead of General Intelligence, you'll have a extra-smart AI, understanding your business.
# MAGIC
# MAGIC Let's see how Databricks let us generate ultra-personalize content and instagram messages to drive more cookie love!
# MAGIC
# MAGIC ### 1/ Prepare and review the dataset
# MAGIC
# MAGIC Dbdemos loaded the dataset for you in your catalog. In a real world, you'd have a few Delta Live Table pipeline together with Lakeflow connect to ingest your ERP/Sales Force/website data.
# MAGIC
# MAGIC Let's start by reviewing the dataset and understanding how it can help to make personalized recommendations.
# MAGIC *Note: the cookie data now exists in the marketplace, feel free to pull it from here.*
# MAGIC
# MAGIC #### Let's focus on the main tables
# MAGIC - **sales.franchises** - Contains information about all our business franchises (store name, location, country, size, ect).   
# MAGIC - **sales.transactions** - Contains sales data for all franchises.
# MAGIC - **media.customer_reviews** - Social media reviews for a bunch of different franchises.   

# COMMAND ----------

# DBTITLE 1,Load the dataset
# MAGIC %run ./_resources/00-init-cookie $reset_all=false

# COMMAND ----------

# DBTITLE 1,Our Business has many franchises across the world
# MAGIC %sql
# MAGIC SELECT * FROM cookies_franchises;

# COMMAND ----------

# DBTITLE 1,Each franchise has many transactions
# MAGIC %sql
# MAGIC SELECT * FROM cookies_transactions

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2/ Create our Unity Catalog Functions
# MAGIC
# MAGIC The cookie data now exists in your catalog.
# MAGIC
# MAGIC We'll now start creating custom functions that our Compound AI system will understand and leverage to gain insight on your business
# MAGIC
# MAGIC ### Allow your LLM to retrieve the franchises for a given city

# COMMAND ----------

# DBTITLE 1,Retrieve Franchise by city
# MAGIC %sql
# MAGIC --Now we create our first function. This takes in a city name and returns a table of any franchises that are in that city.
# MAGIC --Note that we've added a comment to the input parameter to help guide the agent later on.
# MAGIC CREATE OR REPLACE FUNCTION
# MAGIC cookies_franchise_by_city ( city_name STRING COMMENT 'City to be searched' )
# MAGIC returns table(franchiseID BIGINT, name STRING, size STRING)
# MAGIC LANGUAGE SQL
# MAGIC -- Make sure to add a comment so that your AI understands what it does
# MAGIC COMMENT 'This function takes in a city name and returns a table of any franchises that are in that city.'
# MAGIC return
# MAGIC (SELECT franchiseID, name, size from cookies_franchises where lower(city)=lower(city_name) order by size desc)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test the function we just created
# MAGIC SELECT * from cookies_franchise_by_city('Seattle')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sum all the sales for a given franchise, grouping the result per product
# MAGIC
# MAGIC The AI will be able to find the top products generating the most revenue

# COMMAND ----------

# MAGIC %sql
# MAGIC --This function takes an ID as input, and this time does an aggregate to return the sales for that franchise_id.
# MAGIC CREATE OR REPLACE FUNCTION
# MAGIC cookies_franchise_sales (franchise_id BIGINT COMMENT 'ID of the franchise to be searched')
# MAGIC returns table(total_sales BIGINT, total_quantity BIGINT, product STRING)
# MAGIC LANGUAGE SQL
# MAGIC COMMENT 'This function takes an ID as input, and this time does an aggregate to return the sales for that franchise_id'
# MAGIC return
# MAGIC   (SELECT SUM(totalPrice) AS total_sales, SUM(quantity) AS total_quantity, product 
# MAGIC     FROM cookies_transactions 
# MAGIC       WHERE franchiseID = franchise_id GROUP BY product)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test the function we just created
# MAGIC SELECT * from cookies_franchise_sales(3000005)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get the best feedbacks for a given franchise
# MAGIC
# MAGIC This will let the AI understand what local customers like about our cookies!

# COMMAND ----------

# MAGIC %sql
# MAGIC --This function takes an ID as input, and this time does an aggregate to return the sales for that franchise_id.
# MAGIC CREATE OR REPLACE FUNCTION
# MAGIC cookies_summarize_best_sellers_feedback (franchise_id BIGINT COMMENT 'ID of the franchise to be searched')
# MAGIC returns STRING
# MAGIC LANGUAGE SQL
# MAGIC COMMENT 'This function will fetch the best feedback from a product and summarize them'
# MAGIC return
# MAGIC   SELECT AI_GEN(SUBSTRING('Extract the top 3 reason people like the cookies based on this list of review:' || ARRAY_JOIN(COLLECT_LIST(review), ' - '), 1, 80000)) AS all_reviews
# MAGIC   FROM cookies_customer_reviews
# MAGIC     where franchiseID = franchise_id and review_stars >= 4

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test the function we just created
# MAGIC SELECT cookies_summarize_best_sellers_feedback(3000005)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## 3/ Testing our AI with Databricks Playground
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/llm-tools-functions/llm-tools-functions-playground.gif?raw=true" width="500px" style="float: right; margin-left: 10px; margin-bottom: 10px;">
# MAGIC
# MAGIC
# MAGIC Your functions are now available in Unity Catalog!
# MAGIC
# MAGIC To try out our functions with playground:
# MAGIC - Open the [Playground](/ml/playground) 
# MAGIC - Select a model supporting tools (like Llama3.1)
# MAGIC - Add the functions you want your model to leverage (`catalog.schema.function_name`)
# MAGIC - Ask a question (for example to write an instagram post for Seattle), and playground will do the magic for you!
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4/ DIY: Create your own Agent chaining the tools with Langchain
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
        .include(f"{catalog}.{db}.cookies_franchise_by_city", 
                 f"{catalog}.{db}.cookies_franchise_sales", 
                 f"{catalog}.{db}.cookies_summarize_best_sellers_feedback")
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

           Use the cookies_summarize_best_sellers_feedback function to understand what customers like the most.

    Make sure to call the function for each step and provide a coherent response to the user. Don't mention tools to your users. Don't skip to the next step without ensuring the function was called and a result was retrieved. Only answer what the user is asking for. If a user ask to generate instagram posts, make sure you know what customers like the most to make the post relevant."""
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

#Put the pieces together to create our Agent
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

answer=agent_str.invoke({"messages": "Help me write an instagram message for the customers of my Seattle store, we want to increase our sales for the top 1 cookie only."})

# COMMAND ----------

# MAGIC %md
# MAGIC ### That's it! our 

# COMMAND ----------

print(answer)
