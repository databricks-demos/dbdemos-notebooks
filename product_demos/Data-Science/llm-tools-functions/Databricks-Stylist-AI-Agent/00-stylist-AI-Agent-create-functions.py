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

# MAGIC %pip install --quiet -U databricks-sdk==0.23.0 langchain-community==0.2.10 langchain-openai==0.1.19 mlflow==2.14.3 faker
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../_resources/00-init-stylist $reset_all=false

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
# MAGIC RETURN SELECT ai_query('databricks-meta-llama-3-70b-instruct',
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
# MAGIC       'databricks-meta-llama-3-70b-instruct', 
# MAGIC       CONCAT(
# MAGIC         'returns a json list of 3 json object clothes: <"id": bigint, "name": string, "color": string, "category": string, "price": double, "description": string>. These clothes should match the following user description: ', description, '. Return only the answer as a javascript json object ready to be parsed, no comment or text or javascript or ``` at the beginning.' ) ), 'ARRAY<STRUCT<id: BIGINT, name: STRING, color: STRING, category: STRING, price: DOUBLE, description: STRING>>' )) AS clothes );
# MAGIC
# MAGIC -- let's test our function:
# MAGIC SELECT * FROM find_clothes_matching_description('a red dress');

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
# MAGIC COMMENT 'Run any python code and returns the output'
# MAGIC AS
# MAGIC $$
# MAGIC   import traceback
# MAGIC   try:
# MAGIC       import re
# MAGIC       python_code = re.sub(r"^\s*```(?:python)?|```\s*$", "", python_code).strip()
# MAGIC       result = eval(python_code, globals())
# MAGIC       return str(result)
# MAGIC   except Exception as ex:
# MAGIC       return traceback.format_exc()
# MAGIC $$;
# MAGIC -- let's test our function:
# MAGIC
# MAGIC SELECT execute_python_code("'Hello Word! '* 3") as result;

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
