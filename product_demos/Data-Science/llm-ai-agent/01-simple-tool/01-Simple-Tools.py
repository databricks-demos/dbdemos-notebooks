# Databricks notebook source
see https://github.com/databricks/tmm/blob/main/agents-workshop/01_create_tools/01_create_tools.py

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC # 1/ Create your first tool
# MAGIC
# MAGIC xxx, add image like this one with the different tools & app?
# MAGIC
# MAGIC See deck template: https://docs.google.com/presentation/d/18aIGHtwmrYFhVtF7gE6fi5g7xeA8jRgoCgNLwle9IfM/edit#slide=id.g2ee8efb26f5_4_0
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/llm-tools-functions/llm-tools-functions-flow.png?raw=true" width="900px">
# MAGIC
# MAGIC
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection or disable tracker during installation. View README for more details.  -->
# MAGIC -- Don't forget the tracker
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=data-science&org_id=1444828305810485&notebook=01-First-Step-RAG-On-Databricks&demo_name=chatbot-rag-llm&event=VIEW">

# COMMAND ----------

#Note: don't pin databricks langchain/agent/mlflow, pin the rest
%pip install -U --quiet databricks-sdk==0.40.0 databricks-langchain databricks-agents mlflow[databricks] databricks-vectorsearch==0.49 langchain==0.3.19 langchain_core==0.3.37 bs4==0.0.2 markdownify==0.14.1 pydantic==2.10.1
dbutils.library.restartPython()

# COMMAND ----------

#Note: Load the dataset directly as delta table
%run ../_resources/00-init $reset_all_data=false

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- Note: Run some select to review the different dataset available
# MAGIC SELECT * FROM databricks_documentation ...

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating our tools: Using Unity Catalog Functions
# MAGIC
# MAGIC Let's start by defining the functions our LLM will be able to execute. These functions can contain any logic, from simple SQL to advanced python.

# COMMAND ----------

# MAGIC %md 
# MAGIC ### First tool: Computing xxx
# MAGIC
# MAGIC xxx what are we doing
# MAGIC
# MAGIC We'll save this function within Unity Catalog. You can open the explorer to review the functions created in this notebook.
# MAGIC
# MAGIC *Note: This is a very simple first example for this demo. We'll implement a broader math tool later on.*

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION query_customer_details(size_in_inch  FLOAT COMMENT 'xxxx')
# MAGIC RETURNS FLOAT
# MAGIC LANGUAGE SQL
# MAGIC COMMENT 'convert size from inch to cm'
# MAGIC RETURN xxx
# MAGIC
# MAGIC -- let's test our function:
# MAGIC SELECT convert_inch_to_cm(10) as 10_inches_in_cm;

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Second tool: Computing xxx
# MAGIC
# MAGIC xxx what are we doing
# MAGIC
# MAGIC We'll save this function within Unity Catalog. You can open the explorer to review the functions created in this notebook.
# MAGIC
# MAGIC *Note: This is a very simple first example for this demo. We'll implement a broader math tool later on.*

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION query_customer_invoice_history(size_in_inch FLOAT COMMENT 'xxxx')
# MAGIC RETURNS FLOAT
# MAGIC LANGUAGE SQL
# MAGIC COMMENT 'convert size from inch to cm'
# MAGIC RETURN xxx
# MAGIC
# MAGIC -- let's test our function:
# MAGIC SELECT convert_inch_to_cm(10) as 10_inches_in_cm;

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Last tool: Generate coupon xxx
# MAGIC
# MAGIC xxx what are we doing
# MAGIC
# MAGIC We'll save this function within Unity Catalog. You can open the explorer to review the functions created in this notebook.
# MAGIC
# MAGIC *Note: This is a very simple first example for this demo. We'll implement a broader math tool later on.*

# COMMAND ----------

TODO mockup python function to show how to have a tool in pyton

%sql
CREATE OR REPLACE FUNCTION compute_math(expr STRING)
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'Run any mathematical function and returns the result as output. Supports python syntax like math.sqrt(13)'
AS
$$
  import ast
  import operator
  import math
  try:
    if expr.startswith('```') and expr.endswith('```'):
      expr = expr[3:-3].strip()      
    node = ast.parse(expr, mode='eval').body
    return eval_node(node)
  except Exception as ex:
    return str(ex)
$$;
-- let's test our function:
SELECT compute_math("(2+2)/3") as result;

# COMMAND ----------

# MAGIC %md
# MAGIC ### ... Continue, add the tool to UC, 

# COMMAND ----------

# MAGIC %md 
# MAGIC ### ... Try on Playground

# COMMAND ----------

# MAGIC %md 
# MAGIC ### ... Try on Playground

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



# COMMAND ----------

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

# MAGIC %md Next: deploy the agent
