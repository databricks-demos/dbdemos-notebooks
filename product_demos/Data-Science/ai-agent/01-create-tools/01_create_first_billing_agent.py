# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC # Building your first Agent Systems with Databricks
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/llm-tools-functions/ai-agent-functions.png?raw=true" style="float: right" width="700px">
# MAGIC LLMs are powerful to answer general knowledge, but don't have any information on your own business.
# MAGIC
# MAGIC Databricks makes it easy to develop custom tools which can be called by your LLM, creating a complete agent with reasoning capability.
# MAGIC
# MAGIC
# MAGIC ### Build Simple UC Tools
# MAGIC In this notebook, we'll register the following to Unity Catalogs:
# MAGIC - **SQL Functions**: Create queries that access customer information, order and subscriptions.
# MAGIC - **Simple Python Function**: Create and register a Python function doing math operations to overcome some common limitations of language models.
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection or disable tracker during installation. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=data-science&org_id=1444828305810485&notebook=01_create_first_billing_agent&demo_name=ai-agent&event=VIEW">

# COMMAND ----------

# MAGIC %md
# MAGIC Note: run this demo with Serverless compute - it was also tested with DBR 17.1

# COMMAND ----------

# DBTITLE 1,Library Installs
# MAGIC %pip install -U -qqqq mlflow>=3.1.4 databricks-langchain pydantic databricks-agents unitycatalog-langchain[databricks] databricks-feature-engineering==0.12.1 protobuf<5  cryptography<43 
# MAGIC # Restart to load the packages into the Python environment
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../_resources/01-setup

# COMMAND ----------

# MAGIC %md
# MAGIC Generate synthetic data we're going to use with our agents.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 1. Get the customer details based on their email
# MAGIC
# MAGIC Let's add a function to retrieve a customer detail based on their email.
# MAGIC
# MAGIC *Note: realtime postgres tables could be used to provide low latencies for point-queries like these. However, to keep this demo simple, we'll use a SQL endpoint. Using a SQL endpoint is also a very good way to provide analytics capabilities to your agent!*
# MAGIC

# COMMAND ----------

# DBTITLE 1,Create a function registered to Unity Catalog
# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION get_customer_by_email(email_input STRING COMMENT 'customer email used to retrieve customer information')
# MAGIC RETURNS TABLE (
# MAGIC     customer_id BIGINT,
# MAGIC     first_name STRING,
# MAGIC     last_name STRING,
# MAGIC     email STRING,
# MAGIC     phone STRING,
# MAGIC     address STRING,
# MAGIC     city STRING,
# MAGIC     state STRING,
# MAGIC     zip_code STRING,
# MAGIC     customer_segment STRING,
# MAGIC     registration_date DATE,
# MAGIC     customer_status STRING,
# MAGIC     loyalty_tier STRING,
# MAGIC     tenure_years DOUBLE,
# MAGIC     churn_risk_score BIGINT,
# MAGIC     customer_value_score BIGINT
# MAGIC )
# MAGIC COMMENT 'Returns the customer record matching the provided email address. Includes its ID, firstname, lastname and more.'
# MAGIC RETURN (
# MAGIC     SELECT * FROM customers
# MAGIC     WHERE email = email_input
# MAGIC     LIMIT 1
# MAGIC );
# MAGIC

# COMMAND ----------

# MAGIC %sql SELECT * FROM get_customer_by_email('john21@example.net');

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 2. Retrieve all billing informations
# MAGIC Let's add a function to get all the customer orders and subscriptions. This function will take a customer ID as input and return som aggregation to filter all the past billing informations and current subscriptions.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION get_customer_billing_and_subscriptions(customer_id_input BIGINT COMMENT 'customer ID used to retrive orders, billing and subscriptiosn')
# MAGIC RETURNS TABLE (
# MAGIC     customer_id BIGINT,
# MAGIC     subscription_id BIGINT,
# MAGIC     service_type STRING,
# MAGIC     plan_name STRING,
# MAGIC     plan_tier STRING,
# MAGIC     monthly_charge BIGINT,
# MAGIC     start_date DATE,
# MAGIC     contract_length_months BIGINT,
# MAGIC     status STRING,
# MAGIC     autopay_enabled BOOLEAN,
# MAGIC     total_billed DOUBLE,
# MAGIC     total_paid DOUBLE,
# MAGIC     total_late_payments BIGINT,
# MAGIC     total_late_fees DOUBLE,
# MAGIC     latest_payment_status STRING
# MAGIC )
# MAGIC COMMENT 'Returns subscription and billing details for a customer.'
# MAGIC RETURN (
# MAGIC     SELECT
# MAGIC         s.customer_id, s.subscription_id, s.service_type, s.plan_name, s.plan_tier,
# MAGIC         s.monthly_charge, s.start_date, s.contract_length_months, s.status, s.autopay_enabled,
# MAGIC         COALESCE(b.total_billed, 0), COALESCE(b.total_paid, 0),
# MAGIC         COALESCE(b.total_late_payments, 0), COALESCE(b.total_late_fees, 0),
# MAGIC         COALESCE(b.latest_payment_status, 'N/A')
# MAGIC     FROM subscriptions s
# MAGIC     LEFT JOIN (
# MAGIC         SELECT
# MAGIC             subscription_id, customer_id,
# MAGIC             SUM(total_amount) AS total_billed,
# MAGIC             SUM(payment_amount) AS total_paid,
# MAGIC             COUNT_IF(payment_date > due_date OR payment_status = 'Late') AS total_late_payments,
# MAGIC             SUM(CASE WHEN payment_date > due_date OR payment_status = 'Late' THEN total_amount - payment_amount ELSE 0 END) AS total_late_fees,
# MAGIC             MAX(payment_status) AS latest_payment_status
# MAGIC         FROM billing
# MAGIC         WHERE customer_id = customer_id_input
# MAGIC         GROUP BY subscription_id, customer_id
# MAGIC     ) b ON s.subscription_id = b.subscription_id
# MAGIC     WHERE s.customer_id = customer_id_input
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM get_customer_billing_and_subscriptions(
# MAGIC   (SELECT customer_id FROM get_customer_by_email('john21@example.net'))
# MAGIC );
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 3. Give the LLM a Python Function to compute Math
# MAGIC LLMs typically struggle to run any advance math. Let's add a tool to let the LLM compute any math expression, using python directly.
# MAGIC
# MAGIC Databricks makes it easy, running safe, sandboxed python functions:

# COMMAND ----------

# DBTITLE 1,Very simple Python function
# -----------------------
# TOOL 2: evaluate math expression
# -----------------------
def calculate_math_expression(expression: str) -> float:
    """
    Evaluates a basic math expression safely.

    Args:
        expression (str): A math expression (e.g., "sqrt(2 + 3 * (4 - 1)), using python math functions.").

    Returns:
        float: The result of the evaluated expression.
    """
    import math
    allowed_names = {k: v for k, v in math.__dict__.items() if not k.startswith("__")}
    allowed_names.update({"abs": abs, "round": round})

    try:
        result = eval(expression, {"__builtins__": None}, allowed_names)
        return float(result)
    except Exception as e:
        raise ValueError(f"Invalid expression: {expression}. Error: {str(e)}")

print(calculate_math_expression("6 + 3 * (13 - 1)"))

# COMMAND ----------

# DBTITLE 1,Register python function to Unity Catalog
from unitycatalog.ai.core.databricks import DatabricksFunctionClient

client = DatabricksFunctionClient()

# this will deploy the tool to UC, automatically setting the metadata in UC based on the tool's docstring & typing hints
python_tool_uc_info = client.create_python_function(func=calculate_math_expression, catalog=catalog, schema=dbName, replace=True)

# the tool will deploy to a function in UC called `{catalog}.{schema}.{func}` where {func} is the name of the function
# Print the deployed Unity Catalog function name
print(f"Deployed Unity Catalog function name: {python_tool_uc_info.full_name}")
# Create HTML link to created functions
displayHTML(f'<a href="/explore/data/functions/{catalog}/{dbName}/calculate_math_expression" target="_blank">Go to Unity Catalog to see Registered Functions</a>')

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Note: There is also a function registered in System.ai.python_exec that will let your LLM run generated code in a sandboxed environment:
# MAGIC
# MAGIC ```
# MAGIC
# MAGIC %sql
# MAGIC SELECT system.ai.python_exec("""
# MAGIC from datetime import datetime
# MAGIC print(datetime.now().strftime('%Y-%m-%d'))
# MAGIC """) as current_date
# MAGIC
# MAGIC ```
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## 4: Let's go over to the AI Playground to see how we can use these functions and assemble our first Agent!
# MAGIC
# MAGIC Open the [Playground](/ml/playground) and select the tools we created to test your agent!
# MAGIC
# MAGIC <div style="float: right; width: 70%;">
# MAGIC   <img 
# MAGIC     src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/refs/heads/main/images/\
# MAGIC cross_demo_assets/AI_Agent_GIFs/AI_agent_function_selection.gif" 
# MAGIC     alt="Function Selection" 
# MAGIC     width="100%"
# MAGIC   >
# MAGIC </div>
# MAGIC
# MAGIC ### Location Guide
# MAGIC
# MAGIC Your functions are organized in Unity Catalog using this structure:
# MAGIC
# MAGIC #### Example Path:
# MAGIC `my_catalog.my_schema.my_awesome_function`
# MAGIC
# MAGIC 💡 Note: Replace the example names with your actual catalog and schema names.

# COMMAND ----------

# MAGIC %md
# MAGIC ## What's next: Evaluation
# MAGIC
# MAGIC Our agent is now ready and leveraging our tools to properly answer our questions.
# MAGIC
# MAGIC But how can we make sure it's working properly, and more importantly will still work well for future questions and modifications?
# MAGIC
# MAGIC To do so, we need to build an Evaluation dataset and leverage MLFlow to automatically analyze our agent!
# MAGIC Open the [02-agent-eval/02.1_agent_evaluation]($../02-agent-eval/02.1_agent_evaluation) notebook to see how to deploy your agent using Langchain and run your first evaluations!
