# Databricks notebook source
# MAGIC %md
# MAGIC # Hands-On Lab: Building Agent Systems with Databricks
# MAGIC
# MAGIC ## Part 1 - Architect Your First Agent
# MAGIC This first agent will follow the workflow of a customer service representative to illustrate the various agent capabilites. 
# MAGIC We'll focus around processing product returns as this gives us a tangible set of steps to follow.
# MAGIC
# MAGIC ### 1.1 Build Simple Tools
# MAGIC - **SQL Functions**: Create queries that access data critical to steps in the customer service workflow for processing a return.
# MAGIC - **Simple Python Function**: Create and register a Python function to overcome some common limitations of language models.
# MAGIC
# MAGIC ### 1.2 Integrate with an LLM [AI Playground]
# MAGIC - Combine the tools you created with a Language Model (LLM) in the AI Playground.
# MAGIC
# MAGIC ### 1.3 Test the Agent [AI Playground]
# MAGIC - Ask the agent a question and observe the response.
# MAGIC - Dive deeper into the agent’s performance by exploring MLflow traces.
# MAGIC

# COMMAND ----------

# DBTITLE 1,Library Installs
# MAGIC %pip install databricks-agents mlflow>=3.1.0 databricks-sdk==0.55.0 unitycatalog-ai[databricks]
# MAGIC # Restart to load the packages into the Python environment
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../_resources/01-setup

# COMMAND ----------

# MAGIC %md
# MAGIC # Customer Service
# MAGIC
# MAGIC Below is a structured outline of the **key steps** a customer service agent would typically follow when **processing a return**. This workflow ensures consistency and clarity across your support team.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 1. Get the customer details based on its email
# MAGIC - **Action**: Identify and retrieve the customer
# MAGIC - **Why**: get customer information based on its ID
# MAGIC
# MAGIC ---

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
# MAGIC ---
# MAGIC
# MAGIC ## 2. Retrieve all billing informations
# MAGIC - **Action**: Access the internal knowledge base or policy documents related to returns, refunds, and exchanges.  
# MAGIC - **Why**: Verifying you’re in compliance with company guidelines prevents potential errors and conflicts.
# MAGIC
# MAGIC ---

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
# MAGIC ---
# MAGIC
# MAGIC ## 3. Give the LLM a Python Function to compute Math
# MAGIC - **Action**: Provide a **Python function** that can supply the Large Language Model (LLM) with the current date.  
# MAGIC - **Why**: Automating date retrieval helps in scheduling pickups, refund timelines, and communication deadlines.
# MAGIC
# MAGIC *Note: This is of course a very heavy way to get the date as you can simply add it to your prompt, but this example shows you how to implement your own python functions in a sandboxed, safe runtime for your agents:*

# COMMAND ----------

# DBTITLE 1,Very simple Python function
# -----------------------
# TOOL 2: evaluate math expression
# -----------------------
def calculate_math_expression(expression: str) -> float:
    """
    Evaluates a basic math expression safely.

    Args:
        expression (str): A math expression (e.g., "2 + 3 * (4 - 1)").

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

print(calculate_math_expression("2 + 3 * (4 - 1)"))

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

# MAGIC %md
# MAGIC ## Now lets go over to the AI Playground to see how we can use these functions and assemble our first Agent!
# MAGIC
# MAGIC ### The AI Playground can be found on the left navigation bar under 'Machine Learning' or you can use the link created below
# MAGIC
# MAGIC Open the [Playground](/ml/playground) and select the tools we created to test your agent!
