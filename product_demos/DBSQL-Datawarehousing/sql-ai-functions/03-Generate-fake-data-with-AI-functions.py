# Databricks notebook source
# MAGIC %md
# MAGIC # 2/ Introduction to SQL AI Function: generating fake data with Open AI apis
# MAGIC
# MAGIC For this demo, we'll start by generating fake data using `AI_GENERATE_TEXT()`. 
# MAGIC
# MAGIC The sample data will mimics customer reviews for grocery products submitted to an e-commerce website.
# MAGIC
# MAGIC ## Working with `AI_GENERATE_TEXT` function
# MAGIC
# MAGIC Our function signature is the following:
# MAGIC
# MAGIC ```
# MAGIC SELECT AI_GENERATE_TEXT(<Prompt to send to Open AI model>,
# MAGIC                         "azure_openai/gpt-35-turbo",
# MAGIC                         "apiKey", <SECRET>,
# MAGIC                         "temperature", <TEMPERATURE>,
# MAGIC                         "deploymentName", <Azure Deployment Name>,
# MAGIC                         "resourceName", <Azure Resource Name>)
# MAGIC ```
# MAGIC
# MAGIC `AI_GENERATE_TEXT` will send the prompt to the remote model configured and retrive the result as SQL.
# MAGIC
# MAGIC Let's see how to use it.
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Ffeatures%2Fsql_ai_functions%2Ffake_data&dt=DBSQL">

# COMMAND ----------

# MAGIC %run ./_resources/00-init $catalog=dbdemos $db=openai_demo

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Our first SQL AI function
# MAGIC
# MAGIC Let's try a simple SQL AI function. We'll ask Open AI to generate a text for a retail product review.
# MAGIC
# MAGIC *Note that for now SQL AI functions will only work on a **Databricks SQL Pro or Serverless warehouse**, and **not** in a Notebook using interactive cluster.*
# MAGIC
# MAGIC To make this demo easy to navigate, we'll use Databricks [SQL Statement API](https://docs.databricks.com/api-explorer/workspace/statementexecution/executestatement) to send our SQL queries to a SQL PRO endpoint that we created for you with this demo.
# MAGIC
# MAGIC *(Alternatively, you can copy/paste the SQL code a new Query using [Databricks SQL Editor](/sql/editor/) to see it in action)*

# COMMAND ----------

#See companion notebook
sql_api = SQLStatementAPI(warehouse_name = "dbdemos-shared-endpoint", catalog = catalog, schema = dbName)

df = sql_api.execute_sql("""
SELECT AI_GENERATE_TEXT("Generate a short product review for a red dress. The customer is very happy with the article.",
                        "azure_openai/gpt-35-turbo",
                        "apiKey", SECRET("dbdemos", "azure-openai"),
                        "temperature", CAST(0.0 AS DOUBLE),
                        "deploymentName", "dbdemo-gpt35",
                        "resourceName", "dbdemos-open-ai",
                        "apiVersion", "2023-03-15-preview") as product_review""")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Adding a wrapper function to simplify the call
# MAGIC
# MAGIC Having to specify all our parameters can be hard to use, especially for Data Analyst who should focus on crafting proper prompt and not secret management. 
# MAGIC
# MAGIC To simplify our demo next steps, we'll create a wrapper SQL function `ASK_OPEN_AI` with a string as input parameter (the question to ask) and wrap all the model configuration.
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/product/sql-ai-functions/sql-ai-function-review-wrapper.png" width="1200px">

# COMMAND ----------

# DBTITLE 1,SQL admin setup wrapper function
sql_api.execute_sql("""CREATE OR REPLACE FUNCTION ASK_OPEN_AI(prompt STRING)
                                RETURNS STRING
                                RETURN 
                                  AI_GENERATE_TEXT(prompt,
                                                   "azure_openai/gpt-35-turbo",
                                                   "apiKey", SECRET("dbdemos", "azure-openai"),
                                                   "temperature", CAST(0.0 AS DOUBLE),
                                                   "deploymentName", "dbdemo-gpt35",
                                                   "resourceName", "dbdemos-open-ai",
                                                   "apiVersion", "2023-03-15-preview")""")

# COMMAND ----------

# DBTITLE 1,SQL Analyst simply use the wrapper
display(sql_api.execute_sql("""SELECT ASK_OPEN_AI("Generate a short product review for a red dress. The customer is very happy with the article.") as product_review"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generating a more complete sample dataset with prompt engineering
# MAGIC
# MAGIC Now that we know how to send a basic query to Open AI using SQL functions, let's ask the model a more detailed question.
# MAGIC
# MAGIC We'll directly ask to model to generate multiple rows and directly return as a json. 
# MAGIC
# MAGIC Here's a prompt example to generate JSON:
# MAGIC ```
# MAGIC Generate a sample dataset for me of 2 rows that contains the following columns: "date" (random dates in 2022), 
# MAGIC "review_id" (random id), "product_name" (use popular grocery product brands), and "review". Reviews should mimic useful product reviews 
# MAGIC left on an e-commerce marketplace website. 
# MAGIC
# MAGIC The reviews should vary in length (shortest: one sentence, longest: 2 paragraphs), sentiment, and complexity. A very complex review 
# MAGIC would talk about multiple topics (entities) about the product with varying sentiment per topic. Provide a mix of positive, negative, 
# MAGIC and neutral reviews
# MAGIC
# MAGIC Return JSON ONLY. No other text outside the JSON. JSON format:
# MAGIC [{"review_date":<date>, "review_id":<review_id>, "product_name":<product_name>, "review":<review>}]
# MAGIC ```

# COMMAND ----------

fake_reviews = sql_api.execute_sql("""
SELECT ASK_OPEN_AI(
      'Generate a sample dataset of 2 rows that contains the following columns: "date" (random dates in 2022), 
      "review_id" (random id), "customer_id" (random long from 1 to 100)  and "review". Reviews should mimic useful product reviews 
      left on an e-commerce marketplace website. 
      
      The reviews should be about a popular grocery brands product

      The reviews should vary in length (shortest: one sentence, longest: 2 paragraphs), sentiment, and complexity. A very complex review 
      would talk about multiple topics (entities) about the product with varying sentiment per topic. Provide a mix of positive, negative, 
      and neutral reviews.

      Give me JSON only. No text outside JSON. No explanations or notes
      [{"review_date":<date>, "review_id":<long>, "customer_id":<long>, "review":<string>}]') as reviews""")
display(fake_reviews)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Converting the results as json 
# MAGIC
# MAGIC Our results are looking good. All we now have to do is transform the results drom text as a JSON and explode the results over N rows.
# MAGIC
# MAGIC Let's create a new function to do that:

# COMMAND ----------

fake_reviews = sql_api.execute_sql("""
CREATE OR REPLACE FUNCTION GENERATE_FAKE_REVIEWS(num_reviews INT DEFAULT 5)
RETURNS array<struct<review_date:date, review_id:long, customer_id:long, review:string>>
RETURN 
SELECT FROM_JSON(
    ASK_OPEN_AI(
      CONCAT('Generate a sample dataset of ', num_reviews, ' rows that contains the following columns: "date" (random dates in 2022), 
      "review_id" (random long), "customer_id" (random long from 1 to 100) and "review". 
      Reviews should mimic useful product reviews from popular grocery brands product left on an e-commerce marketplace website. The review must include the product name.

      The reviews should vary in length (shortest: 5 sentence, longest: 10 sentences).
      Provide a mix of positive, negative, and neutral reviews but mostly negative.

      Give me JSON only. No text outside JSON. No explanations or notes
      [{"review_date":<date>, "review_id":<long>, "customer_id":<long>, "review":<string>}]')), 
      "array<struct<review_date:date, review_id:long, customer_id:long, review:string>>")""")

# COMMAND ----------

# DBTITLE 1,Explode the json result as a table
display(sql_api.execute_sql("""SELECT review.* FROM (
                                SELECT explode(reviews) as review FROM (
                                  SELECT GENERATE_FAKE_REVIEWS(10) as reviews))"""))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Saving our dataset as a table to be used directly in our demo.
# MAGIC
# MAGIC *Note that if you want to create more rows, you can first create a table and add multiple rows, with extra information that you can then concatenate to your prompt like categories, expected customer satisfaction etc. Once your table is created you can then call a new custom GENERATE function taking more parameters and crafting a more advanced prompt*

# COMMAND ----------

# DBTITLE 1,Save the crafted review as a new table
sql_api.execute_sql("""
CREATE OR REPLACE TABLE dbdemos.openai_demo.fake_reviews
COMMENT "Raw Review Data"
AS
SELECT review.* FROM (
  SELECT explode(reviews) as review FROM (
    SELECT GENERATE_FAKE_REVIEWS(10) as reviews))""")

# COMMAND ----------

# DBTITLE 1,In addition, let's generate some users using the same idea:
fake_reviews = sql_api.execute_sql("""
CREATE OR REPLACE FUNCTION GENERATE_FAKE_CUSTOMERS(num_reviews INT DEFAULT 10)
RETURNS array<struct<customer_id:long, firstname:string, lastname:string, order_count:int>>
RETURN 
SELECT FROM_JSON(
    ASK_OPEN_AI(
      CONCAT('Generate a sample dataset of ', num_reviews, ' customers containing the following columns: 
      "customer_id" (long from 1 to ', num_reviews, '), "firstname", "lastname" and order_count (random positive number, smaller than 200)

      Give me JSON only. No text outside JSON. No explanations or notes
      [{"customer_id":<long>, "firstname":<string>, "lastname":<string>, "order_count":<int>}]')), 
      "array<struct<customer_id:long, firstname:string, lastname:string, order_count:int>>")""")

sql_api.execute_sql("""
CREATE OR REPLACE TABLE dbdemos.openai_demo.fake_customers
COMMENT "Raw customers"
AS
SELECT customer.* FROM (
  SELECT explode(customers) as customer FROM (
    SELECT GENERATE_FAKE_CUSTOMERS(10) as customers))""")

display(sql_api.execute_sql("""SELECT * FROM dbdemos.openai_demo.fake_customers"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next steps
# MAGIC We're now ready to implement our pipeline to extract information from our reviews! Open [04-automated-product-review-and-answer]($./04-automated-product-review-and-answer) to continue.
# MAGIC
# MAGIC
# MAGIC Go back to [the introduction]($./01-SQL-AI-Functions-Introduction)
