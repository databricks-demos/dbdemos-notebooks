-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 2/ Introduction to SQL AI Function: generating fake data with custom Model Serving Endpoint
-- MAGIC
-- MAGIC For this demo, we'll start by generating fake data using `AI_QUERY()`. 
-- MAGIC
-- MAGIC The sample data will mimics customer reviews for grocery products submitted to an e-commerce website.
-- MAGIC
-- MAGIC ## Working with `AI_QUERY` function
-- MAGIC
-- MAGIC Our function signature is the following:
-- MAGIC
-- MAGIC ```
-- MAGIC SELECT ai_query(endpointName, request) for external models and foundation models. 
-- MAGIC SELECT ai_query(endpointName, request, returnType) for custom model serving endpoint. 
-- MAGIC ```
-- MAGIC
-- MAGIC `AI_QUERY` will send the prompt to the remote model configured and retrive the result as SQL.
-- MAGIC
-- MAGIC *Note: this will reproduce the behavior or the built-in `gen_ai` function, but leveraging one of the Model Serving Endpoint of your choice.*<br/>
-- MAGIC *If you're looking at quickly generating data, we recommend you to just go with the built-in.*
-- MAGIC
-- MAGIC *This notebook will use the foundation MPT-30B model for inference*

-- COMMAND ----------

-- MAGIC %md
-- MAGIC To run this notebook, connect to <b> SQL endpoint </b>. The AI_QUERY function is available on Databricks SQL Pro and Serverless.

-- COMMAND ----------

-- as previously, make sure you run this notebook using a SQL Warehouse (not a cluster)
SELECT assert_true(current_version().dbsql_version is not null, 'YOU MUST USE A SQL WAREHOUSE');

USE CATALOG main;
CREATE SCHEMA IF NOT EXISTS dbdemos_ai_query;
USE SCHEMA dbdemos_ai_query;

-- COMMAND ----------

SELECT
  AI_QUERY(
    "databricks-mpt-30b-instruct",
    "Generate a short product review for a red dress. The customer is very happy with the article."
  ) as product_review

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Introduction to SQL Function: adding a wrapper function to simplify the call
-- MAGIC
-- MAGIC While it's easy to call this function, having to our model endpoint name as parameter can be harder to use, especially for Data Analyst who should focus on crafting proper prompt. 
-- MAGIC
-- MAGIC To simplify our demo next steps, we'll create a wrapper SQL function `ASK_LLM_MODEL` with a string as input parameter (the question to ask) and wrap all the model configuration.
-- MAGIC
-- MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/product/sql-ai-functions/sql-ai-query-function-review-wrapper.png" width="1200px">

-- COMMAND ----------

-- DBTITLE 1,SQL admin setup wrapper function
CREATE OR REPLACE FUNCTION ASK_LLM_MODEL(prompt STRING) 
  RETURNS STRING
  RETURN 
    AI_QUERY("databricks-mpt-30b-instruct", prompt);

-- ALTER FUNCTION ASK_LLM_MODEL OWNER TO `your_principal`; -- for the demo only, make sure other users can access your function

-- COMMAND ----------

-- DBTITLE 1,SQL Analyst simply use the wrapper
SELECT ASK_LLM_MODEL("Generate a short product review for a red dress. The customer is very happy with the article.")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Generating a more complete sample dataset with prompt engineering
-- MAGIC
-- MAGIC Now that we know how to send a basic query to Open AI using SQL functions, let's ask the model a more detailed question.
-- MAGIC
-- MAGIC We'll directly ask to model to generate multiple rows and directly return as a json. 
-- MAGIC
-- MAGIC Here's a prompt example to generate JSON:
-- MAGIC ```
-- MAGIC Generate a sample dataset for me of 2 rows that contains the following columns: "date" (random dates in 2022), 
-- MAGIC "review_id" (random id), "product_name" (use popular grocery product brands), and "review". Reviews should mimic useful product reviews 
-- MAGIC left on an e-commerce marketplace website. 
-- MAGIC
-- MAGIC The reviews should vary in length (shortest: one sentence, longest: 2 paragraphs), sentiment, and complexity. A very complex review 
-- MAGIC would talk about multiple topics (entities) about the product with varying sentiment per topic. Provide a mix of positive, negative, 
-- MAGIC and neutral reviews
-- MAGIC
-- MAGIC Return JSON ONLY. No other text outside the JSON. JSON format:
-- MAGIC [{"review_date":<date>, "review_id":<review_id>, "product_name":<product_name>, "review":<review>}]
-- MAGIC ```

-- COMMAND ----------

SELECT ASK_LLM_MODEL(
      'Generate a sample dataset of 2 rows that contains the following columns: "date" (random dates in 2022), 
      "review_id" (random id), "customer_id" (random long from 1 to 100)  and "review". Reviews should mimic useful product reviews 
      left on an e-commerce marketplace website. 
      
      The reviews should be about a popular grocery brands product

      The reviews should vary in length (shortest: one sentence, longest: 2 paragraphs), sentiment, and complexity. A very complex review 
      would talk about multiple topics (entities) about the product with varying sentiment per topic. Provide a mix of positive, negative, 
      and neutral reviews.

      Give me JSON only. No text outside JSON. No explanations or notes
      [{"review_date":<date>, "review_id":<long>, "customer_id":<long>, "review":<string>}]') as fake_reviews;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Converting the results as json 
-- MAGIC
-- MAGIC Our results are looking good. All we now have to do is transform the results from text as a JSON and explode the results over N rows.
-- MAGIC
-- MAGIC Let's create a new function to do that:

-- COMMAND ----------

CREATE OR REPLACE FUNCTION GENERATE_FAKE_REVIEWS(num_reviews INT DEFAULT 5)
  RETURNS array<struct<review_date:date, review_id:long, customer_id:long, review:string>>
  RETURN 
  SELECT FROM_JSON(
      ASK_LLM_MODEL(
        CONCAT('Generate a sample dataset of ', num_reviews, ' rows that contains the following columns: "date" (random dates in 2022), 
        "review_id" (random long), "customer_id" (random long from 1 to 100) and "review". 
        Reviews should mimic useful product reviews from popular grocery brands product left on an e-commerce marketplace website. The review must include the product name.

        The reviews should vary in length (shortest: 5 sentence, longest: 10 sentences).
        Provide a mix of positive, negative, and neutral reviews but mostly negative.

        Give me JSON only. No text outside JSON. No explanations or notes
        [{"review_date":<date>, "review_id":<long>, "customer_id":<long>, "review":<string>}]')), 
        "array<struct<review_date:date, review_id:long, customer_id:long, review:string>>")

-- ALTER FUNCTION GENERATE_FAKE_REVIEWS OWNER TO `your_principal`; -- for the demo only, make sure other users can access your function

-- COMMAND ----------

-- DBTITLE 1,Explode the json result as a table
SELECT
  review.*
FROM
  (
    SELECT
      explode(reviews) as review
    FROM
      (
        SELECT
          GENERATE_FAKE_REVIEWS(10) as reviews
      )
  )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Saving our dataset as a table to be used directly in our demo.
-- MAGIC
-- MAGIC *Note that if you want to create more rows, you can first create a table and add multiple rows, with extra information that you can then concatenate to your prompt like categories, expected customer satisfaction etc. Once your table is created you can then call a new custom GENERATE function taking more parameters and crafting a more advanced prompt*

-- COMMAND ----------

-- DBTITLE 1,Save the crafted review as a new table
CREATE
OR REPLACE TABLE fake_reviews COMMENT "Raw Review Data" AS
SELECT
  review.*
FROM
  (
    SELECT
      explode(reviews) as review
    FROM
      (
        SELECT
          GENERATE_FAKE_REVIEWS(10) as reviews
      )
  )

-- COMMAND ----------

-- DBTITLE 1,In addition, let's generate some users using the same idea:
CREATE OR REPLACE FUNCTION GENERATE_FAKE_CUSTOMERS(num_reviews INT DEFAULT 10)
  RETURNS array<struct<customer_id:long, firstname:string, lastname:string, order_count:int>>
  RETURN 
  SELECT FROM_JSON(
      ASK_LLM_MODEL(
        CONCAT('Generate a sample dataset of ', num_reviews, ' customers containing the following columns: 
        "customer_id" (long from 1 to ', num_reviews, '), "firstname", "lastname" and order_count (random positive number, smaller than 200)

        Give me JSON only. No text outside JSON. No explanations or notes
        [{"customer_id":<long>, "firstname":<string>, "lastname":<string>, "order_count":<int>}]')), 
        "array<struct<customer_id:long, firstname:string, lastname:string, order_count:int>>")
        
-- ALTER FUNCTION GENERATE_FAKE_CUSTOMERS OWNER TO `your_principal`; -- for the demo only, make sure other users can access your function

-- COMMAND ----------

CREATE OR REPLACE TABLE fake_customers
  COMMENT "Raw customers"
  AS
  SELECT customer.* FROM (
    SELECT explode(customers) as customer FROM (
      SELECT GENERATE_FAKE_CUSTOMERS(10) as customers))

-- COMMAND ----------

SELECT * FROM fake_reviews

-- COMMAND ----------

SELECT * FROM fake_customers

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Next steps
-- MAGIC We're now ready to implement our pipeline to extract information from our reviews! Open [03-automated-product-review-and-answer]($./03-automated-product-review-and-answer) to continue.
-- MAGIC
-- MAGIC
-- MAGIC Go back to [the introduction]($./00-SQL-AI-Functions-Introduction)
