-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 4/ End to End DLT Production Pipeline for product review and classification with SQL AI QUERY
-- MAGIC
-- MAGIC
-- MAGIC In this demo, we will create a production ready pipeline for the product review and classification using DLT framework
-- MAGIC
-- MAGIC - Join review and customer data
-- MAGIC - Generate response per customer review
-- MAGIC
-- MAGIC
-- MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/product/sql-ai-functions/sql-ai-query-function-flow.png" width="1000">
-- MAGIC
-- MAGIC
-- MAGIC <!-- Collect usage data (view). Remove it to disable collection or disable tracker during installation. View README for more details.  -->
-- MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=dbsql&notebook=03-automated-product-review-and-answer&demo_name=sql-ai-functions&event=VIEW">

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Create `AI_QUERY` functions

-- COMMAND ----------

-- Create a wrapper SQL function ASK_LLM_MODEL with string input parameters prompt, response_format, and wrap all the model configuration.
CREATE OR REPLACE FUNCTION ASK_LLM_MODEL(prompt STRING, response_format STRING) 
  RETURNS STRING
  RETURN 
    AI_QUERY("databricks-meta-llama-3-3-70b-instruct", 
              prompt,
              response_format)
;

-- Ask the model to return the result in a string that reflects JSON representation
CREATE OR REPLACE FUNCTION ANNOTATE_REVIEW(review STRING)
    RETURNS STRUCT<product_name: STRING, entity_sentiment: STRING, followup: STRING, followup_reason: STRING>
    RETURN FROM_JSON(
      ASK_LLM_MODEL(CONCAT(
        'A customer left a review. We follow up with anyone who appears unhappy.
         extract the following information:
          - classify sentiment as ["POSITIVE","NEUTRAL","NEGATIVE"]
          - returns whether customer requires a follow-up: Y or N
          - if followup is required, explain what is the main reason

        Return JSON ONLY. No other text outside the JSON. JSON format:
        {
            "product_name": <entity name>,
            "entity_sentiment": <entity sentiment>,
            "followup": <Y or N for follow up>,
            "followup_reason": <reason for followup>
        }
        
        Review:', review), "{'type': 'json_object'}"),
      "STRUCT<product_name: STRING, entity_sentiment: STRING, followup: STRING, followup_reason: STRING>"
    )
;

-- Ask the model to generate responses based on customer reviews
CREATE OR REPLACE FUNCTION GENERATE_RESPONSE(firstname STRING, lastname STRING, order_count INT, product_name STRING, reason STRING)
  RETURNS STRING
  RETURN ASK_LLM_MODEL(
    CONCAT("Our customer named ", firstname, " ", lastname, " who ordered ", order_count, " ", product_name, " was unhappy about ", product_name, "specifically due to ", reason, ". Provide an empathetic message I can send to my customer 
    including the offer to have a call with the relevant product manager to leave feedback. I want to win back their 
    favour and I do not want the customer to churn"), 
    "{'type': 'text'}"
  )
;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Create `review_annotated_prod` streaming table

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE reviews_annotated_silver(
  CONSTRAINT valid_customer_id EXPECT (customer_id IS NOT NULL)
) 
COMMENT "Annotate review with Llama 3.3 70B"
AS SELECT
  * EXCEPT (review_annotated), review_annotated.* FROM (
      SELECT *, ANNOTATE_REVIEW(review) AS review_annotated
        FROM stream(fake_reviews))
    INNER JOIN stream(fake_customers) using (customer_id)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Create `review_answer` table

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW reviews_answer_gold 
COMMENT "Create review answer and response"
AS SELECT 
  *,
  generate_response(firstname, lastname, order_count, product_name, followup_reason) AS response_draft
FROM live.reviews_annotated_silver where followup='Y'
