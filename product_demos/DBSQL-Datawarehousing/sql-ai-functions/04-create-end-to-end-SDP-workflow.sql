-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 4/ End to End SDP Production Pipeline for product review and classification with SQL AI QUERY
-- MAGIC
-- MAGIC
-- MAGIC In this demo, we will create a production ready pipeline for the product review and classification using Spark Declarative Pipelines framework
-- MAGIC
-- MAGIC - `reviews_annotated_silver` table: Join review and customer data, annotate reviews
-- MAGIC - `review_answer_gold` table: generate response per customer review
-- MAGIC
-- MAGIC Please note, we need to use the full Unity Catalog path of the AI SQL Functions
-- MAGIC
-- MAGIC
-- MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/product/sql-ai-functions/sql-ai-query-function-flow.png" width="1000">
-- MAGIC
-- MAGIC
-- MAGIC <!-- Collect usage data (view). Remove it to disable collection or disable tracker during installation. View README for more details.  -->
-- MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=dbsql&notebook=03-automated-product-review-and-answer&demo_name=sql-ai-functions&event=VIEW">

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Create `reviews_annotated_silver` streaming table

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE reviews_annotated_silver(
  CONSTRAINT valid_customer_id EXPECT (customer_id IS NOT NULL)
) 
COMMENT "Annotate review with Llama 3.3 70B"
AS SELECT
  * EXCEPT (review_annotated), review_annotated.* FROM (
      SELECT *, main.dbdemos_ai_query.ANNOTATE_REVIEW(review) AS review_annotated
        FROM stream(fake_reviews))
    INNER JOIN stream(fake_customers) using (customer_id)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Create `review_answer_gold` materialized view

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW reviews_answer_gold 
COMMENT "Create review answer and response"
AS SELECT 
  *,
  main.dbdemos_ai_query.generate_response(firstname, lastname, order_count, product_name, followup_reason) AS response_draft
FROM live.reviews_annotated_silver where followup='Y'
