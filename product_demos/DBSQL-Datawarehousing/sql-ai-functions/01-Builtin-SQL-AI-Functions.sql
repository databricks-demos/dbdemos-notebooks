-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Process and analyse text with Built In Databricks SQL AI functions
-- MAGIC
-- MAGIC Databricks SQL provides [built-in GenAI capabilities](https://docs.databricks.com/en/large-language-models/ai-functions.html), letting you perform adhoc operation, leveraging state of the art LLM, optimized for these tasks.
-- MAGIC
-- MAGIC These functions are the following:
-- MAGIC
-- MAGIC - `ai_analyze_sentiment`
-- MAGIC - `ai_classify`
-- MAGIC - `ai_extract`
-- MAGIC - `ai_fix_grammar`
-- MAGIC - `ai_gen`
-- MAGIC - `ai_mask`
-- MAGIC - `ai_similarity`
-- MAGIC - `ai_summarize`
-- MAGIC - `ai_translate`
-- MAGIC
-- MAGIC Using these functions is pretty straightforward. Under the hood, they call specialized LLMs with a custom prompt, providing fast answers.
-- MAGIC
-- MAGIC You can use them on any column in any SQL text.
-- MAGIC
-- MAGIC Let's give it a try.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Using a SQL Warehouse to run this demo
-- MAGIC
-- MAGIC This demo runs using a SQL Warehouse! 
-- MAGIC
-- MAGIC Make sure you select one using the dropdown on the top right of your notebook (don't select a classic compute/cluster)
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- DBTITLE 1,Generate dataset
-- verify that we're running on a SQL Warehouse
SELECT assert_true(current_version().dbsql_version is not null, 'YOU MUST USE A SQL WAREHOUSE, not a cluster');

SELECT ai_gen('Generate a concise, cheerful email title for a summer bike sale with 20% discount');

-- COMMAND ----------

-- DBTITLE 1,Fix grammar
SELECT ai_fix_grammar('This sentence have some mistake');

-- COMMAND ----------

-- DBTITLE 1,Automatically classify text into categories
SELECT ai_classify("My password is leaked.", ARRAY("urgent", "not urgent"));

-- COMMAND ----------

-- DBTITLE 1,Translate into other language
SELECT ai_translate("This function is so amazing!", "fr")

-- COMMAND ----------

-- DBTITLE 1,Compute similarity between sentences
SELECT ai_similarity('Databricks', 'Apache Spark'),  ai_similarity('Apache Spark', 'The Apache Spark Engine');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Going further: creating your own AI function with your Model Serving Endpoint or other Foundation Models
-- MAGIC
-- MAGIC We saw how Databricks simplifies your SQL operation with built in AI functions.
-- MAGIC
-- MAGIC However, you might need more  advanced capabilities:
-- MAGIC
-- MAGIC - Use your own custom LLMs behind a Model Serving Endpoint
-- MAGIC - Create more advanced AI Function
-- MAGIC - Add more control on the prompt  
-- MAGIC
-- MAGIC That's what the `AI_QUERY` function can offer.
-- MAGIC
-- MAGIC Open the next notebook: [02-Generate-fake-data-with-AI-functions]($./02-Generate-fake-data-with-AI-functions) to see how you can leverage `AI_FUNCTION` to automate product review analysis and create customized answer.
-- MAGIC
-- MAGIC Go back to [the introduction]($./00-SQL-AI-Functions-Introduction)
