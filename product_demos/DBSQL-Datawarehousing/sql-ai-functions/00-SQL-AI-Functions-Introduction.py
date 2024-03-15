# Databricks notebook source
# MAGIC %md
# MAGIC # 1/ Process and analyse text with Built In Databricks SQL AI functions
# MAGIC
# MAGIC Databricks SQL provides [built-in GenAI capabilities](https://docs.databricks.com/en/large-language-models/ai-functions.html), letting you perform adhoc operation, leveraging state of the art LLM, optimized for these tasks.
# MAGIC
# MAGIC Discover how to leverage functions such as  `ai_analyze_sentiment`,  `ai_classify` or `ai_gen` with Databricks SQL.
# MAGIC
# MAGIC Open the [01-Builtin-SQL-AI-Functions]($./01-Builtin-SQL-AI-Functions) to get started with Databricks SQL AI builtin functions.

# COMMAND ----------

# MAGIC %md
# MAGIC # 2/ Going Further: Actioning Customer Reviews at Scale with Databricks SQL AI Functions and custom Model Endpoint (LLM or other)
# MAGIC
# MAGIC Databricks builtin AI functions are powerful and let you quickly achieve many tasks with text.
# MAGIC
# MAGIC However, you might sometime require more fine-grained control and select wich Foundation Model you want to call (Mistral, Llama, MPT, OpenAI or one of your own fine-tuning model), passing specific instruction. 
# MAGIC
# MAGIC You might also need to query your own fine-tuned LLMs, providing a solution to leverage small models to perform extremely well on specialized tasks, at a lower cost.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Use-case: Increasing customer satisfaction and churn reduction with automatic reviews analysis
# MAGIC
# MAGIC In this demo, we'll build a data pipeline that takes customer reviews as text, analyze them by leveraging LLMs (Databricks Foundation Model MPT or Mistral). 
# MAGIC
# MAGIC We'll even provide recommendations for next best actions to our customer service team - i.e. whether a customer requires follow-up, and a sample message to follow-up with.
# MAGIC
# MAGIC For each review, we:
# MAGIC - Extract information such as sentiment and whether a response is required back to the customer
# MAGIC - Generate a response mentioning alternative products that may satisfy the customer
# MAGIC
# MAGIC &nbsp;
# MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/product/sql-ai-functions/sql-ai-function-review.png" width="1200">

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 2.1/ Introducing Databricks AI_QUERY() function: generating dataset
# MAGIC
# MAGIC Databricks provide a `ai_query()` function that you can use to call any model endpoint. 
# MAGIC
# MAGIC This lets you wrap functions around any Model Serving Endpoint. Your endpoint can then implement any logic (a Foundation Model, a more advanced Chain Of Thought, or a classic ML model). 
# MAGIC
# MAGIC AI Functions abstracts away the technical complexities of calling LLMs, enabling analysts and data scientists to start using these models without worrying about the underlying infrastructure.
# MAGIC
# MAGIC In this demo, we use one of our [Foundation models](https://docs.databricks.com/en/machine-learning/foundation-models/index.html), [Databricks-MPT-30B](https://www.databricks.com/blog/mpt-30b) as our LLM that will be used by AI_QUERY() to generate fake reviews. 
# MAGIC
# MAGIC Open the next Notebook to generate some sample data for our demo: [02-Generate-fake-data-with-AI-functions]($./02-Generate-fake-data-with-AI-functions)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.2/ Building our SQL pipeline with our LLM to extract review sentiments
# MAGIC
# MAGIC We are now ready to use create our full data pipeline:
# MAGIC
# MAGIC &nbsp;
# MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/product/sql-ai-functions/sql-ai-function-flow.png" width="1000">
# MAGIC
# MAGIC Open the [03-automated-product-review-and-answer]($./03-automated-product-review-and-answer) to process our text using SQL and automate our review answer!

# COMMAND ----------

# MAGIC %md
# MAGIC ### Optional: If you cannot access a foundation model, follow the instructions in the notebook [04-Extra-setup-external-model-OpenAI]($./04-Extra-setup-external-model-OpenAI) to use an OpenAI model instead. 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Further Reading and Resources
# MAGIC - [Documentation](https://docs.databricks.com/en/large-language-models/ai-query-external-model.html)
# MAGIC - [Introducing AI Functions: Integrating Large Language Models with Databricks SQL](https://www.databricks.com/blog/2023/04/18/introducing-ai-functions-integrating-large-language-models-databricks-sql.html)
