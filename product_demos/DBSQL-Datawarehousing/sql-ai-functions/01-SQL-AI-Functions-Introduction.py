# Databricks notebook source
# MAGIC %md
# MAGIC # Actioning Customer Reviews at Scale with Databricks SQL AI Functions (*CHANGE THIS IMAGE)
# MAGIC
# MAGIC AI Functions are built-in Databricks SQL functions, allowing you to access Large Language Models (LLMs) directly from SQL.
# MAGIC
# MAGIC Popular open sources LLMs models let you apply all sort of transformations on top of text, from classification, information extraction to automatic answers.
# MAGIC
# MAGIC Leveraging Databricks New Foundation Model Capabilities and SQL AI functions `ai_query()`, you can now apply these transformations and experiment with foudations LLMs on your data from within a familiar SQL interface. 
# MAGIC
# MAGIC Once you have developed the correct LLM prompt, you can quickly turn that into a production pipeline using existing Databricks tools such as Delta Live Tables or scheduled Jobs. This greatly simplifies both the development and productionization workflow for LLMs.
# MAGIC
# MAGIC AI Functions abstracts away the technical complexities of calling LLMs, enabling analysts and data scientists to start using these models without worrying about the underlying infrastructure.
# MAGIC
# MAGIC ## Increasing customer satisfaction and churn reduction with automatic reviews analysis
# MAGIC
# MAGIC In this demo, we'll build a data pipeline that takes customer reviews, in the form of freeform text, and enrich them with meaning derived by asking natural language questions of Databricks Mixtral model. We'll even provide recommendations for next best actions to our customer service team - i.e. whether a customer requires follow-up, and a sample message to follow-up with
# MAGIC
# MAGIC For each review, we:
# MAGIC - Determine sentiment and whether a response is required back to the customer
# MAGIC - Generate a response mentioning alternative products that may satisfy the customer
# MAGIC
# MAGIC &nbsp;
# MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/product/sql-ai-functions/sql-ai-function-review.png" width="1200">
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Ffeatures%2Fsql_ai_functions%2Fintro&dt=DBSQL">

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1/ `AI_QUERY` introduction: Generating fake data for our demo with our foundation model
# MAGIC
# MAGIC To start our demo, we will leverage `ai_query()` to generate fake reviews to use in our data pipeline. 
# MAGIC
# MAGIC The sample data mimics customer reviews for grocery products submitted to an e-commerce website, and we will craft a prompt for our foundation model to generate this data for us.
# MAGIC
# MAGIC In this demo, we use one of our [Foundation models](https://docs.databricks.com/en/machine-learning/foundation-models/index.html), [Databricks-MPT-30B](https://www.databricks.com/blog/mpt-30b) as our LLM that will be used by AI_QUERY() to generate fake reviews. 
# MAGIC
# MAGIC To use this notebook and the following notebooks, connect to a SQL warehouse using the steps described in the following image.  
# MAGIC
# MAGIC Open the next Notebook to generate some sample data for our demo: [02-Generate-fake-data-with-AI-functions-Foundation-Model]($./02-Generate-fake-data-with-AI-functions-Foundation-Model)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2/ Building our SQL pipeline with our LLM to extract review sentiments
# MAGIC
# MAGIC We are now ready to use create our full data pipeline:
# MAGIC
# MAGIC &nbsp;
# MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/product/sql-ai-functions/sql-ai-function-flow.png" width="1000">
# MAGIC
# MAGIC Open the [03-automated-product-review-and-answer]($./03-automated-product-review-and-answer) to process our text using SQL and automate our review answer!

# COMMAND ----------

# MAGIC %md
# MAGIC #### If you cannot access a foundation model, follow the instructions in the notebook [Optional-Generate-fake-data-with-AI-functions-ExternalModel]($./Optional-Generate-fake-data-with-AI-functions-ExternalModel) to use an OpenAI model instead. 

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Going further: creating your own chatbot with Dolly, Databricks open source LLM
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/product/llm-dolly/llm-dolly.png" style="float: right" width="200px"/>
# MAGIC
# MAGIC Databricks not only let you call foundations LLMs and external model such as Open AI. 
# MAGIC
# MAGIC The lakehouse also provides an 2e2 ML platform so that you can build and specialize your own LLM, covering your custom dataset ingestion, model training and model serving using Serverless rest endpoints.
# MAGIC
# MAGIC For more details, run `dbdemos.install('llm-dolly-chatbot')`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Further Reading and Resources
# MAGIC - [Documentation](https://docs.databricks.com/en/large-language-models/ai-query-external-model.html)
# MAGIC - [Introducing AI Functions: Integrating Large Language Models with Databricks SQL](https://www.databricks.com/blog/2023/04/18/introducing-ai-functions-integrating-large-language-models-databricks-sql.html)
# MAGIC - Check out more Databricks demos at [dbdemos.ai](https://www.dbdemos.ai/index.html)
