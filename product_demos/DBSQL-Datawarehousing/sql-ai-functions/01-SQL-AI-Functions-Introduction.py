# Databricks notebook source
# MAGIC %md
# MAGIC # Actioning Customer Reviews at Scale with Databricks SQL AI Functions
# MAGIC
# MAGIC AI Functions are built-in Databricks SQL functions, allowing you to access Large Language Models (LLMs) directly from SQL.
# MAGIC
# MAGIC Popular LLMs such as the one provided by OpenAI APIs let you apply all sort of transformations on top of text, from classification, information extraction to automatic answers.
# MAGIC
# MAGIC Leveraging Databricks SQL AI functions `AI_GENERATE_TEXT()`, you can now apply these transformations and experiment with LLMs on your data from within a familiar SQL interface. 
# MAGIC
# MAGIC Once you have developed the correct LLM prompt, you can quickly turn that into a production pipeline using existing Databricks tools such as Delta Live Tables or scheduled Jobs. This greatly simplifies both the development and productionization workflow for LLMs.
# MAGIC
# MAGIC AI Functions abstracts away the technical complexities of calling LLMs, enabling analysts and data scientists to start using these models without worrying about the underlying infrastructure.
# MAGIC
# MAGIC ## Increasing customer satisfaction and churn reduction with automatic reviews analysis
# MAGIC
# MAGIC In this demo, we'll build a data pipeline that takes customer reviews, in the form of freeform text, and enrich them with meaning derived by asking natural language questions of Azure OpenAI's GPT-3.5 Turbo model. We'll even provide recommendations for next best actions to our customer service team - i.e. whether a customer requires follow-up, and a sample message to follow-up with
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
# MAGIC ## 1/ Setup: get your Open AI configuration and save key as Databricks secret
# MAGIC
# MAGIC ### Prerequisites
# MAGIC
# MAGIC In order to run this demo in your own environment you will need to satisfy these prerequisites:
# MAGIC
# MAGIC - Enrolled in the Public Preview. Request enrolment [here](https://docs.google.com/forms/d/e/1FAIpQLSdHOk5Wmk38zGqGhi27Q3ZiTpV7aIHipSa1Al9C0vfX0wYHfQ/viewform)
# MAGIC - Access to a Databricks SQL Pro or Serverless [warehouse](https://docs.databricks.com/sql/admin/create-sql-warehouse.html#what-is-a-sql-warehouse)
# MAGIC - An [Azure OpenAI key](https://learn.microsoft.com/en-us/azure/cognitive-services/openai/overview#how-do-i-get-access-to-azure-openai)
# MAGIC - Store the API key in Databricks Secrets (documentation: [AWS](https://docs.databricks.com/security/secrets/index.html), [Azure](https://learn.microsoft.com/en-gb/azure/databricks/security/secrets/), [GCP](https://docs.gcp.databricks.com/security/secrets/index.html)).
# MAGIC
# MAGIC ### Setting up Open AI
# MAGIC
# MAGIC In order for `AI_GENERATE_TEXT()` to call your Azure OpenAI model, you need to provide the key to your Azure OpenAI resource endpoint. We'll explore how we retrieve this information and store it secure in Databricks secrets.
# MAGIC
# MAGIC Open the [02-Create-OpenAI-model-and-store-secrets]($./02-Create-OpenAI-model-and-store-secrets) to see how to setup your Open AI service and retrieve its configuration.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2/ `AI_GENERATE_TEXT` introduction: Generating fake data for our demo with Open AI
# MAGIC
# MAGIC To start our demo, we will leverage `AI_GENERATE_TEXT()` to generate fake reviews to use in our data pipeline. 
# MAGIC
# MAGIC The sample data mimics customer reviews for grocery products submitted to an e-commerce website, and we will craft a prompt for Open AI to generate this data for us.
# MAGIC
# MAGIC Open the [03-Generate-fake-data-with-AI-functions]($./03-Generate-fake-data-with-AI-functions) to start with your first SQL AI Function!

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4/ Building our SQL pipeline with Open AI to extract review sentiments
# MAGIC
# MAGIC We are now ready to use create our full data pipeline:
# MAGIC
# MAGIC &nbsp;
# MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/product/sql-ai-functions/sql-ai-function-flow.png" width="1000">
# MAGIC
# MAGIC Open the [04-automated-product-review-and-answer]($./04-automated-product-review-and-answer) to process our text using SQL and automate our review answer!

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Going further: creating your own chatbot with Dolly, Databricks open source LLM
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/product/llm-dolly/llm-dolly.png" style="float: right" width="200px"/>
# MAGIC
# MAGIC
# MAGIC Databricks not only let you call external LLM such as Open AI. 
# MAGIC
# MAGIC The lakehouse also provides an 2e2 ML platform so that you can build and specialize your own LLM, covering your custom dataset ingestion, model training and model serving using Serverless rest endpoints.
# MAGIC
# MAGIC For more details, run `dbdemos.install('llm-dolly-chatbot')`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Further Reading and Resources
# MAGIC - [Documentation](https://docs.databricks.com/sql/language-manual/functions/ai_generate_text.html)
# MAGIC - [Introducing AI Functions: Integrating Large Language Models with Databricks SQL](https://www.databricks.com/blog/2023/04/18/introducing-ai-functions-integrating-large-language-models-databricks-sql.html)
# MAGIC - Check out more Databricks demos at [dbdemos.ai](https://www.dbdemos.ai/index.html)
