# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC # Democratizing the magic of ChatGPT with open models and Databricks Lakehouse
# MAGIC
# MAGIC <img style="float:right" width="400px" src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/product/llm-dolly/llm-dolly.png">
# MAGIC
# MAGIC Large Language Models produce some amazing results, chatting and answering questions with seeming intelligence. 
# MAGIC
# MAGIC But how can you get LLMs to answer questions about _your_ specific datasets? Imagine answering questions based on your company's knowledge base, docs or Slack chats. 
# MAGIC
# MAGIC The good news is that this is easy to build on Databricks, leveraging open-source tooling and open LLMs. 
# MAGIC
# MAGIC ## Databricks Dolly: World's First Truly Open Instruction-Tuned LLM
# MAGIC
# MAGIC As of now, most of the state of the art models comes with restrictive license and can't easily be used for commercial purpose. This is because they were trained or fine-tuned using non-open datasets.
# MAGIC
# MAGIC To solve this challenge, Databricks released Dolly, the first truly open LLM. Because Dolly was fine tuned using `databricks-dolly-15k` (15,000 high-quality human-generated prompt / response pairs specifically designed for instruction tuning large language models), it can be used as starting point to create your own commercial model.
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Ffeatures%2Fdolly-chatbot%2Fintro&dt=ML">

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Building a gardening chat bot to answer our questions
# MAGIC
# MAGIC In this demo, we'll be building a chat bot to based on Dolly. As a gardening shop, we want to add a bot in our application to answer our customer questions and give them recommendations on how they can care for their plants.
# MAGIC
# MAGIC We will split this demo in 2 sections:
# MAGIC
# MAGIC - 1/ **Data preparation**: ingest and clean our Q&A dataset, transforming them as embeddings in a vector database.
# MAGIC - 2/ **Q&A inference**: leverage Dolly to answer our query, leveraging our Q&A as extra context for Dolly. This is also known as Prompt Engineering.
# MAGIC
# MAGIC
# MAGIC <img style="margin: auto; display: block" width="1200px" src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/product/llm-dolly/llm-dolly-full.png">

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## 1/ Data Preparation & Vector database creation with Databricks Lakehouse
# MAGIC
# MAGIC
# MAGIC <img style="float: right" width="500px" src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/product/llm-dolly/llm-dolly-data-prep-small.png">
# MAGIC
# MAGIC The most difficult part in our chat bot creation is the data collection, preparation and cleaning. 
# MAGIC
# MAGIC Databricks Lakehouse makes this simple! Leveraging Delta Lake, Delta Live Tables and Databricks specialized execution engine, you can build simple data pipeline and drastically reduce your pipeline TCO.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Open the [02-Data-preparation]($./02-Data-preparation) notebook to ingest our data & start building our vector database. 

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## 2/ Prompt engineering for Question & Answers
# MAGIC
# MAGIC <img style="float: right" width="500px" src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/product/llm-dolly/llm-dolly-inference-small.png">
# MAGIC
# MAGIC Now that our specialized dataset is available, we can start leveraging dolly to answer our questions.
# MAGIC
# MAGIC We will start with a simple Question / Answer process:
# MAGIC
# MAGIC * Customer asks a question
# MAGIC * We fetch similar content from our Q&A dataset
# MAGIC * Engineer a prompt containing the content
# MAGIC * Send the content to Dolly
# MAGIC * Display the answer to our customer

# COMMAND ----------

# MAGIC %md
# MAGIC Open the next [03-Q&A-prompt-engineering-for-dolly]($./03-Q&A-prompt-engineering-for-dolly) notebook to learn how to improve your prompt using `langchain` to send your questions to Dolly & get great answers.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## 3/ Prompt engineering for a chat bot
# MAGIC
# MAGIC <img style="float: right" width="600px" src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/product/llm-dolly/llm-dolly-chatbot.png">
# MAGIC
# MAGIC Let's increase our bot capabilities to make him behave as a chat bot.
# MAGIC
# MAGIC In this example, we'll improve our bots by allowing chaining multiple question / answers.
# MAGIC
# MAGIC We'll keep the previous behavior (adding context from our Q&A dataset), and on top of that make sure we add a memory between each question.
# MAGIC
# MAGIC Let's leverage `langchain` `ConversationSummaryMemory` object with an intermediate model to summarize our ongoing discussion and keep our prompt under control!

# COMMAND ----------

# MAGIC %md
# MAGIC Open the next [04-chat-bot-prompt-engineering-dolly]($./04-chat-bot-prompt-engineering-dolly) notebook to build your own chat bot!

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## 4/ Fine tuning dolly
# MAGIC
# MAGIC If you have a clean dataset and want to specialized the way Dolly answers, you can fine tune the LLM for your own requirements!
# MAGIC
# MAGIC This is a more advanced requirement, and could be useful to specialize Dolly to answer with a specific chat format and a formatted type of dataset, example:
# MAGIC
# MAGIC ```
# MAGIC IA: I'm a gardening bot, how can I help ?
# MAGIC Human: How to grow bluberries?
# MAGIC IA: Blueberries are most successful when growing in acidic soil. They like a lot of water and sun.
# MAGIC Human: What's the ideal soil PH?
# MAGIC IA: to grow your blublerries, it's best to have a soil with a PH of 6, slightly acid.
# MAGIC ...
# MAGIC ```
# MAGIC
# MAGIC As Dolly is under active development, we recommend exploring the official [Dolly repository](https://github.com/databrickslabs/dolly) for up-to-date fine-tuning examples!
