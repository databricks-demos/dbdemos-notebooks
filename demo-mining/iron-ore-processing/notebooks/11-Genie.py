# Databricks notebook source
# MAGIC %md
# MAGIC # 9. What is a Genie Space? 
# MAGIC
# MAGIC Genie Spaces is a Databricks feature that allows business teams to interact with their data using natural language. It uses generative AI tailored to your organization's terminology and data, with the ability to monitor and refine its performance through user feedback.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9.1 What data does this Genie space contain?
# MAGIC
# MAGIC This Genie space contains the iron ore processing plant data we ingested and transformed. By ensuring the metadata was well described and the primary and foreign keys were added to allow the LLM to connect the tables, we're able to interrogate the data in Natural language 

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9.2 How do Genie spaces work?
# MAGIC
# MAGIC By providing the space with tables and their metadata, the LLM powering genie spaces can build SQL queries or graph templates which attempt to answer the question posed. Because of this the LLM never receives any of the data, it simply builds a query which is then run against the tables enabling the results to be returned to the user
# MAGIC
# MAGIC [Link to Genie Space](https://e2-demo-field-eng.cloud.databricks.com/genie/rooms/01f03008dd8310ceb8dcd7f11463f053?o=1444828305810485)
# MAGIC
# MAGIC ![](/Workspace/Shared/iron_ore_precessing_demo/demo_setup/images/Genie_Space.png)