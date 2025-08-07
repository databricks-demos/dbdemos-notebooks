# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Measure the impact of your AI Agent with Databricks Lakeflow, AI/BI Dashboard and Genie!
# MAGIC
# MAGIC Deploying new AI project is great, but ultimately, you want to be able to track how your business is impacted by your agent.
# MAGIC
# MAGIC Typically, a customer support assistant should:
# MAGIC
# MAGIC - Lower time to resolution
# MAGIC - Increase customer satisfaction
# MAGIC - Drive more business outcomes
# MAGIC - Empower support team to do more, with higher quality interaction...
# MAGIC
# MAGIC Databricks lets you track all of these metrics : Connect your CRM or Ticket system with Lakeflow Connect or any ingestion, prepare and transform your data if needed and deploy an Business-First dashboard, and ask any followup question about your data with Databricks Genie!
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/ai-agent/customer-support-dashboard.png?raw=true" width="900px">

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Want to try an AI/BI customer support setup?
# MAGIC
# MAGIC Run the following cells to install our dbdemos customer support:

# COMMAND ----------

# MAGIC %pip install dbdemos -U
# MAGIC %restart_python

# COMMAND ----------

import dbdemos
dbdemos.install('aibi-customer-support', catalog='main', schema='dbdemos_aibi_customer_support')
