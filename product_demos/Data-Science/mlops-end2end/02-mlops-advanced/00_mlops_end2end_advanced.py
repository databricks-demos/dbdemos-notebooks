# Databricks notebook source
# MAGIC %md
# MAGIC # Full MLOps pipeline with the Data Intelligence Platform 
# MAGIC In the first [quickstart notebooks]($../01-mlops-quickstart/00_mlops_end2end_quickstart_presentation), we'll cover the foundation of MLOps.
# MAGIC
# MAGIC Now that you master the foundation of MLOps, we'll dive into a more complete, production grade pipeline, including:
# MAGIC - Model serving
# MAGIC - Realtime Feature serving with Online Tables
# MAGIC - A/B testing 
# MAGIC - Automated re-training
# MAGIC - Infra setup abd hooks with Databricks MLOps Stack
# MAGIC - ...
# MAGIC
# MAGIC Run this demo on a __DBR 15.4 ML LTS__ cluster. A demo cluster has been created for you.

# COMMAND ----------

# MAGIC %run ../_resources/00-setup $reset_all_data=true

# COMMAND ----------

telcoDF = spark.table("mlops_churn_bronze_customers")
display(telcoDF)
