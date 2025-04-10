# Databricks notebook source
# MAGIC %run ./00-init

# COMMAND ----------

TODO: add faker script to generate all the data + ai_query spark function for the documentation. Save data in volume

See 
https://github.com/databricks-demos/dbdemos-notebooks/blob/main/demo-retail/lakehouse-retail-c360/_resources/01-load-data.py

then save the data under https://github.com/databricks-demos/dbdemos-dataset (PR) then goes in s3://dbdemos-dataset/llm/ (cal can help to publish if needed there)

Make sure it's designed to work with the script (and the tools, ex: add PII field and have them masked), and for multiple use-case
 - telco subcriptions
 - retail orders
