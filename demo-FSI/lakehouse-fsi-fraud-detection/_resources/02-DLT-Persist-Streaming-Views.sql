-- Databricks notebook source
-- MAGIC %md 
-- MAGIC ## Persist DLT streaming view
-- MAGIC To easily support DLT / UC / ML during the preview, we temporary recopy the final DLT view to another UC table 

-- COMMAND ----------

CREATE OR REPLACE TABLE dbdemos.fsi_fraud_detection.gold_transactions_ml AS SELECT * FROM dbdemos.fsi_fraud_detection.gold_transactions;
