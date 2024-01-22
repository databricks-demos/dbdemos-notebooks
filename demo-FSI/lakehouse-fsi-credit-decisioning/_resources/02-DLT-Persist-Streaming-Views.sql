-- Databricks notebook source
-- MAGIC %md 
-- MAGIC ## Persist DLT streaming view
-- MAGIC To easily support DLT / UC / ML during the preview, we temporary recopy the final DLT view to another UC table 

-- COMMAND ----------

CREATE OR REPLACE TABLE dbdemos.fsi_credit_decisioning.customer_gold_features AS SELECT * FROM dbdemos.fsi_credit_decisioning.customer_gold;
CREATE OR REPLACE TABLE dbdemos.fsi_credit_decisioning.telco_gold_features AS SELECT * FROM dbdemos.fsi_credit_decisioning.telco_gold;
CREATE OR REPLACE TABLE dbdemos.fsi_credit_decisioning.fund_trans_gold_features AS SELECT * FROM dbdemos.fsi_credit_decisioning.fund_trans_gold;
CREATE OR REPLACE TABLE dbdemos.fsi_credit_decisioning.credit_bureau_gold_features AS SELECT * FROM dbdemos.fsi_credit_decisioning.credit_bureau_gold;
CREATE OR REPLACE TABLE dbdemos.fsi_credit_decisioning.customer_silver_features AS SELECT * FROM dbdemos.fsi_credit_decisioning.customer_silver;
