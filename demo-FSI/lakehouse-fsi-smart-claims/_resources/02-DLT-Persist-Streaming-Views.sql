-- Databricks notebook source
-- MAGIC %md 
-- MAGIC ## Persist DLT streaming view
-- MAGIC To easily support DLT / UC / ML during the preview, we temporary recopy the final DLT view to another UC table 

-- COMMAND ----------

CREATE OR REPLACE TABLE main__build.fsi_smart_claims.claim_policy_telematics_ml AS SELECT * FROM main__build.fsi_smart_claims.claim_policy_telematics;
