-- Databricks notebook source
-- MAGIC %md 
-- MAGIC ## Persist DLT streaming view
-- MAGIC To easily support DLT / UC / ML during the preview with all cluster types, we temporary recopy the final DLT view to another UC table 

-- COMMAND ----------

CREATE OR REPLACE TABLE dbdemos.hls_patient_readmission.drug_exposure_ml AS SELECT * FROM dbdemos.hls_patient_readmission.drug_exposure;
CREATE OR REPLACE TABLE dbdemos.hls_patient_readmission.person_ml AS SELECT * FROM dbdemos.hls_patient_readmission.person;
CREATE OR REPLACE TABLE dbdemos.hls_patient_readmission.patients_ml AS SELECT * FROM dbdemos.hls_patient_readmission.patients;
CREATE OR REPLACE TABLE dbdemos.hls_patient_readmission.encounters_ml AS SELECT * FROM dbdemos.hls_patient_readmission.encounters;
CREATE OR REPLACE TABLE dbdemos.hls_patient_readmission.condition_occurrence_ml AS SELECT * FROM dbdemos.hls_patient_readmission.condition_occurrence;
CREATE OR REPLACE TABLE dbdemos.hls_patient_readmission.conditions_ml AS SELECT * FROM dbdemos.hls_patient_readmission.conditions;
