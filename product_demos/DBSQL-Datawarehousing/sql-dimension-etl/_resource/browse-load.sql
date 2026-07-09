-- Databricks notebook source
-- MAGIC %run "../01-Setup/01.1-initialize"

-- COMMAND ----------

USE IDENTIFIER(full_schema_name)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **ETL Log Table**

-- COMMAND ----------

SELECT * FROM IDENTIFIER(run_log_table) ORDER BY load_start_time;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Patient Staging Table**

-- COMMAND ----------

-- DBTITLE 1,Bronze
SELECT Id, CHANGEDONDATE, data_source, * EXCEPT(Id, CHANGEDONDATE, data_source)
FROM patient_stg
ORDER BY data_source, Id, CHANGEDONDATE

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Patient Integration Table**
-- MAGIC
-- MAGIC The integration process filters out records where the **Id is NULL or CHANGEDONDATE is NULL or Last Name is NULL**.  These and other business errors can be part of the exception logging process.

-- COMMAND ----------

-- DBTITLE 1,Silver
SELECT patient_src_id, src_changed_on_dt, data_source, * EXCEPT(patient_src_id, src_changed_on_dt, data_source)
FROM patient_int
ORDER BY data_source, patient_src_id, src_changed_on_dt

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Patient Dimension Table**
-- MAGIC
-- MAGIC The MERGE process ignores **duplicate versions**.

-- COMMAND ----------

-- DBTITLE 1,Gold
SELECT
  patient_sk, patient_src_id, effective_start_date, effective_end_date, data_source, * EXCEPT(patient_sk, patient_src_id, effective_start_date, effective_end_date, data_source)
FROM patient_dim
ORDER BY data_source, patient_src_id, effective_start_date
