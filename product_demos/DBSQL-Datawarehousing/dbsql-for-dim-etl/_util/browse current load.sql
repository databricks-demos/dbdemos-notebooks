-- Databricks notebook source
-- MAGIC %run "../00-Setup/Initialize"

-- COMMAND ----------

declare or replace variable br_table string; -- bronze table identifier
declare or replace variable si_table string; -- silver table
declare or replace variable gd_table string; -- gold dimension table

-- COMMAND ----------

set variable (br_table, si_table, gd_table) = (select catalog_nm || '.' || schema_nm || '.' || 'patient_stg', catalog_nm || '.' || schema_nm || '.' || 'patient_int', catalog_nm || '.' || schema_nm || '.' || 'g_patient_d');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Log table**

-- COMMAND ----------

select * from identifier(run_log_table) order by load_start_time;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Patient Staging table**

-- COMMAND ----------

-- DBTITLE 1,Bronze
select id, CHANGEDONDATE, data_source, * except(id, CHANGEDONDATE, data_source) from identifier(br_table)
order by data_source, id, CHANGEDONDATE

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Patient Integration table**
-- MAGIC
-- MAGIC The integration process filters out records where the **Id is NULL or CHANGEDONDATE is NULL**.  These and other business errors can be part of the exception logging process.

-- COMMAND ----------

-- DBTITLE 1,Silver
select patient_src_id, src_changed_on_dt, data_source, * except(patient_src_id, src_changed_on_dt, data_source) from identifier(si_table)
order by data_source, patient_src_id, src_changed_on_dt

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Patient Dimension table**

-- COMMAND ----------

-- DBTITLE 1,Gold
select patient_sk, patient_src_id, effective_start_date, effective_end_date, data_source, * except(patient_sk, patient_src_id, effective_start_date, effective_end_date, data_source) from identifier(gd_table)
order by data_source, patient_src_id, effective_start_date