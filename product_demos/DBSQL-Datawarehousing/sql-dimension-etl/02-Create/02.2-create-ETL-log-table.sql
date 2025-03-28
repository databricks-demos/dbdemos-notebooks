-- Databricks notebook source
-- MAGIC %run ../01-Setup/01.1-initialize

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Create the Log Table to track our ETL runs
-- MAGIC
-- MAGIC This table captures the metadata for a given table that includes the table name, load start time and load end time, and other operations metadata.

-- COMMAND ----------

CREATE OR REPLACE TABLE IDENTIFIER(run_log_table) (
  data_source STRING,
  table_name STRING,
  load_start_time TIMESTAMP,
  load_end_time TIMESTAMP,
  num_inserts INT,
  num_updates INT,
  process_id STRING
);

-- COMMAND ----------

SELECT * FROM IDENTIFIER(run_log_table);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Next: [create the Patient table]($./02.3-create-patient-tables)
