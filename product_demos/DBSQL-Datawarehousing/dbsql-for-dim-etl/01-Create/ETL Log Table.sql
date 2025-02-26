-- Databricks notebook source
-- MAGIC %run "../00-Setup/Initialize"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Create Tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Config/Log Table for ETL
-- MAGIC This table captures the metadata for a given table that includes the table name, load start time and load end time.

-- COMMAND ----------

drop table if exists identifier(run_log_table);

-- COMMAND ----------

create table identifier(run_log_table) (data_source string, table_name string, load_start_time timestamp, locked boolean, load_end_time timestamp, num_inserts int, num_updates int, process_id string)
;