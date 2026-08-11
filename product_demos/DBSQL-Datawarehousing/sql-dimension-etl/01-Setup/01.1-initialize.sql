-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC # Parametrize your SQL Script
-- MAGIC
-- MAGIC In this initial notebook, we're defining our catalog / schema / table names as global variables.
-- MAGIC This makes it easy to run your ETL pipeline on different catalogs (for e.g., dev/test)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Configure:**
-- MAGIC 1. Catalog name (to contain demo schema and objects)
-- MAGIC 2. Schema name (to create data warehouse tables, staging volume)
-- MAGIC <br>
-- MAGIC
-- MAGIC <b>NOTE</b>
-- MAGIC - Ensure that the Catalog and Schema exist.<br>
-- MAGIC - Ensure that the user running the demo has <b>CREATE TABLE</b> and <b>CREATE VOLUME</b> privileges in the above schema.

-- COMMAND ----------

-- Name of catalog under which to create the demo schema
DECLARE OR REPLACE VARIABLE catalog_name STRING = 'main';

-- Name of the demo schema under which to create tables, volume
DECLARE OR REPLACE VARIABLE schema_name STRING = 'dbdemos_sql_etl';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Configure Predictive Optimization (PO)**
-- MAGIC <br>
-- MAGIC Specify (true/false) whether to enable Predictive Optimization (PO) for the DW schema

-- COMMAND ----------

-- Enable PO prodictive optimization at schema level / else inherit from account setting
-- User needs to have ALTER SCHEMA privilege
DECLARE OR REPLACE VARIABLE enable_po_for_schema BOOLEAN = false;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Configure Volume**
-- MAGIC <br>
-- MAGIC A folder named "patient" will be created in this volume, and used to stage the source data files that comprise the demo.
-- MAGIC <br>
-- MAGIC Please note, the code removes any existing folder named "patient" from this volume.

-- COMMAND ----------

-- Name of the UC volume where patient source data will be staged
-- Created in the demo schema
DECLARE OR REPLACE VARIABLE volume_name STRING = 'staging';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Additional global variables**

-- COMMAND ----------

-- Path of the UC volume where patient source data will be staged
DECLARE OR REPLACE VARIABLE staging_path STRING
  = '/Volumes/' || catalog_name || "/" || schema_name || "/" || volume_name;

-- COMMAND ----------

SELECT staging_path;

-- COMMAND ----------

-- Two-level schema name
DECLARE OR REPLACE VARIABLE full_schema_name STRING
  = catalog_name || '.' || schema_name;

-- COMMAND ----------

-- Three-level name of ETL Log Table
DECLARE OR REPLACE VARIABLE run_log_table STRING
  = full_schema_name || '.' || 'etl_run_log';

-- Three-level name of Code Master Table
DECLARE OR REPLACE VARIABLE code_table STRING
  = full_schema_name || '.' || 'code_m';
