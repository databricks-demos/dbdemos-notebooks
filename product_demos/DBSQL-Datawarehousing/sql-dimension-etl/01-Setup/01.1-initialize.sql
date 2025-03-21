-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC **Configure Catalog and Schema** <br>
-- MAGIC
-- MAGIC 1. Specify the catalog name (to create demo schema and objects) <br>
-- MAGIC
-- MAGIC 2. Specify the schema name (to create data warehouse tables, staging volume) <br>
-- MAGIC
-- MAGIC <u>NOTE:</u>
-- MAGIC The catalog and schema can be create beforehand.  If not, ensure that the user running the workflow has permissions to create catalog and schema.

-- COMMAND ----------

-- DBTITLE 1,dimension schema
-- Name of catalog under which to create the demo schema
declare or replace variable catalog_name string = 'main';

-- Name of the demo schema under which to create tables, volume
declare or replace variable schema_name string  = 'dbdemos_sql_etl';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Configure Predictive Optimization (PO)**
-- MAGIC <br><br>
-- MAGIC Specify (true/false) whether to enable Predictive Optimization (PO) for the DW schema

-- COMMAND ----------

-- Enable PO at schema level / else inherit from account setting
declare or replace variable enable_po_for_schema boolean = true;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Configure Volume**
-- MAGIC <br><br>
-- MAGIC A folder named "patient" will be created in this volume, and used to stage the source data files that comprise the demo.
-- MAGIC <br><br>
-- MAGIC Please note, the code removes any existing folder named "patient" from this volume.

-- COMMAND ----------

-- Name of the UC volume where patient source data will be staged
-- Created in the demo schema
declare or replace variable volume_name string = 'staging';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Additional variables**

-- COMMAND ----------

-- variable to store qualified name of ETL log table
declare or replace variable run_log_table string;

-- variable to store qualified name of Code Master table
declare or replace variable code_table string;

-- COMMAND ----------

set variable (run_log_table, code_table) = (select catalog_name || '.' || schema_name || '.' || 'etl_run_log', catalog_name || '.' || schema_name || '.' || 'code_m');

-- COMMAND ----------

-- Path of the UC volume where patient source data will be staged
declare or replace variable staging_path string;
set variable staging_path = '/Volumes/' || catalog_name || "/" || schema_name || "/" || volume_name;
