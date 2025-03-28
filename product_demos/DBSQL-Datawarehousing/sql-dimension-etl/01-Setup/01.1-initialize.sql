-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC # Parametrize your SQL Script
-- MAGIC
-- MAGIC In this initial notebook, we're defining our table and schema as parameter.
-- MAGIC This makes it easy to run your ETL pipeline on different catalogs (for e.g., dev/test)
-- MAGIC <br><br>
-- MAGIC Specify the following:
-- MAGIC 1. Catalog name (to create demo schema and objects)
-- MAGIC 2. Schema name (to create data warehouse tables, staging volume)
-- MAGIC
-- MAGIC <br>
-- MAGIC *NOTE: DBDemos will create the catalog and schema for you if they do not exist. Ensure that the user running the workflow has permissions to create catalog and schema.*

-- COMMAND ----------

-- MAGIC %md
-- MAGIC CREATE WIDGET TEXT catalog_name DEFAULT "main";
-- MAGIC CREATE WIDGET TEXT schema_name DEFAULT "dbdemos_sql_etl";

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
DECLARE OR REPLACE VARIABLE enable_po_for_schema BOOLEAN = true;

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

-- Path of the UC volume where patient source data will be staged
DECLARE OR REPLACE VARIABLE staging_path STRING;
SET VARIABLE staging_path = '/Volumes/' || catalog_name || "/" || schema_name || "/" || volume_name;

SELECT staging_path;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Additional variables**
-- MAGIC <br>
-- MAGIC Defines our logging table to store our operation details

-- COMMAND ----------

-- Two-level schema name
DECLARE OR REPLACE VARIABLE full_schema_name STRING = catalog_name || '.' || schema_name;

-- Full volume name
DECLARE OR REPLACE VARIABLE full_volume_name STRING = full_schema_name || "." || volume_name;

-- COMMAND ----------

DECLARE OR REPLACE VARIABLE run_log_table STRING;
SET VARIABLE run_log_table = full_schema_name || '.' || 'etl_run_log';

DECLARE OR REPLACE VARIABLE code_table STRING;
SET VARIABLE code_table = full_schema_name || '.' || 'code_m';

SELECT run_log_table, code_table;
