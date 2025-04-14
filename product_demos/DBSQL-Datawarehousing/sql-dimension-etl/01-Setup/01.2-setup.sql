-- Databricks notebook source
-- MAGIC %run ./01.1-initialize

-- COMMAND ----------

DECLARE OR REPLACE VARIABLE sqlstr STRING; -- Variable to hold any SQL statement for EXECUTE IMMEDIATE

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Create Catalog and Schema(s) if required**

-- COMMAND ----------

SET VARIABLE sqlstr = "CREATE CATALOG IF NOT EXISTS " || catalog_name;
EXECUTE IMMEDIATE sqlstr;

-- COMMAND ----------

EXECUTE IMMEDIATE 'create schema if not exists IDENTIFIER(?)' USING full_schema_name;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Enable/disable Predictive Optimization for schema**

-- COMMAND ----------

SET VARIABLE sqlstr = "ALTER SCHEMA " || full_schema_name || IF(enable_po_for_schema, ' ENABLE', ' INHERIT') || ' PREDICTIVE OPTIMIZATION';
EXECUTE IMMEDIATE sqlstr;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Create Volume for staging source data files**

-- COMMAND ----------

EXECUTE IMMEDIATE "CREATE VOLUME IF NOT EXISTS IDENTIFIER(?)" USING full_volume_name;
