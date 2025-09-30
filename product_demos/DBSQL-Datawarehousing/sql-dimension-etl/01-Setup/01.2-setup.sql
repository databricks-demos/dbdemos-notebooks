-- Databricks notebook source
-- MAGIC %run ./01.1-initialize

-- COMMAND ----------

DECLARE OR REPLACE VARIABLE sqlstr STRING; -- Variable to hold any SQL statement for EXECUTE IMMEDIATE

-- COMMAND ----------

-- MAGIC %md
-- MAGIC -- To CREATE Catalog<br>
-- MAGIC -- This option is disabled<br>
-- MAGIC
-- MAGIC SET VARIABLE sqlstr = "CREATE CATALOG IF NOT EXISTS " || catalog_name;
-- MAGIC EXECUTE IMMEDIATE sqlstr;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC -- To CREATE Catalog<br>
-- MAGIC -- This option is disabled<br>
-- MAGIC
-- MAGIC EXECUTE IMMEDIATE 'CREATE SCHEMA IF NOT EXISTS IDENTIFIER(?)' USING full_schema_name;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Enable/disable Predictive Optimization for schema**

-- COMMAND ----------

BEGIN
  DECLARE sqlstr STRING;

  IF enable_po_for_schema THEN
    SET sqlstr = "ALTER SCHEMA " || full_schema_name || ' ENABLE PREDICTIVE OPTIMIZATION';
    EXECUTE IMMEDIATE sqlstr;
  END IF;
END

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Create Volume for staging source data files**

-- COMMAND ----------

DECLARE OR REPLACE full_volume_name = full_schema_name || '.' || volume_name;
EXECUTE IMMEDIATE "CREATE VOLUME IF NOT EXISTS IDENTIFIER(?)" USING full_volume_name;
