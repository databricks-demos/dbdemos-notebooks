-- Databricks notebook source
-- MAGIC %run "./01.1-initialize"

-- COMMAND ----------

declare or replace variable sqlstr string; -- variable to hold any sql statement for EXECUTE IMMEDIATE

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create Catalog and Schema(s) if required

-- COMMAND ----------

set variable sqlstr = "create catalog if not exists " || catalog_name;
execute immediate sqlstr;

-- COMMAND ----------

set variable sqlstr = "create schema if not exists " || catalog_name || "." || schema_name;

-- COMMAND ----------

execute immediate sqlstr;

-- COMMAND ----------

set variable sqlstr = "alter schema " || catalog_name || "." || schema_name || if(enable_po_for_schema, ' enable', ' inherit') || ' predictive optimization';
execute immediate sqlstr;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create Volume for staging source data files

-- COMMAND ----------

set variable sqlstr = "create volume if not exists " || catalog_name || "." || schema_name || "." || volume_name;
execute immediate sqlstr;
