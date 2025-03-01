-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC **Configure settings** <br>
-- MAGIC
-- MAGIC 1. Specify Catalog to create demo schemas <br>
-- MAGIC
-- MAGIC 2. Specify Schema to create data warehouse tables, staging volume <br>
-- MAGIC
-- MAGIC 3. Specify whether to enable Predictive Optimization for DW schema
-- MAGIC
-- MAGIC <u>NOTE:</u>
-- MAGIC The catalog and schema can be create beforehand.  If not, ensure that the user running the workflow has permissions to create catalog and schema.

-- COMMAND ----------

-- DBTITLE 1,dimension schema
/*
Manually update the following, to use a different catalog / schema:
*/

declare or replace variable catalog_nm string = 'dbsqldemos';
declare or replace variable schema_nm string  = 'clinical_star';

-- COMMAND ----------

-- enable PO at schema level? else inherit from account setting
declare or replace variable enable_po_for_schema boolean = true;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Additional settings**

-- COMMAND ----------

declare or replace variable run_log_table string;
declare or replace variable code_table string;

-- COMMAND ----------

set variable (run_log_table, code_table) = (select catalog_nm || '.' || schema_nm || '.' || 'elt_run_log', catalog_nm || '.' || schema_nm || '.' || 'code_m');

-- COMMAND ----------

declare or replace variable volume_name string = 'staging';

-- COMMAND ----------

declare or replace variable staging_path string;
set variable staging_path = '/Volumes/' || catalog_nm || "/" || schema_nm || "/" || volume_name;