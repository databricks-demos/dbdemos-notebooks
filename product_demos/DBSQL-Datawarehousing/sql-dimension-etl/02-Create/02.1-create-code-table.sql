-- Databricks notebook source
-- MAGIC %run ../01-Setup/01.1-initialize

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Creating Code Table
-- MAGIC
-- MAGIC Let's start by creating our initial tables.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Master Data
-- MAGIC Standardized codes used for coded attributes

-- COMMAND ----------

-- We defined our common table names including their catalog and schema in the parent 01-setup notebook
SELECT code_table;

-- COMMAND ----------

CREATE OR REPLACE TABLE IDENTIFIER(code_table) (
  m_code STRING COMMENT 'code',
  m_desc STRING COMMENT 'name or description for the code',
  m_type STRING COMMENT 'attribute type utilizing code'
)
COMMENT 'master table for coded attributes';

-- COMMAND ----------

INSERT INTO IDENTIFIER(code_table)
VALUES
  ('M', 'Male', 'GENDER'),
  ('F', 'Female', 'GENDER'),
  ('hispanic', 'Hispanic', 'ETHNICITY'),
  ('nonhispanic', 'Not Hispanic', 'ETHNICITY');

-- COMMAND ----------

SELECT * FROM IDENTIFIER(code_table);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Next: [create the ETL Log table]($./02.2-create-ETL-log-table)
