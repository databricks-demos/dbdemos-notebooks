-- Databricks notebook source
-- MAGIC %run ../01-Setup/01.1-initialize

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Create and Populate Master Data for Coded Attributes

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create Code Table
-- MAGIC Create the table to store standardized descriptions for coded attributes.

-- COMMAND ----------

-- We defined our common table names including their catalog and schema in the parent 01-setup notebook
SELECT code_table;

-- COMMAND ----------

DROP TABLE IF EXISTS IDENTIFIER(code_table);

CREATE TABLE IDENTIFIER(code_table) (
  m_code STRING COMMENT 'code',
  m_desc STRING COMMENT 'name or description for the code',
  m_type STRING COMMENT 'attribute type utilizing code'
)
COMMENT 'master table for coded attributes';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Populate Sample Data

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
