-- Databricks notebook source
-- MAGIC %run ../01-Setup/01.1-initialize

-- COMMAND ----------

DECLARE OR REPLACE VARIABLE sqlstr STRING;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC # Create Tables for Patient
-- MAGIC Create the staging, integration, and dimension tables for patient entity.<br>
-- MAGIC The patient dimension is part of the clinical data warehouse (star schema), similar to a customer dimension in a Sales Data Warehouse.
-- MAGIC
-- MAGIC <u>NOTE:</u> By default, the tables are created in the **catalog main** and **schema dbdemos_sql_etl**.  To change this, or specify an existing catalog / schema, please see [01.1-initialize notebook]($../01-Setup/01.1-initialize) for more context.

-- COMMAND ----------

-- Set the current schema
USE IDENTIFIER(full_schema_name);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create Staging Table for Patient
-- MAGIC The schema for the staging table will be derived from the source data file(s)

-- COMMAND ----------

DROP TABLE IF EXISTS patient_stg;

CREATE TABLE patient_stg
COMMENT 'Patient staging table ingesting initial and incremental master data from csv files';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create Integration Table for Patient

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Potential clustering columns include (data_source, patient_src_id) <br>
-- MAGIC Also, column src_changed_on_dt will be naturally ordered (ingestion-time clustering) AND data_source will typically be the same for all records in a source file.
-- MAGIC
-- MAGIC **Note: Predictive Optimization** intelligently optimizes your table data layouts for faster queries and reduced storage costs.
-- MAGIC
-- MAGIC
-- MAGIC

-- COMMAND ----------

DROP TABLE IF EXISTS patient_int;

CREATE TABLE patient_int (
  patient_src_id STRING NOT NULL COMMENT 'ID of the record in the source',
  date_of_birth DATE COMMENT 'Date of birth',
  ssn STRING COMMENT 'Social Security Number',
  first_name STRING COMMENT 'First Name of patient',
  last_name STRING NOT NULL COMMENT 'Last Name of patient',
  name_suffix STRING COMMENT 'Name suffix',
  gender_cd STRING COMMENT 'Code for patient\'s gender',
  gender_nm STRING COMMENT 'Description of patient\'s gender',
  src_changed_on_dt TIMESTAMP COMMENT 'Date of last change to record in source',
  -- system columns
  data_source STRING NOT NULL COMMENT 'Source System for record',
  insert_dt TIMESTAMP COMMENT 'Date record inserted',
  update_dt TIMESTAMP COMMENT 'Date record updated',
  process_id STRING COMMENT 'Process ID for run',
  CONSTRAINT c_int_pk PRIMARY KEY (patient_src_id, data_source) RELY
)
COMMENT 'Curated integration table for patient data'
TBLPROPERTIES (delta.enableChangeDataFeed = true);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create Dimension Table for Patient

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Note:** <br>
-- MAGIC For the dimension table, take advantage of **Predictive Optimization** and **Auto clustering**.
-- MAGIC
-- MAGIC Auto Clustering can be used to automatically cluster your tables based on your evolving workload!
-- MAGIC <br>
-- MAGIC Auto Clustering is enabled via **CLUSTER BY AUTO** clause.

-- COMMAND ----------

DROP TABLE IF EXISTS patient_dim;

CREATE TABLE patient_dim (
  patient_sk BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Primary Key (ID)',
  last_name STRING NOT NULL COMMENT 'Last name of the person',
  first_name STRING COMMENT 'First name of the person',
  name_suffix STRING COMMENT 'Suffix of person name',
  gender_code STRING COMMENT 'Gender code',
  gender STRING COMMENT 'Gender description',
  date_of_birth TIMESTAMP COMMENT 'Birth date and time',
  ssn STRING COMMENT 'Patient SSN',
  other_identifiers MAP<STRING, STRING> COMMENT 'Identifier type (passport number, license number except mrn, ssn) and value',
  patient_src_id STRING NOT NULL COMMENT 'Unique reference to the source record',
  -- system columns
  effective_start_date TIMESTAMP NOT NULL COMMENT 'SCD2 effective start date for version',
  effective_end_date TIMESTAMP COMMENT 'SCD2 effective start date for version',
  checksum STRING COMMENT 'Checksum for the record',
  data_source STRING NOT NULL COMMENT 'Code for source system',
  insert_dt TIMESTAMP COMMENT 'Record inserted time',
  update_dt TIMESTAMP COMMENT 'Record updated time',
  process_id STRING COMMENT 'Process ID for run',
  CONSTRAINT c_d_pk PRIMARY KEY (patient_sk) RELY
)
CLUSTER BY AUTO
COMMENT 'Patient dimension'
TBLPROPERTIES (
  delta.deletedFileRetentionDuration = 'interval 30 days'
);

-- COMMAND ----------

-- FK to integration table
SET VARIABLE sqlstr = 'ALTER TABLE patient_dim ADD CONSTRAINT 
  c_d_int_source_fk FOREIGN KEY (patient_src_id, data_source) REFERENCES ' || full_schema_name || '.' || 'patient_int(patient_src_id, data_source) NOT ENFORCED RELY';

EXECUTE IMMEDIATE sqlstr;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### What's next after you finish this notebook?
-- MAGIC
-- MAGIC This notebook highlights the design of dimensional entities in your Data Warehouse while following all best practices.
-- MAGIC
-- MAGIC You can carry this forward to all your Dimension and Fact tables.
-- MAGIC
-- MAGIC See Also: [Star Schema Data Modeling Best Practices on Databricks SQL](https://medium.com/dbsql-sme-engineering/star-schema-data-modeling-best-practices-on-databricks-sql-8fe4bd0f6902)
