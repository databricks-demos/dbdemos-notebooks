-- Databricks notebook source
-- MAGIC %run "../00-Setup/Initialize"

-- COMMAND ----------

declare or replace variable br_table string; -- staging/bronze table identifier
declare or replace variable si_table string; -- integration/silver table identifier
declare or replace variable gd_table string; -- dimension table identifier

-- COMMAND ----------

declare or replace variable sqlstr string;

-- COMMAND ----------

set variable (br_table, si_table, gd_table) = (select catalog_nm || '.' || schema_nm || '.' || 'patient_stg', catalog_nm || '.' || schema_nm || '.' || 'patient_int', catalog_nm || '.' || schema_nm || '.' || 'g_patient_d');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC # Create Tables
-- MAGIC Create the staging, integration, and dimension tables for patient.<br>
-- MAGIC The patient dimension is part of the clinical data warehouse (star schema).
-- MAGIC
-- MAGIC <u>NOTE:</u> By default, the tables are created in the **catalog dbsqldemos**.  To change this, or specify an existing catalog / schema, please see [Configure notebook]($../00-Setup/Configure) for more context.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create Staging Table
-- MAGIC The schema for the staging table will be derived from the source data file(s)

-- COMMAND ----------

drop table if exists identifier(br_table);

-- COMMAND ----------

create table if not exists identifier(br_table)
comment 'Patient staging table ingesting initial and incremental master data from csv files'
;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create Integration Table

-- COMMAND ----------

drop table if exists identifier(si_table);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Potential clustering columns - (data_source, patient_src_id) <br>
-- MAGIC Also, column src_changed_on_dt will be naturally ordered (ingestion-time clustering) AND data_source will typically be the same for all records in a source file.
-- MAGIC
-- MAGIC **Note:** Take advantage of Predictive Optimization (to maintain) and Auto clustering (to automatically cluster your tables based on your evolving workload)!
-- MAGIC
-- MAGIC
-- MAGIC

-- COMMAND ----------

create table if not exists identifier(si_table) (
  patient_src_id string not null comment 'ID of the record in the source',
  date_of_birth date comment 'date of birth',
  ssn string comment 'social security number',
  drivers_license string comment 'driver\'s license',
  name_prefix string comment 'name prefix',
  first_name string comment 'first name of patient',
  last_name string not null comment 'last name of patient',
  name_suffix string comment 'name suffix',
  maiden_name string comment 'maiden name',
  gender_cd string comment 'code for patient\'s gender',
  gender_nm string comment 'description of patient\'s gender',
  marital_status string comment 'marital status',
  ethnicity_cd string comment 'code for patient\'s ethnicity',
  ethnicity_nm string comment 'description of patient\'s ethnicity',
  src_changed_on_dt timestamp comment 'date of last change to record in source',
  data_source string not null comment 'code for source system',
  insert_dt timestamp comment 'date record inserted',
  update_dt timestamp comment 'date record updated',
  process_id string comment 'Process ID for run',
  constraint c_int_pk primary key (patient_src_id, data_source) RELY
)
comment 'curated integration table for patient data'
tblproperties (delta.enableChangeDataFeed = true)
;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create Dimension

-- COMMAND ----------

drop table if exists identifier(gd_table);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Potential clustering columns - attributes used for filtering in end-user queries.  For e.g., Last Name, Gender Code.
-- MAGIC
-- MAGIC Additionally, for large dimensions, using the Source ID (patient_src_id) as a cluster key may help with ETL performance.
-- MAGIC
-- MAGIC **Note:** For the dimension table, take advantage of Predictive Optimization (to maintain) and Auto clustering (to automatically cluster your tables based on your evolving workload)!

-- COMMAND ----------

create table if not exists identifier(gd_table) (
  patient_sk bigint generated always as identity comment 'Primary Key (ID)',
  last_name string NOT NULL  comment 'Last name of the person',
  first_name string NOT NULL  comment 'First name of the person',
  name_prefix string  comment 'Prefix of person name',
  name_suffix string  comment 'Suffix of person name',
  maiden_name string comment 'Maiden name',
  gender_code string  comment 'Gender code',
  gender string  comment 'gender description',
  date_of_birth timestamp  comment 'Birth date and time',
  marital_status string  comment 'Marital status',
  ethnicity_code string,
  ethnicity string,
  ssn string  comment 'Patient SSN',
  other_identifiers map <string, string>  comment 'Identifier type (passport number, license number except mrn, ssn) and value',
  uda map <string, string>  comment 'User Defined Attributes',
  patient_src_id string  comment 'Unique reference to the source record',
  effective_start_date timestamp  comment 'SCD2 effective start date for version',
  effective_end_date timestamp  comment 'SCD2 effective start date for version',
  checksum string  comment 'Checksum for the record',
  data_source string  comment 'Code for source system',
  insert_dt timestamp comment 'record inserted time',
  update_dt timestamp comment 'record updated time',
  process_id string  comment 'Process ID for run',
  constraint c_d_pk primary key (patient_sk) RELY
)
cluster by auto
comment 'Patient dimension'
tblproperties (
  delta.deletedFileRetentionDuration = 'interval 30 days'
)
;


-- COMMAND ----------

-- FK to integration table
set variable sqlstr = 'alter table ' || gd_table || ' add constraint c_d_int_source_fk foreign key (patient_src_id, data_source) references ' || si_table || '(patient_src_id, data_source) not enforced rely';
execute immediate sqlstr;