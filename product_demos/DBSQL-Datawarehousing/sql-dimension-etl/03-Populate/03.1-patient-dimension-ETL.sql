-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC # Patient Dimension ETL
-- MAGIC This notebook contains the code to load the patient dimension which is part of the clinical star schema.<br>
-- MAGIC The same pattern can be used to load any of your business dimensions.<br>
-- MAGIC
-- MAGIC The notebook performs the following tasks:<br>
-- MAGIC -> Load staging table<br>
-- MAGIC -> Curate and load integration table<br>
-- MAGIC -> Transform and load dimension table using SCD2
-- MAGIC
-- MAGIC The staging table is loaded from files extracted to cloud storage. 
-- MAGIC  These files contain incremental data extracts.
-- MAGIC  Zero, one, or more new files loaded during each run.<br>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **The code incorporates the following design elements:**
-- MAGIC - Incremental load
-- MAGIC - Versioning of data (SCD Type 2)
-- MAGIC - Checksum​
-- MAGIC - Code standardization
-- MAGIC
-- MAGIC Simply re-run Job to recover from a runtime error.
-- MAGIC <br>
-- MAGIC <br>
-- MAGIC _The code uses temporary views and single DML for each of the tables._

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Configuration and Settings

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Initialize

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Include variables such as full_schema_name, code_table, staging_path, etc.

-- COMMAND ----------

-- MAGIC %run ../01-Setup/01.1-initialize

-- COMMAND ----------

DECLARE OR REPLACE VARIABLE table_load_start_time TIMESTAMP;

-- COMMAND ----------

DECLARE OR REPLACE VARIABLE data_source STRING DEFAULT 'MedCore12 ADT'; -- Source system of record
DECLARE OR REPLACE VARIABLE process_id STRING; -- A process ID to associate with the load, for e.g., session ID, run ID

-- COMMAND ----------

DECLARE OR REPLACE VARIABLE sqlstr STRING; -- Variable to hold any SQL statement for EXECUTE IMMEDIATE

-- COMMAND ----------

-- (Optional) Set Proceess Id for observability, debugging
-- Pass Workflows {{job.id}}-{{job.run_id}} to notebook parameter
SET VARIABLE process_id = :p_process_id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Set Current Schema

-- COMMAND ----------

-- Set the current schema
USE IDENTIFIER(full_schema_name);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Set Variables for Incremental Load
-- MAGIC For Integration and Dimension tables.
-- MAGIC

-- COMMAND ----------

-- get last load timestamp from target table (metadata-only query!)
-- if table is empty, fallback to initial load

DECLARE OR REPLACE VARIABLE int_last_load_date TIMESTAMP default '1990-01-01';
SET VARIABLE int_last_load_date = COALESCE((SELECT MAX(update_dt) FROM patient_int), session.int_last_load_date);

DECLARE OR REPLACE VARIABLE dim_last_load_date TIMESTAMP default '1990-01-01';
SET VARIABLE dim_last_load_date = COALESCE((SELECT MAX(update_dt) FROM patient_dim), session.dim_last_load_date);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Load staging table
-- MAGIC **Load the incremental (cdc) source files to staging table**<br>
-- MAGIC
-- MAGIC The initial and incremental source CSV files are uploaded to a staging location.<br>
-- MAGIC
-- MAGIC The staging table is insert-only.
-- MAGIC

-- COMMAND ----------

SET VARIABLE table_load_start_time = CURRENT_TIMESTAMP();

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## COPY INTO staging table
-- MAGIC
-- MAGIC For more information, see [Easy Ingestion to Lakehouse With COPY INTO](https://www.databricks.com/blog/easy-ingestion-lakehouse-copy)
-- MAGIC
-- MAGIC Note that Streaming Tables provide advanced capabilities to load from additional sources. See [Load data using streaming tables in Databricks SQL](https://docs.databricks.com/aws/en/tables/streaming).

-- COMMAND ----------

-- Staging path is path to "staging" volume
DECLARE OR REPLACE VARIABLE staging_location STRING = session.staging_path || "/patient";

-- COMMAND ----------

SET VARIABLE sqlstr = "
  COPY INTO patient_stg
  FROM (
    SELECT
      *,
      session.data_source AS data_source,
      _metadata.file_name AS file_name,
      CURRENT_TIMESTAMP() AS insert_dt,
      session.process_id AS process_id
    FROM '" || session.staging_location || "'
  )
  FILEFORMAT = CSV
  FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true', 'mergeSchema' = 'true')
  COPY_OPTIONS ('mergeSchema' = 'true')";

-- Load staging table
EXECUTE IMMEDIATE sqlstr;

-- COMMAND ----------

-- Optionally log the ETL run for easy reporting, monitoring, and alerting
INSERT INTO IDENTIFIER(run_log_table)
WITH op_metrics AS (
  SELECT COALESCE(operationMetrics.numTargetRowsInserted, operationMetrics.numOutputRows) AS num_inserted, operationMetrics.numTargetRowsUpdated AS num_updated FROM (DESCRIBE HISTORY patient_stg) WHERE operation IN ('MERGE', 'WRITE', 'COPY INTO') LIMIT 1
)
SELECT session.data_source, session.full_schema_name || '.' || 'patient_stg', table_load_start_time, CURRENT_TIMESTAMP(), num_inserted, num_updated, session.process_id FROM op_metrics;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Populate integration table
-- MAGIC Validate, curate, and load incremental data into the integration table.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## _Placeholder_
-- MAGIC
-- MAGIC _Validate incoming data
-- MAGIC Handle errors in the newly inserted data, before populating the curated data in the integration table._<br>
-- MAGIC _Exception records (refs) can be captured in common table error table elt_error_table._<br>
-- MAGIC <br>
-- MAGIC
-- MAGIC _Steps would involve:_
-- MAGIC - _Checking business rules / mandatory data, and quarantining records_
-- MAGIC
-- MAGIC _For e.g.,_
-- MAGIC - _ID is null_
-- MAGIC - _CHANGEDONDATE is null_
-- MAGIC - _LAST (name) is null_
-- MAGIC - _Older version in source_

-- COMMAND ----------

SET VARIABLE table_load_start_time = CURRENT_TIMESTAMP();

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Transform incoming rows from staging table
-- MAGIC The temporary view curates the data as follows:<br>
-- MAGIC - Transforms columns
-- MAGIC - Standardizes code description for gender and ethnicity
-- MAGIC
-- MAGIC The integration table is being treated as insert only.<br>

-- COMMAND ----------

-- Transform ingested source data
CREATE OR REPLACE TEMPORARY VIEW stg_transform_temp_v
AS
WITH vars AS (SELECT session.int_last_load_date, session.code_table), -- SELECT the variables for use in later clauses
patient_stg_cdc AS (
  SELECT * FROM patient_stg stg
  WHERE stg.insert_dt > session.int_last_load_date
)
SELECT
  `Id` AS patient_src_id,
  birthdate AS date_of_birth,
  ssn AS ssn,
  drivers AS drivers_license,
  INITCAP(prefix) AS name_prefix,
  `FIRST` AS first_name,
  `LAST` AS last_name,
  suffix AS name_suffix,
  maiden AS maiden_name,
  gender AS gender_cd,
  IFNULL(code_gender.m_desc, gender) AS gender_nm,
  marital AS marital_status,
  ethnicity AS ethnicity_cd,
  IFNULL(code_ethn.m_desc, ethnicity) AS ethnicity_nm,
  CHANGEDONDATE AS src_changed_on_dt,
  data_source,
  CURRENT_TIMESTAMP() AS insert_dt,
  CURRENT_TIMESTAMP() AS update_dt,
  session.process_id AS process_id
FROM patient_stg_cdc
LEFT OUTER JOIN IDENTIFIER(session.code_table) code_gender ON code_gender.m_code = patient_stg_cdc.gender AND code_gender.m_type = 'GENDER'
LEFT OUTER JOIN IDENTIFIER(session.code_table) code_ethn ON code_ethn.m_code = patient_stg_cdc.ethnicity AND code_ethn.m_type = 'ETHNICITY'
WHERE
  -- No error records
  `Id` IS NOT NULL AND CHANGEDONDATE IS NOT NULL AND `LAST` IS NOT NULL -- These conditions could be part of exception handling
;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Insert data
-- MAGIC Insert data into the integration table using transformation view.
-- MAGIC
-- MAGIC Note: The design is to retain all versions of data, hence Insert.  Else Merge.

-- COMMAND ----------

-- Insert new and changed data
INSERT INTO patient_int
SELECT * FROM stg_transform_temp_v
;

-- COMMAND ----------

-- Optionally log the ETL run for easy reporting, monitoring, and alerting
INSERT INTO IDENTIFIER(run_log_table)
WITH op_metrics AS (
  SELECT COALESCE(operationMetrics.numTargetRowsInserted, operationMetrics.numOutputRows) AS num_inserted, operationMetrics.numTargetRowsUpdated AS num_updated FROM (DESCRIBE HISTORY patient_int) WHERE operation IN ('MERGE', 'WRITE', 'COPY INTO') LIMIT 1
)
SELECT session.data_source, session.full_schema_name || '.' || 'patient_int', table_load_start_time, CURRENT_TIMESTAMP(), num_inserted, num_updated, session.process_id FROM op_metrics;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Populate dimension table
-- MAGIC The dimension table patient_dim is created as a SCD2 dimension.<br>

-- COMMAND ----------

SET VARIABLE table_load_start_time = CURRENT_TIMESTAMP();

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Transform incoming rows from integration table
-- MAGIC This view is used to **a)** transform incoming rows as required **b)** create checksum
-- MAGIC
-- MAGIC This view constitutes the new patients and new versions that are part of this batch.

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW int_transform_temp_v
AS
WITH vars AS (SELECT session.dim_last_load_date) -- select the variables for use in later clauses
SELECT
  last_name,
  first_name,
  name_prefix,
  name_suffix,
  maiden_name,
  gender_cd AS gender_code, gender_nm AS gender,
  date_of_birth,
  nvl(marital_status, 'Not Available') AS marital_status,
  ethnicity_cd AS ethnicity_code, ethnicity_nm AS ethnicity,
  ssn,
  NULL AS other_identifiers,
  NULL AS uda,
  patient_src_id,
  src_changed_on_dt AS effective_start_date,
  hash(
    last_name, ifnull(first_name, '#'), ifnull(name_prefix, '#'), ifnull(name_suffix, '#'), ifnull(maiden_name, '#'), ifnull(gender_cd, '#'), ifnull(gender_nm, '#'), ifnull(date_of_birth, '#'), ifnull(marital_status, '#'), ifnull(ethnicity_cd, '#'), ifnull(ethnicity_nm, '#'), ifnull(ssn, '#')
  ) AS checksum,
  data_source
FROM patient_int
WHERE patient_int.update_dt > session.dim_last_load_date
;

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## Create view for merge
-- MAGIC This view builds on the transformations and is used to **a)** handle new and changed instances **b)** handle multiple changes in a single batch **c)** ignore consecutive versions if no changes to business attributes of interest
-- MAGIC
-- MAGIC <u>The view includes the following elements:</u>
-- MAGIC
-- MAGIC 1. CTE curr_version <br>
-- MAGIC   Identify the Current Versions of all Patient instances corresponding to incoming data in this run.
-- MAGIC 3. CTE rows_for_merge
-- MAGIC   This CTE isolates new patients and new versions of existing patients for insert. This also identifies existing versions which need to be updated (potentially), to include an effective_end_date.
-- MAGIC Finally there is- <br>
-- MAGIC 4. CTE no_dup_ver <br>
-- MAGIC   This is used to eliminate any updated records from the source for which there are no changes to business attributes of interest. For e.g., drivers_license is not being populated in the dimension.  Suppose the only update in the source is to drivers_license, it will simply turn out to be a duplicate record.

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW dim_merge_temp_v
AS
WITH curr_version AS (
  -- get current version records from dimension table, if any, corresponding to incoming data
  SELECT * EXCEPT (effective_end_date, insert_dt, update_dt, process_id)
  FROM patient_dim
  WHERE effective_end_date IS NULL AND
    EXISTS (SELECT 1 FROM int_transform_temp_v int_transform
            WHERE int_transform.patient_src_id = patient_dim.patient_src_id AND int_transform.data_source = patient_dim.data_source)
),
rows_for_merge AS (
  SELECT NULL AS patient_sk, * FROM int_transform_temp_v -- new patients and new versions to insert
  UNION ALL
  SELECT * FROM curr_version -- current versions for update (effective_end_date)
),
no_dup_ver AS (
  -- ignore consecutive versions if no changes to any business attributes of interest
  SELECT
    *,
    LAG(checksum, 1, NULL) OVER (PARTITION BY patient_src_id, data_source ORDER BY effective_start_date ASC) AS checksum_next
  FROM rows_for_merge
  QUALIFY checksum <> IFNULL(checksum_next, '#')
)
-- final set of records to be merged
SELECT
  *,
  LEAD(effective_start_date, 1, NULL) OVER (PARTITION BY patient_src_id ORDER BY effective_start_date ASC) AS effective_end_date
FROM no_dup_ver
;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Merge data
-- MAGIC Update the dimension table by:<br>
-- MAGIC - Merge new and changed records.<br>
-- MAGIC - Version existing patient records (by updating effective_end_date).<br>
-- MAGIC
-- MAGIC _Note: The Effective Start Date for a version is based on the CHANGEDONDATE as recieved from the source._

-- COMMAND ----------

MERGE INTO patient_dim d
USING dim_merge_temp_v tr
ON d.patient_sk = tr.patient_sk
WHEN MATCHED THEN UPDATE
  -- UPDATE end date FOR existing version OF patient
  SET d.effective_end_date = tr.effective_end_date,
    update_dt = CURRENT_TIMESTAMP(),
    process_id = session.process_id
WHEN NOT MATCHED THEN INSERT (
  -- INSERT new version, new patient
  last_name,
  first_name,
  name_prefix,
  name_suffix,
  maiden_name,
  gender_code,
  gender,
  date_of_birth,
  marital_status,
  ethnicity_code,
  ethnicity,
  ssn,
  patient_src_id,
  effective_start_date,
  effective_end_date,
  checksum,
  data_source,
  insert_dt,
  update_dt,
  process_id)
  VALUES (last_name, first_name, name_prefix, name_suffix, maiden_name, gender_code, gender, date_of_birth, marital_status, ethnicity_code,
    ethnicity, ssn, patient_src_id, effective_start_date, effective_end_date, checksum, data_source, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), session.process_id)
;

-- COMMAND ----------

-- Optionally log the ETL run for easy reporting, monitoring, and alerting
INSERT INTO IDENTIFIER(run_log_table)
WITH op_metrics AS (
  SELECT COALESCE(operationMetrics.numTargetRowsInserted, operationMetrics.numOutputRows) AS num_inserted, operationMetrics.numTargetRowsUpdated AS num_updated FROM (DESCRIBE HISTORY patient_dim) WHERE operation IN ('MERGE', 'WRITE', 'COPY INTO') LIMIT 1
)
SELECT session.data_source, session.full_schema_name || '.' || 'patient_dim', table_load_start_time, CURRENT_TIMESTAMP(), num_inserted, num_updated, session.process_id FROM op_metrics;
