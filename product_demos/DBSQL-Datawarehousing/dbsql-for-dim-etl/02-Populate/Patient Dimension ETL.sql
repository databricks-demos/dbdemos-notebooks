-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC # Patient Dimension ETL
-- MAGIC This notebook contains the code to load the patient dimension which is part of the clinical star schema.<br>
-- MAGIC The same pattern can be used to load any of your business dimensions.<br>
-- MAGIC
-- MAGIC **<u>Initial and Incremental load of Patient dimension</u>**<br>
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
-- MAGIC # Configuration Settings
-- MAGIC Set the catalog and schema where the tables will be created.

-- COMMAND ----------

-- MAGIC %run "../00-Setup/Initialize"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Set variables
-- MAGIC Variables used in the queries.

-- COMMAND ----------

declare or replace variable br_table string; -- staging table identifier
declare or replace variable si_table string; -- integration table
declare or replace variable gd_table string; -- dimension table

-- COMMAND ----------

set variable (br_table, si_table, gd_table) = (select catalog_nm || '.' || schema_nm || '.' || 'patient_stg', catalog_nm || '.' || schema_nm || '.' || 'patient_int', catalog_nm || '.' || schema_nm || '.' || 'g_patient_d');

-- COMMAND ----------

declare or replace variable data_source string default 'ABC Systems'; -- source system code
declare or replace variable process_id string; -- a process id to associate with the load, for e.g., session id, run id

-- COMMAND ----------

declare or replace variable sqlstr string; -- variable to hold any sql statement for EXECUTE IMMEDIATE

-- COMMAND ----------

-- for logging the run
declare or replace variable load_table string;
declare or replace variable load_start_time timestamp;
declare or replace variable load_end_time timestamp;

-- COMMAND ----------

-- pass Workflows {{job.id}}-{{job.run_id}} to notebook parameter
-- to set process_id
-- Optional
set variable process_id = :p_process_id;

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC **Set variables for incremental load to Integration and Dimension tables.**<br>
-- MAGIC
-- MAGIC
-- MAGIC <u>Note:</u> <br>
-- MAGIC The code guards against runtime failures.  If the load end time failed to update in the config/log table, the next time round, the target table is checked to detrmine the last load time.<br>
-- MAGIC
-- MAGIC To set the last load date-<br>
-- MAGIC 1. Query log table
-- MAGIC 2. If no value found, query actual table to get the largest Update Date value
-- MAGIC 3. If it is the initial load, proceed with default value
-- MAGIC

-- COMMAND ----------

declare or replace variable si_last_load_date timestamp default '1990-01-01';
declare or replace variable gd_last_load_date timestamp default '1990-01-01';

-- COMMAND ----------

-- to get table_changes since integration table last loaded
set variable si_last_load_date = coalesce((select max(load_end_time) from identifier(session.run_log_table) where data_source = session.data_source and table_name = session.si_table), (select max(update_dt) from identifier(session.si_table)), session.si_last_load_date);

-- to get table_changes since dimension table last loaded
set variable gd_last_load_date = coalesce((select max(load_end_time) from identifier(session.run_log_table) where data_source = session.data_source and table_name = session.gd_table), (select max(update_dt) from identifier(session.gd_table)), session.gd_last_load_date);


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

-- MAGIC %md
-- MAGIC ## Log load start
-- MAGIC Update load start time for staging table in the log table.<br>

-- COMMAND ----------

set variable (load_table, load_start_time, load_end_time) = (select session.br_table, current_timestamp(), null);

-- COMMAND ----------

-- MAGIC %run "../04-Utility/Log Run"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## COPY INTO staging table

-- COMMAND ----------

-- staging path is path to "staging" volume
declare or replace variable file_stage string = session.staging_path || "/patient";

-- COMMAND ----------

set variable sqlstr = "
copy into " || session.br_table || "
from (
  select
    *,
    session.data_source as data_source,
    _metadata.file_name as file_name,
    current_timestamp() as insert_dt,
    session.process_id as process_id
  from '" || session.file_stage || "'
)
fileformat = CSV
format_options ('header' = 'true', 'inferSchema' = 'true', 'mergeSchema' = 'true')
copy_options ('mergeSchema' = 'true')
;
"
;

-- load staging table
execute immediate sqlstr;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Log load end
-- MAGIC Update load end time in the log table.

-- COMMAND ----------

set variable load_end_time = current_timestamp();

-- COMMAND ----------

-- MAGIC %run "../04-Utility/Log Run"

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
-- MAGIC - _Older version_

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Log load start
-- MAGIC
-- MAGIC Update load start time for integration table in the log table.

-- COMMAND ----------

set variable (load_table, load_start_time, load_end_time) = (select session.si_table, current_timestamp(), null);

-- COMMAND ----------

-- MAGIC %run "../04-Utility/Log Run"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create transformation view
-- MAGIC The temporary view curates the data as follows:<br>
-- MAGIC - Transforms columns
-- MAGIC - Standardizes code description for gender and ethnicity
-- MAGIC - Omits exception records
-- MAGIC
-- MAGIC The integration table is being treated as insert only.<br>

-- COMMAND ----------

-- transform ingested source data
create or replace temporary view si_transform_tv
as
with vars as (select session.si_table, session.br_table, session.si_last_load_date, session.code_table), -- required for identity(si_table) to work
br_cdc as (
  select * from identifier(session.br_table) br
  where br.insert_dt > session.si_last_load_date
)
select
  id as patient_src_id,
  birthdate as date_of_birth,
  ssn as ssn,
  drivers as drivers_license,
  initcap(prefix) as name_prefix,
  first as first_name,
  last as last_name,
  suffix as name_suffix,
  maiden as maiden_name,
  gender as gender_cd,
  ifnull(code_gr.m_desc, gender) as gender_nm,
  marital as marital_status,
  ethnicity as ethnicity_cd,
  ifnull(code_ethn.m_desc, ethnicity) as ethnicity_nm,
  CHANGEDONDATE as src_changed_on_dt,
  data_source,
  current_timestamp() as insert_dt,
  current_timestamp() as update_dt,
  session.process_id as process_id
from br_cdc
left outer join identifier(session.code_table) code_gr on code_gr.m_code = br_cdc.gender and code_gr.m_type = 'GENDER'
left outer join identifier(session.code_table) code_ethn on code_ethn.m_code = br_cdc.ethnicity and code_ethn.m_type = 'ETHNICITY'
where
  -- no error records
  id is not null and CHANGEDONDATE is not null -- these 2 conditions could be part of exception handling
;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Insert data
-- MAGIC Insert data into the integration table using transformation view.
-- MAGIC
-- MAGIC Note: The design is to retain all versions of data, hence Insert.  Else Merge.

-- COMMAND ----------

-- Insert new and changed data
insert into identifier(si_table)
select * from si_transform_tv
;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Log load end
-- MAGIC Update load end time in the log table.

-- COMMAND ----------

set variable load_end_time = current_timestamp();

-- COMMAND ----------

-- MAGIC %run "../04-Utility/Log Run"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Populate dimension table
-- MAGIC The dimension table g_patient_d is created as a SCD2 dimension.<br>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Log load start
-- MAGIC
-- MAGIC Update load start time for dimension table in the log table.

-- COMMAND ----------

set variable (load_table, load_start_time, load_end_time) = (select session.gd_table, current_timestamp(), null);

-- COMMAND ----------

-- MAGIC %run "../04-Utility/Log Run"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create transformation view
-- MAGIC The view performs multiple functions:<br>
-- MAGIC - Transform incoming rows as required
-- MAGIC - Create checksum
-- MAGIC - Handle new and changed instances
-- MAGIC - Handle multiple changes in a single batch
-- MAGIC - Ignore consecutive versions if no changes to business attributes of interest
-- MAGIC
-- MAGIC Note: The effective start date for a version is based on the CHANGEDONDATE as recieved from the source. This needs to be taken into consideration when doing a lookup of the patient table.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC The view includes the following elements:
-- MAGIC 1. **CTE si_tc** <br>
-- MAGIC   This is used to transform any columns from the integration table
-- MAGIC 2. **CTE curr_v** <br>
-- MAGIC   Identify the Current Versions of all Patient instances corresponding to incoming data in this run.
-- MAGIC 3. **CTE ins_upd_rows**
-- MAGIC   This CTE isolates new patients and new versions of existing patients for insert. This also identifies existing versions which need to be updated (potentially), to include an effective_end_date.
-- MAGIC Finally there is- <br>
-- MAGIC 4. CTE no_dup_ver <br>
-- MAGIC   This is used to eliminate any updated records from the source for which there are no changes to business attributes of interest. For e.g., drivers_license is not being populated in the dimension.  Suppose the only update in the source is to drivers_license, it will simply turn out to be a duplicate record.

-- COMMAND ----------

create or replace temporary view dim_transform_tv
as
with vars as (select session.si_table, session.gd_table, session.gd_last_load_date), -- select the variables for use in later clauses
si_tc as (
  select
    last_name, first_name, name_prefix, name_suffix, maiden_name,
    gender_cd as gender_code, gender_nm as gender,
    date_of_birth, nvl(marital_status, 'Not Available') as marital_status,
    ethnicity_cd as ethnicity_code, ethnicity_nm as ethnicity,
    ssn, null as other_identifiers, null as uda,
    patient_src_id, src_changed_on_dt as effective_start_date,
    md5(ifnull(last_name, '#') || ifnull(first_name, '#') || ifnull(name_prefix, '#') || ifnull(name_suffix, '#') || ifnull(maiden_name, '#') ||
        ifnull(gender_cd, '#') || ifnull(gender_nm, '#') || ifnull(date_of_birth, '#') || ifnull(marital_status, '#') || ifnull(ethnicity_cd, '#') || ifnull(ethnicity_nm, '#') || ifnull(ssn, '#')) as checksum,
    data_source
  from identifier(session.si_table) si
  where si.update_dt > session.gd_last_load_date -- CDC
),
curr_v as (
  -- GET current version records in dimension table, if any, corresponding to incoming data
  select gd.* except (effective_end_date, insert_dt, update_dt, process_id)
  from identifier(session.gd_table) gd
  where effective_end_date is null and
    exists (select 1 from si_tc where si_tc.patient_src_id = gd.patient_src_id AND si_tc.data_source = gd.data_source)
),
ins_upd_rows as (
  -- ISOLATE new patients and new versions
  select null as patient_sk, * from si_tc
  union all
  -- use this to update effective_end_date of existing version in gd
  select * from curr_v
),
no_dup_ver as (
  -- IGNORE consecutive versions if no changes to business attributes of interest
  select
    *,
    lag(checksum, 1, null) over (partition by patient_src_id, data_source order by effective_start_date asc) as checksum_next
  from ins_upd_rows
  qualify checksum <> ifnull(checksum_next, '#') -- initially no records (for checksum_next)
)
-- FINAL set (new patients and new versions, existing versions for updating effective_end_date)
select
  *,
  lead(effective_start_date, 1, null) over (partition by patient_src_id order by effective_start_date asc) as effective_end_date
from no_dup_ver
;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Merge data
-- MAGIC Update the dimension table by:<br>
-- MAGIC - Merge new and changed records.<br>
-- MAGIC - Version existing patient records (by updating effective_end_date).<br>

-- COMMAND ----------

merge into identifier(session.gd_table) d
using dim_transform_tv tr
on d.patient_sk = tr.patient_sk
when matched then update
  -- update end date for existing version of patient
  set d.effective_end_date = tr.effective_end_date,
    update_dt = current_timestamp(),
    process_id = session.process_id
when not matched then insert (
  -- insert new vesrions and new patients
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
  values (last_name, first_name, name_prefix, name_suffix, maiden_name, gender_code, gender, date_of_birth, marital_status, ethnicity_code,
    ethnicity, ssn, patient_src_id, effective_start_date, effective_end_date, checksum, data_source, current_timestamp(), current_timestamp(), session.process_id)
;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Log load end
-- MAGIC Update load end time in the log table.

-- COMMAND ----------

set variable load_end_time = current_timestamp();

-- COMMAND ----------

-- MAGIC %run "../04-Utility/Log Run"