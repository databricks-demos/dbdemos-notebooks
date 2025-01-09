-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC # Ingesting and preparing data to build the OMOP 5.3.1 Lakehouse
-- MAGIC
-- MAGIC
-- MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/hls/patient-readmission/hls-patient-readmision-flow-1.png" style="float: left; margin-right: 30px; margin-top:10px" width="650px" />
-- MAGIC
-- MAGIC Our first step in reducing patient readmission risk is to ingest and prepare data from multiple external sources as simple tables our downstream Analysts can leverage. To do so, we will leverage the OMOP data model.
-- MAGIC
-- MAGIC The OMOP database is a Common Data Model to harmonize data analysis required for medical product safety surveillance, comparative effectiveness, quality of care, and patient-level predictive modeling.
-- MAGIC
-- MAGIC Databricks is uniquely positioned to create a leverage this model: not only it provides capabilities to build, ingest and transform the data, but it also let your Data Analyst and Data Scientist team to run advanced analysis on top of it.
-- MAGIC
-- MAGIC <br style="clear: both">
-- MAGIC
-- MAGIC ## Implementing the OMOP model
-- MAGIC
-- MAGIC <img src="https://ohdsi.github.io/TheBookOfOhdsi/images/CommonDataModel/cdmDiagram.png" width="400px" style="float: right; margin-left: 50px" >
-- MAGIC
-- MAGIC The OMOP database contains several tables.
-- MAGIC
-- MAGIC - Clinical Data Tables (CDT)
-- MAGIC - Health System Data Tables (HSDT)
-- MAGIC - Health Economics Data Tables (HEDT)
-- MAGIC - Standardized Derived Elements (SDE)
-- MAGIC - Metadata Tables (MT)
-- MAGIC - Vocabulary Tables (VT)
-- MAGIC
-- MAGIC In this demo, we will consume raw data artificially generated from Synthea and apply transformations to translate them as OMOP schema.
-- MAGIC
-- MAGIC *To keep this demo simple, we will only implement a subste of the OMOP data model*
-- MAGIC
-- MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
-- MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=lakehouse&notebook=01.1-DLT-patient-readmission-SQL&demo_name=lakehouse-patient-readmission&event=VIEW">

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## Delta Live Table: A simple way to build and manage data pipelines for fresh, high quality data!
-- MAGIC
-- MAGIC
-- MAGIC In this notebook, we'll work as a Data Engineer to build our OMOP CDM database. <br>
-- MAGIC We'll consume and clean our raw data sources to prepare the tables required for our BI & ML workload.
-- MAGIC
-- MAGIC The lakehouse makes it easy to ingest from any external or internal HLS datasources, such as HL7 FHIR, EHR, ioMT, SQL databases). For our demo, our data will be files received in a cloud blob storage in different format. We will then apply a couple of transformations while ensuring data quality.
-- MAGIC
-- MAGIC Databricks simplifies this task with Delta Live Table (DLT) by making Data Engineering accessible to all.
-- MAGIC
-- MAGIC DLT allows Data Analysts to create advanced pipeline with plain SQL.
-- MAGIC
-- MAGIC <div>
-- MAGIC   <div style="width: 45%; float: left; margin-bottom: 10px; padding-right: 45px">
-- MAGIC     <p>
-- MAGIC       <img style="width: 50px; float: left; margin: 0px 5px 30px 0px;" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/logo-accelerate.png"/> 
-- MAGIC       <strong>Accelerate ETL development</strong> <br/>
-- MAGIC       Enable analysts and data engineers to innovate rapidly with simple pipeline development and maintenance 
-- MAGIC     </p>
-- MAGIC     <p>
-- MAGIC       <img style="width: 50px; float: left; margin: 0px 5px 30px 0px;" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/logo-complexity.png"/> 
-- MAGIC       <strong>Remove operational complexity</strong> <br/>
-- MAGIC       By automating complex administrative tasks and gaining broader visibility into pipeline operations
-- MAGIC     </p>
-- MAGIC   </div>
-- MAGIC   <div style="width: 48%; float: left">
-- MAGIC     <p>
-- MAGIC       <img style="width: 50px; float: left; margin: 0px 5px 30px 0px;" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/logo-trust.png"/> 
-- MAGIC       <strong>Trust your data</strong> <br/>
-- MAGIC       With built-in quality controls and quality monitoring to ensure accurate and useful BI, Data Science, and ML 
-- MAGIC     </p>
-- MAGIC     <p>
-- MAGIC       <img style="width: 50px; float: left; margin: 0px 5px 30px 0px;" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/logo-stream.png"/> 
-- MAGIC       <strong>Simplify batch and streaming</strong> <br/>
-- MAGIC       With self-optimization and auto-scaling data pipelines for batch or streaming processing 
-- MAGIC     </p>
-- MAGIC </div>
-- MAGIC </div>
-- MAGIC
-- MAGIC <br style="clear:both">
-- MAGIC
-- MAGIC <img src="https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-logo.png" style="float: right;" width="200px">
-- MAGIC
-- MAGIC ## Delta Lake
-- MAGIC
-- MAGIC All the tables we'll create in the Lakehouse will be stored as Delta Lake tables. Delta Lake is an open storage framework for reliability and performance.<br>
-- MAGIC It provides many functionalities (ACID Transaction, DELETE/UPDATE/MERGE, Clone zero copy, Change data Capture...)<br>
-- MAGIC For more details on Delta Lake, run dbdemos.install('delta-lake')

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC
-- MAGIC ## Building a Delta Live Table pipeline to ingest and prepare HLS data with the OMOP model
-- MAGIC
-- MAGIC In this example, we'll implement an end-to-end DLT pipeline consuming the aforementioned information. We'll use the medaillon architecture but we could build star schema, data vault, or any other modelisation.
-- MAGIC
-- MAGIC We'll incrementally load new data with the autoloader, clean and enrich this information.
-- MAGIC
-- MAGIC This information will then be used to build our cohorts and build SQL dashboard to analyze our patients.
-- MAGIC
-- MAGIC Let's implement the following flow: 
-- MAGIC
-- MAGIC  <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/hls/patient-readmission/hls-patient-readmision-dlt-0.png" width="1000px" />

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC
-- MAGIC
-- MAGIC The DLT pipeline was started for you! Click to <a dbdemos-pipeline-id="dlt-patient-readmission" href="#joblist/pipelines/ff2fd2cb-733b-4166-85ed-a34b84129a35" target="_blank">open your Pipeline</a> and review its execution.

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ### 1/ Loading our data using Databricks Autoloader (cloud_files)
-- MAGIC
-- MAGIC <img width="650px" style="float:right" src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/hls/patient-readmission/hls-patient-readmision-dlt-1.png"/>
-- MAGIC
-- MAGIC   
-- MAGIC Our raw files are available in our landing zone as CSV. We need to ingest them at scale, while handling schema inference and evolution.
-- MAGIC Databricks Autoloader makes this very easy.
-- MAGIC
-- MAGIC For more details on autoloader, run `dbdemos.install('auto-loader')`

-- COMMAND ----------

-- Databricks Auto Loader cloud_files will incrementally load new files, infering the column types and handling schema evolution for us.
-- data could be from any source: csv, json, parquet...
CREATE OR REFRESH STREAMING TABLE encounters
  AS SELECT * EXCEPT(START, STOP), to_timestamp(START) as START, to_timestamp(STOP) as STOP
      FROM cloud_files("/Volumes/main__build/dbdemos_hls_readmission/synthea/landing_zone/encounters", "parquet", 
                                map("cloudFiles.inferColumnTypes", "true"));

CREATE OR REFRESH STREAMING TABLE patients
  AS SELECT * FROM cloud_files("/Volumes/main__build/dbdemos_hls_readmission/synthea/landing_zone/patients", "parquet", 
                                map("cloudFiles.inferColumnTypes", "true"));

CREATE OR REFRESH STREAMING TABLE conditions
  AS SELECT * FROM cloud_files("/Volumes/main__build/dbdemos_hls_readmission/synthea/landing_zone/conditions", "parquet", 
                                map("cloudFiles.inferColumnTypes", "true"));

CREATE OR REFRESH STREAMING TABLE medications
  AS SELECT * FROM cloud_files("/Volumes/main__build/dbdemos_hls_readmission/synthea/landing_zone/medications", "parquet", 
                                map("cloudFiles.inferColumnTypes", "true"));

CREATE OR REFRESH STREAMING TABLE immunizations
  AS SELECT * FROM cloud_files("/Volumes/main__build/dbdemos_hls_readmission/synthea/landing_zone/immunizations", "parquet", 
                                map("cloudFiles.inferColumnTypes", "true"));

CREATE OR REFRESH STREAMING TABLE concept
  AS SELECT * FROM cloud_files("/Volumes/main__build/dbdemos_hls_readmission/synthea/landing_vocab/CONCEPT", "parquet", 
                                map("cloudFiles.inferColumnTypes", "true"));

CREATE OR REFRESH STREAMING TABLE concept_relationship
  AS SELECT * FROM cloud_files("/Volumes/main__build/dbdemos_hls_readmission/synthea/landing_vocab/CONCEPT_RELATIONSHIP", "parquet", 
                                map("cloudFiles.inferColumnTypes", "true"));

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ### 2/ Enforce quality and materialize our tables for Data Analysts
-- MAGIC
-- MAGIC <img width="650px" style="float:right" src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/hls/patient-readmission/hls-patient-readmision-dlt-2.png"/>
-- MAGIC
-- MAGIC The next layer often call silver is consuming **incremental** data from the bronze one, and cleaning up some information.
-- MAGIC
-- MAGIC We're also adding an TODO [expectation](https://docs.databricks.com/workflows/delta-live-tables/delta-live-tables-expectations.html) on different field to enforce and track our Data Quality. This will ensure that our dashboard are relevant and easily spot potential errors due to data anomaly.
-- MAGIC
-- MAGIC For more advanced DLT capabilities run `dbdemos.install('dlt-loans')` or `dbdemos.install('dlt-cdc')` for CDC/SCDT2 example.
-- MAGIC
-- MAGIC These tables are clean and ready to be used by the BI team!
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### ALL_VISITS table
-- MAGIC Derivated from 3 views

-- COMMAND ----------

-- DBTITLE 1,IP_VISITS
CREATE LIVE VIEW ip_visits AS (
WITH CTE_END_DATES AS (
  SELECT
    patient,
    encounterclass,
    DATE_ADD(EVENT_DATE, -1) AS END_DATE
  FROM
    (
      SELECT
        patient,
        encounterclass,
        EVENT_DATE,
        EVENT_TYPE,
        MAX(START_ORDINAL) OVER (
          PARTITION BY patient,
          encounterclass
          ORDER BY
            EVENT_DATE,
            EVENT_TYPE ROWS UNBOUNDED PRECEDING
        ) AS START_ORDINAL,
        ROW_NUMBER() OVER (
          PARTITION BY patient,
          encounterclass
          ORDER BY
            EVENT_DATE,
            EVENT_TYPE
        ) AS OVERALL_ORD
      FROM
        (
          SELECT
            patient,
            encounterclass,
            start AS EVENT_DATE,
            -1 AS EVENT_TYPE,
            ROW_NUMBER () OVER (
              PARTITION BY patient,
              encounterclass
              ORDER BY
                start,
                stop
            ) AS START_ORDINAL
          FROM
            live.encounters
          WHERE
            encounterclass = 'inpatient'
          UNION ALL
          SELECT
            patient,
            encounterclass,
            DATE_ADD(stop, 1),
            1 AS EVENT_TYPE,
            NULL
          FROM
            live.encounters
          WHERE
            encounterclass = 'inpatient'
        ) RAWDATA
    ) E
  WHERE
    (2 * E.START_ORDINAL - E.OVERALL_ORD = 0)
),
CTE_VISIT_ENDS AS (
  SELECT
    MIN(V.id) AS encounter_id,
    V.patient,
    V.encounterclass,
    V.start AS VISIT_START_DATE,
    MIN(E.END_DATE) AS VISIT_END_DATE
  FROM
    live.encounters V
    INNER JOIN CTE_END_DATES E ON V.patient = E.patient
    AND V.encounterclass = E.encounterclass
    AND E.END_DATE >= V.start
  GROUP BY
    V.patient,
    V.encounterclass,
    V.start
)
  SELECT
    encounter_id,
    patient,
    encounterclass,
    MIN(VISIT_START_DATE) AS VISIT_START_DATE,
    VISIT_END_DATE
  FROM
    CTE_VISIT_ENDS
  GROUP BY
    encounter_id,
    patient,
    encounterclass,
    VISIT_END_DATE
);

-- COMMAND ----------

-- DBTITLE 1,ER_VISITS
CREATE LIVE VIEW ER_VISITS AS
    SELECT
      MIN(encounter_id) AS encounter_id,
      patient,
      encounterclass,
      VISIT_START_DATE,
      MAX(VISIT_END_DATE) AS VISIT_END_DATE
    FROM
      (
        SELECT
          CL1.id AS encounter_id,
          CL1.patient,
          CL1.encounterclass,
          CL1.start AS VISIT_START_DATE,
          CL2.stop AS VISIT_END_DATE
        FROM
          live.encounters CL1
          INNER JOIN live.encounters CL2 ON CL1.patient = CL2.patient
          AND CL1.start = CL2.start
          AND CL1.encounterclass = CL2.encounterclass
        WHERE
          CL1.encounterclass in ('emergency', 'urgent')
      ) T1
    GROUP BY
      patient,
      encounterclass,
      VISIT_START_DATE;

-- COMMAND ----------

-- DBTITLE 1,OP_VISITS
CREATE LIVE VIEW op_visits AS 
WITH CTE_VISITS_DISTINCT AS (
  SELECT
    MIN(id) AS encounter_id,
    patient,
    encounterclass,
    start AS VISIT_START_DATE,
    stop AS VISIT_END_DATE
  FROM
    live.encounters
  WHERE
    encounterclass in ('ambulatory', 'wellness', 'outpatient')
  GROUP BY
    patient,
    encounterclass,
    start,
    stop
)
SELECT
  MIN(encounter_id) AS encounter_id,
  patient,
  encounterclass,
  VISIT_START_DATE,
  MAX(VISIT_END_DATE) AS VISIT_END_DATE
FROM
  CTE_VISITS_DISTINCT
GROUP BY
  patient,
  encounterclass,
  VISIT_START_DATE;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### ASSIGN_ALL_VISITS_IDS 

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW assign_all_visit_ids AS
SELECT
  E.id AS encounter_id,
  E.patient as person_source_value,
  E.start AS date_service,
  E.stop AS date_service_end,
  E.encounterclass,
  AV.encounterclass AS VISIT_TYPE,
  AV.VISIT_START_DATE,
  AV.VISIT_END_DATE,
  AV.VISIT_OCCURRENCE_ID,
  CASE
    WHEN E.encounterclass = 'inpatient'
    and AV.encounterclass = 'inpatient' THEN VISIT_OCCURRENCE_ID
    WHEN E.encounterclass in ('emergency', 'urgent') THEN (
      CASE
        WHEN AV.encounterclass = 'inpatient'
        AND E.start > AV.VISIT_START_DATE THEN VISIT_OCCURRENCE_ID
        WHEN AV.encounterclass in ('emergency', 'urgent')
        AND E.start = AV.VISIT_START_DATE THEN VISIT_OCCURRENCE_ID
        ELSE NULL
      END
    )
    WHEN E.encounterclass in ('ambulatory', 'wellness', 'outpatient') THEN (
      CASE
        WHEN AV.encounterclass = 'inpatient'
        AND E.start >= AV.VISIT_START_DATE THEN VISIT_OCCURRENCE_ID
        WHEN AV.encounterclass in ('ambulatory', 'wellness', 'outpatient') THEN VISIT_OCCURRENCE_ID
        ELSE NULL
      END
    )
    ELSE NULL
  END AS VISIT_OCCURRENCE_ID_NEW
FROM
  live.encounters E
  INNER JOIN live.all_visits AV ON E.patient = AV.patient
  AND E.start >= AV.VISIT_START_DATE
  AND E.start <= AV.VISIT_END_DATE;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### ALL_VISITS

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW all_visits AS
SELECT
  *, ROW_NUMBER() OVER(ORDER BY patient) as visit_occurrence_id
FROM
  (
    SELECT * FROM live.ip_visits 
    UNION ALL
    SELECT * FROM live.er_visits
    UNION ALL
    SELECT * FROM live.op_visits
  );

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### FINAL_VISITS_IDS

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW final_visit_ids AS 
SELECT encounter_id, VISIT_OCCURRENCE_ID_NEW
FROM(
	SELECT *, ROW_NUMBER () OVER (PARTITION BY encounter_id ORDER BY PRIORITY) AS RN
	FROM (
		SELECT *,
			CASE
				WHEN encounterclass in ('emergency','urgent')
					THEN (
						CASE
							WHEN VISIT_TYPE = 'inpatient' AND VISIT_OCCURRENCE_ID_NEW IS NOT NULL
								THEN 1
							WHEN VISIT_TYPE in ('emergency','urgent') AND VISIT_OCCURRENCE_ID_NEW IS NOT NULL
								THEN 2
							ELSE 99
						END
					)
				WHEN encounterclass in ('ambulatory', 'wellness', 'outpatient')
					THEN (
						CASE
							WHEN VISIT_TYPE = 'inpatient' AND VISIT_OCCURRENCE_ID_NEW IS NOT NULL
								THEN  1
							WHEN VISIT_TYPE in ('ambulatory', 'wellness', 'outpatient') AND VISIT_OCCURRENCE_ID_NEW IS NOT NULL
								THEN 2
							ELSE 99
						END
					)
				WHEN encounterclass = 'inpatient' AND VISIT_TYPE = 'inpatient' AND VISIT_OCCURRENCE_ID_NEW IS NOT NULL
					THEN 1
				ELSE 99
			END AS PRIORITY
	FROM live.assign_all_visit_ids
	) T1
) T2
WHERE RN=1;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### SOURCE_TO_STANDARD_VOCAB_MAP

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW source_to_standard_vocab_map (
  CONSTRAINT source_concept_valid_id EXPECT (SOURCE_CONCEPT_ID IS NOT NULL) ON VIOLATION DROP ROW
  )
  AS SELECT
    c.concept_code AS SOURCE_CODE,
    c.concept_id AS SOURCE_CONCEPT_ID,
    c.concept_name AS SOURCE_CODE_DESCRIPTION,
    c.vocabulary_id AS SOURCE_VOCABULARY_ID,
    c.domain_id AS SOURCE_DOMAIN_ID,
    c.CONCEPT_CLASS_ID AS SOURCE_CONCEPT_CLASS_ID,
    c.VALID_START_DATE AS SOURCE_VALID_START_DATE,
    c.VALID_END_DATE AS SOURCE_VALID_END_DATE,
    c.INVALID_REASON AS SOURCE_INVALID_REASON,
    c1.concept_id AS TARGET_CONCEPT_ID,
    c1.concept_name AS TARGET_CONCEPT_NAME,
    c1.VOCABULARY_ID AS TARGET_VOCABULARY_ID,
    c1.domain_id AS TARGET_DOMAIN_ID,
    c1.concept_class_id AS TARGET_CONCEPT_CLASS_ID,
    c1.INVALID_REASON AS TARGET_INVALID_REASON,
    c1.standard_concept AS TARGET_STANDARD_CONCEPT
  FROM
    live.concept C
    JOIN live.concept_relationship CR ON C.CONCEPT_ID = CR.CONCEPT_ID_1
    AND CR.invalid_reason IS NULL
    AND lower(cr.relationship_id) = 'maps to'
    JOIN live.concept C1 ON CR.CONCEPT_ID_2 = C1.CONCEPT_ID
    AND C1.INVALID_REASON IS NULL

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### SOURCE_TO_SOURCE_VOCAB_MAP

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW source_to_source_vocab_map AS 
  SELECT
    c.concept_code AS SOURCE_CODE,
    c.concept_id AS SOURCE_CONCEPT_ID,
    c.CONCEPT_NAME AS SOURCE_CODE_DESCRIPTION,
    c.vocabulary_id AS SOURCE_VOCABULARY_ID,
    c.domain_id AS SOURCE_DOMAIN_ID,
    c.concept_class_id AS SOURCE_CONCEPT_CLASS_ID,
    c.VALID_START_DATE AS SOURCE_VALID_START_DATE,
    c.VALID_END_DATE AS SOURCE_VALID_END_DATE,
    c.invalid_reason AS SOURCE_INVALID_REASON,
    c.concept_ID as TARGET_CONCEPT_ID,
    c.concept_name AS TARGET_CONCEPT_NAME,
    c.vocabulary_id AS TARGET_VOCABULARY_ID,
    c.domain_id AS TARGET_DOMAIN_ID,
    c.concept_class_id AS TARGET_CONCEPT_CLASS_ID,
    c.INVALID_REASON AS TARGET_INVALID_REASON,
    c.STANDARD_CONCEPT AS TARGET_STANDARD_CONCEPT
  FROM
    live.CONCEPT c

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ### 2/ Final tables for our Data Analysis and ML model
-- MAGIC
-- MAGIC <img width="650px" style="float:right" src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/hls/patient-readmission/hls-patient-readmision-dlt-3.png"/>
-- MAGIC
-- MAGIC Finally, let's cbuild our final tables containing clean data that we'll be able to use of to build our cohorts and predict patient risks.
-- MAGIC
-- MAGIC These tables are clean and ready to be used by the BI team!
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### PERSON

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW person AS
  SELECT
  ROW_NUMBER() OVER(ORDER BY p.id) as PERSON_ID,
  case upper(p.gender) when 'M' then 8507 when 'F' then 8532 end as GENDER_CONCEPT_ID,
  YEAR(p.birthdate) as YEAR_OF_BIRTH,
  MONTH(p.birthdate) as MONTH_OF_BIRTH,
  DAY(p.birthdate) as DAY_OF_BIRTH,
  p.birthdate as BIRTH_DATETIME,
  case upper(p.race) when 'WHITE' then 8527 when 'BLACK' then 8516 when 'ASIAN' then 8515 else 0 end as RACE_CONCEPT_ID, 
  case when upper(p.race) = 'HISPANIC' then 38003563 else 0 end as ETHNICITY_CONCEPT_ID,
  1 as LOCATION_ID,
  0 as PROVIDER_ID,
  0 as CARE_SITE_ID,
  p.id as PERSON_SOURCE_VALUE,
  p.gender as GENDER_SOURCE_VALUE,
  0 as GENDER_SOURCE_CONCEPT_ID,
  p.race as RACE_SOURCE_VALUE,
  0 as RACE_SOURCE_CONCEPT_ID,
  p.ethnicity as ETHNICITY_SOURCE_VALUE,
  0 as ETHNICITY_SOURCE_CONCEPT_ID
from
  live.patients p
where
  p.gender is not null;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### CONDITION_OCCURRENCE

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW condition_occurrence AS 
select
  row_number() over(order by p.person_id) as CONDITION_OCCURRENCE_ID, 
  p.person_id as PERSON_ID,
  coalesce(srctostdvm.target_concept_id,0) AS CONDITION_CONCEPT_ID,
  c.start as CONDITION_START_DATE, 
  c.start as CONDITION_START_DATETIME,  
  c.stop as CONDITION_END_DATE, 
  c.stop as CONDITION_END_DATETIME, 
  32020 as CONDITION_TYPE_CONCEPT_ID,  
  "" as STOP_REASON,  
  0 as PROVIDER_ID,
  fv.visit_occurrence_id_new AS VISIT_OCCURRENCE_ID, 
  0 as VISIT_DETAIL_ID, 
  c.code as CONDITION_SOURCE_VALUE, 
  coalesce(srctosrcvm.source_concept_id,0) as CONDITION_SOURCE_CONCEPT_ID,  
  0 as CONDITION_STATUS_SOURCE_VALUE, 
  0 as CONDITION_STATUS_CONCEPT_ID
from live.conditions c
inner join live.source_to_standard_vocab_map srctostdvm
on srctostdvm.source_code             = c.code
 and srctostdvm.target_domain_id        = 'Condition'
 and srctostdvm.source_vocabulary_id    = 'SNOMED'
 and srctostdvm.target_standard_concept = 'S'
 and (srctostdvm.target_invalid_reason IS NULL OR srctostdvm.target_invalid_reason = '')
left join live.source_to_source_vocab_map srctosrcvm
  on srctosrcvm.source_code             = c.code
 and srctosrcvm.source_vocabulary_id    = 'SNOMED'
left join live.final_visit_ids fv
  on fv.encounter_id = c.encounter
inner join live.person p
  on c.patient = p.person_source_value

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### DRUG_EXPOSURE

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW drug_exposure AS 
  SELECT row_number() over(order by person_id) AS drug_exposure_id, 
  *
  FROM (
    SELECT
      p.person_id,
      coalesce(srctostdvm.target_concept_id, 0) AS drug_concept_id,
      c.start AS drug_exposure_start_date,
      c.start AS drug_exposure_start_datetime,
      coalesce(c.stop, c.start) AS drug_exposure_end_date,
      coalesce(c.stop, c.start) AS drug_exposure_end_datetime,
      c.stop AS verbatim_end_date,
      581452 AS drug_type_concept_id,
      '' AS stop_reason,  -- Changed from null to empty string
      0 AS refills,
      0 AS quantity,
      coalesce(datediff(c.stop, c.start), 0) AS days_supply,
      '' AS sig,  -- Changed from null to empty string
      0 AS route_concept_id,
      0 AS lot_number,
      0 AS provider_id,
      fv.visit_occurrence_id_new AS visit_occurrence_id,
      0 AS visit_detail_id,
      c.code AS drug_source_value,
      coalesce(srctosrcvm.source_concept_id, 0) AS drug_source_concept_id,
      '' AS route_source_value,  -- Changed from null to empty string
      '' AS dose_unit_source_value  -- Changed from null to empty string
    FROM live.conditions c
    JOIN live.source_to_standard_vocab_map srctostdvm
      ON srctostdvm.source_code = c.code
      AND srctostdvm.target_domain_id = 'Drug'
      AND srctostdvm.source_vocabulary_id = 'RxNorm'
      AND srctostdvm.target_standard_concept = 'S'
      AND (srctostdvm.target_invalid_reason IS NULL OR srctostdvm.target_invalid_reason = '')
    LEFT JOIN live.source_to_source_vocab_map srctosrcvm
      ON srctosrcvm.source_code = c.code
      AND srctosrcvm.source_vocabulary_id = 'RxNorm'
    LEFT JOIN live.final_visit_ids fv
      ON fv.encounter_id = c.encounter
    JOIN live.person p
      ON p.person_source_value = c.patient

    UNION ALL

    SELECT
      p.person_id,
      coalesce(srctostdvm.target_concept_id, 0) AS drug_concept_id,
      m.start, m.start, coalesce(m.stop, m.start), coalesce(m.stop, m.start), m.stop,
      38000177, '' AS stop_reason, 0, 0, coalesce(datediff(m.stop, m.start), 0),
      '' AS sig, 0, 0, 0, fv.visit_occurrence_id_new AS visit_occurrence_id,
      0, m.code, coalesce(srctosrcvm.source_concept_id, 0),
      '' AS route_source_value, '' AS dose_unit_source_value
    FROM live.medications m
    JOIN live.source_to_standard_vocab_map srctostdvm
      ON srctostdvm.source_code = m.code
      AND srctostdvm.target_domain_id = 'Drug'
      AND srctostdvm.source_vocabulary_id = 'RxNorm'
      AND srctostdvm.target_standard_concept = 'S'
      AND (srctostdvm.target_invalid_reason IS NULL OR srctostdvm.target_invalid_reason = '')
    LEFT JOIN live.source_to_source_vocab_map srctosrcvm
      ON srctosrcvm.source_code = m.code
      AND srctosrcvm.source_vocabulary_id = 'RxNorm'
    LEFT JOIN live.final_visit_ids fv
      ON fv.encounter_id = m.encounter
    JOIN live.person p
      ON p.person_source_value = m.patient

    UNION ALL

    SELECT
      p.person_id,
      coalesce(srctostdvm.target_concept_id, 0) AS drug_concept_id,
      i.date, i.date, i.date, i.date, i.date,
      581452, '' AS stop_reason, 0, 0, 0,
      '' AS sig, 0, 0, 0, fv.visit_occurrence_id_new AS visit_occurrence_id,
      0, i.code, coalesce(srctosrcvm.source_concept_id, 0),
      '' AS route_source_value, '' AS dose_unit_source_value
    FROM live.immunizations i
    LEFT JOIN live.source_to_standard_vocab_map srctostdvm
      ON srctostdvm.source_code = i.code
      AND srctostdvm.target_domain_id = 'Drug'
      AND srctostdvm.source_vocabulary_id = 'CVX'
      AND srctostdvm.target_standard_concept = 'S'
      AND (srctostdvm.target_invalid_reason IS NULL OR srctostdvm.target_invalid_reason = '')
    LEFT JOIN live.source_to_source_vocab_map srctosrcvm
      ON srctosrcvm.source_code = i.code
      AND srctosrcvm.source_vocabulary_id = 'CVX'
    LEFT JOIN live.final_visit_ids fv
      ON fv.encounter_id = i.encounter
    JOIN live.person p
      ON p.person_source_value = i.patient
  ) tmp;


-- COMMAND ----------

-- MAGIC %md ## Our pipeline is now ready!
-- MAGIC
-- MAGIC As you can see, building Data Pipeline with databricks let you focus on your business implementation while the engine solves all hard data engineering work for you.
-- MAGIC
-- MAGIC Now that these tables are available in our Lakehouse, let's review how we can share them with the Data Scientists and Data Analysts teams.
-- MAGIC
-- MAGIC Open the <a dbdemos-pipeline-id="dlt-patient-readmission" href="#joblist/pipelines/ff2fd2cb-733b-4166-85ed-a34b84129a35" target="_blank">OMOP data model Delta Live Table pipeline</a> and click on start to visualize your lineage and consume the new data incrementally!

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Next: secure and share data with Unity Catalog
-- MAGIC
-- MAGIC Now that these tables are available in our Lakehouse, let's review how we can share them with the Data Scientists and Data Analysts teams.
-- MAGIC
-- MAGIC Jump to the [Governance with Unity Catalog notebook]($../02-Data-Governance/02-Data-Governance-patient-readmission) or [Go back to the introduction]($../00-patient-readmission-introduction)
