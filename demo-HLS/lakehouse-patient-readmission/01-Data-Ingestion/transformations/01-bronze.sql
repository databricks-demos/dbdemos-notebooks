-- Databricks Auto Loader STREAM read_files will incrementally load new files, infering the column types and handling schema evolution for us.
-- data could be from any source: csv, json, parquet...
CREATE OR REFRESH STREAMING TABLE encounters
  AS SELECT * EXCEPT(START, STOP), to_timestamp(START) as START, to_timestamp(STOP) as STOP
      FROM STREAM read_files("/Volumes/main__build/dbdemos_hls_readmission/synthea/landing_zone/encounters", format => "parquet");

CREATE OR REFRESH STREAMING TABLE patients
  AS SELECT * FROM STREAM read_files("/Volumes/main__build/dbdemos_hls_readmission/synthea/landing_zone/patients", format => "parquet");

CREATE OR REFRESH STREAMING TABLE conditions
  AS SELECT * FROM STREAM read_files("/Volumes/main__build/dbdemos_hls_readmission/synthea/landing_zone/conditions", format => "parquet");

CREATE OR REFRESH STREAMING TABLE medications
  AS SELECT * FROM STREAM read_files("/Volumes/main__build/dbdemos_hls_readmission/synthea/landing_zone/medications", format => "parquet");

CREATE OR REFRESH STREAMING TABLE immunizations
  AS SELECT * FROM STREAM read_files("/Volumes/main__build/dbdemos_hls_readmission/synthea/landing_zone/immunizations", format => "parquet");

CREATE OR REFRESH STREAMING TABLE concept
  AS SELECT * FROM STREAM read_files("/Volumes/main__build/dbdemos_hls_readmission/synthea/landing_vocab/CONCEPT", format => "parquet");

CREATE OR REFRESH STREAMING TABLE concept_relationship
  AS SELECT * FROM STREAM read_files("/Volumes/main__build/dbdemos_hls_readmission/synthea/landing_vocab/CONCEPT_RELATIONSHIP", format => "parquet");