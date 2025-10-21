-- ----------------------------------
-- TODO quick description of what we do here
-- ----------------------------------
CREATE STREAMING TABLE historical_turbine_status (
  CONSTRAINT correct_schema EXPECT (_rescued_data IS NULL)
)
COMMENT "Turbine status to be used as label in our predictive maintenance model (to know which turbine is potentially faulty)"
AS SELECT
  *
FROM STREAM READ_FILES(
    "/Volumes/main_build/dbdemos_iot_platform/turbine_raw_landing/historical_turbine_status",
    format => "json",
    inferColumnTypes => true
);


-- ----------------------------------
-- TODO quick description of what we do here
-- ----------------------------------
CREATE STREAMING TABLE sensor_bronze (
  CONSTRAINT correct_schema EXPECT (_rescued_data IS NULL),
  CONSTRAINT correct_energy EXPECT (energy IS NOT NULL and energy > 0) ON VIOLATION DROP ROW
)
COMMENT "Raw sensor data coming from json files ingested in incremental with Auto Loader: vibration, energy produced etc. 1 point every X sec per sensor."
AS SELECT
  * 
FROM STREAM READ_FILES(
    "/Volumes/main_build/dbdemos_iot_platform/turbine_raw_landing/incoming_data",
    format => "parquet",
    inferColumnTypes => true;


-- ----------------------------------
-- TODO quick description of what we do here
-- ----------------------------------
CREATE STREAMING TABLE turbine (
  CONSTRAINT correct_schema EXPECT (_rescued_data IS NULL)
)
COMMENT "Turbine details, with location, wind turbine model type etc"
AS SELECT
  *
FROM STREAM READ_FILES(
    "/Volumes/main_build/dbdemos_iot_platform/turbine_raw_landing/turbine",
    format => "json",
    inferColumnTypes => true
);