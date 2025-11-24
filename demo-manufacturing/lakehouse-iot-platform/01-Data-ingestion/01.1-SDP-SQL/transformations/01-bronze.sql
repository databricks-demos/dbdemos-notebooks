-- ----------------------------------
-- Ingest historical turbine status from JSON files
-- This contains the historical failure data used as labels for our ML model to identify faulty turbines
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
-- Ingest raw sensor data from parquet files using Auto Loader
-- Contains real-time sensor readings: vibration levels (sensor A-F), energy produced, timestamps, etc.
-- Data quality: drop rows with invalid energy values
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
    inferColumnTypes => true);


-- ----------------------------------
-- Ingest turbine metadata from JSON files
-- Contains static turbine information: location (lat/long, state, country), model type, turbine ID
-- This reference data enriches sensor readings with contextual information
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