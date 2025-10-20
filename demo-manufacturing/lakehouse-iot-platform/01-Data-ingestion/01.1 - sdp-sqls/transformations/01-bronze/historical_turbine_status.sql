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
)