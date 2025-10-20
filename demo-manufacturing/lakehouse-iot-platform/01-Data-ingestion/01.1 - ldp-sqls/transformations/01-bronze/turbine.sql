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
)