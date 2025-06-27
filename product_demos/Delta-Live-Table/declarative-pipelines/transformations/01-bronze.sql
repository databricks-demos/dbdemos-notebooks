-- Let's start out by ingesting our raw files from our UC Volume
-- ==========================================================================
-- == Incrementally loaw RAW RIDES from CSV                                ==
-- ==========================================================================
CREATE OR REFRESH STREAMING TABLE RIDES_RAW 
COMMENT "Raw rides data streamed in from CSV files."
AS SELECT * FROM 
  STREAM READ_FILES("/Volumes/${catalog}/${schema}/raw_data/rides/*.csv", FORMAT => "csv");


-- ==========================================================================
-- == Incrementally loaw RAW MAINTENANCE LOG from CSV                      ==
-- ==========================================================================
CREATE OR REFRESH STREAMING TABLE MAINTENANCE_LOGS_RAW
COMMENT "Raw maintenance logs streamed in from CSV files."
AS SELECT * FROM 
  STREAM READ_FILES("/Volumes/${catalog}/${schema}/raw_data/maintenance_logs/*.csv", FORMAT => "csv", MULTILINE => TRUE);


-- ==========================================================================
-- == Incrementally loaw RAW WEATHER from CSV                              ==
-- ==========================================================================
CREATE OR REFRESH STREAMING TABLE WEATHER_RAW
COMMENT "Raw weather data streamed in from JSON files."
AS SELECT * FROM 
  STREAM READ_FILES("/Volumes/${catalog}/${schema}/raw_data/weather/*.json", FORMAT => "json");

-- Next up, let's clean up our data for our silver layer in 02-silver.sql