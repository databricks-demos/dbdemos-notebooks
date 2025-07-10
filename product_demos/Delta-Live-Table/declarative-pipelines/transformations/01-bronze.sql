-- Let's start out by ingesting our raw files from our UC Volume
-- ==========================================================================
-- == Incrementally load RAW RIDES from CSV                                ==
-- ==========================================================================
CREATE OR REFRESH STREAMING TABLE RIDES_RAW 
COMMENT "Raw rides data streamed in from CSV files."
AS SELECT * FROM 
  STREAM READ_FILES("/Volumes/${catalog}/${schema}/raw_data/rides/*.csv", FORMAT => "csv");


-- ==========================================================================
-- == Incrementally load RAW MAINTENANCE LOG from CSV                      ==
-- ==========================================================================
CREATE OR REFRESH STREAMING TABLE MAINTENANCE_LOGS_RAW
COMMENT "Raw maintenance logs streamed in from CSV files."
AS SELECT * FROM 
  STREAM READ_FILES("/Volumes/${catalog}/${schema}/raw_data/maintenance_logs/*.csv", FORMAT => "csv", MULTILINE => TRUE);


-- ==========================================================================
-- == Incrementally load RAW WEATHER from CSV                              ==
-- ==========================================================================
CREATE OR REFRESH STREAMING TABLE WEATHER_RAW
COMMENT "Raw weather data streamed in from JSON files."
AS SELECT * FROM 
  STREAM READ_FILES("/Volumes/${catalog}/${schema}/raw_data/weather/*.json", FORMAT => "json");

-- ==========================================================================
-- == Incrementally load RAW CUSTOMER CDC from PARQUET                     ==
-- ==========================================================================
CREATE OR REFRESH STREAMING TABLE CUSTOMERS_CDC_RAW
COMMENT "Raw customer CDC data streamed in from Parquet files for Auto CDC processing."
AS SELECT * FROM 
  STREAM READ_FILES("/Volumes/${catalog}/${schema}/raw_data/customers_cdc/*.parquet", FORMAT => "parquet");

-- Next up, let's clean up our data for our silver layer in 02-silver.sql