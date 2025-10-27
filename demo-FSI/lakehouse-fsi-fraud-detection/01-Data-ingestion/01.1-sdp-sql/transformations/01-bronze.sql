-- ----------------------------------
-- Ingest raw transaction data (JSON format)
-- Loads historical banking transactions for fraud detection analysis
-- Uses autoloader to incrementally process new transaction files
-- ----------------------------------

CREATE STREAMING TABLE bronze_transactions
COMMENT "Historical banking transaction to be trained on fraud detection"
AS
SELECT *
FROM STREAM read_files(
  '/Volumes/main__build/dbdemos_fsi_fraud_detection/fraud_raw_data/transactions',
  format => 'json',
  maxFilesPerTrigger => 1,
  inferColumnTypes => true
);

-- ----------------------------------
-- Ingest raw customer data (CSV format)
-- Customer information with schema validation
-- Drops rows with rescued data to ensure data quality
-- ----------------------------------


CREATE STREAMING TABLE banking_customers (
  CONSTRAINT correct_schema EXPECT (_rescued_data IS NULL)
)
COMMENT "Customer data coming from csv files ingested in incremental with Auto Loader to support schema inference and evolution"
AS
SELECT * 
FROM STREAM read_files(
  '/Volumes/main__build/dbdemos_fsi_fraud_detection/fraud_raw_data/customers',
  format => 'csv',
  multiLine => true,
  inferColumnTypes => true
);


-- ----------------------------------
-- Ingest country reference data (CSV format)
-- Country codes with geographic coordinates for transaction enrichment
-- Reference table for mapping country codes to coordinates
-- ----------------------------------

CREATE STREAMING TABLE country_coordinates
AS 
SELECT * 
FROM STREAM read_files(
  '/Volumes/main__build/dbdemos_fsi_fraud_detection/fraud_raw_data/country_code',
  format => 'csv'
);


-- ----------------------------------
-- Ingest fraud report labels (CSV format)
-- Known fraud cases used as labels for ML model training
-- Essential for supervised learning fraud detection models
-- ----------------------------------

CREATE STREAMING TABLE fraud_reports
AS 
SELECT *
FROM STREAM read_files(
  '/Volumes/main__build/dbdemos_fsi_fraud_detection/fraud_raw_data/fraud_report',
  format => 'csv'
);




