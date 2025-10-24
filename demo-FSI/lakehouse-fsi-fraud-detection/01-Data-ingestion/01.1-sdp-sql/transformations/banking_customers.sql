-- Customers
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
