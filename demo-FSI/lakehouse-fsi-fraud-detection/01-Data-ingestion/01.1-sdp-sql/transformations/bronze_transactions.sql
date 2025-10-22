-- Ingest Transactions
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