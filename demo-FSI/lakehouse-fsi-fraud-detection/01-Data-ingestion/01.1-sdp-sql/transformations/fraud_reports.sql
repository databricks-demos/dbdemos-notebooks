--Fraud report (labels for ML training)
CREATE STREAMING TABLE fraud_reports
AS 
SELECT *
FROM STREAM read_files(
  '/Volumes/main__build/dbdemos_fsi_fraud_detection/fraud_raw_data/fraud_report',
  format => 'csv'
);