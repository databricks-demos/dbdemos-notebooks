-- Reference table
CREATE STREAMING TABLE country_coordinates
AS 
SELECT * 
FROM STREAM read_files(
  '/Volumes/main__build/dbdemos_fsi_fraud_detection/fraud_raw_data/country_code',
  format => 'csv'
);