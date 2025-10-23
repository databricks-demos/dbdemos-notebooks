-- ----------------------------------
-- Ingest credit bureau data (JSON format)
-- Credit bureau data contains information about customer credit history and creditworthiness
-- Monthly data accessed through API from government agencies or central banks
-- ----------------------------------
CREATE OR REFRESH STREAMING TABLE credit_bureau_bronze AS
  SELECT * FROM
    cloud_files('/Volumes/main__build/dbdemos_fsi_credit_decisioning/credit_raw_data/credit_bureau', 'json',
                 map('header', 'true',
                     'inferSchema', 'true',
                     'cloudFiles.inferColumnTypes', 'true'))

-- COMMAND ----------

-- ----------------------------------
-- Ingest customer data (CSV format)
-- Customer table from internal KYC processes containing customer-related data
-- Daily ingestion from internal relational databases via CDC pipeline
-- ----------------------------------
CREATE OR REFRESH STREAMING TABLE customer_bronze AS
  SELECT * FROM
    cloud_files('/Volumes/main__build/dbdemos_fsi_credit_decisioning/credit_raw_data/internalbanking/customer', 'csv',
                 map('header', 'true',
                     'inferSchema', 'true',
                     'cloudFiles.inferColumnTypes', 'true',
                     'cloudFiles.schemaHints', 'passport_expiry date, visa_expiry date, join_date date, dob date'))

-- COMMAND ----------

-- ----------------------------------
-- Ingest relationship data (CSV format)
-- Represents the relationship between the bank and the customer
-- Source: Internal banking databases
-- ----------------------------------
CREATE OR REFRESH STREAMING TABLE relationship_bronze AS
  SELECT * FROM
    cloud_files('/Volumes/main__build/dbdemos_fsi_credit_decisioning/credit_raw_data/internalbanking/relationship', 'csv',
                 map('header', 'true',
                     'inferSchema', 'true',
                     'cloudFiles.inferColumnTypes', 'true'))

-- COMMAND ----------

-- ----------------------------------
-- Ingest account data (CSV format)
-- Customer account information from internal banking systems
-- Daily ingestion via CDC pipeline
-- ----------------------------------
CREATE OR REFRESH STREAMING TABLE account_bronze AS
  SELECT * FROM
    cloud_files('/Volumes/main__build/dbdemos_fsi_credit_decisioning/credit_raw_data/internalbanking/account', 'csv',
                 map('header', 'true',
                     'inferSchema', 'true',
                     'cloudFiles.inferColumnTypes', 'true'))

-- COMMAND ----------

-- ----------------------------------
-- Ingest fund transfer data (JSON format)
-- Real-time payment transactions performed by customers
-- Streaming data available in real-time through Kafka
-- ----------------------------------
CREATE OR REFRESH STREAMING TABLE fund_trans_bronze AS
  SELECT * FROM
    cloud_files('/Volumes/main__build/dbdemos_fsi_credit_decisioning/credit_raw_data/fund_trans', 'json',
                map('inferSchema', 'true',
                    'cloudFiles.inferColumnTypes', 'true'))

-- COMMAND ----------

-- ----------------------------------
-- Ingest telco partner data (JSON format)
-- External partner data to augment internal banking data
-- Weekly ingestion containing payment features for common customers
-- Used to evaluate creditworthiness through alternative data sources
-- ----------------------------------
CREATE OR REFRESH STREAMING TABLE telco_bronze AS
  SELECT * FROM
    cloud_files('/Volumes/main__build/dbdemos_fsi_credit_decisioning/credit_raw_data/telco', 'json',
                 map('inferSchema', 'true',
                     'cloudFiles.inferColumnTypes', 'true'))
