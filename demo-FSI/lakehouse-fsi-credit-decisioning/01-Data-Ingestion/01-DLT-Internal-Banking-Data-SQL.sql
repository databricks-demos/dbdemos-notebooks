-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC # Simplify Ingestion and Transformation with Delta Live Tables
-- MAGIC
-- MAGIC In this notebook, we'll work as a Data Engineer to build our Credit Decisioning database. <br>
-- MAGIC We'll consume and clean our raw data sources to prepare the tables required for our BI & ML workload.
-- MAGIC
-- MAGIC We have four data sources sending new files in our blob storage and we want to incrementally load this data into our Data warehousing tables:
-- MAGIC
-- MAGIC - **Internal banking** data *(KYC, accounts, collections, applications, relationship)* come from the bank's internal relational databases and is ingested *once a day* through a CDC pipeline,
-- MAGIC - **Credit Bureau** data (usually in XML or CSV format and *accessed monthly* through API) comes from government agencies (such as a central banks) and contains a lot of valuable information for every customer. We also use this data to re-calculate whether a user has defaulted in the past 60 days,
-- MAGIC - **Partner** data - used to augment the internal banking data and ingested *once a week*. In this case we use telco data in order to further evaluate the character and creditworthiness of banking customers,
-- MAGIC - **Fund transfer** are the banking transactions (such as credit card transactions) and are *available real-time* through Kafka streams.
-- MAGIC
-- MAGIC
-- MAGIC ## Delta Live Table: A simple way to build and manage data pipelines for fresh, high quality data!
-- MAGIC
-- MAGIC
-- MAGIC Databricks simplifies this task with Delta Live Table (DLT) by making Data Engineering accessible to all.
-- MAGIC
-- MAGIC DLT allows Data Analysts to create advanced pipeline with plain SQL.
-- MAGIC
-- MAGIC <div>
-- MAGIC   <div style="width: 45%; float: left; margin-bottom: 10px; padding-right: 45px">
-- MAGIC     <p>
-- MAGIC       <img style="width: 50px; float: left; margin: 0px 5px 30px 0px;" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/logo-accelerate.png"/> 
-- MAGIC       <strong>Accelerate ETL development</strong> <br/>
-- MAGIC       Enable analysts and data engineers to innovate rapidly with simple pipeline development and maintenance 
-- MAGIC     </p>
-- MAGIC     <p>
-- MAGIC       <img style="width: 50px; float: left; margin: 0px 5px 30px 0px;" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/logo-complexity.png"/> 
-- MAGIC       <strong>Remove operational complexity</strong> <br/>
-- MAGIC       By automating complex administrative tasks and gaining broader visibility into pipeline operations
-- MAGIC     </p>
-- MAGIC   </div>
-- MAGIC   <div style="width: 48%; float: left">
-- MAGIC     <p>
-- MAGIC       <img style="width: 50px; float: left; margin: 0px 5px 30px 0px;" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/logo-trust.png"/> 
-- MAGIC       <strong>Trust your data</strong> <br/>
-- MAGIC       With built-in quality controls and quality monitoring to ensure accurate and useful BI, Data Science, and ML 
-- MAGIC     </p>
-- MAGIC     <p>
-- MAGIC       <img style="width: 50px; float: left; margin: 0px 5px 30px 0px;" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/logo-stream.png"/> 
-- MAGIC       <strong>Simplify batch and streaming</strong> <br/>
-- MAGIC       With self-optimization and auto-scaling data pipelines for batch or streaming processing 
-- MAGIC     </p>
-- MAGIC </div>
-- MAGIC </div>
-- MAGIC
-- MAGIC <br style="clear:both">
-- MAGIC
-- MAGIC <img src="https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-logo.png" style="float: right;" width="200px">
-- MAGIC
-- MAGIC ## Delta Lake
-- MAGIC
-- MAGIC All the tables we'll create in the Lakehouse will be stored as Delta Lake tables. Delta Lake is an open storage framework for reliability and performance.<br>
-- MAGIC It provides many functionalities (ACID Transaction, DELETE/UPDATE/MERGE, Clone zero copy, Change data Capture...)<br>
-- MAGIC For more details on Delta Lake, run dbdemos.install('delta-lake')
-- MAGIC
-- MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
-- MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=lakehouse&notebook=01-DLT-Internal-Banking-Data-SQL&demo_name=lakehouse-fsi-credit-decisioning&event=VIEW">

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## Building a Delta Live Table pipeline to analyze consumer credit
-- MAGIC
-- MAGIC In this example, we'll implement an end-to-end DLT pipeline consuming the aforementioned information. We'll use the medaillon architecture but we could build star schema, data vault, or any other modelisation.
-- MAGIC
-- MAGIC We'll incrementally load new data with the autoloader, enrich this information and then load a model from MLFlow to perform our credit decisioning prediction.
-- MAGIC
-- MAGIC This information will then be used to build our DBSQL dashboard to create credit scores, decisioning, and risk.
-- MAGIC
-- MAGIC Let's implement the following flow: 
-- MAGIC  
-- MAGIC <div><img width="1000px" src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/fsi/credit_decisioning/fsi_credit_decisioning_dlt_0.png" /></div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Your DLT Pipeline has been installed and started for you! Open the <a dbdemos-pipeline-id="dlt-fsi-credit-decisioning" href="#joblist/pipelines" target="_blank">Delta Live Table pipeline</a> to see it in action.<br/>
-- MAGIC *(Note: The pipeline will automatically start once the initialization job is completed, this might take a few minutes... Check installation logs for more details)*

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ### 1/ Loading our data using Databricks Autoloader (cloud_files)
-- MAGIC
-- MAGIC <img width="650px" style="float:right" src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/fsi/credit_decisioning/fsi_credit_decisioning_dlt_1.png"/>
-- MAGIC   
-- MAGIC Autoloader allow us to efficiently ingest millions of files from a cloud storage, and support efficient schema inference and evolution at scale.
-- MAGIC
-- MAGIC For more details on autoloader, run `dbdemos.install('auto-loader')`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 1. Credit Bureau
-- MAGIC
-- MAGIC Credit bureau data refers to information about an individual's credit history, financial behavior, and creditworthiness that is collected and maintained by credit bureaus. Credit bureaus are companies that collect and compile information about consumers' credit activities, such as credit card usage, loan payments, and other financial transactions.

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE credit_bureau_bronze AS
  SELECT * FROM
    cloud_files('/Volumes/main__build/dbdemos_fsi_credit_decisioning/credit_raw_data/credit_bureau', 'json',
                 map('header', 'true', 
                     'inferSchema', 'true', 
                     'cloudFiles.inferColumnTypes', 'true'))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### 1. Customer table
-- MAGIC
-- MAGIC The customer table comes from the internal KYC processes and contains customer-related data.

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE customer_bronze AS
  SELECT * FROM
    cloud_files('/Volumes/main__build/dbdemos_fsi_credit_decisioning/credit_raw_data/internalbanking/customer', 'csv',
                 map('header', 'true', 
                     'inferSchema', 'true', 
                     'cloudFiles.inferColumnTypes', 'true',
                     'cloudFiles.schemaHints', 'passport_expiry date, visa_expiry date, join_date date, dob date'))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### 2. Relationship table
-- MAGIC
-- MAGIC The relationship table represents the relationship between the bank and the customer. It also comes from the raw databases.

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE relationship_bronze AS
  SELECT * FROM
    cloud_files('/Volumes/main__build/dbdemos_fsi_credit_decisioning/credit_raw_data/internalbanking/relationship', 'csv',
                 map('header', 'true', 
                     'inferSchema', 'true', 
                     'cloudFiles.inferColumnTypes', 'true'))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### 3. Account table

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE account_bronze AS
  SELECT * FROM
    cloud_files('/Volumes/main__build/dbdemos_fsi_credit_decisioning/credit_raw_data/internalbanking/account', 'csv',
                 map('header', 'true', 
                     'inferSchema', 'true', 
                     'cloudFiles.inferColumnTypes', 'true'))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### 4. Fund Transfer Table
-- MAGIC
-- MAGIC Fund transfer is a real-time data stream that contains payment transactions performed by the client.

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE fund_trans_bronze AS
  SELECT * FROM
    cloud_files('/Volumes/main__build/dbdemos_fsi_credit_decisioning/credit_raw_data/fund_trans', 'json',
                map('inferSchema', 'true', 
                    'cloudFiles.inferColumnTypes', 'true'))

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC
-- MAGIC #### 5. Telco
-- MAGIC
-- MAGIC This is where we augment the internal banking data through external and alternative data sources - in this case, telecom partner data, containing payment features for the common customers (between the bank and the telco provider).

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE telco_bronze AS
  SELECT * FROM
    cloud_files('/Volumes/main__build/dbdemos_fsi_credit_decisioning/credit_raw_data/telco', 'json',
                 map('inferSchema', 'true',
                     'cloudFiles.inferColumnTypes', 'true'))

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ### 2/ Enforce quality and materialize our tables for Data Analysts
-- MAGIC
-- MAGIC <img width="650px" style="float:right" src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/fsi/credit_decisioning/fsi_credit_decisioning_dlt_2.png"/>
-- MAGIC
-- MAGIC The next layer often call silver is consuming **incremental** data from the bronze one, and cleaning up some information.
-- MAGIC
-- MAGIC We're also adding an [expectation](https://docs.databricks.com/workflows/delta-live-tables/delta-live-tables-expectations.html) on different field to enforce and track our Data Quality. This will ensure that our dashboard are relevant and easily spot potential errors due to data anomaly.
-- MAGIC
-- MAGIC For more advanced DLT capabilities run `dbdemos.install('dlt-loans')` or `dbdemos.install('dlt-cdc')` for CDC/SCDT2 example.
-- MAGIC
-- MAGIC These tables are clean and ready to be used by the BI team!

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### 1. Fund transfer table

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW fund_trans_silver AS
  SELECT
    payer_account.cust_id payer_cust_id,
    payee_account.cust_id payee_cust_id,
    fund.*
  FROM
    live.fund_trans_bronze fund
  LEFT OUTER JOIN live.account_bronze payer_account ON fund.payer_acc_id = payer_account.id
  LEFT OUTER JOIN live.account_bronze payee_account ON fund.payee_acc_id = payee_account.id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### 2. Customer table

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW customer_silver AS
  SELECT
    * EXCEPT (dob, customer._rescued_data, relationship._rescued_data, relationship.id, relationship.operation),
    year(dob) AS birth_year
  FROM
    live.customer_bronze customer
  LEFT OUTER JOIN live.relationship_bronze relationship ON customer.id = relationship.cust_id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### 3. Account table

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW account_silver AS
  WITH cust_acc AS (
      SELECT cust_id, count(1) num_accs, avg(balance) avg_balance 
        FROM live.account_bronze
        GROUP BY cust_id
    )
  SELECT
    acc_usd.cust_id,
    num_accs,
    avg_balance,
    balance balance_usd,
    available_balance available_balance_usd,
    operation
  FROM
    cust_acc
  LEFT OUTER JOIN live.account_bronze acc_usd ON cust_acc.cust_id = acc_usd.cust_id AND acc_usd.currency = 'USD'

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC ### 3/ Aggregation layer for analytics & ML
-- MAGIC
-- MAGIC <img width="650px" style="float:right" src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/fsi/credit_decisioning/fsi_credit_decisioning_dlt_3.png"/>
-- MAGIC
-- MAGIC We curate all the tables in Delta Lake using Delta Live Tables so we can apply all the joins, masking, and data constraints in real-time. Data scientists can now use these datasets to built high-quality models, particularly to predict credit worthiness. Because we are masking sensitive data as part of Unity Catalog capabilities, we are able to confidently expose the data to many downstream users from data scientists to data analysts and business users.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### 1. Credit bureau cleanup
-- MAGIC
-- MAGIC We begin by ingesting credit bureau data, sourced from a Delta Lake table here. Typically, this data would be curated via API ingestion and dumped into cloud object stores. 

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW credit_bureau_gold
  (CONSTRAINT CustomerID_not_null EXPECT (CUST_ID IS NOT NULL) ON VIOLATION DROP ROW)
AS
  SELECT * FROM live.credit_bureau_bronze

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### 1. Fund transfer table
-- MAGIC
-- MAGIC The fund transfer table represents peer-to-peer payments made between the customer and another person. This helps us to understand the frequency and monetary attributes for payments for each customer as a credit risk source.

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW fund_trans_gold AS (
  WITH 12m_payer AS (SELECT
                      payer_cust_id,
                      COUNT(DISTINCT payer_cust_id) dist_payer_cnt_12m,
                      COUNT(1) sent_txn_cnt_12m,
                      SUM(txn_amt) sent_txn_amt_12m,
                      AVG(txn_amt) sent_amt_avg_12m
                    FROM live.fund_trans_silver WHERE cast(datetime AS date) >= cast('2022-01-01' AS date)
                    GROUP BY payer_cust_id),
      12m_payee AS (SELECT
                        payee_cust_id,
                        COUNT(DISTINCT payee_cust_id) dist_payee_cnt_12m,
                        COUNT(1) rcvd_txn_cnt_12m,
                        SUM(txn_amt) rcvd_txn_amt_12m,
                        AVG(txn_amt) rcvd_amt_avg_12m
                      FROM live.fund_trans_silver WHERE CAST(datetime AS date) >= CAST('2022-01-01' AS date)
                      GROUP BY payee_cust_id),
      6m_payer AS (SELECT
                    payer_cust_id,
                    COUNT(DISTINCT payer_cust_id) dist_payer_cnt_6m,
                    COUNT(1) sent_txn_cnt_6m,
                    SUM(txn_amt) sent_txn_amt_6m,
                    AVG(txn_amt) sent_amt_avg_6m
                  FROM live.fund_trans_silver WHERE CAST(datetime AS date) >= CAST('2022-07-01' AS date)
                  GROUP BY payer_cust_id),
      6m_payee AS (SELECT
                    payee_cust_id,
                    COUNT(DISTINCT payee_cust_id) dist_payee_cnt_6m,
                    COUNT(1) rcvd_txn_cnt_6m,
                    SUM(txn_amt) rcvd_txn_amt_6m,
                    AVG(txn_amt) rcvd_amt_avg_6m
                  FROM live.fund_trans_silver WHERE CAST(datetime AS date) >= CAST('2022-07-01' AS date)
                  GROUP BY payee_cust_id),
      3m_payer AS (SELECT
                    payer_cust_id,
                    COUNT(DISTINCT payer_cust_id) dist_payer_cnt_3m,
                    COUNT(1) sent_txn_cnt_3m,
                    SUM(txn_amt) sent_txn_amt_3m,
                    AVG(txn_amt) sent_amt_avg_3m
                  FROM live.fund_trans_silver WHERE CAST(datetime AS date) >= CAST('2022-07-01' AS date)
                  GROUP BY payer_cust_id),
      3m_payee AS (SELECT
                    payee_cust_id,
                    COUNT(DISTINCT payee_cust_id) dist_payee_cnt_3m,
                    COUNT(1) rcvd_txn_cnt_3m,
                    SUM(txn_amt) rcvd_txn_amt_3m,
                    AVG(txn_amt) rcvd_amt_avg_3m
                  FROM live.fund_trans_silver WHERE CAST(datetime AS date) >= CAST('2022-07-01' AS date)
                  GROUP BY payee_cust_id)        
  SELECT c.cust_id, 
    12m_payer.* EXCEPT (payer_cust_id),
    12m_payee.* EXCEPT (payee_cust_id), 
    6m_payer.* EXCEPT (payer_cust_id), 
    6m_payee.* EXCEPT (payee_cust_id), 
    3m_payer.* EXCEPT (payer_cust_id), 
    3m_payee.* EXCEPT (payee_cust_id) 
  FROM live.customer_silver c 
    LEFT JOIN 12m_payer ON 12m_payer.payer_cust_id = c.cust_id
    LEFT JOIN 12m_payee ON 12m_payee.payee_cust_id = c.cust_id
    LEFT JOIN 6m_payer ON 6m_payer.payer_cust_id = c.cust_id
    LEFT JOIN 6m_payee ON 6m_payee.payee_cust_id = c.cust_id
    LEFT JOIN 3m_payer ON 3m_payer.payer_cust_id = c.cust_id
    LEFT JOIN 3m_payee ON 3m_payee.payee_cust_id = c.cust_id)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### 3. Telco table
-- MAGIC
-- MAGIC The telco table represents all the payments data for a given prospect or customer to understand credit worthiness based on a non-bank credit source.

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW telco_gold AS
SELECT
  customer.id cust_id,
  telco.*
FROM
  live.telco_bronze telco
  LEFT OUTER JOIN live.customer_bronze customer ON telco.user_phone = customer.mobile_phone

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### 4. Customer table
-- MAGIC
-- MAGIC The customer data represents the system of record for PII and customer attributes that will be joined to other fact tables. 

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW customer_gold AS
SELECT
  customer.*,
  account.avg_balance,
  account.num_accs,
  account.balance_usd,
  account.available_balance_usd
FROM
  live.customer_silver customer
  LEFT OUTER JOIN live.account_silver account ON customer.cust_id = account.cust_id 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### 5. Adding a view removing firstname for our datas scientist users
-- MAGIC
-- MAGIC The best practice for masking data in Databricks Delta Lake tables is to use dynamic views and functions like is_member to encrypt or mask data based on the group. In this case, we want to mask PII data based on user group, and we are using built-in encryption functions `aes_encrypt` to do this. Moreover, the key itself is saved into a Databricks secret for security reasons.

-- COMMAND ----------

CREATE OR REPLACE LIVE VIEW customer_gold_secured AS
SELECT
  c.* EXCEPT (first_name),
  CASE
    WHEN is_member('data-science-users')
    THEN base64(aes_encrypt(c.first_name, 'YOUR_SECRET_FROM_MANAGER')) -- save secret in Databricks manager and load it in SQL with secret('<YOUR_SCOPE> ', '<YOUR_SECRET_NAME>')
    ELSE c.first_name
  END AS first_name
FROM
  live.customer_gold AS c

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Next: secure and share data with Unity Catalog
-- MAGIC
-- MAGIC Now that we have ingested all these various sources of data, we can jump to the:
-- MAGIC
-- MAGIC * [Governance with Unity Catalog notebook]($../02-Data-Governance/02-Data-Governance-credit-decisioning) to see how to grant fine-grained access to every user and persona and explore the **data lineage graph**,
-- MAGIC * [Feature Engineering notebook]($../03-Data-Science-ML/03.1-Feature-Engineering-credit-decisioning) and start creating features for our machine learning models,
-- MAGIC * Go back to the [Introduction]($../00-Credit-Decisioning).
