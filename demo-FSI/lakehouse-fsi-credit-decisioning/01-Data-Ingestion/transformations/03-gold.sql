-- ----------------------------------
-- Clean credit bureau data with data quality constraints
-- Validate that customer ID is not null
-- Drop rows with missing customer IDs to ensure data quality
-- ----------------------------------
CREATE OR REFRESH MATERIALIZED VIEW credit_bureau_gold
  (CONSTRAINT CustomerID_not_null EXPECT (CUST_ID IS NOT NULL) ON VIOLATION DROP ROW)
AS
  SELECT * FROM live.credit_bureau_bronze

-- COMMAND ----------

-- ----------------------------------
-- Create fund transfer aggregation features
-- Calculate transaction metrics over 3, 6, and 12 month windows:
-- - Count of distinct payers/payees
-- - Count of sent/received transactions
-- - Total and average transaction amounts
-- Provides behavioral payment patterns for credit risk assessment
-- ----------------------------------
CREATE OR REFRESH MATERIALIZED VIEW fund_trans_gold AS (
  WITH
    max_date AS (SELECT max(datetime) AS max_date FROM live.fund_trans_silver),
    12m_payer AS (SELECT
                      payer_cust_id,
                      COUNT(DISTINCT payer_cust_id) dist_payer_cnt_12m,
                      COUNT(1) sent_txn_cnt_12m,
                      SUM(txn_amt) sent_txn_amt_12m,
                      AVG(txn_amt) sent_amt_avg_12m
                    FROM live.fund_trans_silver WHERE cast(datetime AS date) >= date_add(MONTH, -12, (SELECT CAST(max_date AS date) FROM max_date))
                    GROUP BY payer_cust_id),
      12m_payee AS (SELECT
                        payee_cust_id,
                        COUNT(DISTINCT payee_cust_id) dist_payee_cnt_12m,
                        COUNT(1) rcvd_txn_cnt_12m,
                        SUM(txn_amt) rcvd_txn_amt_12m,
                        AVG(txn_amt) rcvd_amt_avg_12m
                      FROM live.fund_trans_silver WHERE CAST(datetime AS date) >= date_add(MONTH, -12, (SELECT CAST(max_date AS date) FROM max_date))
                      GROUP BY payee_cust_id),
      6m_payer AS (SELECT
                    payer_cust_id,
                    COUNT(DISTINCT payer_cust_id) dist_payer_cnt_6m,
                    COUNT(1) sent_txn_cnt_6m,
                    SUM(txn_amt) sent_txn_amt_6m,
                    AVG(txn_amt) sent_amt_avg_6m
                  FROM live.fund_trans_silver WHERE CAST(datetime AS date) >= date_add(MONTH, -6, (SELECT CAST(max_date AS date) FROM max_date))
                  GROUP BY payer_cust_id),
      6m_payee AS (SELECT
                    payee_cust_id,
                    COUNT(DISTINCT payee_cust_id) dist_payee_cnt_6m,
                    COUNT(1) rcvd_txn_cnt_6m,
                    SUM(txn_amt) rcvd_txn_amt_6m,
                    AVG(txn_amt) rcvd_amt_avg_6m
                  FROM live.fund_trans_silver WHERE CAST(datetime AS date) >= date_add(MONTH, -6, (SELECT CAST(max_date AS date) FROM max_date))
                  GROUP BY payee_cust_id),
      3m_payer AS (SELECT
                    payer_cust_id,
                    COUNT(DISTINCT payer_cust_id) dist_payer_cnt_3m,
                    COUNT(1) sent_txn_cnt_3m,
                    SUM(txn_amt) sent_txn_amt_3m,
                    AVG(txn_amt) sent_amt_avg_3m
                  FROM live.fund_trans_silver WHERE CAST(datetime AS date) >= date_add(MONTH, -3, (SELECT CAST(max_date AS date) FROM max_date))
                  GROUP BY payer_cust_id),
      3m_payee AS (SELECT
                    payee_cust_id,
                    COUNT(DISTINCT payee_cust_id) dist_payee_cnt_3m,
                    COUNT(1) rcvd_txn_cnt_3m,
                    SUM(txn_amt) rcvd_txn_amt_3m,
                    AVG(txn_amt) rcvd_amt_avg_3m
                  FROM live.fund_trans_silver WHERE CAST(datetime AS date) >= date_add(MONTH, -3, (SELECT CAST(max_date AS date) FROM max_date))
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

-- ----------------------------------
-- Enrich telco partner data with customer information
-- Join telco data with customer records using phone number
-- Alternative data source to evaluate creditworthiness
-- ----------------------------------
CREATE OR REFRESH MATERIALIZED VIEW telco_gold AS
SELECT
  customer.id cust_id,
  telco.*
FROM
  live.telco_bronze telco
  LEFT OUTER JOIN live.customer_bronze customer ON telco.user_phone = customer.mobile_phone

-- COMMAND ----------

-- ----------------------------------
-- Create comprehensive customer profile
-- Join customer data with account aggregations
-- System of record for customer attributes and financial summary
-- ----------------------------------
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

-- ----------------------------------
-- Create secured customer view with PII masking
-- Mask first name using AES encryption for data science users
-- Best practice for protecting sensitive data with dynamic views
-- Uses is_member function to encrypt based on user group
-- ----------------------------------
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
