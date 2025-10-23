-- ----------------------------------
-- Clean and transform fund transfer data
-- Join fund transfers with account information to get customer IDs
-- Links payer and payee accounts to customer records
-- ----------------------------------
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

-- ----------------------------------
-- Clean and transform customer data
-- - Join customer with relationship data
-- - Extract birth year from date of birth
-- - Remove rescued data and unnecessary columns
-- Creates comprehensive customer profile for downstream analysis
-- ----------------------------------
CREATE OR REFRESH MATERIALIZED VIEW customer_silver AS
  SELECT
    * EXCEPT (dob, customer._rescued_data, relationship._rescued_data, relationship.id, relationship.operation),
    year(dob) AS birth_year
  FROM
    live.customer_bronze customer
  LEFT OUTER JOIN live.relationship_bronze relationship ON customer.id = relationship.cust_id

-- COMMAND ----------

-- ----------------------------------
-- Clean and transform account data
-- - Calculate account aggregations per customer (count, average balance)
-- - Filter USD currency accounts
-- Provides financial summary for credit decisioning
-- ----------------------------------
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
