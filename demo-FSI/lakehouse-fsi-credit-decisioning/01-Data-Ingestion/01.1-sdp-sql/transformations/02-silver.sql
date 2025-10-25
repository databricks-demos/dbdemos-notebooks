-- ----------------------------------
-- Clean and transform fund transfer data
-- Join fund transfers with account information to get customer IDs
-- Links payer and payee accounts to customer records
-- ----------------------------------

CREATE OR REFRESH MATERIALIZED VIEW fund_trans_silver
AS
SELECT
  payer_account.cust_id AS payer_cust_id,
  payee_account.cust_id AS payee_cust_id,
  fund.*
FROM fund_trans_bronze AS fund
LEFT OUTER JOIN account_bronze AS payer_account
  ON fund.payer_acc_id = payer_account.id
LEFT OUTER JOIN account_bronze AS payee_account
  ON fund.payee_acc_id = payee_account.id;


-- ----------------------------------
-- Clean and transform customer data
-- - Join customer with relationship data
-- - Extract birth year from date of birth
-- - Remove rescued data and unnecessary columns
-- Creates comprehensive customer profile for downstream analysis
-- ----------------------------------

CREATE OR REFRESH MATERIALIZED VIEW customer_silver
AS
SELECT
  customer.* EXCEPT (dob, _rescued_data),
  relationship.* EXCEPT (_rescued_data, id, operation),
  year(customer.dob) AS birth_year
FROM customer_bronze AS customer
LEFT OUTER JOIN relationship_bronze AS relationship
  ON customer.id = relationship.cust_id;


-- ----------------------------------
-- Clean and transform account data
-- - Calculate account aggregations per customer (count, average balance)
-- - Filter USD currency accounts
-- Provides financial summary for credit decisioning
-- ----------------------------------

CREATE OR REFRESH MATERIALIZED VIEW account_silver
AS
WITH cust_acc AS (
  SELECT cust_id,
         COUNT(1) AS num_accs,
         AVG(balance) AS avg_balance
  FROM account_bronze
  GROUP BY cust_id
)
SELECT
  acc_usd.cust_id,
  cust_acc.num_accs,
  cust_acc.avg_balance,
  acc_usd.balance AS balance_usd,
  acc_usd.available_balance AS available_balance_usd,
  acc_usd.operation
FROM cust_acc
LEFT OUTER JOIN account_bronze AS acc_usd
  ON cust_acc.cust_id = acc_usd.cust_id
 AND acc_usd.currency = 'USD';

