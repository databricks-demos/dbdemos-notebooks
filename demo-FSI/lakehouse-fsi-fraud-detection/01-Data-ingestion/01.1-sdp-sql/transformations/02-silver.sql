-- ----------------------------------
-- Clean and transform transaction data
-- - Clean up country codes (remove "--" characters)
-- - Calculate balance differences for originating and destination accounts
-- - Join with fraud reports to add fraud labels
-- - Enforce data quality with expectations on critical fields
-- Creates clean dataset ready for analytics and ML feature engineering
-- ----------------------------------

CREATE STREAMING TABLE silver_transactions (
  CONSTRAINT correct_data EXPECT (id IS NOT NULL),
  CONSTRAINT correct_customer_id EXPECT (customer_id IS NOT NULL)
)
AS 
  SELECT * EXCEPT(countryOrig, countryDest, t._rescued_data, f._rescued_data), 
          regexp_replace(countryOrig, "\-\-", "") as countryOrig, 
          regexp_replace(countryDest, "\-\-", "") as countryDest, 
          newBalanceOrig - oldBalanceOrig as diffOrig, 
          newBalanceDest - oldBalanceDest as diffDest
FROM STREAM bronze_transactions t
  LEFT JOIN fraud_reports f using(id);

