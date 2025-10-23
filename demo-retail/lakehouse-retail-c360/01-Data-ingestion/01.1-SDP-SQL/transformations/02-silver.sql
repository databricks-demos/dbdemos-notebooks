-- ----------------------------------
-- Clean and anonymize User data
-- Transform raw user data into clean, analysis-ready format
-- - Hash email addresses for privacy
-- - Parse and standardize date formats
-- - Standardize name capitalization
-- - Cast data types appropriately
-- ----------------------------------
CREATE STREAMING TABLE churn_users (
  CONSTRAINT user_valid_id EXPECT (user_id IS NOT NULL) ON VIOLATION DROP ROW
)
TBLPROPERTIES (pipelines.autoOptimize.zOrderCols = "id")
COMMENT "User data cleaned and anonymized for analysis."
AS SELECT
  id as user_id,
  sha1(email) as email,
  to_timestamp(creation_date, "MM-dd-yyyy HH:mm:ss") as creation_date,
  to_timestamp(last_activity_date, "MM-dd-yyyy HH:mm:ss") as last_activity_date,
  initcap(firstname) as firstname,
  initcap(lastname) as lastname,
  address,
  canal,
  country,
  cast(gender as int),
  cast(age_group as int),
  cast(churn as int) as churn
from STREAM(live.churn_users_bronze)

-- COMMAND ----------

-- ----------------------------------
-- Clean orders data
-- Transform raw order data into clean, analysis-ready format
-- - Cast numeric fields to appropriate types
-- - Parse transaction dates
-- - Validate order and user IDs
-- ----------------------------------
CREATE STREAMING LIVE TABLE churn_orders (
  CONSTRAINT order_valid_id EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT order_valid_user_id EXPECT (user_id IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT "Order data cleaned and anonymized for analysis."
AS SELECT
  cast(amount as int),
  id as order_id,
  user_id,
  cast(item_count as int),
  to_timestamp(transaction_date, "MM-dd-yyyy HH:mm:ss") as creation_date

from STREAM(live.churn_orders_bronze)
