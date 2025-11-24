-- ----------------------------------
-- Create ML features by aggregating and joining data
-- Enrich user data with behavioral and transaction metrics
-- Features include:
-- - Order statistics: count, total amount, items, last transaction date
-- - App event statistics: platform, event count, session count, last event
-- - Temporal features: days since creation, last activity, last event
-- ----------------------------------
CREATE OR REFRESH MATERIALIZED VIEW churn_features
COMMENT "Final user table with all information for Analysis / ML"
AS
  WITH
    churn_orders_stats AS (SELECT user_id, count(*) as order_count, sum(amount) as total_amount, sum(item_count) as total_item, max(creation_date) as last_transaction
      FROM live.churn_orders GROUP BY user_id),
    churn_app_events_stats as (
      SELECT first(platform) as platform, user_id, count(*) as event_count, count(distinct session_id) as session_count, max(to_timestamp(date, "MM-dd-yyyy HH:mm:ss")) as last_event
        FROM live.churn_app_events GROUP BY user_id)

  SELECT *,
         datediff(now(), creation_date) as days_since_creation,
         datediff(now(), last_activity_date) as days_since_last_activity,
         datediff(now(), last_event) as days_last_event
       FROM live.churn_users
         INNER JOIN churn_orders_stats using (user_id)
         INNER JOIN churn_app_events_stats using (user_id)

-- COMMAND ----------

-- ----------------------------------
-- Apply ML model to predict customer churn
-- Uses the predict_churn UDF (loaded from MLflow registry in 04-churn-UDF.py) to score each customer
-- Identifies customers at risk of churning based on their behavioral and demographic features
-- ----------------------------------
CREATE OR REFRESH MATERIALIZED VIEW churn_prediction
COMMENT "Customer at risk of churn"
  AS SELECT predict_churn(struct(user_id, 1 as age_group, canal, country, gender, order_count, total_amount, total_item, last_transaction, platform, event_count, session_count, days_since_creation, days_since_last_activity, days_last_event)) as churn_prediction, * FROM live.churn_features
