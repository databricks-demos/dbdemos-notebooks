-- ----------------------------------
-- Create enriched transaction dataset for ML and analytics
-- - Join transactions with customer information
-- - Enrich with geographic coordinates for originating and destination countries
-- - Convert fraud flag to boolean type
-- - Validate transaction amounts
-- Creates comprehensive feature set for fraud detection models
-- ----------------------------------

--Gold, ready for Data Scientists to consume
CREATE MATERIALIZED VIEW gold_transactions (
  CONSTRAINT amount_decent EXPECT (amount > 10)
)
AS 
  SELECT t.* EXCEPT(countryOrig, countryDest, is_fraud), c.* EXCEPT(id, _rescued_data),
          boolean(coalesce(is_fraud, 0)) as is_fraud,
          o.alpha3_code as countryOrig, o.country as countryOrig_name, o.long_avg as countryLongOrig_long, o.lat_avg as countryLatOrig_lat,
          d.alpha3_code as countryDest, d.country as countryDest_name, d.long_avg as countryLongDest_long, d.lat_avg as countryLatDest_lat
FROM silver_transactions t
  INNER JOIN country_coordinates o ON t.countryOrig=o.alpha3_code 
  INNER JOIN country_coordinates d ON t.countryDest=d.alpha3_code 
  INNER JOIN banking_customers c ON c.id=t.customer_id;