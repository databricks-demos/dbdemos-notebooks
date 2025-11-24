-- ----------------------------------
-- Create feature table by enriching hourly sensor stats with turbine metadata
-- Selects the most recent metrics per turbine and joins with location/model information
-- This creates a unified feature set ready for ML model inference
-- ----------------------------------
-- specify all the field to enforce the primary key
CREATE MATERIALIZED VIEW turbine_current_features
 (
    turbine_id STRING NOT NULL,
    hourly_timestamp TIMESTAMP,
    avg_energy DOUBLE,
    std_sensor_A DOUBLE,
    std_sensor_B DOUBLE,
    std_sensor_C DOUBLE,
    std_sensor_D DOUBLE,
    std_sensor_E DOUBLE,
    std_sensor_F DOUBLE,
    country STRING,
    lat STRING,
    location STRING,
    long STRING,
    model STRING,
    state STRING,
   CONSTRAINT turbine_current_features_pk PRIMARY KEY (turbine_id))
COMMENT "Wind turbine features based on model prediction"
AS
WITH latest_metrics AS (
  SELECT *, ROW_NUMBER() OVER(PARTITION BY turbine_id, hourly_timestamp ORDER BY hourly_timestamp DESC) AS row_number FROM sensor_hourly
)
SELECT * EXCEPT(m.row_number,_rescued_data, percentiles_sensor_A,percentiles_sensor_B, percentiles_sensor_C, percentiles_sensor_D, percentiles_sensor_E, percentiles_sensor_F)
FROM latest_metrics m
   INNER JOIN turbine t USING (turbine_id)
   WHERE m.row_number=1 and turbine_id is not null;


-- ----------------------------------
-- Apply ML model to predict turbine maintenance needs
-- Uses the predict_maintenance UDF (loaded from MLflow registry) to score each turbine
-- Identifies which turbines are likely to fail and need preventive maintenance
-- ----------------------------------
-- Note: The AI model predict_maintenance is loaded from the 01.2-DLT-Wind-Turbine-SQL-UDF notebook
CREATE MATERIALIZED VIEW turbine_current_status
COMMENT "Wind turbine last status based on model prediction"
AS
SELECT *,
    predict_maintenance(hourly_timestamp, avg_energy, std_sensor_A, std_sensor_B, std_sensor_C, std_sensor_D, std_sensor_E, std_sensor_F, location, model, state) as prediction
  FROM turbine_current_features;


-- ----------------------------------
-- Create ML training dataset by joining sensor metrics with historical failure labels
-- Combines hourly sensor features with known failure periods to create labeled training data
-- The sensor_vector array format is optimized for ML model training
-- ----------------------------------
CREATE MATERIALIZED VIEW turbine_training_dataset
COMMENT "Hourly sensor stats, used to describe signal and detect anomalies"
AS
SELECT CONCAT(t.turbine_id, '-', s.start_time) AS composite_key, array(std_sensor_A, std_sensor_B, std_sensor_C, std_sensor_D, std_sensor_E, std_sensor_F) AS sensor_vector, * except(t._rescued_data, s._rescued_data, m.turbine_id) FROM sensor_hourly m
    INNER JOIN turbine t USING (turbine_id)
    INNER JOIN historical_turbine_status s ON m.turbine_id = s.turbine_id AND from_unixtime(s.start_time) < m.hourly_timestamp AND from_unixtime(s.end_time) > m.hourly_timestamp;

