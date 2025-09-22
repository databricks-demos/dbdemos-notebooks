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
   WHERE m.row_number=1 and turbine_id is not null