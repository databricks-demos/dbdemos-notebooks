-- ----------------------------------
-- Aggregate raw sensor data into hourly statistical features
-- Compute standard deviations and percentiles for each sensor to detect anomalies and signal degradation
-- These aggregated features are used for ML model training and real-time anomaly detection
-- ----------------------------------

CREATE MATERIALIZED VIEW sensor_hourly (
  CONSTRAINT turbine_id_valid EXPECT (turbine_id IS not NULL)  ON VIOLATION DROP ROW,
  CONSTRAINT timestamp_valid EXPECT (hourly_timestamp IS not NULL)  ON VIOLATION DROP ROW
)
COMMENT "Hourly sensor stats, used to describe signal and detect anomalies"
AS
SELECT turbine_id,
      date_trunc('hour', from_unixtime(timestamp)) AS hourly_timestamp,
      avg(energy)          as avg_energy,
      stddev_pop(sensor_A) as std_sensor_A,
      stddev_pop(sensor_B) as std_sensor_B,
      stddev_pop(sensor_C) as std_sensor_C,
      stddev_pop(sensor_D) as std_sensor_D,
      stddev_pop(sensor_E) as std_sensor_E,
      stddev_pop(sensor_F) as std_sensor_F,
      percentile_approx(sensor_A, array(0.1, 0.3, 0.6, 0.8, 0.95)) as percentiles_sensor_A,
      percentile_approx(sensor_B, array(0.1, 0.3, 0.6, 0.8, 0.95)) as percentiles_sensor_B,
      percentile_approx(sensor_C, array(0.1, 0.3, 0.6, 0.8, 0.95)) as percentiles_sensor_C,
      percentile_approx(sensor_D, array(0.1, 0.3, 0.6, 0.8, 0.95)) as percentiles_sensor_D,
      percentile_approx(sensor_E, array(0.1, 0.3, 0.6, 0.8, 0.95)) as percentiles_sensor_E,
      percentile_approx(sensor_F, array(0.1, 0.3, 0.6, 0.8, 0.95)) as percentiles_sensor_F
  FROM sensor_bronze GROUP BY hourly_timestamp, turbine_id