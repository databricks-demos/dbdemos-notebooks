CREATE MATERIALIZED VIEW turbine_training_dataset 
COMMENT "Hourly sensor stats, used to describe signal and detect anomalies"
AS
SELECT CONCAT(t.turbine_id, '-', s.start_time) AS composite_key, array(std_sensor_A, std_sensor_B, std_sensor_C, std_sensor_D, std_sensor_E, std_sensor_F) AS sensor_vector, * except(t._rescued_data, s._rescued_data, m.turbine_id) FROM sensor_hourly m
    INNER JOIN turbine t USING (turbine_id)
    INNER JOIN historical_turbine_status s ON m.turbine_id = s.turbine_id AND from_unixtime(s.start_time) < m.hourly_timestamp AND from_unixtime(s.end_time) > m.hourly_timestamp