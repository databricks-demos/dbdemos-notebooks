-- Note: The AI model predict_maintenance is loaded from the 01.2-DLT-Wind-Turbine-SQL-UDF notebook
CREATE MATERIALIZED VIEW turbine_current_status 
COMMENT "Wind turbine last status based on model prediction"
AS
SELECT *, 
    predict_maintenance(hourly_timestamp, avg_energy, std_sensor_A, std_sensor_B, std_sensor_C, std_sensor_D, std_sensor_E, std_sensor_F, location, model, state) as prediction 
  FROM turbine_current_features