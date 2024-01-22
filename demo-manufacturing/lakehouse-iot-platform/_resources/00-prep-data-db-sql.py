# Databricks notebook source
# MAGIC %md 
# MAGIC # Save the data for DBSQL dashboards

# COMMAND ----------

dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")
reset_all_data = dbutils.widgets.get("reset_all_data") == "true"

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE main__build.dbdemos_iot_turbine.turbine_training_dataset_ml AS SELECT * FROM main__build.dbdemos_iot_turbine.turbine_training_dataset;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE main__build.dbdemos_iot_turbine.sensor_bronze_ml AS SELECT * FROM main__build.dbdemos_iot_turbine.sensor_bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS main__build.dbdemos_iot_turbine.turbine_power_prediction ( hour INT, min FLOAT, max FLOAT, prediction FLOAT);
# MAGIC
# MAGIC insert into main__build.dbdemos_iot_turbine.turbine_power_prediction values (0, 377, 397, 391), (1, 393, 423, 412), (2, 399, 455, 426), (3, 391, 445, 404), (4, 345, 394, 365), (5, 235, 340, 276), (6, 144, 275, 195), (7, 93, 175, 133), (8, 45, 105, 76), (9, 55, 125, 95), (10, 35, 99, 77), (11, 14, 79, 44)
