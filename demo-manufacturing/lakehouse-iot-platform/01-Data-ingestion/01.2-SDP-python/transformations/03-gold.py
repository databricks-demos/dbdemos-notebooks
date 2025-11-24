from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ----------------------------------
# Create feature table by enriching hourly sensor stats with turbine metadata
# Selects the most recent metrics per turbine and joins with location/model information
# This creates a unified feature set ready for ML model inference
# ----------------------------------
@dp.materialized_view(
    name="turbine_current_features",
    comment="Wind turbine features based on model prediction"
)
def turbine_current_features():
    latest_metrics = (
        spark.read.table("sensor_hourly")
        .withColumn(
            "row_number",
            F.row_number().over(
                Window.partitionBy("turbine_id", "hourly_timestamp").orderBy(F.col("hourly_timestamp").desc())
            )
        )
    )
    turbine = spark.read.table("turbine")
    joined = (
        latest_metrics.alias("m")
        .join(turbine.alias("t"), on="turbine_id", how="inner")
        .where((F.col("m.row_number") == 1) & (F.col("turbine_id").isNotNull()))
        .drop("row_number", "_rescued_data", "percentiles_sensor_A", "percentiles_sensor_B", 
              "percentiles_sensor_C", "percentiles_sensor_D", "percentiles_sensor_E", "percentiles_sensor_F")
    )
    return joined


import mlflow

# Load ML model from Unity Catalog registry and register as UDF
mlflow.set_registry_uri('databricks-uc')
predict_maintenance_udf = mlflow.pyfunc.spark_udf(spark, "models:/main_build.dbdemos_iot_platform.dbdemos_turbine_maintenance@prod", "string", env_manager='virtualenv')
spark.udf.register("predict_maintenance", predict_maintenance_udf)


# ----------------------------------
# Apply ML model to predict turbine maintenance needs
# Uses the predict_maintenance UDF (loaded from MLflow registry) to score each turbine
# Identifies which turbines are likely to fail and need preventive maintenance
# ----------------------------------
@dp.materialized_view(
    name="turbine_current_status",
    comment="Wind turbine last status based on model prediction"
)
def turbine_current_status():
    df = spark.read.table("turbine_current_features")

    return df.withColumn(
        "prediction",
        F.expr("predict_maintenance(hourly_timestamp, avg_energy, std_sensor_A, std_sensor_B, std_sensor_C, std_sensor_D, std_sensor_E, std_sensor_F, location, model, state)")
    )



# ----------------------------------
# Create ML training dataset by joining sensor metrics with historical failure labels
# Combines hourly sensor features with known failure periods to create labeled training data
# The sensor_vector array format is optimized for ML model training
# ----------------------------------
@dp.materialized_view(
    name="turbine_training_dataset",
    comment="Hourly sensor stats, used to describe signal and detect anomalies"
)
def turbine_training_dataset():
    sensor_hourly = spark.read.table("sensor_hourly")
    turbine = spark.read.table("turbine")
    historical_turbine_status = spark.read.table("historical_turbine_status")

    joined_df = (
        sensor_hourly
        .join(turbine, sensor_hourly.turbine_id == turbine.turbine_id)
        .join(
            historical_turbine_status,
            (sensor_hourly.turbine_id == historical_turbine_status.turbine_id) &
            (F.from_unixtime(historical_turbine_status.start_time) < sensor_hourly.hourly_timestamp) &
            (F.from_unixtime(historical_turbine_status.end_time) > sensor_hourly.hourly_timestamp)
        )
    ).drop(historical_turbine_status.turbine_id, turbine.turbine_id, "_rescued_data")

    result_df = (
        joined_df
        .withColumn("composite_key", F.concat_ws("-", F.col("turbine_id"), F.col("start_time")))
        .withColumn(
            "sensor_vector",
            F.array(
                "std_sensor_A", "std_sensor_B", "std_sensor_C",
                "std_sensor_D", "std_sensor_E", "std_sensor_F"
            )
        )
    )
    return result_df