import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window

@dlt.table(
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

mlflow.set_registry_uri('databricks-uc')     
predict_maintenance_udf = mlflow.pyfunc.spark_udf(spark, "models:/main_build.dbdemos_iot_platform.dbdemos_turbine_maintenance@prod", "string", env_manager='virtualenv')
spark.udf.register("predict_maintenance", predict_maintenance_udf)


@dlt.table(
    name="turbine_current_status",
    comment="Wind turbine last status based on model prediction"
)
def turbine_current_status():
    df = spark.read.table("turbine_current_features")

    return df.withColumn(
        "prediction",
        F.expr("predict_maintenance(hourly_timestamp, avg_energy, std_sensor_A, std_sensor_B, std_sensor_C, std_sensor_D, std_sensor_E, std_sensor_F, location, model, state)")
    )



@dlt.table(
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