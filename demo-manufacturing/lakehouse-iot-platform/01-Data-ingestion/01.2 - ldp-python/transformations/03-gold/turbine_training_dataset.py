import dlt
from pyspark.sql import functions as F

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