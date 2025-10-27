from pyspark import pipelines as dp

# ----------------------------------
# Aggregate raw sensor data into hourly statistical features
# Compute standard deviations and percentiles for each sensor to detect anomalies and signal degradation
# These aggregated features are used for ML model training and real-time anomaly detection
# ----------------------------------
@dp.materialized_view(
    name="sensor_hourly",
    comment="Hourly sensor stats, used to describe signal and detect anomalies"
)
@dp.expect_or_drop("turbine_id_valid", "turbine_id IS NOT NULL")
@dp.expect_or_drop("timestamp_valid", "hourly_timestamp IS NOT NULL")
def sensor_hourly():
    from pyspark.sql.functions import col, date_trunc, from_unixtime, avg, stddev_pop, expr
    return (
        spark.read.table("sensor_bronze")
        .withColumn("hourly_timestamp", date_trunc("hour", from_unixtime(col("timestamp"))))
        .groupBy("hourly_timestamp", "turbine_id")
        .agg(
            avg("energy").alias("avg_energy"),
            stddev_pop("sensor_A").alias("std_sensor_A"),
            stddev_pop("sensor_B").alias("std_sensor_B"),
            stddev_pop("sensor_C").alias("std_sensor_C"),
            stddev_pop("sensor_D").alias("std_sensor_D"),
            stddev_pop("sensor_E").alias("std_sensor_E"),
            stddev_pop("sensor_F").alias("std_sensor_F"),
            expr("percentile_approx(sensor_A, array(0.1, 0.3, 0.6, 0.8, 0.95))").alias("percentiles_sensor_A"),
            expr("percentile_approx(sensor_B, array(0.1, 0.3, 0.6, 0.8, 0.95))").alias("percentiles_sensor_B"),
            expr("percentile_approx(sensor_C, array(0.1, 0.3, 0.6, 0.8, 0.95))").alias("percentiles_sensor_C"),
            expr("percentile_approx(sensor_D, array(0.1, 0.3, 0.6, 0.8, 0.95))").alias("percentiles_sensor_D"),
            expr("percentile_approx(sensor_E, array(0.1, 0.3, 0.6, 0.8, 0.95))").alias("percentiles_sensor_E"),
            expr("percentile_approx(sensor_F, array(0.1, 0.3, 0.6, 0.8, 0.95))").alias("percentiles_sensor_F"),
        )
    )