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