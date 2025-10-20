import dlt
from pyspark.sql import functions as F

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