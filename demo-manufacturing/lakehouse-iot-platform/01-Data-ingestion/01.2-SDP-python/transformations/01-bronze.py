import dlt

@dlt.table(
    name="historical_turbine_status",
    comment="Turbine status to be used as label in our predictive maintenance model (to know which turbine is potentially faulty)"
)
@dlt.expect("correct_schema", "_rescued_data IS NULL")
def historical_turbine_status():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .load("/Volumes/main_build/dbdemos_iot_platform/turbine_raw_landing/historical_turbine_status")
    )



@dlt.table(
    comment="Raw sensor data coming from json files ingested in incremental with Auto Loader: vibration, energy produced etc. 1 point every X sec per sensor."
)
@dlt.expect("correct_schema", "_rescued_data IS NULL")
@dlt.expect_or_drop("correct_energy", "energy IS NOT NULL and energy > 0")
def sensor_bronze():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.inferColumnTypes", "true")
        .load("/Volumes/main_build/dbdemos_iot_platform/turbine_raw_landing/incoming_data")
    )



@dlt.table(
    name="turbine",
    comment="Turbine details, with location, wind turbine model type etc"
)
@dlt.expect("correct_schema", "_rescued_data IS NULL")
def turbine():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .load("/Volumes/main_build/dbdemos_iot_platform/turbine_raw_landing/turbine")
    )
