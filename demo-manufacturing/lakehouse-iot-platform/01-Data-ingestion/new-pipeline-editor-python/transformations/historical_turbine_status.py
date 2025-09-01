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