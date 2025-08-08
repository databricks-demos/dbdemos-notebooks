import dlt

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