from pyspark import pipelines as dp

# ----------------------------------
# Ingest historical turbine status from JSON files
# This contains the historical failure data used as labels for our ML model to identify faulty turbines
# ----------------------------------
@dp.table(
    name="historical_turbine_status",
    comment="Turbine status to be used as label in our predictive maintenance model (to know which turbine is potentially faulty)"
)
@dp.expect("correct_schema", "_rescued_data IS NULL")
def historical_turbine_status():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .load("/Volumes/main_build/dbdemos_iot_platform/turbine_raw_landing/historical_turbine_status")
    )



# ----------------------------------
# Ingest raw sensor data from parquet files using Auto Loader
# Contains real-time sensor readings: vibration levels (sensor A-F), energy produced, timestamps, etc.
# Data quality: drop rows with invalid energy values
# ----------------------------------
@dp.table(
    comment="Raw sensor data coming from json files ingested in incremental with Auto Loader: vibration, energy produced etc. 1 point every X sec per sensor."
)
@dp.expect("correct_schema", "_rescued_data IS NULL")
@dp.expect_or_drop("correct_energy", "energy IS NOT NULL and energy > 0")
def sensor_bronze():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.inferColumnTypes", "true")
        .load("/Volumes/main_build/dbdemos_iot_platform/turbine_raw_landing/incoming_data")
    )



# ----------------------------------
# Ingest turbine metadata from JSON files
# Contains static turbine information: location (lat/long, state, country), model type, turbine ID
# This reference data enriches sensor readings with contextual information
# ----------------------------------
@dp.table(
    name="turbine",
    comment="Turbine details, with location, wind turbine model type etc"
)
@dp.expect("correct_schema", "_rescued_data IS NULL")
def turbine():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .load("/Volumes/main_build/dbdemos_iot_platform/turbine_raw_landing/turbine")
    )
