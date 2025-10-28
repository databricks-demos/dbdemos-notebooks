from pyspark import pipelines as dp

catalog = spark.conf.get("catalog")
schema = spark.conf.get("schema")

# RIDES from CSV
@dp.table(name="RIDES_RAW", comment="Raw rides data streamed in from CSV files.")
def rides_raw():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .load(f"/Volumes/{catalog}/{schema}/raw_data/rides/*.csv")
    )

# MAINTENANCE LOGS from CSV
@dp.table(name="MAINTENANCE_LOGS_RAW", comment="Raw maintenance logs streamed in from CSV files.")
def maintenance_logs_raw():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .load(f"/Volumes/{catalog}/{schema}/raw_data/maintenance_logs/*.csv")
    )

# WEATHER from JSON
@dp.table(name="WEATHER_RAW", comment="Raw weather data streamed in from JSON files.")
def weather_raw():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load(f"/Volumes/{catalog}/{schema}/raw_data/weather/*.json")
    )

# CUSTOMER CDC from Parquet (for later AUTO CDC processing)
@dp.table(name="CUSTOMERS_CDC_RAW", comment="Raw customer CDC data streamed in from Parquet files for Auto CDC processing.")
def customers_cdc_raw():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .load(f"/Volumes/{catalog}/{schema}/raw_data/customers_cdc/*.parquet")
    )