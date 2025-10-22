import dlt

@dlt.table(name="country_coordinates")
def country_coordinates():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .load("/Volumes/main__build/dbdemos_fsi_fraud_detection/fraud_raw_data/country_code")
    )