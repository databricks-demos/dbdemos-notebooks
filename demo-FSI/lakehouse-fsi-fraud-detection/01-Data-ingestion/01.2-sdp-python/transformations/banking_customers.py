import dlt

@dlt.table(
    name="banking_customers",
    comment="Customer data coming from csv files ingested in incremental with Auto Loader to support schema inference and evolution"
)
@dlt.expect("correct_schema", "_rescued_data IS NULL")
def banking_customers():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("multiLine", "true")
            .load("/Volumes/main__build/dbdemos_fsi_fraud_detection/fraud_raw_data/customers")
    )