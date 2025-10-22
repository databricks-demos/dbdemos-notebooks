import dlt

@dlt.table(
    name="bronze_transactions",
    comment="Historical banking transaction to be trained on fraud detection"
)
def bronze_transactions():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.maxFilesPerTrigger", "1")
            .load("/Volumes/main__build/dbdemos_fsi_fraud_detection/fraud_raw_data/transactions")
    )