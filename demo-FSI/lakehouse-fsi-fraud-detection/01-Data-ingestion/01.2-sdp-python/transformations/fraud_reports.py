import dlt

@dlt.table(name="fraud_reports")
def fraud_reports():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .load("/Volumes/main__build/dbdemos_fsi_fraud_detection/fraud_raw_data/fraud_report")
    )