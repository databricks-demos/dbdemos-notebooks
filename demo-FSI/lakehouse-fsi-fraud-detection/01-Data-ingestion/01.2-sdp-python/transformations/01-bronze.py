# -- ----------------------------------
# -- Ingest raw transaction data (JSON format)
# -- Loads historical banking transactions for fraud detection analysis
# -- Uses autoloader to incrementally process new transaction files
# -- ----------------------------------
from pyspark import pipelines as dp

@dp.table(
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

# -- ----------------------------------
# -- Ingest raw customer data (CSV format)
# -- Customer information with schema validation
# -- Drops rows with rescued data to ensure data quality
# -- ----------------------------------
@dp.table(
    name="banking_customers",
    comment="Customer data coming from csv files ingested in incremental with Auto Loader to support schema inference and evolution"
)
@dp.expect("correct_schema", "_rescued_data IS NULL")
def banking_customers():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("multiLine", "true")
            .load("/Volumes/main__build/dbdemos_fsi_fraud_detection/fraud_raw_data/customers")
    )


# -- ----------------------------------
# -- Ingest country reference data (CSV format)
# -- Country codes with geographic coordinates for transaction enrichment
# -- Reference table for mapping country codes to coordinates
# -- ----------------------------------

@dp.table(name="country_coordinates")
def country_coordinates():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .load("/Volumes/main__build/dbdemos_fsi_fraud_detection/fraud_raw_data/country_code")
    )

#   -- ----------------------------------
# -- Ingest fraud report labels (CSV format)
# -- Known fraud cases used as labels for ML model training
# -- Essential for supervised learning fraud detection models
# -- ----------------------------------

@dp.table(name="fraud_reports")
def fraud_reports():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .load("/Volumes/main__build/dbdemos_fsi_fraud_detection/fraud_raw_data/fraud_report")
    )

