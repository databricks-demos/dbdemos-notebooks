from pyspark import pipelines as dp
# -- ----------------------------------
# -- Ingest credit bureau data (JSON format)
# -- Credit bureau data contains information about customer credit history and creditworthiness
# -- Monthly data accessed through API from government agencies or central banks
# -- ----------------------------------
@dp.table
def credit_bureau_bronze():
    return (
        spark.readStream
             .format("cloudFiles")
             .option("cloudFiles.format", "json")
             .option("cloudFiles.inferColumnTypes", "true")
             .load("/Volumes/main__build/dbdemos_fsi_credit_decisioning/credit_raw_data/credit_bureau")
    )

# -- ----------------------------------
# -- Ingest customer data (CSV format)
# -- Customer table from internal KYC processes containing customer-related data
# -- Daily ingestion from internal relational databases via CDC pipeline
# -- ----------------------------------
@dp.table()
def customer_bronze():
    return (
        spark.readStream
             .format("cloudFiles")
             .option("cloudFiles.format", "csv")
             .option("header", "true")
             .option("inferSchema", "true")
             .option("cloudFiles.inferColumnTypes", "true")
             .option("cloudFiles.schemaHints",
                     "passport_expiry DATE, visa_expiry DATE, join_date DATE, dob DATE")
             .load("/Volumes/main__build/dbdemos_fsi_credit_decisioning/credit_raw_data/internalbanking/customer")
    )



# -- ----------------------------------
# -- Ingest relationship data (CSV format)
# -- Represents the relationship between the bank and the customer
# -- Source: Internal banking databases
# -- ----------------------------------

@dp.table()
def relationship_bronze():
    return (
        spark.readStream
             .format("cloudFiles")
             .option("cloudFiles.format", "csv")
             .option("header", "true")
             .option("inferSchema", "true")
             .option("cloudFiles.inferColumnTypes", "true")
             .load("/Volumes/main__build/dbdemos_fsi_credit_decisioning/credit_raw_data/internalbanking/relationship")
    )
  

# -- ----------------------------------
# -- Ingest account data (CSV format)
# -- Customer account information from internal banking systems
# -- Daily ingestion via CDC pipeline
# -- ----------------------------------

@dp.table()
def account_bronze():
    return (
        spark.readStream
             .format("cloudFiles")
             .option("cloudFiles.format", "csv")
             .option("header", "true")
             .option("inferSchema", "true")
             .option("cloudFiles.inferColumnTypes", "true")
             .load("/Volumes/main__build/dbdemos_fsi_credit_decisioning/credit_raw_data/internalbanking/account")
    )

# -- ----------------------------------
# -- Ingest fund transfer data (JSON format)
# -- Real-time payment transactions performed by customers
# -- Streaming data available in real-time through Kafka
# -- ----------------------------------

@dp.table()
def fund_trans_bronze():
    return (
        spark.readStream
             .format("cloudFiles")
             .option("cloudFiles.format", "json")
             .option("cloudFiles.inferColumnTypes", "true")
             .load("/Volumes/main__build/dbdemos_fsi_credit_decisioning/credit_raw_data/fund_trans")
    )


# -- ----------------------------------
# -- Ingest telco partner data (JSON format)
# -- External partner data to augment internal banking data
# -- Weekly ingestion containing payment features for common customers
# -- Used to evaluate creditworthiness through alternative data sources
# -- ----------------------------------


@dp.table()
def telco_bronze():
    return (
        spark.readStream
             .format("cloudFiles")
             .option("cloudFiles.format", "json")
             .option("cloudFiles.inferColumnTypes", "true")
             .load("/Volumes/main__build/dbdemos_fsi_credit_decisioning/credit_raw_data/telco")
    )