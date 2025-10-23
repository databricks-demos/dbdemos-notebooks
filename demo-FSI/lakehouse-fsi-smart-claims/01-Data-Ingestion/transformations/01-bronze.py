import dlt
from pyspark.sql import functions as F

catalog = "main__build"
schema = dbName = db = "dbdemos_fsi_smart_claims"
volume_name = "volume_claims"

# ----------------------------------
# Ingest raw claims data from JSON files
# Contains claim information: claim numbers, dates, driver details, incident information
# ----------------------------------
@dlt.table(comment="The raw claims data loaded from json files.")
def raw_claim():
  return (
    spark.readStream.format("cloudFiles")
          .option("cloudFiles.format", "json")
          .option("cloudFiles.inferColumnTypes", "true")
          .load(f"/Volumes/{catalog}/{db}/{volume_name}/Claims"))

# ----------------------------------
# Ingest raw policy data from CSV files
# Contains policy information: policy numbers, dates, premiums, location details
# ----------------------------------
@dlt.table(comment="Policy data loaded from csv files.")
def raw_policy():
    return (
      spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("cloudFiles.schemaHints", "ZIPCODE int")
            .option("cloudFiles.inferColumnTypes", "true")
            .load(f"/Volumes/{catalog}/{db}/{volume_name}/Policies"))

# ----------------------------------
# Ingest raw telematics (IoT) streaming data from parquet files
# Contains vehicle telemetry: speed, GPS coordinates, chassis numbers
# ----------------------------------
@dlt.table(comment="Load Telematics (IoT) streaming data")
def raw_telematics():
  return (
    spark.readStream.format("cloudFiles")
          .option("cloudFiles.format", "parquet")
          .load(f"/Volumes/{catalog}/{db}/{volume_name}/Telematics"))
