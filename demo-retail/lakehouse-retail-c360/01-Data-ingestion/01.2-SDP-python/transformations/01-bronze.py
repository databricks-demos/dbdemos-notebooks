from pyspark import pipelines as dp

# ----------------------------------
# Ingest raw app events stream in incremental mode
# Application events and sessions from CSV files
# Contains user activity data: page views, clicks, session information
# ----------------------------------
@dp.table(comment="Application events and sessions")
@dp.expect("App events correct schema", "_rescued_data IS NULL")
def churn_app_events():
  return (
    spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "csv")
      .option("cloudFiles.inferColumnTypes", "true")
      .load("/Volumes/main__build/dbdemos_retail_c360/c360/events"))

# ----------------------------------
# Ingest raw orders from ERP system
# Order history from JSON files containing customer purchases
# Contains transaction data: amounts, items, dates, customer IDs
# ----------------------------------
@dp.table(comment="Spending score from raw data")
@dp.expect("Orders correct schema", "_rescued_data IS NULL")
def churn_orders_bronze():
  return (
    spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "json")
      .option("cloudFiles.inferColumnTypes", "true")
      .load("/Volumes/main__build/dbdemos_retail_c360/c360/orders"))

# ----------------------------------
# Ingest raw user data
# Customer profile data from JSON files
# Contains demographic information: name, age, address, gender, age group
# ----------------------------------
@dp.table(comment="Raw user data coming from json files ingested in incremental with Auto Loader to support schema inference and evolution")
@dp.expect("Users correct schema", "_rescued_data IS NULL")
def churn_users_bronze():
  return (
    spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "json")
      .option("cloudFiles.inferColumnTypes", "true")
      .load("/Volumes/main__build/dbdemos_retail_c360/c360/users"))
