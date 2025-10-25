from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.functions import lit, concat, col
from pyspark.sql import types as T

# Helper function to flatten nested struct fields
def flatten_struct(df):
    for field in df.schema.fields:
        if isinstance(field.dataType, T.StructType):
            for child in field.dataType:
                df = df.withColumn(field.name + '_' + child.name, F.col(field.name + '.' + child.name))
            df = df.drop(field.name)
    return df

# ----------------------------------
# Aggregate telematics data by chassis number
# Compute average speed and GPS coordinates per vehicle
# Provides vehicle behavior metrics for risk assessment
# ----------------------------------
@dp.materialized_view(comment="Average telematics")
def telematics():
  return (spark.read.table('raw_telematics').groupBy("chassis_no").agg(
                F.avg("speed").alias("telematics_speed"),
                F.avg("latitude").alias("telematics_latitude"),
                F.avg("longitude").alias("telematics_longitude")))

# ----------------------------------
# Clean and transform policy data
# - Standardize date formats
# - Fix premium values (make absolute)
# - Create combined address field
# - Remove rescued data
# - Validate policy numbers
# ----------------------------------
@dp.table()
@dp.expect_all({"valid_policy_number": "policy_no IS NOT NULL"})
def policy():
    # Read the staged policy records into memory
    return (spark.readStream.table("raw_policy")
                .withColumn("premium", F.abs(col("premium")))
                # Reformat the incident date values
                .withColumn("pol_eff_date", F.to_date(col("pol_eff_date"), "dd-MM-yyyy"))
                .withColumn("pol_expiry_date", F.to_date(col("pol_expiry_date"), "dd-MM-yyyy"))
                .withColumn("pol_issue_date", F.to_date(col("pol_issue_date"), "dd-MM-yyyy"))
                .withColumn("address", concat(col("BOROUGH"), lit(", "), col("ZIP_CODE").cast("string")))
                .drop('_rescued_data'))

# ----------------------------------
# Clean and transform claim data
# - Flatten nested struct fields
# - Standardize date formats for claim, incident, and license dates
# - Remove rescued data
# - Validate claim numbers
# ----------------------------------
@dp.table()
@dp.expect_all({"valid_claim_number": "claim_no IS NOT NULL"})
def claim():
    # Read the staged claim records into memory
    claim = spark.readStream.table("raw_claim")
    claim = flatten_struct(claim)

    # Update the format of all date/time features
    return (claim.withColumn("claim_date", F.to_date(F.col("claim_date")))
                 .withColumn("incident_date", F.to_date(F.col("incident_date"), "yyyy-MM-dd"))
                 .withColumn("driver_license_issue_date", F.to_date(F.col("driver_license_issue_date"), "dd-MM-yyyy"))
                 .drop('_rescued_data'))
