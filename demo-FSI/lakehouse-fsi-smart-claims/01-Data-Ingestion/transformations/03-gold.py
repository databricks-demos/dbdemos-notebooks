from pyspark import pipelines as dp
import geopy
import pandas as pd
from pyspark.sql.functions import col, pandas_udf
from typing import Iterator
import random

# ----------------------------------
# Join claim and policy data
# Combines cleaned claim records with corresponding policy information
# Creates comprehensive claim-policy dataset for downstream analysis and ML models
# ----------------------------------
@dp.table(comment = "Curated claim joined with policy records")
def claim_policy():
    # Read the staged policy records
    policy = spark.read.table("policy")
    # Read the staged claim records
    claim = spark.readStream.table("claim")

    return claim.join(policy, on="policy_no")

# ----------------------------------
# Geolocation enrichment functions
# Add latitude/longitude to policy addresses for visualization
# Uses geopy library for geocoding (mocked for demo performance)
# ----------------------------------

# Function to get latitude and longitude from geocoding result
def geocode(geolocator, address):
    try:
      # Skip the API call for faster demo (remove this line for real usage)
      return pd.Series({'latitude':  random.uniform(-90, 90), 'longitude': random.uniform(-180, 180)})
      location = geolocator.geocode(address)
      if location:
          return pd.Series({'latitude': location.latitude, 'longitude': location.longitude})
    except Exception as e:
      print(f"error getting lat/long: {e}")
    return pd.Series({'latitude': None, 'longitude': None})

@pandas_udf("latitude float, longitude float")
def get_lat_long(batch_iter: Iterator[pd.Series]) -> Iterator[pd.DataFrame]:
  geolocator = geopy.Nominatim(user_agent="claim_lat_long", timeout=5, scheme='https')
  for address in batch_iter:
    yield address.apply(lambda x: geocode(geolocator, x))

# ----------------------------------
# Enrich claims with geolocation and telematics data
# Joins claim_policy with telematics and adds lat/long coordinates
# Final enriched table for Claims Investigators dashboards
# ----------------------------------
@dp.materialized_view(comment="Claims with geolocation latitude/longitude and telematics data")
def claim_policy_telematics():
  t = spark.read.table("telematics")
  claim = spark.read.table("claim_policy").where("address is not null")
  return (claim.withColumn("lat_long", get_lat_long(col("address")))
               .join(t, on="chassis_no"))
