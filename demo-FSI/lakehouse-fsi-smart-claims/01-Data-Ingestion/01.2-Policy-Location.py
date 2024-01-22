# Databricks notebook source
# MAGIC %pip install geopy

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC # Augment data with geolocation
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/fsi/smart-claims/fsi-claims-dlt-4.png?raw=true" style="float: right" width="900px">
# MAGIC
# MAGIC This notebook uses geopy library to add lat/long to the policy location for better visualization of the data in the end result dashboard for the Claims Investigators.
# MAGIC
# MAGIC Telematics data include mileage and driving habits and provide a wealth of information that is useful for insurance companies. <br />
# MAGIC This is useful across multiple use cases and serves as data to calculate new rates for policyholders, often on a rolling basis.
# MAGIC
# MAGIC Cleansing this data and enriching it with geo-spatial features of latitude ad longitude will accelerate processing and can provide visual pointers on dashboards eg. the route of the trip and where it encountered the accident.
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection or disable tracker during installation. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=data-science&org_id=1444828305810485&notebook=01.2-Policy-Location&demo_name=lakehouse-fsi-smart-claims&event=VIEW">

# COMMAND ----------

import dlt
import geopy
import pandas as pd
from pyspark.sql.functions import col, lit, concat, pandas_udf
from typing import Iterator
import random

# Function to get latitude and longitude from geocoding result
def geocode(geolocator, address):
    try:
      #Skip the API call for faster demo (remove this line for ream)
      return pd.Series({'latitude':  random.uniform(-90, 90), 'longitude': random.uniform(-180, 180)})
      location = geolocator.geocode(address)
      if location:
          return pd.Series({'latitude': location.latitude, 'longitude': location.longitude})
    except Exception as e:
      print(f"error getting lat/long: {e}")
    return pd.Series({'latitude': None, 'longitude': None})
      
@pandas_udf("latitude float, longitude float")
def get_lat_long(batch_iter: Iterator[pd.Series]) -> Iterator[pd.DataFrame]:
  #ctx = ssl.create_default_context(cafile=certifi.where())
  #geopy.geocoders.options.default_ssl_context = ctx
  geolocator = geopy.Nominatim(user_agent="claim_lat_long", timeout=5, scheme='https')
  for address in batch_iter:
    yield address.apply(lambda x: geocode(geolocator, x))

# COMMAND ----------

@dlt.table(comment="claims with geolocation latitude/longitude")
def claim_policy_telematics():
  t = dlt.read("telematics")
  claim = dlt.read("claim_policy").where("address is not null")
  return (claim.withColumn("lat_long", get_lat_long(col("address")))
               .join(t, on="chassis_no"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conclusion
# MAGIC
# MAGIC In this notebook, we demonstrated how open-source libraries can be installed easily and they help fortify the telematics data with latitude/longitude.
# MAGIC
# MAGIC Go back to the [00-Smart-Claims-Introduction]($../00-Smart-Claims-Introduction) to explore AI and Data Visualization.
# MAGIC
# MAGIC
