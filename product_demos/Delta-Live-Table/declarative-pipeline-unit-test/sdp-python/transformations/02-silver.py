from pyspark.sql.functions import *
from pyspark import pipelines as dp
from config import get_rules


# Clean and anonymize User data
@dp.table(comment="User data cleaned and anonymized for analysis.")
@dp.expect_all_or_drop(get_rules('user_silver_sdp'))
def user_silver_sdp():
  return (
    spark.readStream.table("user_bronze_sdp").select(
      col("id").cast("int"),
      sha1("email").alias("email"),
      to_timestamp(col("creation_date"),"MM-dd-yyyy HH:mm:ss").alias("creation_date"),
      to_timestamp(col("last_activity_date"),"MM-dd-yyyy HH:mm:ss").alias("last_activity_date"),
      "firstname", 
      "lastname", 
      "address", 
      "city", 
      "last_ip", 
      "postcode"
    )
  )


# Ingest user spending score
@dp.table(comment="Spending score from raw data")
@dp.expect_all_or_drop(get_rules('spend_silver_sdp'))
def spend_silver_sdp():
    return spark.readStream.table("raw_spend_data")