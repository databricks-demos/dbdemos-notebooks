from pyspark import pipelines as dp
from pyspark.sql import functions as F

# ----------------------------------
# Clean and anonymize User data
# Transform raw user data into clean, analysis-ready format
# - Hash email addresses for privacy
# - Parse and standardize date formats
# - Standardize name capitalization
# - Cast data types appropriately
# ----------------------------------
@dp.table(comment="User data cleaned and anonymized for analysis.")
@dp.expect_or_drop("user_valid_id", "user_id IS NOT NULL")
def churn_users():
  return (spark
          .readStream.table("churn_users_bronze")
          .select(F.col("id").alias("user_id"),
                  F.sha1(F.col("email")).alias("email"),
                  F.to_timestamp(F.col("creation_date"), "MM-dd-yyyy HH:mm:ss").alias("creation_date"),
                  F.to_timestamp(F.col("last_activity_date"), "MM-dd-yyyy HH:mm:ss").alias("last_activity_date"),
                  F.initcap(F.col("firstname")).alias("firstname"),
                  F.initcap(F.col("lastname")).alias("lastname"),
                  F.col("address"),
                  F.col("canal"),
                  F.col("country"),
                  F.col("gender").cast("int").alias("gender"),
                  F.col("age_group").cast("int").alias("age_group"),
                  F.col("churn").cast("int").alias("churn")))

# ----------------------------------
# Clean orders data
# Transform raw order data into clean, analysis-ready format
# - Cast numeric fields to appropriate types
# - Parse transaction dates
# - Validate order and user IDs
# ----------------------------------
@dp.table(comment="Order data cleaned and anonymized for analysis.")
@dp.expect_or_drop("order_valid_id", "order_id IS NOT NULL")
@dp.expect_or_drop("order_valid_user_id", "user_id IS NOT NULL")
def churn_orders():
  return (spark
          .readStream.table("churn_orders_bronze")
          .select(F.col("amount").cast("int").alias("amount"),
                  F.col("id").alias("order_id"),
                  F.col("user_id"),
                  F.col("item_count").cast("int").alias("item_count"),
                  F.to_timestamp(F.col("transaction_date"), "MM-dd-yyyy HH:mm:ss").alias("creation_date"))
         )
