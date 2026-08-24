# Databricks notebook source
# MAGIC %md
# MAGIC # Data initialization notebook. 
# MAGIC Do not run outside of the main notebook. This will automatically be called based on the reste_all widget value to setup the data required for the demo.

# COMMAND ----------

# MAGIC %uv pip install Faker

# COMMAND ----------

dbutils.widgets.text("volume_folder", "/dbdemos/raw_data", "Raw data location (stating dir)")

# COMMAND ----------

from pyspark.sql import functions as F
from faker import Faker
import pandas as pd
import numpy as np
fake = Faker()

# Generate the fake data with Faker in pandas (driver-side), then create a Spark
# DataFrame. Spark Python UDFs + non-deterministic columns trip the serverless
# Spark Connect analyzer (spurious MISSING_GROUP_BY on write); a materialized
# createDataFrame is deterministic and serverless-safe.
N = 100000
pdf = pd.DataFrame({
  "id": range(N),
  "creation_date": [fake.date_time_this_month().strftime("%m-%d-%Y %H:%M:%S") for _ in range(N)],
  "firstname": [fake.first_name() for _ in range(N)],
  "lastname": [fake.last_name() for _ in range(N)],
  "email": [fake.ascii_company_email() for _ in range(N)],
  "address": [fake.address().replace(',', '').replace('\n', ' ') for _ in range(N)],
  "gender": np.random.randint(0, 2, N),
  "age_group": np.random.randint(0, 11, N),
})
df = spark.createDataFrame(pdf)

volume_folder = dbutils.widgets.get("volume_folder")

# Generate multiple formats from the same data
print("Generating JSON format...")
df.repartition(100).write.mode("overwrite").format("json").save(volume_folder+"/user_json")

print("Generating CSV format...")
df.repartition(10).write.mode("overwrite")\
  .option("header", "true")\
  .format("csv")\
  .save(volume_folder+"/user_csv")

# CSV variations for demonstration
print("Creating CSV variations...")

# CSV without headers
df.limit(5000).write.mode("overwrite")\
  .option("header", "false")\
  .format("csv")\
  .save(volume_folder+"/user_csv_no_headers")

# CSV with different delimiter
df.limit(5000).write.mode("overwrite")\
  .option("header", "true")\
  .option("delimiter", "|")\
  .format("csv")\
  .save(volume_folder+"/user_csv_pipe_delimited")

print("Generating Parquet format...")
df.repartition(10).write.mode("overwrite")\
  .format("parquet")\
  .save(volume_folder+"/user_parquet")

# Partitioned Parquet (great for read_files partition inference)
print("Creating partitioned data...")
df.withColumn("year", F.lit("2024"))\
  .withColumn("month", F.expr("lpad(cast(id % 12 + 1 as int), 2, '0')"))\
  .write.mode("overwrite")\
  .format("parquet")\
  .partitionBy("year", "month")\
  .save(volume_folder+"/user_parquet_partitioned")
