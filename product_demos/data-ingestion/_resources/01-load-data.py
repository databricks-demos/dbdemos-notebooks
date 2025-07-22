# Databricks notebook source
# MAGIC %md
# MAGIC # Data initialization notebook. 
# MAGIC Do not run outside of the main notebook. This will automatically be called based on the reste_all widget value to setup the data required for the demo.

# COMMAND ----------

# MAGIC %pip install Faker

# COMMAND ----------

dbutils.widgets.text("volume_folder", "/dbdemos/raw_data", "Raw data location (stating dir)")

# COMMAND ----------

from pyspark.sql import functions as F
from faker import Faker
from collections import OrderedDict 
import uuid
fake = Faker()

fake_firstname = F.udf(fake.first_name)
fake_lastname = F.udf(fake.last_name)
fake_email = F.udf(fake.ascii_company_email)
fake_date = F.udf(lambda:fake.date_time_this_month().strftime("%m-%d-%Y %H:%M:%S"))
fake_address = F.udf(lambda: fake.address().replace(',', '').replace('\n', ' '))
fake_id = F.udf(lambda: str(uuid.uuid4()))

# Generate the base DataFrame (same as original)
df = spark.range(0, 100000)
df = df.withColumn("id", F.monotonically_increasing_id())
df = df.withColumn("creation_date", fake_date())
df = df.withColumn("firstname", fake_firstname())
df = df.withColumn("lastname", fake_lastname())
df = df.withColumn("email", fake_email())
df = df.withColumn("address", fake_address())
df = df.withColumn("gender", F.round(F.rand()+0.2))
df = df.withColumn("age_group", F.round(F.rand()*10))

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
  .withColumn("month", F.expr("lpad(cast(rand() * 12 + 1 as int), 2, '0')"))\
  .write.mode("overwrite")\
  .format("parquet")\
  .partitionBy("year", "month")\
  .save(volume_folder+"/user_parquet_partitioned")
