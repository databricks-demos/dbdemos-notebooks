# Databricks notebook source
# MAGIC %md
# MAGIC # Data initialization notebook. 
# MAGIC Do not run outside of the main notebook. This will automatically be called based on the reste_all widget value to setup the data required for the demo.

# COMMAND ----------

# MAGIC %pip install Faker

# COMMAND ----------

dbutils.widgets.text("raw_data_location", "/demos/retail/delta_cdf", "Raw data location (stating dir)")

# COMMAND ----------

from pyspark.sql import functions as F
from faker import Faker
import pandas as pd
import numpy as np
fake = Faker()

raw_data_location = dbutils.widgets.get("raw_data_location")

# Generate the fake data with Faker in pandas (driver-side), then create a Spark
# DataFrame. Spark Python UDFs (F.udf(fake.xxx)) + non-deterministic columns
# (monotonically_increasing_id / rand) trip the serverless Spark Connect analyzer
# (spurious [MISSING_GROUP_BY] on write); a materialized createDataFrame is
# deterministic and serverless-safe. operation_date stays a real timestamp.
def create_dataset(size, id_offset=0):
  Faker.seed(0)
  np.random.seed(0)
  pdf = pd.DataFrame({
    "id": range(id_offset, id_offset + size),
    "name": [fake.first_name() for _ in range(size)],
    "email": [fake.ascii_company_email() for _ in range(size)],
    "address": [fake.address().replace('\n', '') for _ in range(size)],
  })
  return spark.createDataFrame(pdf).withColumn("operation_date", F.current_timestamp())

#APPEND
df = create_dataset(10000)
df = df.withColumn("operation", F.lit('APPEND'))
df.repartition(5).write.mode("overwrite").option("header", "true").format("csv").save(raw_data_location+"/user_csv")
df.repartition(1).write.mode("overwrite").option("header", "true").format("csv").save(raw_data_location+"/cdc/users")

#DELETES
df = create_dataset(400)
df = df.withColumn("operation", F.lit('DELETE'))
df.repartition(1).write.mode("append").option("header", "true").format("csv").save(raw_data_location+"/user_csv")
df.repartition(1).write.mode("append").option("header", "true").format("csv").save(raw_data_location+"/cdc/users")

#UPDATE
df = create_dataset(400, id_offset=1000)
df = df.withColumn("operation", F.lit('UPDATE'))
df.repartition(1).write.mode("append").option("header", "true").format("csv").save(raw_data_location+"/user_csv")
df.repartition(1).write.mode("append").option("header", "true").format("csv").save(raw_data_location+"/cdc/users")

def cleanup_folder(path):
  #Cleanup to have something nicer
  for f in dbutils.fs.ls(path):
    if f.name.startswith('_committed') or f.name.startswith('_started') or f.name.startswith('_SUCCESS') :
      dbutils.fs.rm(f.path)


#Transactions
np.random.seed(2)
tx_size = 1000
tx_pdf = pd.DataFrame({
  "id": range(tx_size),
  "item_count": (np.random.rand(tx_size) * 3).astype('int') + 1,
  "amount": (np.random.rand(tx_size) * 1000).astype('int') + 10,
})
df = spark.createDataFrame(tx_pdf)
df = df.withColumn("operation_date", F.current_timestamp())
df = df.withColumn("operation", F.lit('UPDATE'))
df.repartition(1).write.mode("append").option("header", "true").format("csv").save(raw_data_location+"/cdc/transactions")

cleanup_folder(raw_data_location+"/user_csv")  
cleanup_folder(raw_data_location+"/cdc/users")  
cleanup_folder(raw_data_location+"/cdc/transactions")  

