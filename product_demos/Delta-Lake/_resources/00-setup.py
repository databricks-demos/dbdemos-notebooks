# Databricks notebook source
dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")

# COMMAND ----------

# MAGIC %run ../config

# COMMAND ----------

# MAGIC %run ../../../_resources/00-global-setup-v2

# COMMAND ----------

reset_all_data = dbutils.widgets.get("reset_all_data") == "true"

DBDemos.setup_schema(catalog, db, reset_all_data, volume_name)
volume_folder =  f"/Volumes/{catalog}/{db}/{volume_name}"

import time
def get_chkp_folder(folder):
    import random
    import string
    randomCar = ''.join(random.choices(string.ascii_letters + string.digits, k=8))  # 
    return folder+'/checkpoint/streams/'+randomCar

# COMMAND ----------

import json
folder = f"/Volumes/{catalog}/{db}/{volume_name}"
load_data = reset_all_data or DBDemos.is_any_folder_empty([folder+"/user_json", folder+"/user_parquet"])
if not load_data:
    dbutils.notebook.exit('data already existing. Run with reset_all_data=true to force a data cleanup for your local demo.')

# COMMAND ----------

# MAGIC %pip install faker

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
N = 10000
pdf = pd.DataFrame({
  "id": range(N),
  "creation_date": [fake.date_time_this_month().strftime("%Y-%m-%d %H:%M:%S") for _ in range(N)],
  "firstname": [fake.first_name() for _ in range(N)],
  "lastname": [fake.last_name() for _ in range(N)],
  "email": [fake.ascii_company_email() for _ in range(N)],
  "address": [fake.address().replace('\n', ' ') for _ in range(N)],
  "gender": np.random.randint(0, 2, N),
  "age_group": np.random.randint(0, 11, N),
})
df = spark.createDataFrame(pdf)
df = df.withColumn("creation_date", F.to_timestamp(F.col("creation_date")))
df.repartition(5).write.mode("overwrite").format("json").save(folder+"/user_json")
df.repartition(5).write.mode("overwrite").format("parquet").save(folder+"/user_parquet")

spark.sql("CREATE TABLE IF NOT EXISTS user_delta (id BIGINT, creation_date TIMESTAMP, firstname STRING, lastname STRING, email STRING, address STRING, gender INT, age_group INT)")
spark.sql("ALTER TABLE user_delta SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")

#TODO: remove once ES-1302640 is fixed
if not spark.catalog.tableExists(f"{catalog}.{schema}.user_uniform"):
    spark.sql("""CREATE TABLE user_uniform ( id BIGINT, firstname STRING, lastname STRING, email STRING)
    TBLPROPERTIES ('delta.universalFormat.enabledFormats' = 'iceberg',  'delta.enableIcebergCompatV2' = 'true')""")
