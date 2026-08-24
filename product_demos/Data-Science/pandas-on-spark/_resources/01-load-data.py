# Databricks notebook source
# MAGIC %md
# MAGIC # Data initialization

# COMMAND ----------

# MAGIC %uv pip install Faker

# COMMAND ----------

dbutils.widgets.text("raw_data_path", "/demos/features/koalas", "Data storage")
raw_data_path = dbutils.widgets.get("raw_data_path")

# COMMAND ----------

# DBTITLE 1,Loading data...
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
  "creation_date": [fake.date_time_this_month().strftime("%m-%d-%Y %H:%M:%S") for _ in range(N)],
  "firstname": [fake.first_name() for _ in range(N)],
  "lastname": [fake.last_name() for _ in range(N)],
  "email": [fake.ascii_company_email() for _ in range(N)],
  "address": [fake.address().replace('\n', ' ') for _ in range(N)],
  "gender": np.random.randint(0, 2, N),
  "age_group": np.random.randint(0, 11, N),
})
df = spark.createDataFrame(pdf)
df.repartition(2).write.mode("overwrite").format("json").save(raw_data_path+"/users")
print(f"Data is ready to be used under {raw_data_path}!")
