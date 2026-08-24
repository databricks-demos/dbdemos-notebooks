# Databricks notebook source
# MAGIC %uv pip install Faker

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Retail CDC Data Generator
# MAGIC
# MAGIC Run this notebook to create new data. It's added in the pipeline to make sure data exists when we run it.
# MAGIC
# MAGIC You can also run it in the background to periodically add data.
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=data-engineering&notebook=00-Data_CDC_Generator&demo_name=dlt-cdc&event=VIEW">

# COMMAND ----------

catalog = "main__build"
schema = dbName = db = "dbdemos_sdp_cdc"

volume_name = "raw_data"

# COMMAND ----------

spark.sql(f'CREATE CATALOG IF NOT EXISTS `{catalog}`')
spark.sql(f'USE CATALOG `{catalog}`')
spark.sql(f'CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`')
spark.sql(f'USE SCHEMA `{schema}`')
spark.sql(f'CREATE VOLUME IF NOT EXISTS `{catalog}`.`{schema}`.`{volume_name}`')
volume_folder =  f"/Volumes/{catalog}/{db}/{volume_name}"

# COMMAND ----------

try:
  dbutils.fs.ls(volume_folder+"/transactions")
  dbutils.fs.ls(volume_folder+"/customers")
except:  
  print(f"folder doesn't exists, generating the data under {volume_folder}...")
  from pyspark.sql import functions as F
  from faker import Faker
  import pandas as pd
  import numpy as np
  import uuid
  fake = Faker()

  # Generate all fake data with Faker in pandas (driver-side), then create Spark
  # DataFrames. Spark Python Faker-UDFs + non-deterministic columns trip the
  # serverless Spark Connect analyzer with a spurious [MISSING_GROUP_BY] on write.
  # A shared integer "cust_key" keeps the transaction->customer join deterministic.
  op_vals = ["APPEND", "DELETE", "UPDATE", None]
  op_p = np.array([0.5, 0.1, 0.3, 0.01]); op_p = op_p / op_p.sum()

  def fake_ids(n):
    # ~2% None, mirroring the original uuid generator
    return [str(uuid.uuid4()) if np.random.rand() < 0.98 else None for _ in range(n)]

  def dates(n):
    return [fake.date_time_this_month().strftime("%m-%d-%Y %H:%M:%S") for _ in range(n)]

  N_CUST = 100000
  pdf_customers = pd.DataFrame({
    "cust_key": range(N_CUST),
    "id": fake_ids(N_CUST),
    "firstname": [fake.first_name() for _ in range(N_CUST)],
    "lastname": [fake.last_name() for _ in range(N_CUST)],
    "email": [fake.ascii_company_email() for _ in range(N_CUST)],
    "address": [fake.address().replace('\n', ' ') for _ in range(N_CUST)],
    "operation": np.random.choice(op_vals, N_CUST, p=op_p),
    "operation_date": dates(N_CUST),
  })
  df_customers = spark.createDataFrame(pdf_customers)
  df_customers.drop("cust_key").repartition(100).write.format("json").mode("overwrite").save(volume_folder+"/customers")

  # customer_id lookup keyed on the deterministic range key
  customers_ids = df_customers.select(F.col("cust_key"), F.col("id").alias("customer_id"))

  N_TX = 10000
  pdf_tx = pd.DataFrame({
    "cust_key": range(N_TX),
    "id": fake_ids(N_TX),
    "transaction_date": dates(N_TX),
    "amount": np.round(np.random.rand(N_TX) * 1000),
    "item_count": np.round(np.random.rand(N_TX) * 10),
    "operation": np.random.choice(op_vals, N_TX, p=op_p),
    "operation_date": dates(N_TX),
  })
  df = spark.createDataFrame(pdf_tx)
  # deterministic join: transaction range key 0..9999 -> same customer range key
  df = df.join(customers_ids, "cust_key").drop("cust_key")
  df.repartition(10).write.format("json").mode("overwrite").save(volume_folder+"/transactions")
