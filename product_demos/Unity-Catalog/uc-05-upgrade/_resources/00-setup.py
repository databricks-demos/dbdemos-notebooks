# Databricks notebook source
# MAGIC %uv pip install faker

# COMMAND ----------

catalog = "main__build"
schema = dbName = db = "dbdemos_upgraded_on_uc"

dbutils.widgets.text("external_location_path", "s3a://databricks-e2demofieldengwest/external_location_uc_upgrade", "External location path")
external_location_path = dbutils.widgets.get("external_location_path")

# COMMAND ----------

# MAGIC %run ../../../../_resources/00-global-setup-v2

# COMMAND ----------

volume_name = 'raw_data'
DBDemos.setup_schema(catalog, db, False, volume_name)

# COMMAND ----------

folder = f"/Volumes/{catalog}/{db}/{volume_name}"
spark.sql(f'drop database if exists hive_metastore.dbdemos_uc_database_to_upgrade cascade')
spark.sql(f'drop table if exists {db}.transactions')
#fix a bug from legacy version

print("generating the data...")
# Generate the fake data with Faker in pandas (driver-side), then create a Spark
# DataFrame. Spark Python UDFs + non-deterministic columns trip the serverless
# Spark Connect analyzer (spurious MISSING_GROUP_BY on write); a materialized
# createDataFrame is deterministic and serverless-safe.
from faker import Faker
import pandas as pd
import numpy as np
fake = Faker()

N = 10000
countries = ['FR', 'USA', 'SPAIN']
pdf = pd.DataFrame({
  "id": range(N),
  "creation_date": [fake.date_time_this_month().strftime("%m-%d-%Y %H:%M:%S") for _ in range(N)],
  "customer_firstname": [fake.first_name() for _ in range(N)],
  "customer_lastname": [fake.last_name() for _ in range(N)],
  "country": [countries[np.random.randint(0, 3)] for _ in range(N)],
  "customer_email": [fake.ascii_company_email() for _ in range(N)],
  "address": [fake.address().replace('\n', ' ') for _ in range(N)],
  "gender": np.round(np.random.rand(N) + 0.2),
  "age_group": np.round(np.random.rand(N) * 10),
})
df = spark.createDataFrame(pdf)
df.repartition(3).write.mode('overwrite').format("delta").save(folder+"/users")


N = 10000
tx_pdf = pd.DataFrame({
  "id": range(N),
  "customer_id": range(N),
  "transaction_date": [fake.date_time_this_month().strftime("%m-%d-%Y %H:%M:%S") for _ in range(N)],
  "credit_card_expire": [fake.credit_card_expire() for _ in range(N)],
  "amount": np.round(np.random.rand(N) * 1000 + 200),
})
df = spark.createDataFrame(tx_pdf)

spark.sql('create database if not exists hive_metastore.dbdemos_uc_database_to_upgrade')
df.repartition(3).write.mode('overwrite').format("delta").saveAsTable("hive_metastore.dbdemos_uc_database_to_upgrade.users")

#Note: this requires hard-coded external location.
spark.table("hive_metastore.dbdemos_uc_database_to_upgrade.users").repartition(3).write.mode('overwrite').format("delta").save(external_location_path+"/transactions")

# COMMAND ----------

df.repartition(3).write.mode('overwrite').format("delta").save(external_location_path+"/transactions")

# COMMAND ----------

#Need to switch to hive metastore to avoid having a : org.apache.spark.SparkException: Your query is attempting to access overlapping paths through multiple authorization mechanisms, which is not currently supported.
spark.sql(f"create table if not exists hive_metastore.dbdemos_uc_database_to_upgrade.transactions location '{external_location_path}/transactions'")
spark.sql(f"create or replace view `hive_metastore`.`dbdemos_uc_database_to_upgrade`.users_view_to_upgrade as select * from hive_metastore.dbdemos_uc_database_to_upgrade.users where id is not null")

spark.sql(f"USE CATALOG {catalog}")
