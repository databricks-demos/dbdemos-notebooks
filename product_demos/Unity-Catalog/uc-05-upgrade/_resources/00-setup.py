# Databricks notebook source
# MAGIC %pip install faker

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
from pyspark.sql import functions as F
from faker import Faker
from collections import OrderedDict 
import uuid
import random
fake = Faker()

fake_firstname = F.udf(fake.first_name)
fake_lastname = F.udf(fake.last_name)
fake_email = F.udf(fake.ascii_company_email)
fake_date = F.udf(lambda:fake.date_time_this_month().strftime("%m-%d-%Y %H:%M:%S"))
fake_address = F.udf(fake.address)
fake_credit_card_expire = F.udf(fake.credit_card_expire)

fake_id = F.udf(lambda: str(uuid.uuid4()))
countries = ['FR', 'USA', 'SPAIN']
fake_country = F.udf(lambda: countries[random.randint(0,2)])

df = spark.range(0, 10000)
df = df.withColumn("id", F.monotonically_increasing_id())
df = df.withColumn("creation_date", fake_date())
df = df.withColumn("customer_firstname", fake_firstname())
df = df.withColumn("customer_lastname", fake_lastname())
df = df.withColumn("country", fake_country())
df = df.withColumn("customer_email", fake_email())
df = df.withColumn("address", fake_address())
df = df.withColumn("gender", F.round(F.rand()+0.2))
df = df.withColumn("age_group", F.round(F.rand()*10))
df.repartition(3).write.mode('overwrite').format("delta").save(folder+"/users")


df = spark.range(0, 10000)
df = df.withColumn("id", F.monotonically_increasing_id())
df = df.withColumn("customer_id",  F.monotonically_increasing_id())
df = df.withColumn("transaction_date", fake_date())
df = df.withColumn("credit_card_expire", fake_credit_card_expire())
df = df.withColumn("amount", F.round(F.rand()*1000+200))

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
