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
from collections import OrderedDict 
import uuid
fake = Faker()

fake_firstname = F.udf(fake.first_name)
fake_lastname = F.udf(fake.last_name)
fake_email = F.udf(fake.ascii_company_email)
fake_date = F.udf(lambda:fake.date_time_this_month().strftime("%Y-%m-%d %H:%M:%S"))
fake_address = F.udf(fake.address)
fake_id = F.udf(lambda: str(uuid.uuid4()))

df = spark.range(0, 10000)
#TODO: need to increment ID for each write batch to avoid duplicate. Could get the max reading existing data, zero if none, and add it ti the ID to garantee almost unique ID (doesn't have to be perfect)  
df = df.withColumn("id", F.monotonically_increasing_id())
df = df.withColumn("creation_date", fake_date())

df = df.withColumn("creation_date", F.to_timestamp(F.col("creation_date")))
df = df.withColumn("firstname", fake_firstname())
df = df.withColumn("lastname", fake_lastname())
df = df.withColumn("email", fake_email())
df = df.withColumn("address", fake_address())
df = df.withColumn("gender", F.round(F.rand()+0.2).cast('int'))
df = df.withColumn("age_group", F.round(F.rand()*10).cast('int'))
df.repartition(5).write.mode("overwrite").format("json").save(folder+"/user_json")
df.repartition(5).write.mode("overwrite").format("parquet").save(folder+"/user_parquet")

spark.sql("CREATE TABLE IF NOT EXISTS user_delta (id BIGINT, creation_date TIMESTAMP, firstname STRING, lastname STRING, email STRING, address STRING, gender INT, age_group INT)")
spark.sql("ALTER TABLE user_delta SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")

#TODO: remove once ES-1302640 is fixed
if not spark.catalog.tableExists(f"{catalog}.{schema}.user_uniform"):
    spark.sql("""CREATE TABLE user_uniform ( id BIGINT, firstname STRING, lastname STRING, email STRING)
    TBLPROPERTIES ('delta.universalFormat.enabledFormats' = 'iceberg',  'delta.enableIcebergCompatV2' = 'true')""")
