# Databricks notebook source
# MAGIC %md
# MAGIC # Data initialization notebook. 
# MAGIC Do not run outside of the main notebook. This will automatically be called based on the reste_all widget value to setup the data required for the demo.

# COMMAND ----------

# MAGIC %pip install Faker

# COMMAND ----------

dbutils.widgets.text("raw_data_location", "/demos/raw_data", "Raw data location (stating dir)")

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
fake_address = F.udf(fake.address)
fake_id = F.udf(lambda: str(uuid.uuid4()))

df = spark.range(0, 100000)
#TODO: need to increment ID for each write batch to avoid duplicate. Could get the max reading existing data, zero if none, and add it ti the ID to garantee almost unique ID (doesn't have to be perfect)  
df = df.withColumn("id", F.monotonically_increasing_id())
df = df.withColumn("creation_date", fake_date())
df = df.withColumn("firstname", fake_firstname())
df = df.withColumn("lastname", fake_lastname())
df = df.withColumn("email", fake_email())
df = df.withColumn("address", fake_address())
df = df.withColumn("gender", F.round(F.rand()+0.2))
df = df.withColumn("age_group", F.round(F.rand()*10))
raw_data_location = dbutils.widgets.get("raw_data_location")
df.repartition(100).write.mode("overwrite").format("json").save(raw_data_location+"/user_json")
