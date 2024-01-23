# Databricks notebook source
# MAGIC %md
# MAGIC # Data initialization

# COMMAND ----------

# MAGIC %pip install Faker

# COMMAND ----------

dbutils.widgets.text("raw_data_path", "/demos/features/koalas", "Data storage")
raw_data_path = dbutils.widgets.get("raw_data_path")

# COMMAND ----------

# DBTITLE 1,Loading data...
from pyspark.sql import functions as F
from faker import Faker
import uuid
fake = Faker()

fake_firstname = F.udf(fake.first_name)
fake_lastname = F.udf(fake.last_name)
fake_email = F.udf(fake.ascii_company_email)
fake_date = F.udf(lambda:fake.date_time_this_month().strftime("%m-%d-%Y %H:%M:%S"))
fake_address = F.udf(fake.address)
fake_id = F.udf(lambda: str(uuid.uuid4()))

df = spark.range(0, 10000)
df = df.withColumn("id", F.monotonically_increasing_id())
df = df.withColumn("creation_date", fake_date())
df = df.withColumn("firstname", fake_firstname())
df = df.withColumn("lastname", fake_lastname())
df = df.withColumn("email", fake_email())
df = df.withColumn("address", fake_address())
df = df.withColumn("gender", F.round(F.rand()+0.2))
df = df.withColumn("age_group", F.round(F.rand()*10))
df.repartition(2).write.mode("overwrite").format("json").save(raw_data_path+"/users")
print(f"Data is ready to be used under {raw_data_path}!")
