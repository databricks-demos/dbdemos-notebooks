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
from collections import OrderedDict 
import uuid
fake = Faker()

fake_firstname = F.udf(fake.first_name)
fake_lastname = F.udf(fake.last_name)
fake_email = F.udf(fake.ascii_company_email)
fake_date = F.udf(lambda:fake.date_time_this_month().strftime("%Y-%m-%d %H:%M:%S"))
fake_address = F.udf(lambda: fake.address().replace('\n', ''))
fake_id = F.udf(lambda: str(uuid.uuid4()))

raw_data_location = dbutils.widgets.get("raw_data_location")

#TODO: need to increment ID for each write batch to avoid duplicate. Could get the max reading existing data, zero if none, and add it ti the ID to garantee almost unique ID (doesn't have to be perfect)  
def create_dataset(df):
  df = df.withColumn("id", F.monotonically_increasing_id())
  df = df.withColumn("operation_date", F.current_timestamp())
  df = df.withColumn("name", fake_firstname())
  df = df.withColumn("email", fake_email())
  df = df.withColumn("address", fake_address())
  return df
#APPEND
Faker.seed(0)
df = spark.range(0, 10000)
df = create_dataset(df)
df = df.withColumn("operation", F.lit('APPEND'))
df.repartition(5).write.mode("overwrite").option("header", "true").format("csv").save(raw_data_location+"/user_csv")
df.repartition(1).write.mode("overwrite").option("header", "true").format("csv").save(raw_data_location+"/cdc/users")

#DELETES
Faker.seed(0)
df = spark.range(0, 400).repartition(1)
df = create_dataset(df)
df = df.withColumn("operation", F.lit('DELETE'))
df.repartition(1).write.mode("append").option("header", "true").format("csv").save(raw_data_location+"/user_csv")
df.repartition(1).write.mode("append").option("header", "true").format("csv").save(raw_data_location+"/cdc/users")

#UPDATE
Faker.seed(2)
df = spark.range(0, 400).repartition(1)
df = create_dataset(df)
df = df.withColumn("operation", F.lit('UPDATE'))
df = df.withColumn("id", F.col('id') + 1000)
df.repartition(1).write.mode("append").option("header", "true").format("csv").save(raw_data_location+"/user_csv")
df.repartition(1).write.mode("append").option("header", "true").format("csv").save(raw_data_location+"/cdc/users")

def cleanup_folder(path):
  #Cleanup to have something nicer
  for f in dbutils.fs.ls(path):
    if f.name.startswith('_committed') or f.name.startswith('_started') or f.name.startswith('_SUCCESS') :
      dbutils.fs.rm(f.path)
    
    
#Transactions
Faker.seed(2)
df = spark.range(0, 1000).repartition(1)
df = df.withColumn("id", F.monotonically_increasing_id())
df = df.withColumn("operation_date", F.current_timestamp())
df = df.withColumn("item_count", (F.rand(10)*3).cast('int')+1)
df = df.withColumn("amount", (F.rand(10)*1000).cast('int')+10)
df = df.withColumn("operation", F.lit('UPDATE'))
df.repartition(1).write.mode("append").option("header", "true").format("csv").save(raw_data_location+"/cdc/transactions")

cleanup_folder(raw_data_location+"/user_csv")  
cleanup_folder(raw_data_location+"/cdc/users")  
cleanup_folder(raw_data_location+"/cdc/transactions")  

