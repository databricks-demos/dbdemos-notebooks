# Databricks notebook source
# MAGIC %pip install Faker

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
  from collections import OrderedDict 
  import uuid
  fake = Faker()
  import random


  fake_firstname = F.udf(fake.first_name)
  fake_lastname = F.udf(fake.last_name)
  fake_email = F.udf(fake.ascii_company_email)
  fake_date = F.udf(lambda:fake.date_time_this_month().strftime("%m-%d-%Y %H:%M:%S"))
  fake_address = F.udf(fake.address)
  operations = OrderedDict([("APPEND", 0.5),("DELETE", 0.1),("UPDATE", 0.3),(None, 0.01)])
  fake_operation = F.udf(lambda:fake.random_elements(elements=operations, length=1)[0])
  fake_id = F.udf(lambda: str(uuid.uuid4()) if random.uniform(0, 1) < 0.98 else None)

  df = spark.range(0, 100000).repartition(100)
  df = df.withColumn("id", fake_id())
  df = df.withColumn("firstname", fake_firstname())
  df = df.withColumn("lastname", fake_lastname())
  df = df.withColumn("email", fake_email())
  df = df.withColumn("address", fake_address())
  df = df.withColumn("operation", fake_operation())
  df_customers = df.withColumn("operation_date", fake_date())
  df_customers.repartition(100).write.format("json").mode("overwrite").save(volume_folder+"/customers")

  df = spark.range(0, 10000).repartition(20)
  df = df.withColumn("id", fake_id())
  df = df.withColumn("transaction_date", fake_date())
  df = df.withColumn("amount", F.round(F.rand()*1000))
  df = df.withColumn("item_count", F.round(F.rand()*10))
  df = df.withColumn("operation", fake_operation())
  df = df.withColumn("operation_date", fake_date())
  #Join with the customer to get the same IDs generated.
  df = df.withColumn("t_id", F.monotonically_increasing_id()).join(spark.read.json(volume_folder+"/customers").select("id").withColumnRenamed("id", "customer_id").withColumn("t_id", F.monotonically_increasing_id()), "t_id").drop("t_id")
  df.repartition(10).write.format("json").mode("overwrite").save(volume_folder+"/transactions")
