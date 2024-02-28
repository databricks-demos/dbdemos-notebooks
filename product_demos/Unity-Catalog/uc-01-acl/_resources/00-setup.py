# Databricks notebook source
# MAGIC %md 
# MAGIC #Permission-setup Data generation for UC demo notebook

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "Catalog")
catalog = dbutils.widgets.get("catalog")
database = "uc_acl"
import pandas as pd
from glob import glob

df = pd.read_parquet("https://raw.githubusercontent.com/databricks-demos/dbdemos-dataset/main/retail/c360/users_parquet/users.parquet.snappy")

# COMMAND ----------

catalog_exists = False
for r in spark.sql("SHOW CATALOGS").collect():
    if r['catalog'] == catalog:
        catalog_exists = True

#As non-admin users don't have permission by default, let's do that only if the catalog doesn't exist (an admin need to run it first)     
if not catalog_exists:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
    if catalog == "dbdemos":
      spark.sql(f"GRANT CREATE, USAGE on CATALOG {catalog} TO `account users`")
spark.sql(f"USE CATALOG {catalog}")

db_not_exist = len([r for r in spark.sql('show databases').collect() if r['databaseName'] == database]) == 0
if db_not_exist:
  print(f"creating {database} database")
  spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.{database}")
  if catalog == "dbdemos":
    spark.sql(f"GRANT CREATE, USAGE on DATABASE {catalog}.{database} TO `account users`")
spark.sql(f"USE SCHEMA {database}")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS analyst_permissions (
# MAGIC   analyst_email STRING,
# MAGIC   country_filter STRING,
# MAGIC   gdpr_filter LONG); 
# MAGIC
# MAGIC -- ALTER TABLE uc_acl.users OWNER TO `account users`;
# MAGIC -- ALTER TABLE analyst_permissions OWNER TO `account users`;
# MAGIC GRANT SELECT, MODIFY on TABLE analyst_permissions TO `account users`;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS customers (
# MAGIC   id STRING,
# MAGIC   creation_date STRING,
# MAGIC   firstname STRING,
# MAGIC   lastname STRING,
# MAGIC   country STRING,
# MAGIC   email STRING,
# MAGIC   address STRING,
# MAGIC   gender DOUBLE,
# MAGIC   age_group DOUBLE); 
# MAGIC -- ALTER TABLE customers OWNER TO `account users`; -- for the demo only, allow all users to edit the table - don't do that in production!
# MAGIC GRANT SELECT, MODIFY on TABLE customers TO `account users`;

# COMMAND ----------

from pyspark.sql.functions import col
spark.createDataFrame(df).withColumn('age_group', col("age_group").cast("double")) \
                         .withColumn('gender', col("gender").cast("double")) \
                         .write.mode('overwrite').option('mergeSchema', 'true').saveAsTable("customers")

# COMMAND ----------

import random

countries = ['FR', 'USA', 'SPAIN']
current_user = spark.sql('select current_user() as user').collect()[0]["user"]
workspace_users = df['email'][-30:].to_list() + [current_user]

user_data = [(u, countries[random.randint(0,2)], random.randint(0,1)) for u in workspace_users]

spark.createDataFrame(user_data, ['analyst_email', 'country_filter',"gdpr_filter"]) \
       .repartition(3).write.mode('overwrite').saveAsTable("analyst_permissions")
