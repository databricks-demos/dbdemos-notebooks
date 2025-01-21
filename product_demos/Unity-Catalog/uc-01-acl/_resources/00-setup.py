# Databricks notebook source
# MAGIC %md 
# MAGIC #Permission-setup Data generation for UC demo notebook

# COMMAND ----------

# MAGIC %run ../config

# COMMAND ----------

# MAGIC %run ../../../../_resources/00-global-setup-v2

# COMMAND ----------

DBDemos.setup_schema(catalog, db, False)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS analyst_permissions (
# MAGIC   analyst_email STRING,
# MAGIC   country_filter STRING,
# MAGIC   gdpr_filter LONG); 
# MAGIC
# MAGIC -- ALTER TABLE uc_acl.users OWNER TO `account users`;
# MAGIC -- ALTER TABLE analyst_permissions OWNER TO `account users`;
# MAGIC -- GRANT SELECT, MODIFY on TABLE analyst_permissions TO `account users`;
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
# MAGIC -- GRANT SELECT, MODIFY on TABLE customers TO `account users`;

# COMMAND ----------

from pyspark.sql.functions import col
import pandas as pd
df = pd.read_parquet("https://dbdemos-dataset.s3.amazonaws.com/retail/c360/users_parquet/users.parquet.snappy")

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
