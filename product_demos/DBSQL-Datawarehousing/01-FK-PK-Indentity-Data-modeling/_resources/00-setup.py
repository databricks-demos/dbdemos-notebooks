# Databricks notebook source
# MAGIC %md 
# MAGIC #### Setup for PK/FK demo notebook
# MAGIC 
# MAGIC Hide this notebook result.

# COMMAND ----------

import pyspark.sql.functions as F
import re
current_user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
if current_user.rfind('@') > 0:
  current_user_no_at = current_user[:current_user.rfind('@')]
else:
  current_user_no_at = current_user
current_user_no_at = re.sub(r'\W+', '_', current_user_no_at)

catalog = "uc_demos_"+current_user_no_at

catalog_exists = False
for r in spark.sql("SHOW CATALOGS").collect():
    if r['catalog'] == catalog:
        catalog_exists = True

#As non-admin users don't have permission by default, let's do that only if the catalog doesn't exist (an admin need to run it first)     
if not catalog_exists:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
    spark.sql(f"GRANT CREATE, USAGE on CATALOG {catalog} TO `account users`")
spark.sql(f"USE CATALOG {catalog}")

database = 'uc_pkfk'
db_not_exist = len([db for db in spark.catalog.listDatabases() if db.name == database]) == 0
if db_not_exist:
  print(f"creating {database} database")
  spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.{database}")
  spark.sql(f"GRANT CREATE, USAGE on DATABASE {catalog}.{database} TO `account users`")
  spark.sql(f"ALTER SCHEMA {catalog}.{database} OWNER TO `account users`")
spark.sql(f"USE DATABASE {database}")
